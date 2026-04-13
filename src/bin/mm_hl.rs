use anyhow::{Context, Result};
use crypto_feeds::app_config::{load_perp, load_spot};
use crypto_feeds::bar_manager::{BarManager, BarSymbol};
use crypto_feeds::historical_bars::{aggregate_bars, load_1m_bars_with_backfill};
use crypto_feeds::market_data::{Exchange, InstrumentType};
use crypto_feeds::symbol_registry::REGISTRY;
use crypto_feeds::vol_engine::VolEngine;
use crypto_feeds::AllMarketData;
use crypto_oms::hyperliquid::HyperliquidOms;
use crypto_oms::mm::config::{MmConfig, WatchParams};
use crypto_oms::mm::fair_price::FairPriceEngine;
use crypto_oms::mm::inventory::start_inventory_manager;
use crypto_oms::mm::MmEngine;
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::sync::Notify;
use tracing::{info, warn};

#[tokio::main]
async fn main() -> Result<()> {
    let _ = dotenv::dotenv();

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    // Parse CLI args: mm_hl [--ghost] [--spin-core N] [config_path]
    let mut ghost = false;
    let mut spin_core: Option<usize> = None;
    let mut config_path = "configs/mm_hl.yaml".to_string();
    let args: Vec<String> = std::env::args().skip(1).collect();
    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--ghost" => ghost = true,
            "--spin-core" => {
                i += 1;
                spin_core = Some(args[i].parse().expect("--spin-core requires a number"));
            }
            _ => config_path = args[i].clone(),
        }
        i += 1;
    }

    if ghost {
        info!("*** GHOST MODE — no orders will be sent ***");
    }

    // Load config
    let contents = std::fs::read_to_string(&config_path)
        .with_context(|| format!("failed to read config: {config_path}"))?;
    let config: MmConfig = serde_yaml::from_str(&contents)
        .with_context(|| format!("failed to parse config: {config_path}"))?;

    // HL credentials from env
    let private_key = std::env::var("HL_PRIVATE_KEY")
        .context("HL_PRIVATE_KEY env var not set")?;
    let account_address = std::env::var("HL_ACCOUNT_ADDRESS")
        .context("HL_ACCOUNT_ADDRESS env var not set")?;

    let shutdown = Arc::new(Notify::new());
    let engine_shutdown = Arc::new(AtomicBool::new(false));

    // Ctrl-C handler
    let sd = shutdown.clone();
    let esd = engine_shutdown.clone();
    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.ok();
        info!("Ctrl-C received, shutting down...");
        esd.store(true, Ordering::Relaxed);
        sd.notify_waiters();
        tokio::time::sleep(Duration::from_secs(3)).await;
        warn!("graceful shutdown timed out, forcing exit");
        std::process::exit(1);
    });

    // Start crypto-feeds
    let feeds_config = config.to_feeds_config();
    let market_data = Arc::new(AllMarketData::new());
    let mut handles = Vec::new();
    load_spot(&mut handles, &feeds_config, &market_data, &shutdown)?;
    load_perp(&mut handles, &feeds_config, &market_data, &shutdown)?;

    // Start HL OMS
    let oms_config = config.to_oms_config(private_key, account_address);
    let oms = HyperliquidOms::new(oms_config)?;
    oms.start();

    // Start FairPriceEngine
    let fair_price = Arc::new(FairPriceEngine::new(market_data.clone(), config.fair_price)?);
    fair_price.start(shutdown.clone());

    // Start inventory manager
    let asset = config.strategy.symbol
        .strip_prefix("PERP_")
        .or_else(|| config.strategy.symbol.strip_prefix("SPOT_"))
        .and_then(|s| s.split('_').next())
        .unwrap_or("BTC");
    let (target_rx, _inv_handle) = start_inventory_manager(
        &config.inventory,
        asset,
        shutdown.clone(),
    );

    // Build params
    let (params, _controller) = WatchParams::new(&config.strategy, target_rx);

    // Initialize vol engine if vol_models is configured
    let vol_model_name = config.vol_models
        .as_ref()
        .map(|v| v.model.clone())
        .unwrap_or_else(|| "har_qlike".to_string());

    let vol_engine = if let Some(ref vol_cfg) = config.vol_models {
        info!("initializing vol engine from {}", vol_cfg.params_dir);

        let params_dir = Path::new(&vol_cfg.params_dir);
        let all_params = crypto_feeds::vol_params::load_all_vol_params(params_dir)
            .with_context(|| format!("loading vol params from {}", params_dir.display()))?;

        info!("loaded vol params for {} symbols: {:?}",
            all_params.len(), all_params.keys().collect::<Vec<_>>());

        let target_min = all_params.values().next().map(|p| p.target_min).unwrap_or(5);
        let max_window = all_params.values()
            .flat_map(|p| [
                p.har_ols.as_ref().map(|h| h.max_window()),
                p.har_qlike.as_ref().map(|h| h.max_window()),
            ])
            .flatten()
            .max()
            .unwrap_or(288) + 16;

        // Resolve BarSymbol for each vol symbol (Binance perp preferred)
        let mut bar_symbols = Vec::new();
        let mut param_map = HashMap::new();
        for (sym, params) in all_params {
            let perp_sym = format!("{}_USDT", sym);
            let spot_sym = format!("{}_USDT", sym);
            let venue = if let Some(&id) = REGISTRY.lookup(&perp_sym, &InstrumentType::Perp) {
                Some((Exchange::Binance, id))
            } else if let Some(&id) = REGISTRY.lookup(&spot_sym, &InstrumentType::Spot) {
                Some((Exchange::Binance, id))
            } else {
                warn!("no Binance venue found for {}, skipping vol", sym);
                None
            };
            if let Some((exchange, symbol_id)) = venue {
                bar_symbols.push(BarSymbol { name: sym.clone(), exchange, symbol_id });
                param_map.insert(sym, params);
            }
        }

        // Create bar manager
        let bar_mgr = Arc::new(BarManager::new(bar_symbols, target_min, max_window));

        // Warmup from historical bars
        let bar_data_dir = Path::new(&vol_cfg.bar_data_dir);
        for sym in bar_mgr.symbols() {
            match load_1m_bars_with_backfill(bar_data_dir, &sym, vol_cfg.warmup_days).await {
                Ok(bars_1m) => {
                    let target_bars = aggregate_bars(&bars_1m, target_min);
                    bar_mgr.warmup(&sym, target_bars, &bars_1m);
                    info!("vol warmup: {} bars for {}", bar_mgr.symbols().len(), sym);
                }
                Err(e) => warn!("vol warmup failed for {}: {}", sym, e),
            }
        }

        // Spawn bar maintenance (converts live ticks → bars)
        handles.push(bar_mgr.spawn_maintenance(Arc::clone(&market_data), Arc::clone(&shutdown)));

        // Create vol engine
        let engine = Arc::new(VolEngine::new(param_map, Arc::clone(&bar_mgr)));
        engine.replay_history();
        info!("vol engine ready (model={})", vol_model_name);

        Some(engine)
    } else {
        info!("no vol_models configured, vol_mult=1.0");
        None
    };

    // Run engine
    let engine = MmEngine::new(
        oms,
        fair_price,
        vol_engine,
        market_data,
        vol_model_name,
        Box::new(params),
        config.strategy,
        ghost,
        spin_core,
        engine_shutdown,
    )?;

    engine.run().await?;
    std::process::exit(0);
}
