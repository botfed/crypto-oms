use anyhow::{Context, Result};
use crypto_feeds::app_config::{load_perp, load_spot};
use crypto_feeds::historical_bars::{aggregate_bars, load_1m_bars_with_backfill};
use crypto_feeds::vol_provider::VolProvider;
use crypto_feeds::AllMarketData;
use crypto_oms::hyperliquid::HyperliquidOms;
use crypto_oms::mm::config::{MmConfig, WatchParams};
use crypto_oms::mm::fair_price::FairPriceEngine;
use crypto_oms::mm::inventory::start_inventory_manager;
use crypto_oms::mm::MmEngine;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::sync::Notify;
use tracing::{info, warn};

fn main() -> Result<()> {
    let _ = dotenv::dotenv();

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    // SCHED_FIFO is set per-thread on the engine spin loop, not process-wide.
    // See spin_loop() in mm/mod.rs. Requires --features sched_fifo + CAP_SYS_NICE.

    // Parse CLI args: mm_hl [--ghost] [--spin-core N] [--tokio-cores 2,3] [config_path]
    let mut ghost = false;
    let mut spin_core: Option<usize> = None;
    let mut tokio_cores: Option<Vec<usize>> = None;
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
            "--tokio-cores" => {
                i += 1;
                tokio_cores = Some(
                    args[i].split(',')
                        .map(|s| s.trim().parse().expect("--tokio-cores requires comma-separated numbers"))
                        .collect()
                );
            }
            _ => config_path = args[i].clone(),
        }
        i += 1;
    }

    // Build tokio runtime — pin workers to specific cores if requested.
    // on_thread_start fires for worker threads at build() time (before block_on).
    // Blocking pool threads are created lazily later, so the counter cap ensures
    // only the first N threads (the workers) get pinned.
    let mut rt_builder = tokio::runtime::Builder::new_multi_thread();
    rt_builder.enable_all();
    if let Some(ref cores) = tokio_cores {
        let n_workers = cores.len();
        rt_builder.worker_threads(n_workers);
        let pinned = std::sync::atomic::AtomicUsize::new(0);
        let cores = cores.clone();
        rt_builder.on_thread_start(move || {
            let idx = pinned.fetch_add(1, Ordering::SeqCst);
            if idx < n_workers {
                core_affinity::set_for_current(core_affinity::CoreId { id: cores[idx] });
            }
        });
    }
    let rt = rt_builder.build().context("failed to build tokio runtime")?;

    rt.block_on(async_main(ghost, spin_core, config_path))
}

async fn async_main(ghost: bool, spin_core: Option<usize>, config_path: String) -> Result<()> {

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

    // FairPriceEngine — basis updated by engine in slow path, no background task
    let fair_price = Arc::new(FairPriceEngine::new(market_data.clone(), config.fair_price)?);

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

    // Initialize vol provider if vol_models is configured
    let vol_model_name = config.vol_models
        .as_ref()
        .map(|v| v.model.clone())
        .unwrap_or_else(|| "har_qlike".to_string());

    let vol_provider = if let Some(ref vol_cfg) = config.vol_models {
        info!("initializing vol provider from {}", vol_cfg.params_dir);

        let params_dir = Path::new(&vol_cfg.params_dir);
        let all_params = crypto_feeds::vol_params::load_all_vol_params(params_dir)
            .with_context(|| format!("loading vol params from {}", params_dir.display()))?;

        info!("loaded vol params for {} symbols: {:?}",
            all_params.len(), all_params.keys().collect::<Vec<_>>());

        let target_min = all_params.values().next().map(|p| p.target_min).unwrap_or(5);

        // Build HAR vol provider: load warmup bars per symbol, pair with HAR params
        let bar_data_dir = Path::new(&vol_cfg.bar_data_dir);
        let mut har_groups = Vec::new();
        for (sym, params) in all_params {
            let har = match vol_model_name.as_str() {
                "har_ols" => params.har_ols.clone(),
                _ => params.har_qlike.clone(),
            };
            let Some(har_params) = har else { continue };

            // Load historical bars for warmup
            let warmup_bars = match load_1m_bars_with_backfill(bar_data_dir, &sym, vol_cfg.warmup_days).await {
                Ok(bars_1m) => {
                    let target_bars = aggregate_bars(&bars_1m, target_min);
                    info!("vol warmup: {} target bars for {}", target_bars.len(), sym);
                    target_bars
                }
                Err(e) => {
                    warn!("vol warmup failed for {}: {}", sym, e);
                    Vec::new()
                }
            };

            har_groups.push((har_params, params.seasonality.clone(), target_min, warmup_bars));
        }

        let seed_vol = config.strategy.ref_vol;
        let provider = VolProvider::new_har(har_groups, seed_vol);
        info!("vol provider ready (model={}, lock-free HAR)", vol_model_name);

        Some(provider)
    } else {
        info!("no vol_models configured, vol_mult=1.0");
        None
    };

    // Run engine
    let engine = MmEngine::new(
        oms,
        fair_price,
        vol_provider,
        market_data,
        Box::new(params),
        config.strategy,
        ghost,
        spin_core,
        engine_shutdown,
    )?;

    engine.run().await?;
    std::process::exit(0);
}
