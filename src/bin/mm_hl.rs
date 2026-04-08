use anyhow::{Context, Result};
use crypto_feeds::app_config::{load_perp, load_spot};
use crypto_feeds::AllMarketData;
use crypto_oms::hyperliquid::HyperliquidOms;
use crypto_oms::mm::config::{MmConfig, WatchParams};
use crypto_oms::mm::fair_price::FairPriceEngine;
use crypto_oms::mm::inventory::start_inventory_manager;
use crypto_oms::mm::MmEngine;
use std::sync::Arc;
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

    // Parse CLI args: mm_hl [--ghost] [config_path]
    let mut ghost = false;
    let mut config_path = "configs/mm_hl.yaml".to_string();
    for arg in std::env::args().skip(1) {
        if arg == "--ghost" {
            ghost = true;
        } else {
            config_path = arg;
        }
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

    // Ctrl-C handler
    let sd = shutdown.clone();
    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.ok();
        info!("Ctrl-C received, shutting down...");
        sd.notify_waiters();
        // Force exit after 3s if graceful shutdown is stuck
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
    // Extract the base asset from the strategy symbol (e.g., "PERP_BTC_USDC" → "BTC")
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

    // Run engine
    let mut engine = MmEngine::new(
        oms,
        fair_price,
        Box::new(params),
        config.strategy,
        ghost,
        shutdown,
    )?;

    engine.run().await?;
    std::process::exit(0);
}
