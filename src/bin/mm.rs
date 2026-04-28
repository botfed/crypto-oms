use anyhow::{Context, Result};
use crypto_feeds::app_config::{load_perp, load_spot};
use crypto_feeds::historical_bars::{aggregate_bars, load_1m_bars_with_backfill};
use crypto_feeds::vol_provider::VolProvider;
use crypto_feeds::AllMarketData;
use crypto_oms::ExchangeOms;
use crypto_oms::hibachi::HibachiOms;
use crypto_oms::hyperliquid::HyperliquidOms;
use crypto_oms::mm::config::MmConfig;
use crypto_oms::mm::fair_price::FairPriceEngine;
use crypto_oms::mm::inventory::start_inventory_manager;
use crypto_oms::mm::MmEngine;
use oms_core::state_tracker::{OmsStateTracker, StateTrackerConfig};
use oms_core::OmsEvent;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::Notify;
use tracing::{info, warn};

use crypto_oms::mm::display::{DisplayBus, DisplayMsg};

type DisplayState = (Option<DisplayBus>, Option<crossbeam_channel::Receiver<DisplayMsg>>);

fn init_logging() -> DisplayState {
    #[cfg(feature = "display")]
    {
        let (bus, rx) = crypto_oms::mm::display::init();
        return (Some(bus), Some(rx));
    }
    #[cfg(not(feature = "display"))]
    {
        tracing_subscriber::fmt()
            .with_env_filter(
                tracing_subscriber::EnvFilter::try_from_default_env()
                    .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
            )
            .init();
        (None, None)
    }
}

fn main() -> Result<()> {
    let _ = dotenv::dotenv();

    let (display_bus, display_rx) = init_logging();

    let mut ghost = false;
    let mut spin_core: Option<usize> = None;
    let mut tokio_cores: Option<Vec<usize>> = None;
    let mut config_path = "configs/mm.yaml".to_string();
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

    rt.block_on(async_main(ghost, spin_core, config_path, display_bus, display_rx))
}

async fn async_main(
    ghost: bool,
    spin_core: Option<usize>,
    config_path: String,
    display_bus: Option<DisplayBus>,
    display_rx: Option<crossbeam_channel::Receiver<DisplayMsg>>,
) -> Result<()> {
    if ghost {
        info!("*** GHOST MODE — no orders will be sent ***");
    }

    let contents = std::fs::read_to_string(&config_path)
        .with_context(|| format!("failed to read config: {config_path}"))?;
    let config: MmConfig = serde_yaml::from_str(&contents)
        .with_context(|| format!("failed to parse config: {config_path}"))?;

    let exchange = config.exchange.clone();
    info!("target exchange: {exchange}");

    match exchange.as_str() {
        "hyperliquid" => {
            let private_key = std::env::var("HL_PRIVATE_KEY")
                .context("HL_PRIVATE_KEY env var not set")?;
            let account_address = std::env::var("HL_ACCOUNT_ADDRESS")
                .context("HL_ACCOUNT_ADDRESS env var not set")?;
            let oms_config = config.to_hl_oms_config(private_key, account_address);
            let oms = HyperliquidOms::new(oms_config)?;
            oms.start();
            run_mm(oms, config, ghost, spin_core, display_bus, display_rx).await
        }
        "hibachi" => {
            let api_key = std::env::var("HIBACHI_API_KEY")
                .context("HIBACHI_API_KEY env var not set")?;
            let private_key = std::env::var("HIBACHI_PRIVATE_KEY")
                .context("HIBACHI_PRIVATE_KEY env var not set")?;
            let account_id: u64 = std::env::var("HIBACHI_ACCOUNT_ID")
                .context("HIBACHI_ACCOUNT_ID env var not set")?
                .parse().context("HIBACHI_ACCOUNT_ID must be a number")?;
            let oms_config = config.to_hibachi_oms_config(api_key, private_key, account_id);
            let oms = HibachiOms::new(oms_config)?;
            oms.start();
            run_mm(oms, config, ghost, spin_core, display_bus, display_rx).await
        }
        other => anyhow::bail!("unsupported exchange: {other}"),
    }
}

async fn run_mm<O: ExchangeOms + 'static>(
    oms: Arc<O>,
    config: MmConfig,
    ghost: bool,
    spin_core: Option<usize>,
    display_bus: Option<DisplayBus>,
    display_rx: Option<crossbeam_channel::Receiver<DisplayMsg>>,
) -> Result<()> {
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

    // Seed symbol registry
    let feeds_config = config.to_feeds_config();
    crypto_feeds::symbol_registry::seed_extra_bases(feeds_config.base_assets());

    // Start crypto-feeds
    let market_data = Arc::new(AllMarketData::new());
    let mut handles = Vec::new();
    load_spot(&mut handles, &feeds_config, &market_data, &shutdown)?;
    load_perp(&mut handles, &feeds_config, &market_data, &shutdown)?;

    // Wait for OMS ready
    oms.wait_ready().await?;

    anyhow::ensure!(!config.symbols.is_empty(), "config must have at least one symbol in 'symbols'");
    let mut symbols = config.symbols;
    for sym in &mut symbols {
        sym.skew_scale_usd.get_or_insert(config.skew_scale_usd);
        sym.post_only.get_or_insert(config.post_only);
        sym.ref_half_spread_bps.get_or_insert(config.ref_half_spread_bps);
        sym.ref_min_spread_bps.get_or_insert(config.ref_min_spread_bps);
        sym.max_skew_bps.get_or_insert(config.max_skew_bps);
        sym.min_edge_bps.get_or_insert(config.min_edge_bps);
        sym.ref_requote_tolerance_bps.get_or_insert(config.ref_requote_tolerance_bps);
    }

    let fair_price = Arc::new(FairPriceEngine::new(market_data.clone(), config.fair_price)?);
    let use_target_rx = config.inventory.mode == crypto_oms::mm::config::InventoryMode::Hedge;

    // Vol provider
    let vol_model_name = config.vol_models
        .as_ref()
        .map(|v| v.model.clone())
        .unwrap_or_else(|| "har_qlike".to_string());

    let (mut vol_provider, has_vol_params) = if let Some(ref vol_cfg) = config.vol_models {
        info!("initializing vol provider from {}", vol_cfg.params_dir);

        let params_dir = Path::new(&vol_cfg.params_dir);
        let all_params = crypto_feeds::vol_params::load_all_vol_params(params_dir)
            .with_context(|| format!("loading vol params from {}", params_dir.display()))?;

        info!("loaded vol params for {} symbols: {:?}",
            all_params.len(), all_params.keys().collect::<Vec<_>>());

        let target_min = all_params.values().next().map(|p| p.target_min).unwrap_or(5);
        let bar_data_dir = Path::new(&vol_cfg.bar_data_dir);

        let mut har_groups = Vec::new();
        for sym_cfg in &symbols {
            let vol_sym = &sym_cfg.vol_symbol;
            if vol_sym.is_empty() {
                har_groups.push(None);
                continue;
            }
            match all_params.get(vol_sym.as_str()) {
                Some(params) => {
                    let har = match vol_model_name.as_str() {
                        "har_ols" => params.har_ols.clone(),
                        _ => params.har_qlike.clone(),
                    };
                    match har {
                        Some(har_params) => {
                            let warmup_bars = match load_1m_bars_with_backfill(bar_data_dir, vol_sym, vol_cfg.warmup_days).await {
                                Ok(bars_1m) => {
                                    let target_bars = aggregate_bars(&bars_1m, target_min);
                                    info!("vol warmup: {} target bars for {}", target_bars.len(), vol_sym);
                                    target_bars
                                }
                                Err(e) => {
                                    warn!("vol warmup failed for {}: {}", vol_sym, e);
                                    Vec::new()
                                }
                            };
                            har_groups.push(Some((har_params, params.seasonality.clone(), target_min, warmup_bars, sym_cfg.ref_vol)));
                        }
                        None => {
                            warn!("no HAR params for vol_symbol={}, using vol_mult=1.0", vol_sym);
                            har_groups.push(None);
                        }
                    }
                }
                None => {
                    warn!("vol_symbol={} not found in params dir, using vol_mult=1.0", vol_sym);
                    har_groups.push(None);
                }
            }
        }

        let has_vol_params: Vec<bool> = har_groups.iter().map(|g| g.is_some()).collect();

        let final_groups: Vec<_> = har_groups.into_iter().enumerate().map(|(i, g)| {
            g.unwrap_or_else(|| {
                let har = crypto_feeds::vol_params::HarParams {
                    intercept: 0.0,
                    betas: vec![],
                    windows: vec![],
                };
                let seasonality = crypto_feeds::vol_params::SeasonalFactors {
                    f_h: [1.0; 24],
                    f_d: [1.0; 7],
                };
                (har, seasonality, target_min, Vec::new(), symbols[i].ref_vol)
            })
        }).collect();
        let provider = VolProvider::new_har(final_groups);
        info!("vol provider ready (model={}, {} groups)", vol_model_name, provider.group_count());

        (Some(provider), has_vol_params)
    } else {
        info!("no vol_models configured, vol_mult=1.0");
        (None, vec![false; symbols.len()])
    };

    // Build engines
    let mut engines: Vec<MmEngine<O>> = Vec::with_capacity(symbols.len());
    for (i, sym_cfg) in symbols.iter().enumerate() {
        let asset = sym_cfg.symbol
            .strip_prefix("PERP_")
            .or_else(|| sym_cfg.symbol.strip_prefix("SPOT_"))
            .and_then(|s| s.split('_').next())
            .unwrap_or("BTC");
        let (target_rx, _handle) = start_inventory_manager(
            &config.inventory, asset, shutdown.clone(),
        );
        let target_rx = if use_target_rx { Some(target_rx) } else { None };
        let engine = MmEngine::new(
            Arc::clone(&oms),
            Arc::clone(&fair_price),
            Arc::clone(&market_data),
            target_rx,
            sym_cfg.clone(),
            ghost,
            i,
            has_vol_params[i],
            display_bus.clone(),
        )?;
        engines.push(engine);
    }

    let sym_names: Vec<&str> = engines.iter().map(|e| e.symbol()).collect();
    info!("MM engine starting: symbols=[{}] ghost={}", sym_names.join(", "), ghost);

    let mut oms_state = OmsStateTracker::new(StateTrackerConfig::default());
    oms_state.apply_event(&OmsEvent::Ready);
    info!("OMS ready, entering main loop (dedicated thread)");

    let oms_events = oms.event_receiver();

    if let Some(rx) = display_rx {
        let sd = shutdown.clone();
        tokio::spawn(crypto_oms::mm::display::run_display(rx, sd));
    }

    tokio::task::spawn_blocking(move || {
        spin_loop(
            &mut engines,
            &mut oms_state,
            &oms_events,
            &mut vol_provider,
            &oms,
            &fair_price,
            &engine_shutdown,
            spin_core,
        );
    })
    .await
    .map_err(|e| anyhow::anyhow!("engine thread panicked: {e}"))?;

    std::process::exit(0);
}

fn spin_loop<O: ExchangeOms + 'static>(
    engines: &mut [MmEngine<O>],
    oms_state: &mut OmsStateTracker,
    oms_events: &crossbeam_channel::Receiver<OmsEvent>,
    vol_provider: &mut Option<VolProvider>,
    oms: &Arc<O>,
    fair_price: &Arc<FairPriceEngine>,
    shutdown: &AtomicBool,
    spin_core: Option<usize>,
) {
    if let Some(cpu) = spin_core {
        let ok = core_affinity::set_for_current(core_affinity::CoreId { id: cpu });
        if ok {
            info!("hot loop pinned to CPU {cpu}");
        } else {
            warn!("failed to pin hot loop to CPU {cpu}");
        }
    }

    #[cfg(all(target_os = "linux", feature = "sched_fifo"))]
    unsafe {
        let param = libc::sched_param { sched_priority: 50 };
        if libc::sched_setscheduler(0, libc::SCHED_FIFO, &param) == 0 {
            info!("SCHED_FIFO enabled on engine thread (priority=50)");
        } else {
            warn!(
                "SCHED_FIFO failed (errno={}). Grant CAP_SYS_NICE to fix.",
                *libc::__errno_location()
            );
        }
    }

    let mut last_basis_save = Instant::now();

    loop {
        // Drain OMS events
        loop {
            match oms_events.try_recv() {
                Ok(event) => {
                    oms_state.apply_event(&event);
                    match &event {
                        OmsEvent::Disconnected => warn!("OMS disconnected"),
                        OmsEvent::Ready | OmsEvent::Reconnected => info!("OMS ready/reconnected"),
                        _ => {
                            for engine in engines.iter_mut() {
                                engine.handle_oms_event(&event, oms_state);
                            }
                        }
                    }
                }
                Err(_) => break,
            }
        }

        // Shutdown
        if shutdown.load(Ordering::Relaxed) {
            info!("MM engine shutting down");
            for engine in engines.iter_mut() {
                engine.cancel_all_quotes();
            }
            std::thread::sleep(Duration::from_secs(1));
            if let Err(e) = tokio::runtime::Handle::current().block_on(
                oms.shutdown_cancel_all(None)
            ) {
                warn!("shutdown cancel failed: {e:#}");
            }
            fair_price.save_basis_cache();
            return;
        }

        // Tick engines
        let mut any_ticked = false;
        for engine in engines.iter_mut() {
            if engine.tick(oms_state, vol_provider) {
                any_ticked = true;
            }
        }

        if !any_ticked {
            std::hint::spin_loop();
        }

        // Periodic basis cache save
        if last_basis_save.elapsed() >= Duration::from_secs(10) {
            fair_price.save_basis_cache();
            last_basis_save = Instant::now();
        }
    }
}
