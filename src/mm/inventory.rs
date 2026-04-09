use std::sync::Arc;
use std::time::SystemTime;

use anyhow::{Context, Result};
use tokio::sync::{Notify, watch};
use tokio::task::JoinHandle;
use tracing::{info, warn};

use super::config::{HedgeConfig, InventoryConfig, InventoryMode};

/// Start the appropriate inventory task based on config.
/// Returns the watch::Receiver<f64> for target_position and an optional task handle.
pub fn start_inventory_manager(
    config: &InventoryConfig,
    asset: &str,
    shutdown: Arc<Notify>,
) -> (watch::Receiver<f64>, Option<JoinHandle<()>>) {
    match config.mode {
        InventoryMode::Static => {
            let (tx, rx) = watch::channel(config.static_target.target);
            info!("inventory manager: static target = {:.6}", config.static_target.target);
            drop(tx); // no updater, receiver holds the initial value
            (rx, None)
        }
        InventoryMode::Hedge => {
            let (tx, rx) = watch::channel(0.0);
            let handle = spawn_hedge_task(&config.hedge, asset, tx, shutdown);
            (rx, Some(handle))
        }
    }
}

fn spawn_hedge_task(
    config: &HedgeConfig,
    asset: &str,
    controller: watch::Sender<f64>,
    shutdown: Arc<Notify>,
) -> JoinHandle<()> {
    let exposure_file = config.exposure_file.clone();
    let poll_ms = config.poll_ms;
    let addresses: Vec<String> = config
        .mappings
        .get(asset)
        .cloned()
        .unwrap_or_default();
    let asset_name = asset.to_string();

    info!(
        "inventory manager: hedge mode, asset={}, file={}, addresses={}, poll={}ms",
        asset_name,
        exposure_file,
        addresses.len(),
        poll_ms,
    );

    tokio::spawn(async move {
        let mut last_mtime: Option<SystemTime> = None;

        let shutdown_fut = shutdown.notified();
        tokio::pin!(shutdown_fut);

        let mut interval = tokio::time::interval(
            std::time::Duration::from_millis(poll_ms),
        );
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                _ = &mut shutdown_fut => return,
                _ = interval.tick() => {
                    // Cheap mtime check — only re-read on change
                    let mtime = match std::fs::metadata(&exposure_file)
                        .and_then(|m| m.modified())
                    {
                        Ok(t) => t,
                        Err(e) => {
                            warn!("hedge: cannot stat {}: {e}", exposure_file);
                            continue;
                        }
                    };

                    if last_mtime == Some(mtime) {
                        continue;
                    }
                    last_mtime = Some(mtime);

                    match read_and_compute_target(&exposure_file, &addresses) {
                        Ok(target_usd) => {
                            info!(
                                "hedge target updated: asset={}, target_usd={:.2}",
                                asset_name, target_usd,
                            );
                            let _ = controller.send(target_usd);
                        }
                        Err(e) => {
                            warn!("hedge: failed to read exposures: {e}");
                        }
                    }
                }
            }
        }
    })
}

/// Read the exposure JSON file, sum USD exposures for mapped token addresses, negate for hedge.
/// Returns the negated USD exposure (hedge target in USD).
fn read_and_compute_target(path: &str, addresses: &[String]) -> Result<f64> {
    let contents = std::fs::read_to_string(path)
        .with_context(|| format!("reading {path}"))?;

    let root: serde_json::Value = serde_json::from_str(&contents)
        .with_context(|| format!("parsing {path}"))?;

    // Navigate to valuation.exposures_usd
    let exposures = root
        .get("valuation")
        .and_then(|v| v.get("exposures_usd"))
        .and_then(|v| v.as_object())
        .ok_or_else(|| anyhow::anyhow!("missing valuation.exposures_usd in {path}"))?;

    let mut total_exposure_usd = 0.0;
    for addr in addresses {
        let addr_lower = addr.to_lowercase();
        for (key, val) in exposures {
            if key.to_lowercase() == addr_lower {
                if let Some(amount) = val.as_f64() {
                    total_exposure_usd += amount;
                }
            }
        }
    }

    // Hedge = opposite of on-chain USD exposure
    // The MM engine converts this USD target to base units using fair price
    Ok(-total_exposure_usd)
}
