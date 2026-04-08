/// Quick diagnostic: dump raw HL API responses to understand field semantics.
use anyhow::{Context, Result};
use crypto_oms::hyperliquid::client::HyperliquidClient;
use crypto_oms::hyperliquid::types::InfoRequest;

#[tokio::main]
async fn main() -> Result<()> {
    let _ = dotenv::dotenv();

    let private_key = std::env::var("HL_PRIVATE_KEY")
        .context("HL_PRIVATE_KEY env var not set")?;
    let account_address = std::env::var("HL_ACCOUNT_ADDRESS")
        .context("HL_ACCOUNT_ADDRESS env var not set")?;

    let client = HyperliquidClient::new(&private_key, account_address, None)?;

    println!("Account: {}", client.account_address);
    println!("Agent:   {}", client.agent_address);
    println!();

    // clearinghouseState
    let raw = client.info_raw(&InfoRequest::ClearinghouseState {
        user: &client.account_address,
    }).await?;
    let pretty: serde_json::Value = serde_json::from_str(&raw)?;
    println!("=== clearinghouseState ===");
    println!("{}", serde_json::to_string_pretty(&pretty)?);
    println!();

    // spotClearinghouseState
    let raw = client.info_raw(&InfoRequest::SpotClearinghouseState {
        user: &client.account_address,
    }).await?;
    let pretty: serde_json::Value = serde_json::from_str(&raw)?;
    println!("=== spotClearinghouseState ===");
    println!("{}", serde_json::to_string_pretty(&pretty)?);

    Ok(())
}
