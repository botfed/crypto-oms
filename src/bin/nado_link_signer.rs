use alloy::primitives::{Address, B256, U256};
use alloy::signers::local::PrivateKeySigner;
use alloy::signers::Signer;
use alloy::sol;
use alloy::sol_types::SolStruct;
use anyhow::{Context, Result, bail};
use reqwest::Client;
use serde::Deserialize;
use std::process::{Command, Stdio};

use crypto_oms::nado::types::build_sender;

// ---------------------------------------------------------------------------
// EIP-712 typed struct for LinkSigner
// ---------------------------------------------------------------------------

sol! {
    #[derive(Debug)]
    struct LinkSigner {
        bytes32 sender;
        bytes32 signer;
        uint64 nonce;
    }
}

// ---------------------------------------------------------------------------
// Local response types (minimal, self-contained)
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct GwResponse {
    status: String,
    data: Option<serde_json::Value>,
    error: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ContractsData {
    chain_id: String,
    endpoint_addr: String,
}

#[derive(Debug, Deserialize)]
struct NoncesData {
    tx_nonce: String,
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn parse_bytes32(hex_str: &str) -> B256 {
    let hex = hex_str.strip_prefix("0x").unwrap_or(hex_str);
    let bytes: [u8; 32] = hex::decode(hex)
        .unwrap_or_else(|_| vec![0u8; 32])
        .try_into()
        .unwrap_or([0u8; 32]);
    B256::from(bytes)
}

fn eip712_domain(chain_id: u64, endpoint_addr: &str) -> alloy::sol_types::Eip712Domain {
    let contract: Address = endpoint_addr.parse().unwrap_or(Address::ZERO);
    alloy::sol_types::Eip712Domain {
        name: Some(std::borrow::Cow::Borrowed("Nado")),
        version: Some(std::borrow::Cow::Borrowed("0.0.1")),
        chain_id: Some(U256::from(chain_id)),
        verifying_contract: Some(contract),
        salt: None,
    }
}

// ---------------------------------------------------------------------------
// Nado API calls (direct HTTP, no NadoClient dependency)
// ---------------------------------------------------------------------------

async fn fetch_contracts(http: &Client, gateway_url: &str) -> Result<ContractsData> {
    let resp: GwResponse = http
        .get(format!("{gateway_url}/query?type=contracts"))
        .send()
        .await?
        .json()
        .await?;
    if resp.status != "success" {
        bail!("contracts query failed: {:?}", resp.error);
    }
    Ok(serde_json::from_value(resp.data.context("no data")?)?)
}

async fn fetch_tx_nonce(http: &Client, gateway_url: &str, address: &str) -> Result<u64> {
    let resp: GwResponse = http
        .get(format!("{gateway_url}/query?type=nonces&address={address}"))
        .send()
        .await?
        .json()
        .await?;
    if resp.status != "success" {
        bail!("nonces query failed: {:?}", resp.error);
    }
    let data: NoncesData = serde_json::from_value(resp.data.context("no data")?)?;
    Ok(data.tx_nonce.parse()?)
}

async fn submit_link_signer(
    http: &Client,
    gateway_url: &str,
    sender: &str,
    signer: &str,
    nonce: u64,
    signature: &str,
) -> Result<()> {
    let body = serde_json::json!({
        "link_signer": {
            "tx": {
                "sender": sender,
                "signer": signer,
                "nonce": nonce.to_string(),
            },
            "signature": signature,
        }
    });

    let resp: GwResponse = http
        .post(format!("{gateway_url}/execute"))
        .json(&body)
        .send()
        .await?
        .json()
        .await?;

    if resp.status != "success" {
        bail!("link_signer failed: {:?}", resp.error);
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Signing
// ---------------------------------------------------------------------------

const DEFAULT_SCAN_LIMIT: u32 = 100;

// ---------------------------------------------------------------------------
// Trezor interaction via single trezorlib Python session
// ---------------------------------------------------------------------------
//
// Runs scan + sign in one process so PIN/passphrase is entered only once.
// Uses trezorlib directly (same package as trezorctl, already installed).
// ---------------------------------------------------------------------------

const TREZOR_SCRIPT: &str = r#"
import sys, getpass
from trezorlib.client import get_default_client, PassphraseSetting
from trezorlib import ethereum, tools

target    = sys.argv[1].lower()
limit     = int(sys.argv[2])
poh       = sys.argv[3] == "1"
dom_hash  = bytes.fromhex(sys.argv[4])
msg_hash  = bytes.fromhex(sys.argv[5])
expl_path = sys.argv[6] if len(sys.argv) > 6 else None

def on_pin(req):
    return getpass.getpass("  PIN: ")

def on_button(req):
    pass

client = get_default_client(
    "nado_link_signer",
    pin_callback=on_pin,
    button_callback=on_button,
)

# Prompt passphrase once upfront
if poh:
    pp = getpass.getpass("  Passphrase: ")
else:
    pp = PassphraseSetting.ON_DEVICE

# --- Scan: find derivation path ---
session = client.get_session(passphrase=pp)

if expl_path:
    path_str = expl_path
    n = tools.parse_path(path_str)
    addr = ethereum.get_address(session, n, show_display=False)
    if addr.lower() != target:
        print(f"  Mismatch: Trezor={addr}, expected={sys.argv[1]}", file=sys.stderr)
        sys.exit(1)
    print(f"  Verified: {addr}", file=sys.stderr)
else:
    path_str = None
    print(f"  Scanning Trezor addresses (up to {limit})...", file=sys.stderr)
    for i in range(limit):
        n = tools.parse_path(f"m/44'/60'/0'/0/{i}")
        addr = ethereum.get_address(session, n, show_display=False)
        if addr.lower() == target:
            path_str = f"m/44'/60'/0'/0/{i}"
            print(f"    [{i}] {addr}  <- match!", file=sys.stderr)
            break
        else:
            print(f"    [{i}] {addr}", file=sys.stderr)
    if path_str is None:
        print(f"  Address not found in {limit} indices", file=sys.stderr)
        sys.exit(1)

# --- Sign: fresh session with cached passphrase ---
session = client.get_session(passphrase=pp)
n = tools.parse_path(path_str)
print(f"  Signing with Trezor (confirm on device)...", file=sys.stderr)
ret = ethereum.sign_typed_data_hash(session, n, dom_hash, msg_hash)
sig_hex = "0x" + ret.signature.hex()

print(f"path {path_str}")
print(f"signature {sig_hex}")
"#;

/// Run the trezorlib Python script: scan for matching address + sign, single session.
fn trezor_scan_and_sign(
    target_address: &str,
    scan_limit: u32,
    passphrase_on_host: bool,
    domain_hash: B256,
    struct_hash: B256,
    explicit_path: Option<&str>,
) -> Result<(String, String)> {
    let domain_hex = hex::encode(domain_hash.as_slice());
    let struct_hex = hex::encode(struct_hash.as_slice());
    let poh = if passphrase_on_host { "1" } else { "0" };
    let limit_str = scan_limit.to_string();

    let mut cmd = Command::new("python3");
    cmd.args([
        "-c", TREZOR_SCRIPT,
        target_address, &limit_str, poh, &domain_hex, &struct_hex,
    ]);
    if let Some(path) = explicit_path {
        cmd.arg(path);
    }
    cmd.stdin(Stdio::inherit())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit());

    let child = cmd
        .spawn()
        .context("failed to run python3 — is trezorlib installed? (pip install trezor)")?;

    let output = child.wait_with_output()?;
    if !output.status.success() {
        bail!("Trezor scan/sign failed (exit {})", output.status.code().unwrap_or(-1));
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    let mut path = None;
    let mut signature = None;
    for line in stdout.lines() {
        if let Some(rest) = line.strip_prefix("path ") {
            path = Some(rest.to_string());
        }
        if let Some(rest) = line.strip_prefix("signature ") {
            signature = Some(rest.to_string());
        }
    }

    Ok((
        path.context("no path in trezor output")?,
        signature.context("no signature in trezor output")?,
    ))
}

/// Sign with a local private key (for testing / non-Trezor accounts).
async fn sign_with_key(
    private_key: &str,
    msg: &LinkSigner,
    domain: &alloy::sol_types::Eip712Domain,
) -> Result<String> {
    let signer: PrivateKeySigner = private_key.parse().context("invalid private key")?;
    let signing_hash = msg.eip712_signing_hash(domain);
    let sig = signer.sign_hash(&signing_hash).await?;
    Ok(format!("0x{}", hex::encode(sig.as_bytes())))
}

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------

fn print_usage() {
    eprintln!("Nado Link Signer — authorize a trading key for your subaccount");
    eprintln!();
    eprintln!("Usage:");
    eprintln!("  nado_link_signer keygen                  Generate a random signing key");
    eprintln!("  nado_link_signer link <signer_address>   Link a new signer");
    eprintln!("  nado_link_signer unlink                  Revoke current signer");
    eprintln!();
    eprintln!("Options:");
    eprintln!("  --private-key <key>      Sign with key instead of Trezor");
    eprintln!("  --derivation-path <path> Skip scan, use this exact HD path");
    eprintln!("  --scan-limit <n>         Max indices to scan (default: 100)");
    eprintln!("  --passphrase-on-host     Enter Trezor passphrase in terminal, not on device");
    eprintln!("  --dry-run                Print EIP-712 details, don't sign/submit");
    eprintln!();
    eprintln!("Environment:");
    eprintln!("  NADO_ACCOUNT_ADDRESS     Account EOA address (required)");
    eprintln!("  NADO_SUBACCOUNT_NAME     Subaccount name (default: \"default\")");
    eprintln!("  NADO_GATEWAY_URL         Gateway URL (default: mainnet)");
}

#[tokio::main]
async fn main() -> Result<()> {
    let _ = dotenv::dotenv();

    let args: Vec<String> = std::env::args().collect();

    // Handle keygen early — no env vars needed
    if args.get(1).map(|s| s.as_str()) == Some("keygen") {
        let signer = PrivateKeySigner::random();
        let privkey = hex::encode(signer.credential().to_bytes());
        let address = signer.address();
        println!("  Private key: 0x{privkey}");
        println!("  Address:     {address}");
        return Ok(());
    }

    let mut signer_addr: Option<String> = None;
    let mut unlink = false;
    let mut private_key: Option<String> = None;
    let mut derivation_path: Option<String> = None;
    let mut scan_limit = DEFAULT_SCAN_LIMIT;
    let mut passphrase_on_host = false;
    let mut dry_run = false;

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "link" => {
                i += 1;
                if i >= args.len() {
                    bail!("link requires a signer address");
                }
                signer_addr = Some(args[i].clone());
            }
            "unlink" => {
                unlink = true;
            }
            "keygen" => {
                unreachable!(); // handled above
            }
            "--private-key" => {
                i += 1;
                if i >= args.len() {
                    bail!("--private-key requires a value");
                }
                private_key = Some(args[i].clone());
            }
            "--derivation-path" => {
                i += 1;
                if i >= args.len() {
                    bail!("--derivation-path requires a value");
                }
                derivation_path = Some(args[i].clone());
            }
            "--scan-limit" => {
                i += 1;
                if i >= args.len() {
                    bail!("--scan-limit requires a value");
                }
                scan_limit = args[i].parse().context("--scan-limit must be a number")?;
            }
            "--passphrase-on-host" => {
                passphrase_on_host = true;
            }
            "--dry-run" => {
                dry_run = true;
            }
            "--help" | "-h" => {
                print_usage();
                return Ok(());
            }
            other => bail!("unknown argument: {other}"),
        }
        i += 1;
    }

    if signer_addr.is_none() && !unlink {
        print_usage();
        return Ok(());
    }

    // ------------------------------------------------------------------
    // Load config from env
    // ------------------------------------------------------------------

    let account_address =
        std::env::var("NADO_ACCOUNT_ADDRESS").context("NADO_ACCOUNT_ADDRESS not set")?;
    let subaccount_name = std::env::var("NADO_SUBACCOUNT_NAME")
        .unwrap_or_else(|_| "default".to_string());
    let gateway_url = std::env::var("NADO_GATEWAY_URL")
        .unwrap_or_else(|_| "https://gateway.prod.nado.xyz/v1".to_string());

    let http = Client::new();
    let sender = build_sender(&account_address, &subaccount_name);

    let signer_bytes32 = if unlink {
        format!("0x{}", "0".repeat(64))
    } else {
        build_sender(signer_addr.as_ref().unwrap(), &subaccount_name)
    };

    let action = if unlink { "Unlinking" } else { "Linking" };
    let target = if unlink {
        "revoking current signer".to_string()
    } else {
        signer_addr.as_ref().unwrap().clone()
    };

    eprintln!();
    eprintln!("  Nado Link Signer");
    eprintln!("  ────────────────");
    eprintln!("  Account:    {account_address}");
    eprintln!("  Subaccount: {subaccount_name}");
    eprintln!("  Sender:     {sender}");
    eprintln!("  {action}:   {target}");
    eprintln!();

    // ------------------------------------------------------------------
    // 1. Fetch contracts
    // ------------------------------------------------------------------

    eprint!("  Fetching contracts... ");
    let contracts = fetch_contracts(&http, &gateway_url).await?;
    let chain_id: u64 = contracts.chain_id.parse().context("bad chain_id")?;
    eprintln!(
        "chain_id={chain_id}, endpoint={}",
        contracts.endpoint_addr
    );

    // ------------------------------------------------------------------
    // 2. Fetch nonce
    // ------------------------------------------------------------------

    eprint!("  Fetching nonce... ");
    let tx_nonce = fetch_tx_nonce(&http, &gateway_url, &account_address).await?;
    eprintln!("tx_nonce={tx_nonce}");

    // ------------------------------------------------------------------
    // 3. Build EIP-712
    // ------------------------------------------------------------------

    let domain = eip712_domain(chain_id, &contracts.endpoint_addr);
    let link_msg = LinkSigner {
        sender: parse_bytes32(&sender),
        signer: parse_bytes32(&signer_bytes32),
        nonce: tx_nonce,
    };

    let domain_hash = domain.hash_struct();
    let struct_hash = link_msg.eip712_hash_struct();

    eprintln!();
    eprintln!("  EIP-712 LinkSigner:");
    eprintln!("    sender:       {sender}");
    eprintln!("    signer:       {signer_bytes32}");
    eprintln!("    nonce:        {tx_nonce}");
    eprintln!("    chain_id:     {chain_id}");
    eprintln!("    endpoint:     {}", contracts.endpoint_addr);
    eprintln!();
    eprintln!(
        "  Domain hash:  0x{}",
        hex::encode(domain_hash.as_slice())
    );
    eprintln!(
        "  Struct hash:  0x{}",
        hex::encode(struct_hash.as_slice())
    );
    eprintln!();

    if dry_run {
        eprintln!("  --dry-run: stopping before sign/submit");
        return Ok(());
    }

    // ------------------------------------------------------------------
    // 4. Sign
    // ------------------------------------------------------------------

    let signature = if let Some(ref pk) = private_key {
        eprintln!("  Signing with private key...");
        sign_with_key(pk, &link_msg, &domain).await?
    } else {
        let (path, sig) = trezor_scan_and_sign(
            &account_address,
            scan_limit,
            passphrase_on_host,
            domain_hash,
            struct_hash,
            derivation_path.as_deref(),
        )?;
        eprintln!("  Derivation path: {path}");
        sig
    };

    eprintln!("  Signature: {signature}");
    eprintln!();

    // ------------------------------------------------------------------
    // 5. Submit
    // ------------------------------------------------------------------

    eprint!("  Submitting to Nado... ");
    submit_link_signer(
        &http,
        &gateway_url,
        &sender,
        &signer_bytes32,
        tx_nonce,
        &signature,
    )
    .await?;
    eprintln!("success!");
    eprintln!();

    if unlink {
        eprintln!("  Linked signer revoked.");
    } else {
        eprintln!(
            "  Signer {} linked to subaccount \"{}\".",
            signer_addr.unwrap(),
            subaccount_name,
        );
        eprintln!("  You can now trade with this key by setting NADO_PRIVATE_KEY.");
    }
    eprintln!();

    Ok(())
}
