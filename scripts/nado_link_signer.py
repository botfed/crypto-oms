#!/usr/bin/env python3
"""Nado Link Signer — authorize a trading key for your subaccount.

Usage:
  nado_link_signer.py keygen                         Generate a random signing key
  nado_link_signer.py link <signer_address>          Link a new signer (Trezor)
  nado_link_signer.py unlink                         Revoke current signer (Trezor)

Options:
  --passphrase-on-host     Enter Trezor passphrase in terminal
  --derivation-path <path> Skip scan, use this exact HD path
  --scan-limit <n>         Max indices to scan (default: 100)
  --dry-run                Print EIP-712 details, don't sign/submit

Environment:
  NADO_ACCOUNT_ADDRESS     Account EOA address (required for link/unlink)
  NADO_SUBACCOUNT_NAME     Subaccount name (default: "default")
  NADO_GATEWAY_URL         Gateway URL (default: mainnet)
"""

import argparse
import getpass
import hashlib
import os
import pathlib
import secrets
import sys

import requests


def load_dotenv():
    """Load .env from cwd or parents. No dependencies."""
    path = pathlib.Path.cwd()
    for d in [path, *path.parents]:
        env_file = d / ".env"
        if env_file.is_file():
            for line in env_file.read_text().splitlines():
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, _, val = line.partition("=")
                key = key.strip()
                val = val.strip().strip("\"'")
                os.environ.setdefault(key, val)
            break


load_dotenv()
from trezorlib.client import get_default_client, PassphraseSetting
from trezorlib import ethereum, messages, tools

# ---------------------------------------------------------------------------
# Keccak-256 (Ethereum's hash, NOT SHA-3)
# ---------------------------------------------------------------------------

def keccak256(data: bytes) -> bytes:
    return hashlib.new("keccak-256", data).digest()


# ---------------------------------------------------------------------------
# EIP-712
# ---------------------------------------------------------------------------

DOMAIN_TYPE_HASH = keccak256(
    b"EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)"
)
LINK_SIGNER_TYPE_HASH = keccak256(
    b"LinkSigner(bytes32 sender,bytes32 signer,uint64 nonce)"
)


def domain_separator(chain_id: int, endpoint_addr: str) -> bytes:
    addr_bytes = bytes.fromhex(endpoint_addr.replace("0x", ""))
    return keccak256(
        DOMAIN_TYPE_HASH
        + keccak256(b"Nado")
        + keccak256(b"0.0.1")
        + chain_id.to_bytes(32, "big")
        + (b"\x00" * 12) + addr_bytes
    )


def link_signer_hash(sender_hex: str, signer_hex: str, nonce: int) -> bytes:
    sender_bytes = bytes.fromhex(sender_hex.replace("0x", "").ljust(64, "0"))
    signer_bytes = bytes.fromhex(signer_hex.replace("0x", "").ljust(64, "0"))
    return keccak256(
        LINK_SIGNER_TYPE_HASH
        + sender_bytes
        + signer_bytes
        + nonce.to_bytes(32, "big")
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def build_sender(address: str, subaccount_name: str) -> str:
    addr = address.lower().replace("0x", "")
    name_hex = subaccount_name.encode().hex().ljust(24, "0")[:24]
    return f"0x{addr}{name_hex}"


# ---------------------------------------------------------------------------
# Nado API
# ---------------------------------------------------------------------------

def fetch_contracts(gateway_url: str) -> dict:
    r = requests.get(f"{gateway_url}/query?type=contracts")
    resp = r.json()
    if resp["status"] != "success":
        sys.exit(f"contracts query failed: {resp.get('error')}")
    return resp["data"]


def fetch_tx_nonce(gateway_url: str, address: str) -> int:
    r = requests.get(f"{gateway_url}/query?type=nonces&address={address}")
    resp = r.json()
    if resp["status"] != "success":
        sys.exit(f"nonces query failed: {resp.get('error')}")
    return int(resp["data"]["tx_nonce"])


def submit_link_signer(gateway_url: str, sender: str, signer: str, nonce: int, signature: str):
    body = {
        "link_signer": {
            "tx": {
                "sender": sender,
                "signer": signer,
                "nonce": str(nonce),
            },
            "signature": signature,
        }
    }
    r = requests.post(f"{gateway_url}/execute", json=body)
    resp = r.json()
    if resp["status"] != "success":
        sys.exit(f"link_signer failed: {resp.get('error')}")


# ---------------------------------------------------------------------------
# Keygen
# ---------------------------------------------------------------------------

def cmd_keygen():
    from cryptography.hazmat.primitives.asymmetric import ec
    from cryptography.hazmat.primitives import serialization

    pk_bytes = secrets.token_bytes(32)
    pk_int = int.from_bytes(pk_bytes, "big")
    private_key = ec.derive_private_key(pk_int, ec.SECP256K1())
    pub_bytes = private_key.public_key().public_bytes(
        serialization.Encoding.X962,
        serialization.PublicFormat.UncompressedPoint,
    )
    addr = "0x" + keccak256(pub_bytes[1:])[-20:].hex()
    print(f"  Private key: 0x{pk_bytes.hex()}")
    print(f"  Address:     {addr}")


# ---------------------------------------------------------------------------
# Trezor: scan + sign in a single transport session
# ---------------------------------------------------------------------------

def trezor_scan_and_sign(
    target_address: str,
    chain_id: int,
    endpoint_addr: str,
    sender_hex: str,
    signer_hex: str,
    nonce: int,
    passphrase_on_host: bool,
    explicit_path: str | None,
    scan_limit: int,
) -> tuple[str, str]:
    """Returns (derivation_path, signature_hex)."""

    def on_pin(req):
        return getpass.getpass("  PIN: ")

    def on_button(req):
        pass

    client = get_default_client(
        "nado_link_signer",
        pin_callback=on_pin,
        button_callback=on_button,
    )

    # Prompt passphrase once
    if passphrase_on_host:
        pp = getpass.getpass("  Passphrase: ")
    else:
        pp = PassphraseSetting.ON_DEVICE

    session = client.get_session(passphrase=pp)
    target = target_address.lower()

    # --- Scan: fast, single transport connection ---
    with session:
        if explicit_path:
            path_str = explicit_path
            n = tools.parse_path(path_str)
            resp = session.call(
                messages.EthereumGetAddress(address_n=n, show_display=False),
                expect=messages.EthereumAddress,
            )
            addr = resp.address
            if addr.lower() != target:
                sys.exit(
                    f"  Mismatch: Trezor={addr}, expected={target_address}\n"
                    f"  Check --derivation-path or NADO_ACCOUNT_ADDRESS"
                )
            print(f"  Verified: {addr}", file=sys.stderr)
        else:
            path_str = None
            print(f"  Scanning Trezor addresses (up to {scan_limit})...", file=sys.stderr)
            for i in range(scan_limit):
                n = tools.parse_path(f"m/44'/60'/0'/0/{i}")
                resp = session.call(
                    messages.EthereumGetAddress(address_n=n, show_display=False),
                    expect=messages.EthereumAddress,
                )
                addr = resp.address
                if addr.lower() == target:
                    path_str = f"m/44'/60'/0'/0/{i}"
                    print(f"    [{i}] {addr}  <- match!", file=sys.stderr)
                    break
                else:
                    print(f"    [{i}] {addr}", file=sys.stderr)

            if path_str is None:
                sys.exit(
                    f"  Address {target_address} not found in {scan_limit} indices.\n"
                    f"  Use --derivation-path to specify manually."
                )

    # --- Sign: full EIP-712 typed data (not blind hash) ---
    typed_data = {
        "types": {
            "EIP712Domain": [
                {"name": "name", "type": "string"},
                {"name": "version", "type": "string"},
                {"name": "chainId", "type": "uint256"},
                {"name": "verifyingContract", "type": "address"},
            ],
            "LinkSigner": [
                {"name": "sender", "type": "bytes32"},
                {"name": "signer", "type": "bytes32"},
                {"name": "nonce", "type": "uint64"},
            ],
        },
        "primaryType": "LinkSigner",
        "domain": {
            "name": "Nado",
            "version": "0.0.1",
            "chainId": chain_id,
            "verifyingContract": endpoint_addr,
        },
        "message": {
            "sender": sender_hex,
            "signer": signer_hex,
            "nonce": nonce,
        },
    }

    n = tools.parse_path(path_str)
    print(f"  Signing with Trezor (confirm on device)...", file=sys.stderr)
    ret = ethereum.sign_typed_data(session, n, typed_data)
    sig_hex = "0x" + ret.signature.hex()

    return path_str, sig_hex


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Nado Link Signer")
    sub = parser.add_subparsers(dest="command")

    sub.add_parser("keygen", help="Generate a random signing key")

    link_p = sub.add_parser("link", help="Link a new signer")
    link_p.add_argument("signer_address")

    sub.add_parser("unlink", help="Revoke current signer")

    parser.add_argument("--passphrase-on-host", action="store_true")
    parser.add_argument("--derivation-path", default=None)
    parser.add_argument("--scan-limit", type=int, default=100)
    parser.add_argument("--dry-run", action="store_true")

    args = parser.parse_args()

    if args.command is None:
        parser.print_help()
        return

    if args.command == "keygen":
        cmd_keygen()
        return

    # --- link / unlink ---
    account_address = os.environ.get("NADO_ACCOUNT_ADDRESS")
    if not account_address:
        sys.exit("NADO_ACCOUNT_ADDRESS not set")

    subaccount_name = os.environ.get("NADO_SUBACCOUNT_NAME", "default")
    gateway_url = os.environ.get("NADO_GATEWAY_URL", "https://gateway.prod.nado.xyz/v1")

    sender = build_sender(account_address, subaccount_name)
    is_unlink = args.command == "unlink"

    if is_unlink:
        signer_bytes32 = "0x" + "0" * 64
        signer_display = "revoking current signer"
    else:
        signer_bytes32 = build_sender(args.signer_address, subaccount_name)
        signer_display = args.signer_address

    action = "Unlinking" if is_unlink else "Linking"
    print(file=sys.stderr)
    print(f"  Nado Link Signer", file=sys.stderr)
    print(f"  ────────────────", file=sys.stderr)
    print(f"  Account:    {account_address}", file=sys.stderr)
    print(f"  Subaccount: {subaccount_name}", file=sys.stderr)
    print(f"  Sender:     {sender}", file=sys.stderr)
    print(f"  {action}:   {signer_display}", file=sys.stderr)
    print(file=sys.stderr)

    # 1. Fetch contracts
    print(f"  Fetching contracts... ", end="", flush=True, file=sys.stderr)
    contracts = fetch_contracts(gateway_url)
    chain_id = int(contracts["chain_id"])
    endpoint = contracts["endpoint_addr"]
    print(f"chain_id={chain_id}, endpoint={endpoint}", file=sys.stderr)

    # 2. Fetch nonce
    print(f"  Fetching nonce... ", end="", flush=True, file=sys.stderr)
    tx_nonce = fetch_tx_nonce(gateway_url, account_address)
    print(f"tx_nonce={tx_nonce}", file=sys.stderr)

    # 3. Build EIP-712 hashes
    dom_hash = domain_separator(chain_id, endpoint)
    msg_hash = link_signer_hash(sender, signer_bytes32, tx_nonce)

    print(file=sys.stderr)
    print(f"  EIP-712 LinkSigner:", file=sys.stderr)
    print(f"    sender:       {sender}", file=sys.stderr)
    print(f"    signer:       {signer_bytes32}", file=sys.stderr)
    print(f"    nonce:        {tx_nonce}", file=sys.stderr)
    print(f"    chain_id:     {chain_id}", file=sys.stderr)
    print(f"    endpoint:     {endpoint}", file=sys.stderr)
    print(file=sys.stderr)
    print(f"  Domain hash:  0x{dom_hash.hex()}", file=sys.stderr)
    print(f"  Struct hash:  0x{msg_hash.hex()}", file=sys.stderr)
    print(file=sys.stderr)

    if args.dry_run:
        print("  --dry-run: stopping before sign/submit", file=sys.stderr)
        return

    # 4. Trezor: scan + sign
    path, signature = trezor_scan_and_sign(
        target_address=account_address,
        chain_id=chain_id,
        endpoint_addr=endpoint,
        sender_hex=sender,
        signer_hex=signer_bytes32,
        nonce=tx_nonce,
        passphrase_on_host=args.passphrase_on_host,
        explicit_path=args.derivation_path,
        scan_limit=args.scan_limit,
    )
    print(f"  Derivation path: {path}", file=sys.stderr)
    print(f"  Signature: {signature}", file=sys.stderr)
    print(file=sys.stderr)

    # 5. Submit
    print(f"  Submitting to Nado... ", end="", flush=True, file=sys.stderr)
    submit_link_signer(gateway_url, sender, signer_bytes32, tx_nonce, signature)
    print("success!", file=sys.stderr)
    print(file=sys.stderr)

    if is_unlink:
        print("  Linked signer revoked.", file=sys.stderr)
    else:
        print(f"  Signer {args.signer_address} linked to subaccount \"{subaccount_name}\".", file=sys.stderr)
        print("  You can now trade with this key by setting NADO_PRIVATE_KEY.", file=sys.stderr)
    print(file=sys.stderr)


if __name__ == "__main__":
    main()
