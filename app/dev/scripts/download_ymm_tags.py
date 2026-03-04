from __future__ import annotations

import argparse
from pathlib import Path
import sys


DEV_ROOT = Path(__file__).resolve().parents[1]
if str(DEV_ROOT) not in sys.path:
    sys.path.insert(0, str(DEV_ROOT))

from product_prospector.core.config_store import (
    load_shopify_config,
    load_shopify_token,
    save_shopify_token,
)
from product_prospector.core.shopify_oauth import exchange_client_credentials_for_token
from product_prospector.core.shopify_ymm_tags import (
    default_ymm_tags_path,
    fetch_ymm_tags_table,
    save_ymm_tags_table,
)


def _resolve_required_root() -> Path:
    here = Path(__file__).resolve()
    dev_root = here.parents[1]
    runtime_app = dev_root.parent
    required_root = runtime_app / "required"
    required_root.mkdir(parents=True, exist_ok=True)
    return required_root


def _ensure_access_token():
    token = load_shopify_token()
    if token is not None and token.access_token:
        return token.access_token, None
    config = load_shopify_config()
    if config is None:
        return "", "Invalid app/config/shopify.json."
    if config.admin_api_access_token:
        return config.admin_api_access_token, None
    result = exchange_client_credentials_for_token(config)
    if not result.success:
        return "", result.error or "Could not get Shopify token."
    save_shopify_token(result.access_token, result.scope)
    return result.access_token, None


def main() -> int:
    parser = argparse.ArgumentParser(description="Download Shopify YMM tags into a local mapping file.")
    parser.add_argument(
        "--output",
        default="",
        help="Output .csv/.xlsx path. Default: app/required/mappings/ShopifyYMMTags.csv",
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        default=250,
        help="Maximum GraphQL pages to read (100 products per page).",
    )
    args = parser.parse_args()

    config = load_shopify_config()
    if config is None:
        print("ERROR: Invalid app/config/shopify.json.")
        return 1

    access_token, token_error = _ensure_access_token()
    if token_error:
        print(f"ERROR: {token_error}")
        return 1

    required_root = _resolve_required_root()
    output_path = Path(args.output).expanduser().resolve() if args.output else default_ymm_tags_path(required_root)
    if output_path.suffix.lower() not in {".csv", ".xlsx"}:
        output_path = output_path.with_suffix(".csv")

    def on_progress(page: int, count: int) -> None:
        print(f"Reading Shopify products for YMM tags... page {page}, YMM tags {count}")

    table, fetch_error = fetch_ymm_tags_table(
        config=config,
        access_token=access_token,
        max_pages=max(1, int(args.max_pages)),
        progress_callback=on_progress,
    )
    if fetch_error:
        print(f"ERROR: {fetch_error}")
        return 1

    count, save_error = save_ymm_tags_table(table, output_path=output_path)
    if save_error:
        print(f"ERROR: Could not save mapping file: {save_error}")
        return 1

    print(f"Done. Saved {count} YMM tag row(s) to:")
    print(str(output_path))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
