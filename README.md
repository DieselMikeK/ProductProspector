# Product Prospector

A Windows desktop application for automotive aftermarket product-ops workflows. It normalizes inconsistent vendor spreadsheets, matches records against your live Shopify catalog, and pushes clean product listings as drafts — all from a native GUI.

---

## Features

- **Vendor file ingestion** — Upload CSV or XLSX files with variable, non-standard column names
- **Auto column mapping** — Suggests SKU, title, description, fitment, and year columns from the source file
- **Row stitching** — Merges split/continuation rows into one record per SKU
- **Run modes** — `Update Existing`, `Create New`, and `Upsert`
- **Shopify catalog matching** — Compares vendor SKUs against a live Shopify export or direct API fetch
- **Action plan export** — Produces a reviewable CSV with `update`, `create`, or `skip` per row
- **Product creation** — Scaffolds required Shopify product fields (title, price, cost, weight, type, vendor, metafields) from vendor data
- **Direct Shopify push** — Pushes new products as drafts via the Shopify Admin API
- **Vendor pricing** — Calculates cost-per-item from vendor discount rules
- **Type mapping** — Maps vendor product types to Google Product Types and Shopify-compatible categories
- **Vendor profile resolution** — Normalizes vendor identities across inconsistent aliases
- **Brand metafield support** — Links products to Shopify brand metaobjects

---

## Project Structure

```
ProductProspector/
├── app/
│   ├── config/
│   │   ├── shopify.json.template       # Copy to shopify.json and fill in credentials
│   │   └── product_creation_rules.template.json
│   ├── dev/
│   │   ├── desktop_app.py              # Entry point (GUI)
│   │   ├── run_product_prospector.pyw  # Windowless launcher
│   │   ├── requirements.txt
│   │   ├── ProductProspector.spec      # PyInstaller build spec
│   │   ├── core/                       # All business logic modules
│   │   │   ├── config_store.py         # Settings/credential loaders
│   │   │   ├── create_product_output.py
│   │   │   ├── io_utils.py
│   │   │   ├── mapping.py              # Column suggestion engine
│   │   │   ├── normalization.py
│   │   │   ├── pricing_rules.py        # Vendor discount calculations
│   │   │   ├── processing.py           # Action plan builder, row stitcher
│   │   │   ├── product_model.py
│   │   │   ├── scraper_engine.py
│   │   │   ├── session_state.py
│   │   │   ├── shopify_brand_metaobjects.py
│   │   │   ├── shopify_catalog.py      # Live catalog fetch
│   │   │   ├── shopify_oauth.py        # OAuth / client credentials auth
│   │   │   ├── shopify_push.py         # Draft product push
│   │   │   ├── shopify_sku_cache.py
│   │   │   ├── shopify_vendor_catalog.py
│   │   │   ├── type_mapping_engine.py
│   │   │   ├── vendor_normalization.py
│   │   │   ├── vendor_profiles.py
│   │   │   ├── workflow_build.py       # Orchestrates full build pipeline
│   │   │   └── years.py
│   │   └── scripts/                    # Utility/data download scripts
│   │       ├── download_brand_metaobjects.py
│   │       ├── download_shopify_vendors.py
│   │       ├── download_vendor_sku_prefixes.py
│   │       └── fill_vendor_profile_brands.py
│   ├── docs/
│   │   └── PRODUCT_CREATION_FIELD_SPEC.md
│   ├── required/
│   │   ├── mappings/                   # Vendor mapping CSVs (see below)
│   │   ├── rules/                      # Normalization rules and templates
│   │   └── types/                      # Product type dictionaries
│   ├── icon.ico
│   └── logo.png
└── README.md
```

---

## Setup

### Prerequisites

- Python 3.11+ (developed on CPython 3.14)
- Windows (uses Tkinter native GUI)

### Install dependencies

```powershell
cd app/dev
python -m pip install -r requirements.txt
```

### Configure Shopify credentials

Copy the template and fill in your credentials:

```powershell
copy app\config\shopify.json.template app\config\shopify.json
```

Edit `app/config/shopify.json`:

```json
{
  "shop_domain": "your-store.myshopify.com",
  "storefront_domain": "www.yourstore.com",
  "client_id": "YOUR_CLIENT_ID",
  "client_secret": "YOUR_CLIENT_SECRET",
  "admin_api_access_token": "",
  "auth_mode": "client_credentials",
  "api_version": "2025-10",
  "scopes": ["read_products", "write_products", "read_metafields", "write_metafields"],
  "redirect_uri": "http://127.0.0.1:8787/callback",
  "callback_bind_host": "0.0.0.0",
  "callback_bind_port": 8787,
  "auth_timeout_seconds": 240
}
```

> **Note:** `shopify.json` and `shopify_token.json` are in `.gitignore` and will never be committed.

### Supported auth modes

| `auth_mode` | Description |
|---|---|
| `client_credentials` | Preferred — uses client ID/secret, no browser redirect needed |
| `admin_api_access_token` | Direct token from Shopify admin |
| `oauth` | Full OAuth handshake via browser redirect |
| `auto` | Tries token → client credentials → OAuth in sequence |

### In Shopify app settings

Add the redirect URL (only required for OAuth mode):
```
http://127.0.0.1:8787/callback
```

---

## Running the App

### From source (no console window)

```powershell
cd app/dev
pythonw run_product_prospector.pyw
```

### From source (with console for debugging)

```powershell
cd app/dev
python desktop_app.py
```

### Packaged EXE

```powershell
.\ProductProspector.exe
```

---

## Workflow

1. **Connect** — App auto-connects to Shopify on launch using configured credentials. Status bar shows `Shopify - Connected` when valid.
2. **Upload vendor file** — Select a CSV or XLSX from your vendor.
3. **Map columns** — Confirm the auto-suggested SKU, title, description, fitment, and year columns.
4. **Choose run mode** — `Update Existing`, `Create New`, or `Upsert`.
5. **Build action plan** — Stitches rows, matches against Shopify catalog, assigns `create`/`update`/`skip`.
6. **Review & export** — Download the action plan CSV for review.
7. **Push drafts** — Optionally push new products directly to Shopify as drafts.

---

## Required Mapping Files

Place these in `app/required/mappings/`:

| File | Purpose |
|---|---|
| `VendorProfiles.csv` | Canonical vendor identity, Shopify vendor value, brand GID, discount key |
| `VendorDiscounts.csv` | Vendor pricing / discount rules |
| `vendors.csv` | Vendor alias normalization |
| `ShopifyBrandMetaobjects.csv` | Brand name → Shopify metaobject GID mapping |
| `ShopifyProductVendors.csv` | Shopify vendor list (downloaded via scripts) |
| `VendorSkuPrefixHints.csv` | Inferred SKU prefix candidates per vendor |

See `app/required/README.md` for column specifications.

---

## Product Creation Field Contract

Product creation follows a strict field contract. See [app/docs/PRODUCT_CREATION_FIELD_SPEC.md](app/docs/PRODUCT_CREATION_FIELD_SPEC.md) for the full spec.

Key fields:

| Field | Source |
|---|---|
| `title` | Vendor file, normalized |
| `price` | Vendor file or scraped |
| `cost_per_item` | Derived from vendor discount rules |
| `sku` | Vendor file |
| `weight_lb` | Vendor file, with business-rule fallbacks |
| `type` | Type mapping engine + product type dictionaries |
| `vendor` | Normalized via VendorProfiles |
| `custom.application` | Fitment text (e.g. `2017-2024 GM Duramax 6.6L`) |
| `custom.brand` | Resolved from brand metaobject mapping |
| `custom.google_product_type` | Mapped via GoogleProductTypes.csv |

---

## Building the EXE

Uses PyInstaller with the included spec file:

```powershell
cd app/dev
pyinstaller ProductProspector.spec
```

The compiled binary outputs to `dist/` and should be moved to the project root.

---

## Utility Scripts

Located in `app/dev/scripts/`:

| Script | Purpose |
|---|---|
| `download_shopify_vendors.py` | Fetches all vendor values from your live Shopify catalog |
| `download_brand_metaobjects.py` | Downloads brand metaobjects from Shopify |
| `download_vendor_sku_prefixes.py` | Analyzes catalog SKUs to infer vendor prefixes |
| `fill_vendor_profile_brands.py` | Enriches VendorProfiles with brand GIDs |

---

## Security Notes

- **Never commit `shopify.json` or `shopify_token.json`** — both are excluded by `.gitignore`
- These files contain API credentials and live access tokens
- Use `shopify.json.template` as your starting point and keep credentials local only
