# Product Prospector

Product Prospector is a desktop app for product-ops workflows across inconsistent vendor spreadsheets.

The first version is focused on:

- Uploading vendor CSV/XLSX files with variable column names.
- Mapping source columns to canonical product fields.
- Stitching split/continuation rows into one record per SKU.
- Matching records against Shopify exports by SKU.
- Producing an action plan with `update`, `create`, or `skip`.
- Exporting a reviewable CSV for downstream updates.
- Exporting a create-product template CSV for new listings.

## Quick Start

1. Create and activate a virtual environment.
2. Install dependencies:

```powershell
python -m pip install -r requirements.txt
```

3. Set API credentials in `config/shopify.json`.
4. In Shopify app settings, add this redirect URL:
   - `http://127.0.0.1:8787/callback`
5. Preferred auth modes (in `config/shopify.json`):
   - `auth_mode: "client_credentials"` (same pattern as InvoiceExtractor; no redirect host setup)
   - `admin_api_access_token` (direct token)
6. Launch the app. It auto-tries token/client-credentials and shows `Shopify - Connected` when valid.
7. Optional fallback: click `Connect` to use OAuth handshake (`auth_mode: "auto"` or `"oauth"`).
   - If Shopify shows `invalid_request` host mismatch, the host in `redirect_uri` must match your app URL host in Shopify settings.
   - Example local setup: app URL `http://127.0.0.1:8787` and redirect URL `http://127.0.0.1:8787/callback`.

8. Run the desktop app from source:

```bash
python run_product_prospector.pyw
```

9. Or run a packaged build directly:

```powershell
.\ProductProspector.exe
```

```bash
open dist/ProductProspector.app
```

## Build on Windows

```powershell
.\build_windows_exe.ps1
```

## Build on macOS

```bash
./build_mac_app.sh
```

## v1 Workflow

1. Upload vendor file.
2. Select vendor SKU column.
3. Confirm auto-mapped fitment/year/title/description columns.
4. Upload Shopify export and map Shopify SKU/fitment columns.
5. Build action plan and download CSV.

## Current Scope

- Native desktop UI supports `Update Existing`, `Create New`, and `Upsert`.
- Shopify config and OAuth handshake are supported from `config/shopify.json`.
- Access token is saved to `config/shopify_token.json` after successful handshake.
- Create mode includes a template export with required product fields scaffolded.
- Product write/update flows are next.
- The output CSV is designed for review and safe staged execution.

## Product Creation Rules

- Product creation field contract is documented in:
  - `docs/PRODUCT_CREATION_FIELD_SPEC.md`
- Rule template for normalization/defaults/metafield keys:
  - `config/product_creation_rules.template.json`

## Why This Structure

Vendor files vary heavily in column naming and shape. This app normalizes inputs into a canonical schema, then applies run-mode rules and field-scope rules consistently, regardless of vendor format.
