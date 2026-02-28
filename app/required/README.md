# Required Files Folder

Place operational reference files here for the app to use.

## Subfolders

- `rules/`
  - Normalization rules, title/description logic, business defaults.
- `types/`
  - Product type dictionaries, subtype lists, category code references.
- `mappings/`
  - Vendor-specific and spreadsheet column mapping files.
  - Put vendor discount files in `mappings/` (or `mappings/pricing/`)
    - Supported names include `VendorDiscounts.csv` and `vendor_discounts.csv`
  - Vendor normalization aliases file
    - Supported names include `vendors.csv`, `Vendors.csv`, `vendors.xlsx`
    - Recommended columns:
      - `vendor` (normalized/canonical vendor name used by app)
      - `aliases` (optional alternate names separated by `|` or `,`)
  - Shopify brand metaobject lookup file (optional, used for `custom.brand` metafield push)
    - Supported names include `ShopifyBrandMetaobjects.csv` and `brand_metaobjects.csv`
    - Recommended columns:
      - `brand_name`
      - `brand_gid` (or `gid`)
      - `brand_handle` (optional)
      - `aliases` (optional alternate names separated by `|` or `,`)
  - Vendor profile resolver file (recommended shared canonical mapping)
    - Supported names include `VendorProfiles.csv` and `vendor_profiles.csv`
    - Suggested columns:
      - `canonical_vendor` (single internal identity)
      - `aliases` (all accepted names separated by `|` or `,`)
      - `shopify_vendor_value` (exact value for Shopify Product Organization Vendor field)
      - `brand_name` and/or `brand_gid` (for `custom.brand` metafield)
      - `discount_vendor_key` (exact vendor key used for VendorDiscounts matching)
      - `title_prefix` and `sku_prefix` (reserved for title/SKU formatting rules)
  - Optional SKU prefix hint output from catalog analysis
    - `VendorSkuPrefixHints.csv`
    - Contains inferred prefix candidates and sample SKUs per Shopify vendor

## Included Starter File

- `rules/product_creation_rules.template.json`
- `rules/DATA_SOURCE_RULES.md`
