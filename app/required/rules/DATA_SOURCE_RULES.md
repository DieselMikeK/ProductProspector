# Data Source Rules

## External Site Resolver Source Priority

For every vendor and wholesale-distributor resolver, use the least expensive,
most structured authorized source that reliably returns the requested fields.
The mandatory discovery and runtime order is:

1. Documented/approved API or an authenticated JSON/XHR endpoint observed in the site's normal workflow.
2. Structured page data such as JSON-LD, schema.org markup, or embedded application state.
3. A direct HTTP response parsed with a narrow, product-scoped HTML parser.
4. Browser-assisted navigation only for work that cannot be reproduced over HTTP, such as establishing an approved session or discovering a session-bound product link; return to HTTP parsing immediately afterward when possible.
5. Full Playwright page rendering and DOM extraction only as the last fallback.

Validation and safety rules:

1. Do not promote a resolver from discovery until it succeeds against a SKU known to exist and handles a known-missing SKU without returning an unrelated product.
2. Record which provider produced each result (`search_provider` and `detail_fetch_provider`) so fallbacks are auditable.
3. Never treat analytics, notification, recommendation, anti-bot, or session-maintenance endpoints as authoritative product sources.
4. Reuse only endpoints reached through the site's normal authorized workflow; do not bypass access controls.
5. On 403, 429, bot-challenge, or repeated connection-close responses, stop retrying that host, reduce concurrency, and surface a clear error. Do not create retry storms.
6. Prefer one authenticated HTTP request per product over loading a complete browser page when the initial HTML already contains the required fields.

## Price Selection Rules

Source file:
- `required/rules/pricing_priority_rules.json`

Interpretation rules:
1. `price_priority` controls which discovered value becomes Shopify `price`.
2. Current default order is:
   - `map`
   - `jobber`
   - `msrp`
   - `price`
3. `column_aliases` defines header alias matching for each role.
4. Vendor Mapping UI keeps a single `Price` mapping; MAP/Jobber/MSRP are resolved automatically from rules.

## Vendor Discount / Cost Rules

Source file location:
- `required/mappings/VendorDiscounts.csv` (current)
- `required/mappings/vendor_discounts.csv`
- `required/mappings/pricing/vendor_discounts.csv`

Interpretation rules:
1. Vendor name source is **column A**.
2. Discount percent source is **column C**.
3. Base match: normalize vendor name and find matching rows in column A.
4. If exactly one vendor row matches, use its column C discount percent.
5. If multiple rows match the vendor (example: Bosch subtypes), disambiguate using:
   - product title text
   - product type text
   - subtype clues in the vendor label from column A
6. If still ambiguous, mark row as needs-user-choice and allow manual selection later.
7. Cost formula:
   - `cost = price * (1 - discount_percent / 100)`

## DPP Product Types Rules

Source file: `required/types/DPPProductTypes.*`

Interpretation rules:
1. **Column A** maps to `custom.category_codes_simplified`.
2. **Column B** maps to `custom.product_subtype`.

## Google Product Type Rules

Source file: `required/types/GoogleProductType.*`

Interpretation rules:
1. Base tree is `Vehicle Parts & Accessories`.
2. Most products resolve at:
   - **Column D** and then **Column E**
3. Some categories require deeper drill-down to **Column F**.
4. Selected final leaf is written to `custom.google_product_type`.

## Type Mapping Explicit Hint Rules

Source file locations (first match priority):
- `required/rules/type_mapping_custom_hints.csv` (or `.xlsx`)
- `required/rules/type_mapping_hints.csv` (or `.xlsx`)

Interpretation rules:
1. Files are evaluated in the order above; first matching regex pattern wins.
2. Regex pattern source is `pattern`.
3. Category/Type destination is `category`.
4. Product subtype destination is `subtype` (`custom.product_subtype`).
5. Google destination is `google_leaf` (`custom.google_product_type`).
6. `enabled` accepts `1/0`, `true/false`, `yes/no`, `on/off`.
7. Keep long-term business-specific overrides in `type_mapping_custom_hints.*`.

## YMM Tag Rules

Source file location:
- `required/mappings/ShopifyYMMTags.csv` (or `.xlsx`)

Interpretation rules:
1. Shopify Product Organization tags are mapped to product `tags` (not metafields).
2. Only tags prefixed with `YMM:` are used for automatic tagging.
3. Fitment year ranges are matched against YMM tag year ranges; overlapping ranges are included.
4. GM fitment expands to both `GMC` and `Chevrolet` YMM tags.
5. Valve-specific tags (example: `24-Valve`) are only included when the product context explicitly includes that valve token.
6. `3.0L` tags are only included when the product context explicitly includes `3.0L`.
