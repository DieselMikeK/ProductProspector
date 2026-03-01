# Data Source Rules

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
