# Product Creation Field Spec

This document captures the required product fields for full creation flows.

## Core Product Fields

- `title`
  - Normalized format with strict vendor-first title style.
  - User will provide exact normalization rules.
- `description`
  - Normalized format with strict composition rules.
  - User will provide exact normalization rules.
- `media`
  - Product images/URLs.
- `price`
  - Sourced from vendor sheet and/or vendor website discovery.
- `cost_per_item`
  - Derived from known cost + discount logic (PPU line card workflow).
- `inventory_available`
  - Default `3000000`.
- `sku`
- `barcode` (optional when available).
- `weight_lb`
  - Prefer source value when available.
  - Defaults/fallback constrained to business rules (typically `2` to `149`).

## Product Organization

- `type`
  - Determined via like-product matching and mapping spreadsheets.
- `vendor`
  - Normalized vendor value.
- `tags`
  - Special handling; to be defined later.

## Product Metafields

- `custom.google_product_type`
- `custom.product_subtype`
- `custom.category_codes_simplified`
- `custom.application`
  - Fitment text like `2017-2024 GM Duramax 6.6L`.
- `custom.brand`
  - Usually maps to vendor, but normalized brand list may differ.

## Variant Metafields

- `custom.enable_low_stock` = `true`
- `google.mpn` = `sku`

## Future Scope

- Variants support is intentionally deferred to a later milestone.

