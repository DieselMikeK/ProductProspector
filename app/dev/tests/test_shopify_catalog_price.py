from __future__ import annotations

import unittest
from unittest.mock import patch

from product_prospector.core.shopify_catalog import fetch_shopify_catalog_for_skus


class ShopifyCatalogPriceTests(unittest.TestCase):
    def test_targeted_variant_lookup_returns_shopify_selling_price(self) -> None:
        response = {
            "productVariants": {
                "pageInfo": {"hasNextPage": False, "endCursor": None},
                "edges": [
                    {
                        "node": {
                            "id": "gid://shopify/ProductVariant/456",
                            "sku": "AFE-24-91092",
                            "price": "99.00",
                            "barcode": "",
                            "selectedOptions": [],
                            "inventoryItem": {},
                            "product": {
                                "id": "gid://shopify/Product/123",
                                "title": "AFE Product",
                                "description": "",
                                "productType": "",
                                "vendor": "aFe Power",
                                "tags": [],
                                "collections": {"nodes": []},
                            },
                        }
                    }
                ],
            }
        }
        with patch(
            "core.shopify_catalog._request_graphql",
            return_value=(response, None),
        ):
            catalog, error = fetch_shopify_catalog_for_skus(
                config=object(),
                access_token="private-token",
                skus=["AFE-24-91092"],
            )

        self.assertIsNone(error)
        self.assertEqual(catalog.loc[0, "sku"], "AFE-24-91092")
        self.assertEqual(catalog.loc[0, "price"], "99.00")


if __name__ == "__main__":
    unittest.main()
