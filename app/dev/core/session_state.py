from __future__ import annotations

from dataclasses import dataclass, field

import pandas as pd

from product_prospector.core.product_model import Product


MODE_NEW = "new"
MODE_UPDATE = "update"


@dataclass
class SourceMapping:
    vendor: str = ""
    title: str = ""
    description: str = ""
    media: str = ""
    price: str = ""
    msrp_price: str = ""
    map_price: str = ""
    jobber_price: str = ""
    cost: str = ""
    dealer_cost: str = ""
    core_charge_product_code: str = ""
    sku: str = ""
    barcode: str = ""
    weight: str = ""
    application: str = ""


@dataclass
class ScrapeSettings:
    vendor_search_url: str = ""
    chrome_workers: int = 3
    headless: bool = True
    delay_seconds: float = 0.35
    retry_count: int = 2
    scrape_images: bool = True
    force_scrape: bool = False


@dataclass
class AppSession:
    mode: str = ""
    vendor_df: pd.DataFrame | None = None
    source_mapping: SourceMapping = field(default_factory=SourceMapping)
    pasted_skus: list[str] = field(default_factory=list)
    search_terms_by_sku: dict[str, str] = field(default_factory=dict)
    target_product_ids: list[str] = field(default_factory=list)
    target_skus: list[str] = field(default_factory=list)
    missing_fields: list[str] = field(default_factory=list)
    update_fields: list[str] = field(default_factory=list)
    scrape_settings: ScrapeSettings = field(default_factory=ScrapeSettings)
    products: list[Product] = field(default_factory=list)
    inventory_default: int = 5000000
    setup_complete: bool = False
    processing_complete: bool = False

    def reset_for_new_run(self) -> None:
        self.vendor_df = None
        self.source_mapping = SourceMapping()
        self.pasted_skus = []
        self.search_terms_by_sku = {}
        self.target_product_ids = []
        self.target_skus = []
        self.missing_fields = []
        self.update_fields = []
        self.products = []
        self.inventory_default = 5000000
        self.setup_complete = False
        self.processing_complete = False
