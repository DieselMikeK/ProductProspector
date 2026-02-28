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
    cost: str = ""
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
    target_skus: list[str] = field(default_factory=list)
    missing_fields: list[str] = field(default_factory=list)
    update_fields: list[str] = field(default_factory=list)
    scrape_settings: ScrapeSettings = field(default_factory=ScrapeSettings)
    products: list[Product] = field(default_factory=list)
    setup_complete: bool = False
    processing_complete: bool = False

    def reset_for_new_run(self) -> None:
        self.vendor_df = None
        self.source_mapping = SourceMapping()
        self.pasted_skus = []
        self.target_skus = []
        self.missing_fields = []
        self.update_fields = []
        self.products = []
        self.setup_complete = False
        self.processing_complete = False
