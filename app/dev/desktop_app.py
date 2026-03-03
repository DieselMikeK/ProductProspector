
from __future__ import annotations

import csv
import ctypes
from datetime import datetime, timezone
import queue
from pathlib import Path
import re
import sys
import threading
import time
import tkinter as tk
from tkinter import BOTH, BOTTOM, END, LEFT, RIGHT, VERTICAL, W, X, Y, BooleanVar, Canvas, StringVar, Tk, filedialog, messagebox, ttk

import pandas as pd

from product_prospector.core.config_store import (
    AppSettings,
    load_app_settings,
    load_shopify_config,
    load_shopify_token,
    save_app_settings,
    save_shopify_token,
)
from product_prospector.core.io_utils import read_table_from_path
from product_prospector.core.mapping import suggest_column_for_field
from product_prospector.core.normalization import normalize_product
from product_prospector.core.pricing_rules import (
    DiscountMatch,
    calculate_cost_from_price,
    find_vendor_discount_file,
    load_vendor_discounts,
    resolve_discount_candidates,
)
from product_prospector.core.product_model import PRODUCT_EXPORT_COLUMNS
from product_prospector.core.processing import (
    PlanningConfig,
    RUN_MODE_CREATE,
    RUN_MODE_UPDATE,
    RUN_MODE_UPSERT,
    build_action_plan,
    normalize_sku,
    stitch_rows_by_sku,
)
from product_prospector.core.create_product_output import build_create_product_output
from product_prospector.core.session_state import AppSession, MODE_NEW, MODE_UPDATE
from product_prospector.core.shopify_catalog import fetch_shopify_catalog_dataframe
from product_prospector.core.shopify_catalog import fetch_shopify_catalog_for_skus
from product_prospector.core.shopify_oauth import exchange_client_credentials_for_token, perform_oauth_handshake, validate_access_token
from product_prospector.core.shopify_push import ShopifyDraftPushSummary, push_new_products_as_drafts
from product_prospector.core.shopify_sku_cache import get_shopify_sku_cache_path, load_shopify_sku_cache, save_shopify_sku_cache
from product_prospector.core.type_mapping_engine import TypeCategoryMapper
from product_prospector.core.vendor_profiles import resolve_vendor_profile
from product_prospector.core.vendor_normalization import normalize_vendor_name as normalize_vendor_from_rules
from product_prospector.core.workflow_build import (
    build_products_from_session,
    build_existing_shopify_index,
    collect_session_skus,
    detect_missing_required_fields,
    merge_mode_label,
    products_to_dataframe,
)
from product_prospector.core.scraper_engine import scrape_vendor_records


APP_TITLE = "Product Prospector"
APP_GEOMETRY = "1440x920"
APP_WINDOW_MARGIN_PX = 64
APP_MIN_WINDOW_WIDTH = 1080
APP_MIN_WINDOW_HEIGHT = 680
HEADER_LOGO_VERTICAL_CROP_TOP_PX = 80
HEADER_LOGO_VERTICAL_CROP_BOTTOM_PX = 100
HEADER_LOGO_VERTICAL_TOP_PADDING_PX = 50
HEADER_LOGO_VERTICAL_BOTTOM_PADDING_PX = 25
_SINGLE_INSTANCE_MUTEX = "Global\\ProductProspectorDesktopApp"
_ERROR_ALREADY_EXISTS = 183
INVENTORY_OWNER_VALUES = ["Andrew", "Alondra", "Mike K", "Michael V"]
INVENTORY_BY_OWNER = {
    "Andrew": 1_000_000,
    "Alondra": 2_000_000,
    "Mike K": 3_000_000,
    "Michael V": 4_000_000,
}
DEFAULT_INVENTORY_OWNER = "Mike K"


def _inventory_for_owner(owner_name: str) -> int:
    owner = str(owner_name or "").strip()
    return int(INVENTORY_BY_OWNER.get(owner, INVENTORY_BY_OWNER[DEFAULT_INVENTORY_OWNER]))


def _resolve_runtime_root() -> Path:
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent
    return Path(__file__).resolve().parent.parent


def _resolve_runtime_data_root(runtime_root: Path) -> Path:
    candidates: list[Path] = []

    meipass = getattr(sys, "_MEIPASS", "")
    if meipass:
        candidates.append(Path(meipass) / "app")

    if sys.platform == "darwin":
        candidates.append(runtime_root.parent / "Resources" / "app")

    candidates.append(runtime_root / "app")
    candidates.append(runtime_root)

    for app_dir in candidates:
        if app_dir.exists():
            return app_dir
    return runtime_root


def _resolve_runtime_output_root(runtime_root: Path) -> Path:
    if getattr(sys, "frozen", False):
        # On macOS app bundles, write outputs next to the .app bundle.
        if sys.platform == "darwin":
            try:
                app_bundle = runtime_root.parent.parent
                host_dir = app_bundle.parent
                if host_dir.exists():
                    return host_dir
            except Exception:
                return runtime_root
        return runtime_root

    # In dev, runtime_root is "<repo>/app"; write outputs at repo root.
    try:
        repo_root = runtime_root.parent
        if repo_root.exists():
            return repo_root
    except Exception:
        pass
    return runtime_root


def _safe_head(df: pd.DataFrame | None, rows: int = 30) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    return df.head(rows).fillna("")


def _combobox_set_values(widget: ttk.Combobox, values: list[str]) -> None:
    widget["values"] = values
    current = widget.get()
    if current not in values:
        widget.set("")


def _tree_show_dataframe(
    tree: ttk.Treeview,
    df: pd.DataFrame,
    max_rows: int = 40,
    max_cols: int | None = None,
    max_cell_chars: int = 260,
) -> None:
    tree.delete(*tree.get_children())
    if df.empty:
        tree["columns"] = ()
        return

    all_columns = list(df.columns)
    if max_cols is not None and max_cols > 0:
        columns = all_columns[:max_cols]
    else:
        columns = all_columns
    subset = df.loc[:, columns].head(max_rows).astype(str)

    tree["columns"] = columns
    tree["show"] = "headings"
    for column in columns:
        tree.heading(column, text=column)
        tree.column(column, width=180, minwidth=80, anchor=W, stretch=False)

    def _display_cell(value: object) -> str:
        text = str(value)
        if len(text) <= max_cell_chars:
            return text
        return text[: max_cell_chars - 3] + "..."

    for _, row in subset.iterrows():
        tree.insert("", END, values=[_display_cell(row[col]) for col in columns])


def _sanitize_dataframe_columns(df: pd.DataFrame) -> pd.DataFrame:
    if df is None:
        return pd.DataFrame()
    out = df.copy()
    used: set[str] = set()
    normalized: list[str] = []
    for index, raw in enumerate(out.columns, start=1):
        column = str(raw).strip()
        if not column or column.lower() == "nan":
            column = f"column_{index}"
        base = column
        suffix = 2
        while column in used:
            column = f"{base}_{suffix}"
            suffix += 1
        used.add(column)
        normalized.append(column)
    out.columns = normalized
    return out


def _acquire_single_instance_mutex() -> int | None:
    if sys.platform != "win32":
        return -1
    kernel32 = ctypes.windll.kernel32
    kernel32.SetLastError(0)
    handle = kernel32.CreateMutexW(None, False, _SINGLE_INSTANCE_MUTEX)
    if not handle:
        return -1
    if kernel32.GetLastError() == _ERROR_ALREADY_EXISTS:
        kernel32.CloseHandle(handle)
        return None
    return int(handle)


def _release_single_instance_mutex(handle: int | None) -> None:
    if handle is None or handle == -1 or sys.platform != "win32":
        return
    ctypes.windll.kernel32.CloseHandle(ctypes.c_void_p(handle))


def _show_already_running_message() -> None:
    message = "Product Prospector is already open. Close the existing window first."
    if sys.platform == "win32":
        ctypes.windll.user32.MessageBoxW(None, message, APP_TITLE, 0x10)
        return
    print(message)


class ProductProspectorDesktopApp:
    def __init__(self) -> None:
        self.root = Tk()
        self.root.title(APP_TITLE)
        self.root.geometry(APP_GEOMETRY)
        self._fit_window_to_screen()

        self.vendor_df_raw: pd.DataFrame | None = None
        self.shopify_df_raw: pd.DataFrame | None = None
        self.vendor_df_stitched: pd.DataFrame | None = None
        self.plan_df: pd.DataFrame | None = None
        self.vendor_source_is_sheet = False
        self.runtime_root = _resolve_runtime_root()
        self.runtime_data_root = _resolve_runtime_data_root(self.runtime_root)
        self.runtime_output_root = _resolve_runtime_output_root(self.runtime_root)
        self._window_icon_image: tk.PhotoImage | None = None
        self._header_logo_image: tk.PhotoImage | None = None
        self._header_logo_label: ttk.Label | None = None
        self._header_logo_target_width = 550
        self._header_logo_target_height: int | None = None
        self._header_logo_frame_paths: list[Path] = []
        self._header_logo_frames: list[tk.PhotoImage] = []
        self._header_logo_frame_index = 0
        self._header_logo_anim_job: str | None = None
        self._header_logo_anim_delay_ms = 35
        self._header_logo_anim_mode = "idle"
        self._header_logo_intro_start_frame = 0
        self._header_logo_intro_end_frame = 100
        self._header_logo_loop_start_frame = 36
        self._header_logo_loop_end_frame = 100
        self._header_logo_finish_end_frame = 144
        self._header_logo_segment_start = 0
        self._header_logo_segment_end = 0
        self._header_logo_finish_requested = False
        self._set_window_icon()
        self.required_root = self.runtime_data_root / "required"
        if not self.required_root.exists():
            self.required_root = Path(__file__).resolve().parent / "required"
        self.type_mapper: TypeCategoryMapper | None = None
        self.session = AppSession()
        self.review_index = 0

        self.vendor_path = StringVar(value="")
        self.mode_help_text = StringVar(value="")
        self.sku_scope_help_text = StringVar(value="Enter SKUs that need to be updated or added.")
        self.input_metrics_text = StringVar(value="Vendor SKUs: 0 | Shopify Catalog SKUs: 0")
        self.sku_text_status = StringVar(value="")
        self.source_status_text = StringVar(value="")
        self.duplicate_check_text = StringVar(value="")
        self.setup_status_text = StringVar(value="Select a Run Mode to begin.")
        self.processing_status_text = StringVar(value="")
        self.review_status_text = StringVar(value="")

        self.shopify_connected = False
        self.shopify_connecting = False
        self.shopify_ever_connected = bool(load_shopify_token() is not None)
        self.shopify_cache_ready = False
        self.shopify_cache_warmup_inflight = False
        self.shopify_cache_spinner_job: str | None = None
        self.shopify_cache_spinner_angle = 0
        self._background_connect_running = False
        self._shutdown_requested = False
        self.setup_widgets_enabled = False
        self.processing_inflight = False
        self._processing_request_id = 0
        self._auto_open_review_after_processing = False

        self.run_mode = StringVar(value="")
        self.run_mode_locked = BooleanVar(value=False)
        self.run_mode_summary_text = StringVar(value="")
        self.use_all_sheet_skus = BooleanVar(value=False)
        self.inventory_owner = StringVar(value=DEFAULT_INVENTORY_OWNER)
        self.inventory_owner_inventory_text = StringVar(value=f"Inventory default: {_inventory_for_owner(DEFAULT_INVENTORY_OWNER):,}")

        self.year_policy = StringVar(value="merge")
        self.carry_down_sku = BooleanVar(value=True)
        self.propose_title_year_update = BooleanVar(value=True)
        self.only_rows_with_year_changes = BooleanVar(value=True)

        self.vendor_sku_column = StringVar(value="")
        self.vendor_title_column = StringVar(value="")
        self.vendor_description_column = StringVar(value="")
        self.vendor_fitment_column = StringVar(value="")
        self.vendor_image_column = StringVar(value="")
        self.vendor_price_column = StringVar(value="")
        self.vendor_cost_column = StringVar(value="")
        self.vendor_core_charge_column = StringVar(value="")
        self.vendor_barcode_column = StringVar(value="")
        self.vendor_weight_column = StringVar(value="")
        self.vendor_vendor_column = StringVar(value="")
        self._vendor_mapping_trace_ready = False
        self._vendor_mapping_enforce_inflight = False
        self._vendor_mapping_enforcement_suspended = 0

        self.shopify_sku_column = StringVar(value="sku")
        self.shopify_title_column = StringVar(value="title")
        self.shopify_description_column = StringVar(value="description")
        self.shopify_fitment_column = StringVar(value="fitment")

        self.update_price = BooleanVar(value=False)
        self.update_cost = BooleanVar(value=False)
        self.update_title = BooleanVar(value=False)
        self.update_description = BooleanVar(value=False)
        self.update_images = BooleanVar(value=False)
        self.update_category_fields = BooleanVar(value=False)
        self.update_vendor = BooleanVar(value=False)
        self.update_weight = BooleanVar(value=False)
        self.update_barcode = BooleanVar(value=False)
        self.update_application = BooleanVar(value=False)

        self.scrape_search_url = StringVar(value="")
        self.scrape_workers = StringVar(value="3")
        self.scrape_delay = StringVar(value="0.35")
        self.scrape_retries = StringVar(value="2")
        self.scrape_headless = BooleanVar(value=True)
        self.scrape_images = BooleanVar(value=True)
        self.scrape_force = BooleanVar(value=False)

        self.review_fields: dict[str, StringVar] = {
            "title": StringVar(value=""),
            "description_html": StringVar(value=""),
            "media_urls": StringVar(value=""),
            "price": StringVar(value=""),
            "map_price": StringVar(value=""),
            "msrp_price": StringVar(value=""),
            "jobber_price": StringVar(value=""),
            "cost": StringVar(value=""),
            "dealer_cost": StringVar(value=""),
            "inventory": StringVar(value=str(_inventory_for_owner(DEFAULT_INVENTORY_OWNER))),
            "sku": StringVar(value=""),
            "barcode": StringVar(value=""),
            "weight": StringVar(value=""),
            "vendor": StringVar(value=""),
            "type": StringVar(value=""),
            "google_product_type": StringVar(value=""),
            "category_code": StringVar(value=""),
            "product_subtype": StringVar(value=""),
            "mpn": StringVar(value=""),
            "brand": StringVar(value=""),
            "application": StringVar(value=""),
            "core_charge_product_code": StringVar(value=""),
        }
        self.review_index_text = StringVar(value="Product 0 / 0")
        self.review_cost_rule_text = StringVar(value="")
        self.review_cost_options: list[DiscountMatch] = []
        self.review_cost_option_map: dict[str, DiscountMatch] = {}
        self.vendor_discounts_df: pd.DataFrame | None = None
        self.review_refresh_pending = False
        self.review_refresh_inflight = False
        self.review_tab_unlocked = False
        self.review_table_refresh_job: str | None = None
        self.review_loaded_raw: dict[str, str] = {}
        self.review_loaded_display: dict[str, str] = {}
        self.review_loaded_truncated: dict[str, bool] = {}
        self.review_cost_options_loaded_for_sku: str = ""
        self.review_busy_spinner_job: str | None = None
        self.review_busy_spinner_angle = 0
        self.review_busy_active = False
        self.create_existing_skus: set[str] = set()
        self.create_duplicate_scope: tuple[str, ...] = ()
        self._duplicate_check_request_id = 0
        self._duplicate_check_inflight = False
        self._duplicate_check_active_workers = 0
        self._duplicate_check_pending_scope: tuple[str, ...] = ()
        self._duplicate_check_started_at = 0.0
        self.shopify_push_inflight = False
        self.push_selected_skus: set[str] = set()

        self._mode_initialized = False
        self._ui_task_queue: queue.Queue[tuple[object, tuple[object, ...], dict[str, object]]] = queue.Queue()
        self._ui_task_pump_job: str | None = None
        self._mousewheel_bindings_ready = False

        self.run_mode.trace_add("write", self._on_run_mode_changed)
        self.inventory_owner.trace_add("write", self._on_inventory_owner_changed)
        self.session.inventory_default = _inventory_for_owner(self.inventory_owner.get())

        self._create_layout()
        self._initialize_shopify_cache_state()
        self._load_settings()
        self._on_run_mode_changed()
        self._refresh_vendor_sheet_ui()
        self._refresh_input_metrics()
        self._mode_initialized = True
        self._update_tab_access()
        self._schedule_ui_task_pump()
        self._start_background_api_bootstrap()
        self.root.protocol("WM_DELETE_WINDOW", self._on_close)

    def _create_layout(self) -> None:
        root_frame = ttk.Frame(self.root, padding=12)
        root_frame.pack(fill=BOTH, expand=True)

        header_frame = ttk.Frame(root_frame)
        header_frame.pack(fill=X, pady=(0, 6))
        self._header_logo_label = ttk.Label(header_frame)
        self._header_logo_label.pack(
            anchor="center",
            pady=(HEADER_LOGO_VERTICAL_TOP_PADDING_PX, HEADER_LOGO_VERTICAL_BOTTOM_PADDING_PX),
        )
        self._load_initial_header_logo()

        api_frame = ttk.Frame(root_frame)
        api_frame.pack(anchor="center", pady=(0, 8))
        ttk.Label(api_frame, text="API Connections:", font=("Segoe UI", 10, "bold")).pack(side=LEFT, padx=(0, 10))
        self.shopify_dot = Canvas(api_frame, width=16, height=16, highlightthickness=0, bd=0)
        self.shopify_dot.pack(side=LEFT)
        self.shopify_status_label = ttk.Label(api_frame, text="Shopify - Not Connected")
        self.shopify_status_label.pack(side=LEFT, padx=(6, 0))
        self.shopify_cache_api_text = StringVar(value="")
        self.shopify_cache_api_spinner = Canvas(api_frame, width=16, height=16, highlightthickness=0, bd=0)
        self.shopify_cache_api_label = ttk.Label(api_frame, textvariable=self.shopify_cache_api_text, foreground="#1f4e79")
        self.shopify_connect_button = ttk.Button(api_frame, text="Connect", command=self._connect_shopify_clicked)
        self.shopify_connect_button.pack(side=LEFT, padx=(12, 0))
        self.shopify_cache_newest_button = ttk.Button(
            api_frame,
            text="Download Newest SKUs",
            command=self._download_newest_shopify_skus_clicked,
        )
        self.shopify_cache_newest_button.pack(side=LEFT, padx=(8, 0))
        self.shopify_cache_redownload_button = ttk.Button(
            api_frame,
            text="Redownload All SKUs",
            command=self._redownload_shopify_sku_cache_clicked,
        )
        self.shopify_cache_redownload_button.pack(side=LEFT, padx=(8, 0))
        self._draw_shopify_dot(state="disconnected")
        self._refresh_shopify_cache_action_buttons()

        self.notebook = ttk.Notebook(root_frame)
        self.notebook.pack(fill=BOTH, expand=True)

        self.tab_setup = ttk.Frame(self.notebook)
        self.tab_preview = ttk.Frame(self.notebook)
        self.tab_export = ttk.Frame(self.notebook)

        self.notebook.add(self.tab_setup, text="1) Setup")
        self.notebook.add(self.tab_preview, text="2) Processing")
        self.notebook.add(self.tab_export, text="3) Review & Export")
        self.notebook.bind("<<NotebookTabChanged>>", self._on_notebook_tab_changed, add="+")

        self._build_setup_tab()
        self._build_preview_tab()
        self._build_export_tab()
        self._bind_tab_canvas_mousewheel()

    def _fit_window_to_screen(self) -> None:
        match = re.match(r"^\s*(\d+)x(\d+)", APP_GEOMETRY or "")
        default_width = int(match.group(1)) if match else 1440
        default_height = int(match.group(2)) if match else 920
        try:
            screen_width = max(640, int(self.root.winfo_screenwidth() or default_width))
            screen_height = max(480, int(self.root.winfo_screenheight() or default_height))
        except Exception:
            return

        max_width = max(640, screen_width - APP_WINDOW_MARGIN_PX)
        max_height = max(480, screen_height - APP_WINDOW_MARGIN_PX)
        fitted_width = min(default_width, max_width)
        fitted_height = min(default_height, max_height)
        self.root.geometry(f"{fitted_width}x{fitted_height}")
        self.root.minsize(
            min(fitted_width, APP_MIN_WINDOW_WIDTH),
            min(fitted_height, APP_MIN_WINDOW_HEIGHT),
        )

    def _bind_tab_canvas_mousewheel(self) -> None:
        if self._mousewheel_bindings_ready:
            return
        self._mousewheel_bindings_ready = True
        self.root.bind_all("<MouseWheel>", self._on_tab_canvas_mousewheel, add="+")
        # Linux wheel events.
        self.root.bind_all("<Button-4>", self._on_tab_canvas_mousewheel, add="+")
        self.root.bind_all("<Button-5>", self._on_tab_canvas_mousewheel, add="+")

    def _active_tab_canvas(self) -> Canvas | None:
        try:
            current = self.notebook.select()
        except Exception:
            return None
        if current == str(self.tab_setup) and hasattr(self, "setup_canvas"):
            return self.setup_canvas
        if current == str(self.tab_preview) and hasattr(self, "preview_canvas"):
            return self.preview_canvas
        if current == str(self.tab_export) and hasattr(self, "export_canvas"):
            return self.export_canvas
        return None

    def _on_tab_canvas_mousewheel(self, event) -> str | None:
        canvas = self._active_tab_canvas()
        if canvas is None:
            return None

        delta_steps = 0
        raw_delta = int(getattr(event, "delta", 0) or 0)
        if raw_delta:
            delta_steps = -int(raw_delta / 120)
            if delta_steps == 0:
                delta_steps = -1 if raw_delta > 0 else 1
        else:
            event_num = int(getattr(event, "num", 0) or 0)
            if event_num == 4:
                delta_steps = -1
            elif event_num == 5:
                delta_steps = 1

        if delta_steps == 0:
            return None
        try:
            canvas.yview_scroll(delta_steps, "units")
            return "break"
        except Exception:
            return None

    def _schedule_ui_task_pump(self) -> None:
        if self._ui_task_pump_job is not None:
            return
        try:
            self._ui_task_pump_job = self.root.after(20, self._drain_ui_task_queue)
        except RuntimeError:
            self._ui_task_pump_job = None

    def _drain_ui_task_queue(self) -> None:
        self._ui_task_pump_job = None
        max_tasks = 240
        for _ in range(max_tasks):
            try:
                callback, args, kwargs = self._ui_task_queue.get_nowait()
            except queue.Empty:
                break
            try:
                if callable(callback):
                    callback(*args, **kwargs)
            except Exception:
                continue
        try:
            if self.root.winfo_exists():
                self._ui_task_pump_job = self.root.after(20, self._drain_ui_task_queue)
        except RuntimeError:
            self._ui_task_pump_job = None

    def _run_on_ui_thread(self, callback, *args, **kwargs) -> None:
        if threading.current_thread() is threading.main_thread():
            try:
                callback(*args, **kwargs)
            except Exception:
                pass
            return
        try:
            self._ui_task_queue.put((callback, args, kwargs))
        except Exception:
            return

    def _header_logo_sort_key(self, path: Path) -> tuple[int, int, str]:
        stem = path.stem.lower()
        match = re.search(r"(\d+)$", stem)
        if match:
            return (0, int(match.group(1)), path.name.lower())
        return (1, 0, path.name.lower())

    def _discover_header_logo_frames(self) -> list[Path]:
        video_dir = self.runtime_data_root / "video"
        if not video_dir.exists() or not video_dir.is_dir():
            return []
        allowed = {".png", ".jpg", ".jpeg", ".webp", ".gif"}
        frames = [path for path in video_dir.iterdir() if path.is_file() and path.suffix.lower() in allowed]
        return sorted(frames, key=self._header_logo_sort_key)

    def _estimate_logo_render_height(self, image_path: Path, target_width: int) -> int:
        if not image_path.exists():
            return 0
        try:
            from PIL import Image

            with Image.open(image_path) as src:
                src = self._crop_logo_vertical_padding(src)
                src_w, src_h = src.size
                safe_w = max(1, int(src_w))
                safe_h = max(1, int(src_h))
                resized_w = max(1, int(target_width))
                resized_h = max(1, int(round((safe_h * resized_w) / safe_w)))
                return resized_h
        except Exception:
            return 0

    def _prepare_header_logo_target_height(self) -> None:
        if self._header_logo_target_height and self._header_logo_target_height > 0:
            return

        candidates: list[Path] = list(self._header_logo_frame_paths)
        logo_path = self.runtime_data_root / "logo.png"
        if logo_path.exists():
            candidates.append(logo_path)

        heights = [
            self._estimate_logo_render_height(path, self._header_logo_target_width)
            for path in candidates
        ]
        positive_heights = [height for height in heights if int(height) > 0]
        if positive_heights:
            self._header_logo_target_height = max(positive_heights)

    def _normalize_logo_frame_size(self, image):
        target_height = max(0, int(self._header_logo_target_height or 0))
        width, height = image.size
        if width <= 0 or height <= 0:
            return image
        if target_height <= 0:
            self._header_logo_target_height = int(height)
            return image
        if height == target_height:
            return image

        try:
            from PIL import Image
        except Exception:
            return image

        source = image.convert("RGBA")
        if height > target_height:
            # Keep vertical anchoring stable so animation frames do not "bounce".
            return source.crop((0, 0, width, target_height))

        canvas = Image.new("RGBA", (width, target_height), (0, 0, 0, 0))
        y_offset = 0
        canvas.paste(source, (0, y_offset), source)
        return canvas

    def _load_logo_image(self, image_path: Path, target_width: int) -> tk.PhotoImage | None:
        if not image_path.exists():
            return None
        try:
            from PIL import Image, ImageTk

            with Image.open(image_path) as src:
                src = self._crop_logo_vertical_padding(src)
                src_w, src_h = src.size
                safe_w = max(1, int(src_w))
                safe_h = max(1, int(src_h))
                resized_w = max(1, int(target_width))
                resized_h = max(1, int(round((safe_h * resized_w) / safe_w)))
                resized = src.resize((resized_w, resized_h), Image.Resampling.LANCZOS)
                resized = self._normalize_logo_frame_size(resized)
                return ImageTk.PhotoImage(resized)
        except Exception:
            try:
                fallback = tk.PhotoImage(file=str(image_path))
            except Exception:
                return None
            width = max(1, int(fallback.width()))
            height = max(1, int(fallback.height()))
            if width > target_width:
                scale_down = max(1, int(round(width / target_width)))
                fallback = fallback.subsample(scale_down, scale_down)
                height = max(1, int(fallback.height()))
            elif width < target_width:
                scale_up = max(1, int(round(target_width / width)))
                fallback = fallback.zoom(scale_up, scale_up)
                height = max(1, int(fallback.height()))
            if not self._header_logo_target_height:
                self._header_logo_target_height = int(height)
            return fallback

    def _crop_logo_vertical_padding(self, image):
        crop_top_target = max(0, int(HEADER_LOGO_VERTICAL_CROP_TOP_PX))
        crop_bottom_target = max(0, int(HEADER_LOGO_VERTICAL_CROP_BOTTOM_PX))
        if crop_top_target <= 0 and crop_bottom_target <= 0:
            return image
        try:
            width, height = image.size
            if height <= 1:
                return image
            # Apply a consistent crop per frame so frame-to-frame alpha differences
            # cannot shift the rendered logo position.
            crop_top = min(crop_top_target, max(0, int((height - 1) / 2)))
            crop_bottom = min(crop_bottom_target, max(0, height - crop_top - 1))
            if crop_top <= 0 and crop_bottom <= 0:
                return image

            new_top = crop_top
            new_bottom = max(new_top + 1, height - crop_bottom)
            if new_bottom <= new_top:
                return image
            return image.crop((0, new_top, width, new_bottom))
        except Exception:
            return image

    def _apply_header_logo_image(self, image: tk.PhotoImage | None) -> None:
        self._header_logo_image = image
        if self._header_logo_label is None:
            return
        if image is None:
            self._header_logo_label.configure(text="Product Prospector", image="", font=("Segoe UI", 19, "bold"))
            return
        self._header_logo_label.configure(image=image, text="", font="")

    def _load_initial_header_logo(self) -> None:
        self._header_logo_frame_paths = self._discover_header_logo_frames()
        self._prepare_header_logo_target_height()
        first_frame = self._header_logo_frame_paths[0] if self._header_logo_frame_paths else None
        if first_frame is not None:
            frame_image = self._load_logo_image(first_frame, self._header_logo_target_width)
            if frame_image is not None:
                self._apply_header_logo_image(frame_image)
                return

        logo_path = self.runtime_data_root / "logo.png"
        self._apply_header_logo_image(self._load_logo_image(logo_path, self._header_logo_target_width))

    def _load_header_logo_animation_frames(self) -> bool:
        if self._header_logo_frames:
            return True
        if not self._header_logo_frame_paths:
            self._header_logo_frame_paths = self._discover_header_logo_frames()
        self._prepare_header_logo_target_height()
        loaded: list[tk.PhotoImage] = []
        for frame_path in self._header_logo_frame_paths:
            frame_image = self._load_logo_image(frame_path, self._header_logo_target_width)
            if frame_image is not None:
                loaded.append(frame_image)
        self._header_logo_frames = loaded
        return bool(self._header_logo_frames)

    def _cancel_header_logo_animation(self) -> None:
        if self._header_logo_anim_job is None:
            self._header_logo_anim_mode = "idle"
            self._header_logo_finish_requested = False
            return
        try:
            self.root.after_cancel(self._header_logo_anim_job)
        except Exception:
            pass
        self._header_logo_anim_job = None
        self._header_logo_anim_mode = "idle"
        self._header_logo_finish_requested = False

    def _resolve_header_logo_segment(self, start_frame: int, end_frame: int | None = None) -> tuple[int, int]:
        total = len(self._header_logo_frames)
        if total <= 0:
            return (0, 0)
        last_index = total - 1
        safe_start = max(0, min(int(start_frame), last_index))
        if end_frame is None:
            safe_end = last_index
        else:
            safe_end = max(0, min(int(end_frame), last_index))
        if safe_end < safe_start:
            safe_start = 0
            safe_end = last_index
        return (safe_start, safe_end)

    def _start_header_logo_intro_segment(self) -> None:
        segment_start, segment_end = self._resolve_header_logo_segment(
            self._header_logo_intro_start_frame,
            self._header_logo_intro_end_frame,
        )
        self._header_logo_anim_mode = "intro"
        self._header_logo_segment_start = segment_start
        self._header_logo_segment_end = segment_end
        self._header_logo_frame_index = segment_start

    def _start_header_logo_loop_segment(self) -> None:
        segment_start, segment_end = self._resolve_header_logo_segment(
            self._header_logo_loop_start_frame,
            self._header_logo_loop_end_frame,
        )
        self._header_logo_anim_mode = "loop"
        self._header_logo_segment_start = segment_start
        self._header_logo_segment_end = segment_end
        self._header_logo_frame_index = segment_start

    def _start_header_logo_finish_segment(self, from_frame: int) -> None:
        segment_start, segment_end = self._resolve_header_logo_segment(
            self._header_logo_loop_end_frame,
            self._header_logo_finish_end_frame,
        )
        self._header_logo_anim_mode = "finish"
        self._header_logo_segment_start = segment_start
        self._header_logo_segment_end = segment_end
        next_frame = max(segment_start, int(from_frame) + 1)
        self._header_logo_frame_index = min(segment_end, next_frame)

    def _play_header_logo_animation(self) -> None:
        if not self._load_header_logo_animation_frames():
            return
        self._cancel_header_logo_animation()
        self._header_logo_finish_requested = False
        self._start_header_logo_intro_segment()
        self._advance_header_logo_animation()

    def _finish_header_logo_animation(self) -> None:
        if self._header_logo_anim_mode not in {"intro", "loop", "finish"}:
            return
        if not self._header_logo_frames:
            self._header_logo_anim_mode = "idle"
            return
        self._header_logo_finish_requested = True
        if self._header_logo_anim_mode == "finish":
            return
        if self._header_logo_anim_job is None:
            self._advance_header_logo_animation()

    def _advance_header_logo_animation(self) -> None:
        if not self._header_logo_frames:
            self._header_logo_anim_job = None
            self._header_logo_anim_mode = "idle"
            self._header_logo_finish_requested = False
            return
        last_index = len(self._header_logo_frames) - 1
        segment_start = max(0, min(self._header_logo_segment_start, last_index))
        segment_end = max(segment_start, min(self._header_logo_segment_end, last_index))
        if self._header_logo_anim_mode not in {"intro", "loop", "finish"}:
            segment_start, segment_end = self._resolve_header_logo_segment(0, last_index)
            self._header_logo_anim_mode = "finish"
            self._header_logo_segment_start = segment_start
            self._header_logo_segment_end = segment_end
        index = max(segment_start, min(self._header_logo_frame_index, segment_end))
        self._apply_header_logo_image(self._header_logo_frames[index])

        if self._header_logo_anim_mode == "intro":
            if index >= segment_end:
                if self._header_logo_finish_requested:
                    self._start_header_logo_finish_segment(index)
                else:
                    self._start_header_logo_loop_segment()
            else:
                self._header_logo_frame_index = index + 1
            self._header_logo_anim_job = self.root.after(self._header_logo_anim_delay_ms, self._advance_header_logo_animation)
            return

        if self._header_logo_anim_mode == "loop":
            if index >= segment_end:
                if self._header_logo_finish_requested:
                    self._start_header_logo_finish_segment(index)
                else:
                    self._header_logo_frame_index = segment_start
            else:
                self._header_logo_frame_index = index + 1
            self._header_logo_anim_job = self.root.after(self._header_logo_anim_delay_ms, self._advance_header_logo_animation)
            return

        if index >= segment_end:
            self._header_logo_anim_job = None
            self._header_logo_anim_mode = "idle"
            self._header_logo_finish_requested = False
            return
        self._header_logo_frame_index = index + 1
        self._header_logo_anim_job = self.root.after(self._header_logo_anim_delay_ms, self._advance_header_logo_animation)

    def _set_window_icon(self) -> None:
        icon_path = self.runtime_data_root / "icon.ico"
        if icon_path.exists() and sys.platform == "win32":
            try:
                self.root.iconbitmap(default=str(icon_path))
                return
            except Exception:
                pass

        # Tk on macOS/Linux can reliably consume PNGs via iconphoto.
        logo_path = self.runtime_data_root / "logo.png"
        if not logo_path.exists():
            return
        try:
            self._window_icon_image = tk.PhotoImage(file=str(logo_path))
            self.root.iconphoto(True, self._window_icon_image)
        except Exception:
            return

    def _build_setup_tab(self) -> None:
        self.setup_canvas = Canvas(self.tab_setup, highlightthickness=0, bd=0)
        self.setup_canvas.pack(side=LEFT, fill=BOTH, expand=True)
        self.setup_scrollbar = ttk.Scrollbar(self.tab_setup, orient=VERTICAL, command=self.setup_canvas.yview)
        self.setup_scrollbar.pack(side=RIGHT, fill=Y)
        self.setup_canvas.configure(yscrollcommand=self.setup_scrollbar.set)

        self.setup_inner = ttk.Frame(self.setup_canvas, padding=10)
        self._setup_inner_id = self.setup_canvas.create_window((0, 0), window=self.setup_inner, anchor="nw")
        self.setup_inner.bind(
            "<Configure>",
            lambda _event: self.setup_canvas.configure(scrollregion=self.setup_canvas.bbox("all")),
        )
        self.setup_canvas.bind(
            "<Configure>",
            lambda event: self.setup_canvas.itemconfigure(self._setup_inner_id, width=event.width),
        )

        self.mode_area = ttk.Frame(self.setup_inner)
        self.mode_area.pack(fill=X, pady=(0, 8))

        self.inventory_owner_wrap = ttk.LabelFrame(self.mode_area, text="Operator", padding=8)
        self.inventory_owner_wrap.pack(fill=X, pady=(0, 8))
        owner_row = ttk.Frame(self.inventory_owner_wrap)
        owner_row.pack(fill=X)
        ttk.Label(owner_row, text="Who are you?", width=18).pack(side=LEFT)
        self.inventory_owner_combo = ttk.Combobox(
            owner_row,
            textvariable=self.inventory_owner,
            values=INVENTORY_OWNER_VALUES,
            state="readonly",
            width=18,
        )
        self.inventory_owner_combo.pack(side=LEFT)
        ttk.Label(owner_row, textvariable=self.inventory_owner_inventory_text, foreground="#1f4e79").pack(side=LEFT, padx=(10, 0))

        self.mode_selector_wrap = ttk.LabelFrame(self.mode_area, text="Run Mode", padding=8)
        self.mode_selector_wrap.pack(fill=X)
        ttk.Radiobutton(
            self.mode_selector_wrap,
            text="Update Existing Products",
            variable=self.run_mode,
            value=RUN_MODE_UPDATE,
        ).pack(anchor=W, pady=2)
        ttk.Radiobutton(
            self.mode_selector_wrap,
            text="Create New Products",
            variable=self.run_mode,
            value=RUN_MODE_CREATE,
        ).pack(anchor=W, pady=2)

        self.mode_summary_wrap = ttk.Frame(self.mode_area)
        ttk.Label(self.mode_summary_wrap, textvariable=self.run_mode_summary_text, font=("Segoe UI", 13, "bold")).pack(
            side=LEFT
        )
        ttk.Button(self.mode_summary_wrap, text="Change", command=self._unlock_run_mode).pack(side=LEFT, padx=(10, 0))

        # Keep setup focused at startup; mode-specific workflow controls are hidden
        # until a run mode is selected.
        self.setup_workflow_wrap = ttk.Frame(self.setup_inner)
        self.setup_workflow_wrap.pack(fill=X)

        input_box = ttk.LabelFrame(self.setup_workflow_wrap, text="SKUs In Scope", padding=8)
        input_box.pack(fill=X, pady=(0, 8))
        ttk.Label(input_box, textvariable=self.sku_scope_help_text).pack(anchor=W)

        self.text_input_wrap = ttk.Frame(input_box)
        self.text_input_wrap.pack(fill=X, pady=(6, 6))
        ttk.Label(
            self.text_input_wrap,
            text="Paste SKUs using any delimiter: comma, space, |, or line break.",
        ).pack(anchor=W)
        self.sku_text_widget = tk.Text(self.text_input_wrap, height=6, wrap="word")
        self.sku_text_widget.pack(fill=X, pady=(6, 6))
        paste_btn_row = ttk.Frame(self.text_input_wrap)
        paste_btn_row.pack(fill=X)
        self.load_pasted_btn = ttk.Button(paste_btn_row, text="Load Pasted SKUs", command=self._load_pasted_skus)
        self.load_pasted_btn.pack(side=LEFT)
        self.clear_pasted_btn = ttk.Button(paste_btn_row, text="Clear", command=self._clear_pasted_skus)
        self.clear_pasted_btn.pack(side=LEFT, padx=(8, 0))
        ttk.Label(paste_btn_row, textvariable=self.sku_text_status, foreground="#1f4e79").pack(side=LEFT, padx=(12, 0))

        self.use_all_sheet_check = ttk.Checkbutton(
            input_box,
            text="Use all SKUs from uploaded spreadsheet",
            variable=self.use_all_sheet_skus,
            command=self._on_use_all_sheet_toggle,
        )
        self.use_all_sheet_check.pack(anchor=W, pady=(0, 6))

        self.spreadsheet_input_wrap = ttk.Frame(input_box)
        self.spreadsheet_input_wrap.pack(fill=X)
        self.load_sheet_btn = ttk.Button(
            self.spreadsheet_input_wrap,
            text="Load Vendor Price Sheet (CSV/XLSX)",
            command=self._load_vendor_file,
        )
        self.load_sheet_btn.pack(side=LEFT)
        ttk.Label(self.spreadsheet_input_wrap, textvariable=self.vendor_path).pack(side=LEFT, padx=(10, 0))
        ttk.Label(
            input_box,
            text="If loaded, spreadsheet values are used for in-scope SKUs and scraper fills only missing fields.",
            foreground="#1f4e79",
        ).pack(anchor=W, pady=(6, 0))

        self.vendor_mapping_wrap = ttk.LabelFrame(self.setup_workflow_wrap, text="Vendor Mapping", padding=8)
        mapping_grid = ttk.Frame(self.vendor_mapping_wrap)
        mapping_grid.pack(fill=X)
        mapping_grid.columnconfigure(0, weight=1)
        mapping_grid.columnconfigure(1, weight=1)

        self.vendor_vendor_combo = self._combo_row(mapping_grid, "Vendor", self.vendor_vendor_column, 0, column=0)
        self.vendor_title_combo = self._combo_row(mapping_grid, "Title", self.vendor_title_column, 1, column=0)
        self.vendor_desc_combo = self._combo_row(mapping_grid, "Description", self.vendor_description_column, 2, column=0)
        self.vendor_image_combo = self._combo_row(mapping_grid, "Media", self.vendor_image_column, 3, column=0)
        self.vendor_price_combo = self._combo_row(mapping_grid, "Price", self.vendor_price_column, 4, column=0)

        self.vendor_cost_combo = self._combo_row(mapping_grid, "Cost", self.vendor_cost_column, 0, column=1)
        self.vendor_sku_combo = self._combo_row(mapping_grid, "SKU (required)", self.vendor_sku_column, 1, column=1)
        self.vendor_sku_combo.bind("<<ComboboxSelected>>", lambda _event: self._on_vendor_sku_mapping_changed())
        self.vendor_barcode_combo = self._combo_row(mapping_grid, "Barcode", self.vendor_barcode_column, 2, column=1)
        self.vendor_weight_combo = self._combo_row(mapping_grid, "Weight", self.vendor_weight_column, 3, column=1)
        self.vendor_fitment_combo = self._combo_row(mapping_grid, "Application", self.vendor_fitment_column, 4, column=1)
        self.vendor_core_charge_combo = self._combo_row(
            mapping_grid,
            "Core Charge",
            self.vendor_core_charge_column,
            5,
            column=1,
        )

        vendor_btn_row = ttk.Frame(self.vendor_mapping_wrap)
        vendor_btn_row.pack(anchor=W, pady=(8, 0))
        self.auto_suggest_btn = ttk.Button(vendor_btn_row, text="Auto Suggest Vendor", command=self._auto_suggest_vendor)
        self.auto_suggest_btn.pack(side=LEFT, padx=(0, 8))
        self.stitch_btn = ttk.Button(vendor_btn_row, text="Stitch Vendor Rows", command=self._stitch_vendor_rows)
        self.stitch_btn.pack(side=LEFT)
        ttk.Label(
            self.vendor_mapping_wrap,
            text="Unmapped fields are allowed and can be filled by defaults/rules later.",
            foreground="#1f4e79",
        ).pack(anchor=W, pady=(6, 0))

        self.vendor_preview_wrap = ttk.LabelFrame(self.setup_workflow_wrap, text="Vendor Input Preview", padding=8)
        self.vendor_preview = self._create_tree(self.vendor_preview_wrap, height_rows=10, expand=False, fill_mode=BOTH)

        self.update_fields_wrap = ttk.LabelFrame(self.setup_workflow_wrap, text="Fields To Update (Update Mode)", padding=8)
        update_grid = ttk.Frame(self.update_fields_wrap)
        update_grid.pack(fill=X)
        ttk.Checkbutton(update_grid, text="Title", variable=self.update_title).grid(row=0, column=0, sticky=W, padx=(0, 16))
        ttk.Checkbutton(update_grid, text="Price", variable=self.update_price).grid(row=0, column=1, sticky=W, padx=(0, 16))
        ttk.Checkbutton(update_grid, text="Cost", variable=self.update_cost).grid(row=0, column=2, sticky=W, padx=(0, 16))
        ttk.Checkbutton(update_grid, text="Description", variable=self.update_description).grid(row=1, column=0, sticky=W, padx=(0, 16))
        ttk.Checkbutton(update_grid, text="Images", variable=self.update_images).grid(row=1, column=1, sticky=W, padx=(0, 16))
        ttk.Checkbutton(update_grid, text="Category Fields", variable=self.update_category_fields).grid(row=1, column=2, sticky=W, padx=(0, 16))
        ttk.Checkbutton(update_grid, text="Vendor", variable=self.update_vendor).grid(row=2, column=0, sticky=W, padx=(0, 16))
        ttk.Checkbutton(update_grid, text="Weight", variable=self.update_weight).grid(row=2, column=1, sticky=W, padx=(0, 16))
        ttk.Checkbutton(update_grid, text="Barcode", variable=self.update_barcode).grid(row=2, column=2, sticky=W, padx=(0, 16))
        ttk.Checkbutton(update_grid, text="Application", variable=self.update_application).grid(row=3, column=0, sticky=W, padx=(0, 16))
        ttk.Label(
            self.update_fields_wrap,
            text="Checked fields are the only fields that will be updated on matching Shopify products.",
            foreground="#1f4e79",
        ).pack(anchor=W, pady=(6, 0))

        ttk.Label(self.setup_workflow_wrap, textvariable=self.input_metrics_text, foreground="#1f4e79").pack(anchor=W)
        ttk.Label(self.setup_workflow_wrap, textvariable=self.source_status_text, foreground="#1f4e79").pack(anchor=W, pady=(2, 8))
        self.duplicate_status_wrap = ttk.Frame(self.setup_workflow_wrap)
        self.duplicate_status_wrap.pack(fill=X, pady=(0, 2))
        ttk.Label(self.duplicate_status_wrap, textvariable=self.duplicate_check_text, foreground="#1f4e79").pack(anchor=W)
        self.duplicate_check_progress = ttk.Progressbar(
            self.duplicate_status_wrap,
            mode="determinate",
            length=420,
            maximum=100,
            value=0,
        )
        self.rules_status = ttk.Label(self.setup_workflow_wrap, text="", foreground="#1f4e79")
        self.rules_status.pack(anchor=W)

        self.setup_status = ttk.Label(self.setup_workflow_wrap, textvariable=self.setup_status_text, foreground="#1f4e79")
        self.setup_status.pack(anchor=W)

        continue_row = ttk.Frame(self.setup_workflow_wrap)
        continue_row.pack(fill=X, pady=(6, 6))
        self.setup_continue_btn = ttk.Button(
            continue_row,
            text="Save & Continue to Scraping",
            command=self._continue_from_setup,
        )
        self.setup_continue_btn.pack(side=LEFT)
        self.setup_mode_widgets = [
            self.use_all_sheet_check,
            self.load_sheet_btn,
            self.load_pasted_btn,
            self.clear_pasted_btn,
            self.vendor_vendor_combo,
            self.vendor_title_combo,
            self.vendor_desc_combo,
            self.vendor_image_combo,
            self.vendor_price_combo,
            self.vendor_cost_combo,
            self.vendor_core_charge_combo,
            self.vendor_sku_combo,
            self.vendor_barcode_combo,
            self.vendor_weight_combo,
            self.vendor_fitment_combo,
            self.auto_suggest_btn,
            self.stitch_btn,
            self.setup_continue_btn,
        ]
        self._refresh_sku_action_labels()
        self._set_duplicate_check_busy(False)
        self._refresh_mode_lock_ui()
        self._attach_vendor_mapping_traces()

    def _set_setup_workflow_visible(self, visible: bool) -> None:
        if not hasattr(self, "setup_workflow_wrap"):
            return
        if visible:
            if not self.setup_workflow_wrap.winfo_manager():
                self.setup_workflow_wrap.pack(fill=X)
            return
        self.setup_workflow_wrap.pack_forget()

    def _on_notebook_tab_changed(self, _event=None) -> None:
        try:
            current = self.notebook.select()
            if current == str(self.tab_setup):
                self.setup_canvas.yview_moveto(0)
                return
            if current == str(self.tab_preview) and hasattr(self, "preview_canvas"):
                self.preview_canvas.yview_moveto(0)
                return
            if current == str(self.tab_export) and hasattr(self, "export_canvas"):
                self._ensure_review_ready()
                self.export_canvas.yview_moveto(0)
        except Exception:
            return

    def _ensure_review_ready(self) -> None:
        if not self.review_refresh_pending or self.review_refresh_inflight:
            return
        self.review_refresh_inflight = True
        self.review_status_text.set("Loading review data...")
        self.root.after(1, self._finish_review_refresh)

    def _finish_review_refresh(self) -> None:
        try:
            self._refresh_review_tab()
            self.review_refresh_pending = False
        finally:
            self.review_refresh_inflight = False

    def _open_review_tab(self) -> None:
        self.review_tab_unlocked = True
        self._update_tab_access()
        self.notebook.select(2)
        self._ensure_review_ready()

    def _cancel_review_table_refresh(self) -> None:
        if self.review_table_refresh_job is None:
            return
        try:
            self.root.after_cancel(self.review_table_refresh_job)
        except Exception:
            pass
        self.review_table_refresh_job = None

    def _schedule_review_table_refresh(self) -> None:
        self._cancel_review_table_refresh()
        try:
            self.review_table_refresh_job = self.root.after(25, self._refresh_review_table_async)
        except Exception:
            self.review_table_refresh_job = None

    def _refresh_review_table_async(self) -> None:
        self.review_table_refresh_job = None
        products = self.session.products or []
        if not products:
            self.push_selected_skus = set()
            _tree_show_dataframe(self.review_table, pd.DataFrame())
            self._refresh_push_button_state()
            return

        available = {
            normalize_sku(getattr(product, "sku", ""))
            for product in products
            if normalize_sku(getattr(product, "sku", "")) and not bool(getattr(product, "excluded", False))
        }
        self.push_selected_skus = {sku for sku in self.push_selected_skus if sku in available}

        # Keep review grid lightweight to avoid UI lockups on large/long text payloads.
        rows: list[dict[str, str]] = []
        for product in products[:80]:
            sku = normalize_sku(getattr(product, "sku", ""))
            excluded = bool(getattr(product, "excluded", False))
            exclusion_reason = str(getattr(product, "exclusion_reason", "") or "").strip()
            rows.append(
                {
                    "push": "[-]" if excluded else ("[x]" if sku and sku in self.push_selected_skus else "[ ]"),
                    "sku": str(product.sku or ""),
                    "title": str(product.title or ""),
                    "price": str(product.price or ""),
                    "cost": str(product.cost or ""),
                    "vendor": str(product.vendor or ""),
                    "type": str(product.type or ""),
                    "google_product_type": str(product.google_product_type or ""),
                    "category_code": str(product.category_code or ""),
                    "product_subtype": str(product.product_subtype or ""),
                    "application": str(product.application or ""),
                    "scrape_status": str(product.scrape_status or ""),
                    "status": exclusion_reason or "ready",
                }
            )
        df = pd.DataFrame(rows)
        _tree_show_dataframe(self.review_table, df, max_rows=80, max_cell_chars=180)
        self._highlight_review_table_current_product()
        self._refresh_push_button_state()

    def _find_product_index_by_sku(self, sku_value: str) -> int | None:
        normalized_target = normalize_sku(sku_value)
        if not normalized_target:
            return None
        for index, product in enumerate(self.session.products or []):
            if normalize_sku(getattr(product, "sku", "")) == normalized_target:
                return index
        return None

    def _highlight_review_table_current_product(self) -> None:
        if not hasattr(self, "review_table"):
            return
        products = self.session.products or []
        if not products:
            return
        if self.review_index < 0 or self.review_index >= len(products):
            return
        current_sku = normalize_sku(getattr(products[self.review_index], "sku", ""))
        if not current_sku:
            return
        tree = self.review_table
        for row_id in tree.get_children():
            row_sku = normalize_sku(tree.set(row_id, "sku"))
            if row_sku != current_sku:
                continue
            try:
                tree.selection_set(row_id)
                tree.focus(row_id)
                tree.see(row_id)
            except Exception:
                pass
            break

    def _toggle_review_table_push_selection(self) -> None:
        products = self.session.products or []
        available = {normalize_sku(getattr(product, "sku", "")) for product in products if normalize_sku(getattr(product, "sku", ""))}
        if not available:
            self.push_selected_skus = set()
            self._refresh_review_table_async()
            return
        selected = {sku for sku in self.push_selected_skus if sku in available}
        if len(selected) == len(available):
            self.push_selected_skus = set()
        else:
            self.push_selected_skus = set(available)
        self._refresh_review_table_async()

    def _lock_run_mode(self) -> None:
        self.run_mode_locked.set(True)
        self._refresh_mode_lock_ui()

    def _unlock_run_mode(self) -> None:
        self.run_mode_locked.set(False)
        self.session.reset_for_new_run()
        self.session.inventory_default = _inventory_for_owner(self.inventory_owner.get())
        self.push_selected_skus = set()
        self.run_mode.set("")
        self.setup_status_text.set("Run mode unlocked. Choose mode to continue.")
        self.processing_status_text.set("")
        self.review_status_text.set("")
        self.review_refresh_pending = False
        self.review_refresh_inflight = False
        self._cancel_review_table_refresh()
        self._hide_review_busy_overlay()
        self.review_loaded_raw = {}
        self.review_loaded_display = {}
        self.review_loaded_truncated = {}
        self.review_cost_options_loaded_for_sku = ""
        if hasattr(self, "to_review_btn"):
            self.to_review_btn.configure(state="disabled")
        self._update_tab_access()
        self._refresh_mode_lock_ui()

    def _on_inventory_owner_changed(self, *_args) -> None:
        selected = self.inventory_owner.get().strip()
        if selected not in INVENTORY_BY_OWNER:
            selected = DEFAULT_INVENTORY_OWNER
            if self.inventory_owner.get() != selected:
                self.inventory_owner.set(selected)
                return

        inventory_value = _inventory_for_owner(selected)
        self.session.inventory_default = inventory_value
        self.inventory_owner_inventory_text.set(f"Inventory default: {inventory_value:,}")
        if not self.session.products:
            self.review_fields["inventory"].set(str(inventory_value))

    def _refresh_mode_lock_ui(self) -> None:
        mode_name = self.run_mode.get().strip()
        display_mode_map = {
            RUN_MODE_UPDATE: "Update Existing Products",
            RUN_MODE_CREATE: "Create New Products",
        }
        display_mode = display_mode_map.get(mode_name, "Not Selected")
        self.run_mode_summary_text.set(f"Run Mode - {display_mode}")
        if self.run_mode_locked.get():
            self.mode_selector_wrap.pack_forget()
            self.mode_summary_wrap.pack(fill=X)
            return
        self.mode_summary_wrap.pack_forget()
        self.mode_selector_wrap.pack(fill=X)

    def _build_preview_tab(self) -> None:
        self.preview_canvas = Canvas(self.tab_preview, highlightthickness=0, bd=0)
        self.preview_canvas.pack(side=LEFT, fill=BOTH, expand=True)
        self.preview_scrollbar = ttk.Scrollbar(self.tab_preview, orient=VERTICAL, command=self.preview_canvas.yview)
        self.preview_scrollbar.pack(side=RIGHT, fill=Y)
        self.preview_canvas.configure(yscrollcommand=self.preview_scrollbar.set)

        self.preview_inner = ttk.Frame(self.preview_canvas, padding=10)
        self._preview_inner_id = self.preview_canvas.create_window((0, 0), window=self.preview_inner, anchor="nw")
        self.preview_inner.bind(
            "<Configure>",
            lambda _event: self.preview_canvas.configure(scrollregion=self.preview_canvas.bbox("all")),
        )
        self.preview_canvas.bind(
            "<Configure>",
            lambda event: self.preview_canvas.itemconfigure(self._preview_inner_id, width=event.width),
        )

        settings_wrap = ttk.LabelFrame(self.preview_inner, text="Scraper Settings", padding=8)
        settings_wrap.pack(fill=X, pady=(0, 8))

        ttk.Label(settings_wrap, text="Vendor Search URL", width=24).grid(row=0, column=0, sticky=W, padx=(0, 8), pady=3)
        ttk.Entry(settings_wrap, textvariable=self.scrape_search_url).grid(row=0, column=1, sticky="ew", pady=3)
        ttk.Label(settings_wrap, text="Chrome Workers", width=24).grid(row=1, column=0, sticky=W, padx=(0, 8), pady=3)
        ttk.Entry(settings_wrap, textvariable=self.scrape_workers, width=12).grid(row=1, column=1, sticky=W, pady=3)
        ttk.Label(settings_wrap, text="Delay Between Requests", width=24).grid(row=2, column=0, sticky=W, padx=(0, 8), pady=3)
        ttk.Entry(settings_wrap, textvariable=self.scrape_delay, width=12).grid(row=2, column=1, sticky=W, pady=3)
        ttk.Label(settings_wrap, text="Retry Count", width=24).grid(row=3, column=0, sticky=W, padx=(0, 8), pady=3)
        ttk.Entry(settings_wrap, textvariable=self.scrape_retries, width=12).grid(row=3, column=1, sticky=W, pady=3)
        settings_wrap.columnconfigure(1, weight=1)

        toggles = ttk.Frame(settings_wrap)
        toggles.grid(row=4, column=0, columnspan=2, sticky=W, pady=(6, 0))
        ttk.Checkbutton(toggles, text="Headless Mode", variable=self.scrape_headless).pack(side=LEFT, padx=(0, 12))
        ttk.Checkbutton(toggles, text="Scrape Images", variable=self.scrape_images).pack(side=LEFT, padx=(0, 12))
        ttk.Checkbutton(toggles, text="Force Scrape", variable=self.scrape_force).pack(side=LEFT, padx=(0, 12))

        action_row = ttk.Frame(self.preview_inner)
        action_row.pack(fill=X, pady=(0, 8))
        self.start_processing_btn = ttk.Button(action_row, text="Start Prospecting", command=self._start_processing_clicked)
        self.start_processing_btn.pack(side=LEFT)
        self.to_review_btn = ttk.Button(
            action_row,
            text="Continue to Review",
            command=self._open_review_tab,
            state="disabled",
        )
        self.to_review_btn.pack(side=LEFT, padx=(8, 0))

        self.processing_status = ttk.Label(self.preview_inner, textvariable=self.processing_status_text, foreground="#1f4e79")
        self.processing_status.pack(anchor=W, pady=(0, 8))

        run_preview_wrap = ttk.LabelFrame(self.preview_inner, text="Processing Output Preview", padding=8)
        run_preview_wrap.pack(fill=X, pady=(0, 8))
        self.processing_preview = self._create_tree(run_preview_wrap, height_rows=11, expand=False, fill_mode=X)
        self.processing_preview.configure(selectmode="none", takefocus=0)
        self.processing_preview.bind("<Button-1>", lambda _event: "break", add="+")
        self.processing_preview.bind("<ButtonRelease-1>", lambda _event: "break", add="+")
        self.processing_preview.bind("<Double-1>", lambda _event: "break", add="+")

    def _build_export_tab(self) -> None:
        self.export_canvas = Canvas(self.tab_export, highlightthickness=0, bd=0)
        self.export_canvas.pack(side=LEFT, fill=BOTH, expand=True)
        self.export_scrollbar = ttk.Scrollbar(self.tab_export, orient=VERTICAL, command=self.export_canvas.yview)
        self.export_scrollbar.pack(side=RIGHT, fill=Y)
        self.export_canvas.configure(yscrollcommand=self.export_scrollbar.set)
        self.export_inner = ttk.Frame(self.export_canvas, padding=10)
        self._export_inner_id = self.export_canvas.create_window((0, 0), window=self.export_inner, anchor="nw")
        self.export_inner.bind(
            "<Configure>",
            lambda _event: self.export_canvas.configure(scrollregion=self.export_canvas.bbox("all")),
        )
        self.export_canvas.bind(
            "<Configure>",
            lambda event: self.export_canvas.itemconfigure(self._export_inner_id, width=event.width),
        )

        nav_row = ttk.Frame(self.export_inner)
        nav_row.pack(fill=X, pady=(0, 8))
        ttk.Button(nav_row, text="Previous", command=self._review_prev).pack(side=LEFT)
        ttk.Button(nav_row, text="Next", command=self._review_next).pack(side=LEFT, padx=(8, 0))
        ttk.Label(nav_row, textvariable=self.review_index_text).pack(side=LEFT, padx=(12, 0))

        form_wrap = ttk.LabelFrame(self.export_inner, text="Product Review", padding=8)
        form_wrap.pack(fill=X, pady=(0, 8))
        form_grid = ttk.Frame(form_wrap)
        form_grid.pack(fill=X)
        form_grid.columnconfigure(1, weight=1)
        form_grid.columnconfigure(3, weight=1)

        self._review_entry_row(form_grid, "Title", "title", 0, 0)
        self._review_entry_row(form_grid, "Description", "description_html", 1, 0)
        self._review_entry_row(form_grid, "Media URLs", "media_urls", 2, 0)
        self._review_entry_row(form_grid, "Price", "price", 3, 0)
        self._review_cost_row(form_grid, 4, 0)
        self._review_entry_row(form_grid, "Inventory", "inventory", 5, 0)
        self._review_entry_row(form_grid, "SKU", "sku", 6, 0)
        self._review_entry_row(form_grid, "Barcode", "barcode", 7, 0)
        self._review_entry_row(form_grid, "Weight", "weight", 8, 0)

        self._review_entry_row(form_grid, "Vendor", "vendor", 0, 2)
        self._review_entry_row(form_grid, "Type", "type", 1, 2)
        self._review_entry_row(form_grid, "Google Product Type", "google_product_type", 2, 2)
        self._review_entry_row(form_grid, "Category Type", "category_code", 3, 2)
        self._review_entry_row(form_grid, "Product Subtype", "product_subtype", 4, 2)
        self._review_entry_row(form_grid, "MPN", "mpn", 5, 2)
        self._review_entry_row(form_grid, "Brand", "brand", 6, 2)
        self._review_entry_row(form_grid, "Application", "application", 7, 2)
        self._review_entry_row(form_grid, "Core Charge Code", "core_charge_product_code", 8, 2)

        table_wrap = ttk.LabelFrame(self.export_inner, text="All Products", padding=8)
        table_wrap.pack(fill=BOTH, expand=True)
        self.review_table = self._create_tree(table_wrap)
        self.review_table.bind("<ButtonRelease-1>", self._on_review_table_click, add="+")
        table_action_row = ttk.Frame(self.export_inner)
        table_action_row.pack(fill=X, pady=(4, 0))
        ttk.Button(table_action_row, text="Load All Products Preview", command=self._refresh_review_table_async).pack(side=LEFT)
        ttk.Button(table_action_row, text="Select All for Push", command=self._select_all_for_push).pack(side=LEFT, padx=(8, 0))
        ttk.Button(table_action_row, text="Clear Push Selection", command=self._clear_push_selection).pack(side=LEFT, padx=(8, 0))
        ttk.Label(table_action_row, text="Click the [x]/[ ] cell in the Push column to toggle rows.", foreground="#1f4e79").pack(side=LEFT, padx=(10, 0))

        remap_wrap = ttk.LabelFrame(self.export_inner, text="Remap Fields (Optional)", padding=8)
        remap_wrap.pack(fill=X, pady=(8, 0))
        remap_grid = ttk.Frame(remap_wrap)
        remap_grid.pack(fill=X)
        remap_grid.columnconfigure(0, weight=1)
        remap_grid.columnconfigure(1, weight=1)

        self.remap_vendor_combo = self._combo_row(remap_grid, "Vendor", self.vendor_vendor_column, 0, column=0)
        self.remap_title_combo = self._combo_row(remap_grid, "Title", self.vendor_title_column, 1, column=0)
        self.remap_desc_combo = self._combo_row(remap_grid, "Description", self.vendor_description_column, 2, column=0)
        self.remap_media_combo = self._combo_row(remap_grid, "Media", self.vendor_image_column, 3, column=0)
        self.remap_price_combo = self._combo_row(remap_grid, "Price", self.vendor_price_column, 4, column=0)
        self.remap_cost_combo = self._combo_row(remap_grid, "Cost", self.vendor_cost_column, 0, column=1)
        self.remap_sku_combo = self._combo_row(remap_grid, "SKU (required)", self.vendor_sku_column, 1, column=1)
        self.remap_barcode_combo = self._combo_row(remap_grid, "Barcode", self.vendor_barcode_column, 2, column=1)
        self.remap_weight_combo = self._combo_row(remap_grid, "Weight", self.vendor_weight_column, 3, column=1)
        self.remap_application_combo = self._combo_row(remap_grid, "Application", self.vendor_fitment_column, 4, column=1)
        self.remap_core_charge_combo = self._combo_row(
            remap_grid,
            "Core Charge",
            self.vendor_core_charge_column,
            5,
            column=1,
        )
        remap_action = ttk.Frame(remap_wrap)
        remap_action.pack(fill=X, pady=(6, 0))
        ttk.Button(remap_action, text="Apply Remap & Reprocess", command=self._reprocess_from_review).pack(side=LEFT)

        self.export_status = ttk.Label(self.export_inner, textvariable=self.review_status_text, foreground="#1f4e79")
        self.export_status.pack(anchor=W, pady=(6, 0))

        export_row = ttk.Frame(self.export_inner)
        export_row.pack(fill=X, pady=(8, 0))
        ttk.Button(export_row, text="Save Current", command=self._save_current_review_product).pack(side=LEFT)
        ttk.Button(export_row, text="Generate CSV", command=self._export_review_products).pack(side=LEFT, padx=(8, 0))
        self.push_shopify_btn = ttk.Button(export_row, text="Push to Shopify", command=self._push_to_shopify_clicked)
        self.push_shopify_btn.pack(side=LEFT, padx=(8, 0))
        self._refresh_push_button_state()

        # Review overlay blocks interaction during remap/reprocess and shows spinner progress state.
        self.review_busy_overlay = tk.Frame(self.tab_export, bg="#9CA3AF", highlightthickness=0, bd=0)
        self.review_busy_overlay.bind("<Button-1>", lambda _event: "break", add="+")
        self.review_busy_overlay.bind("<ButtonRelease-1>", lambda _event: "break", add="+")
        self.review_busy_overlay.bind("<Double-1>", lambda _event: "break", add="+")
        self.review_busy_overlay.bind("<MouseWheel>", lambda _event: "break", add="+")

        overlay_card = tk.Frame(self.review_busy_overlay, bg="#F3F4F6", padx=18, pady=14, relief="ridge", bd=1)
        overlay_card.place(relx=0.5, rely=0.5, anchor="center")
        self.review_busy_spinner = Canvas(overlay_card, width=18, height=18, highlightthickness=0, bd=0, bg="#F3F4F6")
        self.review_busy_spinner.pack(side=LEFT)
        self.review_busy_text = StringVar(value="Reprocessing...")
        tk.Label(
            overlay_card,
            textvariable=self.review_busy_text,
            bg="#F3F4F6",
            fg="#111827",
            font=("Segoe UI", 10, "bold"),
        ).pack(side=LEFT, padx=(10, 0))

    def _reorder_processing_preview_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df
        trailing = [
            "excluded",
            "exclusion_reason",
            "scrape_status",
            "scrape_fields_found",
            "scrape_error",
            "media_folder",
        ]
        trailing_present = [column for column in trailing if column in df.columns]
        leading = [column for column in df.columns if column not in trailing_present]
        return df.loc[:, [*leading, *trailing_present]]

    def _combo_row(self, parent, label: str, variable: StringVar, row: int, column: int = 0) -> ttk.Combobox:
        frame = ttk.Frame(parent)
        pad_x = (0, 8) if column == 0 else (8, 0)
        frame.grid(row=row, column=column, sticky="ew", padx=pad_x, pady=3)
        ttk.Label(frame, text=label, width=24).pack(side=LEFT)
        combo = ttk.Combobox(frame, textvariable=variable, state="readonly", width=44)
        combo.pack(side=LEFT, fill=X, expand=True)
        return combo

    def _create_tree(
        self,
        parent,
        height_rows: int = 12,
        expand: bool = True,
        fill_mode=BOTH,
    ) -> ttk.Treeview:
        container = ttk.Frame(parent)
        container.pack(fill=fill_mode, expand=expand)
        tree = ttk.Treeview(container, show="headings", height=height_rows)
        y_scroll = ttk.Scrollbar(container, orient=VERTICAL, command=tree.yview)
        x_scroll = ttk.Scrollbar(container, orient="horizontal", command=tree.xview)
        tree.configure(yscrollcommand=y_scroll.set, xscrollcommand=x_scroll.set)
        container.columnconfigure(0, weight=1)
        if expand:
            container.rowconfigure(0, weight=1)
        tree.grid(row=0, column=0, sticky="nsew")
        y_scroll.grid(row=0, column=1, sticky="ns")
        x_scroll.grid(row=1, column=0, sticky="ew")
        return tree

    def _review_entry_row(self, parent, label: str, field_name: str, row: int, col_offset: int) -> None:
        ttk.Label(parent, text=label, width=18).grid(row=row, column=col_offset, sticky=W, padx=(0, 6), pady=2)
        ttk.Entry(parent, textvariable=self.review_fields[field_name]).grid(
            row=row,
            column=col_offset + 1,
            sticky="ew",
            padx=(0, 12),
            pady=2,
        )

    def _review_cost_row(self, parent, row: int, col_offset: int) -> None:
        ttk.Label(parent, text="Cost", width=18).grid(row=row, column=col_offset, sticky=W, padx=(0, 6), pady=2)
        frame = ttk.Frame(parent)
        frame.grid(row=row, column=col_offset + 1, sticky="ew", padx=(0, 12), pady=2)
        frame.columnconfigure(1, weight=1)
        ttk.Entry(frame, textvariable=self.review_fields["cost"], width=16).grid(row=0, column=0, sticky=W, padx=(0, 6))
        self.review_cost_rule_combo = ttk.Combobox(
            frame,
            textvariable=self.review_cost_rule_text,
            state="readonly",
        )
        self.review_cost_rule_combo.grid(row=0, column=1, sticky="ew")
        self.review_cost_rule_combo.bind("<<ComboboxSelected>>", self._on_review_cost_rule_selected, add="+")
        self.review_cost_rule_combo.bind("<Button-1>", self._on_review_cost_dropdown_open, add="+")
        self.review_cost_rule_combo.configure(values=())
        self.review_cost_rule_combo.configure(state="disabled")

    def _parse_float_value(self, value: object) -> float | None:
        text = str(value or "").strip()
        if not text:
            return None
        cleaned = re.sub(r"[^0-9.\-]", "", text.replace(",", ""))
        if not cleaned:
            return None
        try:
            return float(cleaned)
        except Exception:
            return None

    def _load_vendor_discounts_cached(self) -> pd.DataFrame:
        if self.vendor_discounts_df is not None:
            return self.vendor_discounts_df
        discount_file = find_vendor_discount_file(self.required_root)
        if discount_file is None:
            self.vendor_discounts_df = pd.DataFrame()
            return self.vendor_discounts_df
        try:
            self.vendor_discounts_df = load_vendor_discounts(discount_file)
        except Exception:
            self.vendor_discounts_df = pd.DataFrame()
        return self.vendor_discounts_df

    def _refresh_review_cost_rule_options(self, product) -> None:
        if not hasattr(self, "review_cost_rule_combo"):
            return
        raw_vendor = str(getattr(product, "vendor", "") or "").strip()
        normalized_vendor = normalize_vendor_from_rules(raw_vendor, required_root=self.required_root)
        profile = resolve_vendor_profile(normalized_vendor or raw_vendor, required_root=self.required_root)
        vendor_for_discount = (
            (profile.discount_vendor_key if profile is not None else "")
            or normalized_vendor
            or raw_vendor
        )
        vendor_label = (
            (profile.canonical_vendor if profile is not None else "")
            or normalized_vendor
            or raw_vendor
            or "No vendor"
        )

        discount_df = self._load_vendor_discounts_cached()
        options = resolve_discount_candidates(
            discounts_df=discount_df,
            vendor_name=vendor_for_discount,
            product_title=str(getattr(product, "title", "") or ""),
            product_type=str(getattr(product, "type", "") or ""),
        )
        if not options and raw_vendor and vendor_for_discount.lower() != raw_vendor.lower():
            options = resolve_discount_candidates(
                discounts_df=discount_df,
                vendor_name=raw_vendor,
                product_title=str(getattr(product, "title", "") or ""),
                product_type=str(getattr(product, "type", "") or ""),
            )

        self.review_cost_options = options
        self.review_cost_option_map = {item.vendor_label: item for item in options}
        option_labels = [item.vendor_label for item in options]
        self.review_cost_rule_combo.configure(values=option_labels)

        if option_labels:
            self.review_cost_rule_combo.configure(state="readonly")
            if self.review_cost_rule_text.get() not in option_labels:
                self.review_cost_rule_text.set(vendor_label)
            return

        self.review_cost_rule_text.set(vendor_label)
        self.review_cost_rule_combo.configure(state="disabled")

    def _on_review_cost_rule_selected(self, _event=None) -> None:
        selected = self.review_cost_rule_text.get().strip()
        option = self.review_cost_option_map.get(selected)
        if option is None:
            return
        price_value = self._parse_float_value(self.review_fields["price"].get())
        if price_value is None:
            messagebox.showwarning(APP_TITLE, "Cannot calculate cost because price is blank or invalid.")
            return
        cost_value = calculate_cost_from_price(price=price_value, discount_percent=option.discount_percent)
        self.review_fields["cost"].set(f"{cost_value:.2f}")
        self.review_status_text.set(
            f"Applied discount {option.discount_percent:.2f}% from '{selected}' to calculate cost."
        )

    def _on_review_cost_dropdown_open(self, _event=None) -> None:
        if not self.session.products:
            return
        if self.review_index < 0 or self.review_index >= len(self.session.products):
            return
        sku = str(self.session.products[self.review_index].sku or "").strip().upper()
        if sku and self.review_cost_options_loaded_for_sku == sku and self.review_cost_option_map:
            return
        self._refresh_review_cost_rule_options(self.session.products[self.review_index])
        self.review_cost_options_loaded_for_sku = sku

    def _display_field_value(self, field_name: str, raw_value: str) -> tuple[str, bool]:
        limits = {
            "description_html": 4000,
            "media_urls": 2500,
            "application": 2000,
        }
        limit = limits.get(field_name)
        if limit is None:
            return raw_value, False
        if len(raw_value) <= limit:
            return raw_value, False
        return raw_value[:limit] + " ... [truncated for display]", True

    def _read_review_field(self, field_name: str) -> str:
        current = self.review_fields[field_name].get().strip()
        if self.review_loaded_truncated.get(field_name):
            displayed = self.review_loaded_display.get(field_name, "").strip()
            if current == displayed:
                return self.review_loaded_raw.get(field_name, "").strip()
        return current

    def _start_processing_clicked(self, auto_open_review: bool = False) -> bool:
        if self.processing_inflight:
            return False
        if not self.session.setup_complete:
            messagebox.showwarning(APP_TITLE, "Complete Setup first.")
            return False

        try:
            workers = max(int(float(self.scrape_workers.get().strip() or "1")), 1)
            delay = max(float(self.scrape_delay.get().strip() or "0"), 0.0)
            retries = max(int(float(self.scrape_retries.get().strip() or "0")), 0)
        except Exception:
            messagebox.showerror(APP_TITLE, "Invalid scraper settings. Use numeric values for workers/delay/retries.")
            return False

        self.session.scrape_settings.vendor_search_url = self.scrape_search_url.get().strip()
        self.session.scrape_settings.chrome_workers = workers
        self.session.scrape_settings.delay_seconds = delay
        self.session.scrape_settings.retry_count = retries
        self.session.scrape_settings.headless = bool(self.scrape_headless.get())
        # Images are always scrape-driven in this workflow.
        self.scrape_images.set(True)
        self.session.scrape_settings.scrape_images = True
        self.session.scrape_settings.force_scrape = bool(self.scrape_force.get())
        self.session.inventory_default = _inventory_for_owner(self.inventory_owner.get())

        target_skus = collect_session_skus(self.session)
        if not target_skus:
            messagebox.showwarning(APP_TITLE, "No valid SKUs found in the current scope.")
            return False

        if self.session.missing_fields and not self.session.scrape_settings.vendor_search_url:
            messagebox.showwarning(
                APP_TITLE,
                "Missing fields require scraping. Set Vendor Search URL in Processing or complete mappings in Setup.",
            )
            return False

        self._processing_request_id += 1
        request_id = self._processing_request_id
        self._auto_open_review_after_processing = bool(auto_open_review)
        self.review_tab_unlocked = False
        self._update_tab_access()
        if self.session.scrape_settings.vendor_search_url:
            self._play_header_logo_animation()
        self._set_processing_busy(True)
        self.processing_status_text.set(f"Processing {len(target_skus)} SKU(s) in background...")
        try:
            if hasattr(self, "preview_canvas"):
                self.preview_canvas.yview_moveto(0)
        except Exception:
            pass

        worker = threading.Thread(
            target=self._run_processing_worker,
            kwargs={"request_id": request_id, "target_skus": list(target_skus)},
            daemon=True,
        )
        worker.start()
        return True

    def _set_processing_busy(self, busy: bool) -> None:
        self.processing_inflight = busy
        state = "disabled" if busy else "normal"
        if hasattr(self, "start_processing_btn"):
            self.start_processing_btn.configure(state=state)
        if busy:
            if hasattr(self, "to_review_btn"):
                self.to_review_btn.configure(state="disabled")
            return
        self._finish_header_logo_animation()
        self._hide_review_busy_overlay()
        if hasattr(self, "to_review_btn") and self.session.processing_complete:
            self.to_review_btn.configure(state="normal")

    def _load_shopify_catalog_for_processing(self, target_skus: list[str]) -> tuple[pd.DataFrame | None, str | None]:
        config = load_shopify_config()
        if config is None:
            return None, "Invalid config/shopify.json. Cannot sync Shopify catalog."

        token = load_shopify_token()
        if token is None:
            self._connect_shopify_worker(allow_handshake=False)
            token = load_shopify_token()
        if token is None:
            return None, "Shopify is not connected. Connect Shopify first and retry."

        use_targeted = bool(target_skus)
        if use_targeted:
            df, error = fetch_shopify_catalog_for_skus(
                config=config,
                access_token=token.access_token,
                skus=target_skus,
            )
            if not error and (df is None or df.empty):
                df, error = fetch_shopify_catalog_dataframe(config=config, access_token=token.access_token)
        else:
            df, error = fetch_shopify_catalog_dataframe(config=config, access_token=token.access_token)
        if error:
            return None, error
        return df if df is not None else pd.DataFrame(), None

    def _scope_missing_media_for_scrape(self, target_skus: list[str]) -> bool:
        if not target_skus:
            return False
        if self.session.vendor_df is None or self.session.vendor_df.empty:
            return True

        sku_column = (self.session.source_mapping.sku or "").strip()
        media_column = (self.session.source_mapping.media or "").strip()
        if not sku_column or not media_column:
            return True
        if sku_column not in self.session.vendor_df.columns or media_column not in self.session.vendor_df.columns:
            return True

        try:
            working = self.session.vendor_df[[sku_column, media_column]].copy()
            working["_norm_sku"] = working[sku_column].astype(str).map(normalize_sku)
            working["_media_text"] = working[media_column].astype(str).map(lambda value: str(value).strip())
            working = working[working["_norm_sku"] != ""]
            if working.empty:
                return True

            has_media_by_sku = (
                working.groupby("_norm_sku")["_media_text"]
                .apply(lambda series: any(bool(item) for item in series))
                .to_dict()
            )
            for sku in [normalize_sku(item) for item in target_skus if normalize_sku(item)]:
                if not has_media_by_sku.get(sku, False):
                    return True
            return False
        except Exception:
            return True

    def _run_processing_worker(self, request_id: int, target_skus: list[str]) -> None:
        result_payload: dict[str, object] = {}
        try:
            existing_index: dict[str, dict[str, str]] = {}
            shopify_df: pd.DataFrame | None = None
            if self.session.mode == MODE_UPDATE:
                shopify_df, catalog_error = self._load_shopify_catalog_for_processing(target_skus)
                if catalog_error:
                    raise RuntimeError(catalog_error)
                existing_index = build_existing_shopify_index(shopify_df)

            scrape_records: dict[str, dict[str, str]] = {}
            scrape_sku_errors: dict[str, str] = {}
            scrape_general_errors: list[str] = []
            can_scrape = bool(self.session.scrape_settings.vendor_search_url and target_skus)
            image_scrape_needed = bool(self.session.scrape_settings.scrape_images) and self._scope_missing_media_for_scrape(target_skus)
            should_scrape = can_scrape and (
                self.session.scrape_settings.force_scrape
                or bool(self.session.missing_fields)
                or image_scrape_needed
            )
            if should_scrape:
                scrape_records, scrape_sku_errors, scrape_general_errors = scrape_vendor_records(
                    vendor_search_url=self.session.scrape_settings.vendor_search_url,
                    skus=target_skus,
                    workers=self.session.scrape_settings.chrome_workers,
                    retry_count=self.session.scrape_settings.retry_count,
                    delay_seconds=self.session.scrape_settings.delay_seconds,
                    scrape_images=self.session.scrape_settings.scrape_images,
                    image_output_root=self.runtime_output_root / "images",
                )

            products, build_stats = build_products_from_session(
                session=self.session,
                existing_shopify_index=existing_index,
                scraped_records=scrape_records,
                required_root=self.required_root,
            )

            mapper = self.type_mapper
            if mapper is None:
                mapper = TypeCategoryMapper.from_required_root(self.required_root)

            update_scope = set(self.session.update_fields or [])
            allow_category_overwrite = self.session.mode == MODE_NEW or bool(
                {"type", "google_product_type", "category_code", "product_subtype"}.intersection(update_scope)
            )

            normalized_products = []
            default_inventory = int(self.session.inventory_default or 3000000)
            for product in products:
                normalized = normalize_product(
                    product=product,
                    required_root=self.required_root,
                    mode=self.session.mode,
                    update_fields=update_scope,
                    default_inventory=default_inventory,
                )
                if self.session.mode == MODE_NEW or allow_category_overwrite:
                    normalized = mapper.apply(
                        product=normalized,
                        allow_category_overwrite=allow_category_overwrite,
                    )
                normalized.finalize_defaults()
                normalized_products.append(normalized)

            self._apply_scrape_diagnostics(
                products=normalized_products,
                target_skus=target_skus,
                should_scrape=should_scrape,
                scrape_records=scrape_records,
                scrape_sku_errors=scrape_sku_errors,
            )

            result_payload = {
                "shopify_df": shopify_df,
                "products": normalized_products,
                "build_stats": build_stats,
                "should_scrape": should_scrape,
                "scrape_records": scrape_records,
                "scrape_sku_errors": scrape_sku_errors,
                "scrape_general_errors": scrape_general_errors,
                "image_scrape_needed": image_scrape_needed,
                "can_scrape": can_scrape,
                "mapper": mapper,
            }
            error_text: str | None = None
        except Exception as exc:
            error_text = str(exc)

        def apply() -> None:
            if request_id != self._processing_request_id:
                return
            self._set_processing_busy(False)
            if error_text:
                self.processing_status_text.set(f"Processing failed: {error_text}")
                messagebox.showerror(APP_TITLE, f"Processing failed:\n{error_text}")
                self._auto_open_review_after_processing = False
                return

            mapper_obj = result_payload.get("mapper")
            if mapper_obj is not None:
                self.type_mapper = mapper_obj

            shopify_df = result_payload.get("shopify_df")
            if isinstance(shopify_df, pd.DataFrame):
                self.shopify_df_raw = shopify_df
                self._refresh_input_metrics()
                if self.session.mode == MODE_UPDATE:
                    self.rules_status.configure(text=f"Shopify targeted sync complete: {len(shopify_df)} SKU rows loaded.")

            normalized_products = list(result_payload.get("products") or [])
            build_stats = result_payload.get("build_stats")
            should_scrape = bool(result_payload.get("should_scrape"))
            can_scrape = bool(result_payload.get("can_scrape"))
            image_scrape_needed = bool(result_payload.get("image_scrape_needed"))
            scrape_records = dict(result_payload.get("scrape_records") or {})
            scrape_sku_errors = dict(result_payload.get("scrape_sku_errors") or {})
            scrape_general_errors = list(result_payload.get("scrape_general_errors") or [])

            self.session.products = normalized_products
            self.push_selected_skus = {
                normalize_sku(getattr(product, "sku", ""))
                for product in normalized_products
                if normalize_sku(getattr(product, "sku", "")) and not bool(getattr(product, "excluded", False))
            }
            self.session.processing_complete = True
            self._update_tab_access()
            self._refresh_push_button_state()

            preview_df = products_to_dataframe(normalized_products)
            preview_df = self._reorder_processing_preview_columns(preview_df)
            _tree_show_dataframe(self.processing_preview, _safe_head(preview_df, rows=120))

            rows_considered = int(getattr(build_stats, "rows_considered", len(normalized_products)))
            rows_skipped_no_shopify_match = int(getattr(build_stats, "rows_skipped_no_shopify_match", 0))
            rows_skipped_missing_sku = int(getattr(build_stats, "rows_skipped_missing_sku", 0))
            rows_flagged_gas = int(getattr(build_stats, "rows_flagged_gas", 0))
            eligible_products = sum(1 for product in normalized_products if not bool(getattr(product, "excluded", False)))
            status_parts = [
                f"Processing Complete - {len(normalized_products)} products processed.",
                f"Rows considered: {rows_considered}",
                f"Eligible for export/push: {eligible_products}",
            ]
            if rows_skipped_no_shopify_match:
                status_parts.append(f"Skipped (no Shopify match): {rows_skipped_no_shopify_match}")
            if rows_skipped_missing_sku:
                status_parts.append(f"Skipped (missing SKU): {rows_skipped_missing_sku}")
            if rows_flagged_gas:
                status_parts.append(f"Excluded (gas-only): {rows_flagged_gas}")

            if self.session.missing_fields:
                status_parts.append(f"Missing fields: {', '.join(self.session.missing_fields)}")
            if should_scrape:
                status_parts.append(f"Scraped: {len(scrape_records)} SKU hits")
                if scrape_sku_errors:
                    status_parts.append(f"Scrape SKU failures: {len(scrape_sku_errors)}")
                    first_error = next(iter(scrape_sku_errors.values()), "")
                    if first_error:
                        status_parts.append(f"First scrape error: {first_error}")
                if scrape_general_errors:
                    status_parts.append(f"Scrape warnings: {len(scrape_general_errors)}")
                downloaded_images = sum(
                    len([part for part in re.split(r"[|,\n]+", str(record.get("media_local_files", ""))) if part.strip()])
                    for record in scrape_records.values()
                )
                if downloaded_images:
                    status_parts.append(f"Downloaded images: {downloaded_images}")
            elif can_scrape:
                if image_scrape_needed:
                    status_parts.append("Scraper skipped unexpectedly for images. Enable Force Scrape and retry.")
                else:
                    status_parts.append("Scraper skipped: mapped data already covers requested fields.")
            self.processing_status_text.set(" | ".join(status_parts))
            self.to_review_btn.configure(state="normal")
            self.review_refresh_pending = True
            if self._auto_open_review_after_processing:
                self._open_review_tab()
            self._auto_open_review_after_processing = False

        self._run_on_ui_thread(apply)

    def _apply_scrape_diagnostics(
        self,
        products,
        target_skus: list[str],
        should_scrape: bool,
        scrape_records: dict[str, dict[str, str]],
        scrape_sku_errors: dict[str, str],
    ) -> None:
        records = {normalize_sku(sku): payload for sku, payload in (scrape_records or {}).items()}
        errors = {normalize_sku(sku): str(error).strip() for sku, error in (scrape_sku_errors or {}).items()}

        compact_record_keys: dict[str, dict[str, str]] = {}
        for key, payload in records.items():
            compact = self._compact_sku_for_partial_match(key)
            if compact and compact not in compact_record_keys:
                compact_record_keys[compact] = payload

        compact_error_keys: dict[str, str] = {}
        for key, value in errors.items():
            compact = self._compact_sku_for_partial_match(key)
            if compact and compact not in compact_error_keys:
                compact_error_keys[compact] = value

        def lookup_payload(sku_value: str) -> dict[str, str]:
            normalized = normalize_sku(sku_value)
            payload = records.get(normalized, {})
            if payload:
                return payload
            compact = self._compact_sku_for_partial_match(normalized)
            if compact and compact in compact_record_keys:
                return compact_record_keys[compact]
            if not compact:
                return {}
            for key, candidate_payload in records.items():
                candidate_compact = self._compact_sku_for_partial_match(key)
                if not candidate_compact:
                    continue
                if candidate_compact.endswith(compact) or compact.endswith(candidate_compact):
                    return candidate_payload
            return {}

        def lookup_error(sku_value: str) -> str:
            normalized = normalize_sku(sku_value)
            error_text = errors.get(normalized, "")
            if error_text:
                return error_text
            compact = self._compact_sku_for_partial_match(normalized)
            if compact and compact in compact_error_keys:
                return compact_error_keys[compact]
            if not compact:
                return ""
            for key, candidate_error in errors.items():
                candidate_compact = self._compact_sku_for_partial_match(key)
                if not candidate_compact:
                    continue
                if candidate_compact.endswith(compact) or compact.endswith(candidate_compact):
                    return candidate_error
            return ""

        product_by_sku = {normalize_sku(product.sku): product for product in products if normalize_sku(product.sku)}
        scoped_skus = [normalize_sku(sku) for sku in target_skus if normalize_sku(sku)]
        found_fields_order = [
            "title",
            "description_html",
            "media_urls",
            "price",
            "map_price",
            "msrp_price",
            "jobber_price",
            "cost",
            "dealer_cost",
            "core_charge_product_code",
            "barcode",
            "weight",
            "application",
            "vendor",
            "media_local_files",
        ]

        for sku in scoped_skus:
            product = product_by_sku.get(sku)
            if product is None:
                continue
            if not should_scrape:
                product.scrape_status = "not run"
                product.scrape_fields_found = ""
                product.scrape_error = ""
                continue

            payload = lookup_payload(sku)
            if payload:
                found = [field for field in found_fields_order if str(payload.get(field, "")).strip()]
                product.scrape_status = "success check"
                provider_value = str(payload.get("search_provider", "")).strip().lower()
                if provider_value.endswith("_fuzzy"):
                    found.append("fuzzy_match")
                product.scrape_fields_found = ", ".join(found)
                parse_error = str(payload.get("extract_error", "")).strip()
                image_error = str(payload.get("image_download_error", "")).strip()
                fuzzy_warning = ""
                if provider_value.endswith("_fuzzy"):
                    fuzzy_warning = "Fuzzy SKU match from search provider. Verify product selection."
                product.scrape_error = image_error or parse_error or fuzzy_warning
            else:
                product.scrape_status = "fail X"
                product.scrape_fields_found = ""
                product.scrape_error = lookup_error(sku) or "No scrape data found"

            media_folder = str(payload.get("media_folder", "")).strip()
            if media_folder:
                try:
                    product.media_folder = Path(media_folder).name or media_folder
                except Exception:
                    product.media_folder = media_folder

    def _refresh_review_tab(self) -> None:
        total = len(self.session.products)
        if total == 0:
            self.push_selected_skus = set()
            self.review_index = 0
            self.review_index_text.set("Product 0 / 0")
            for var in self.review_fields.values():
                var.set("")
            self.review_cost_rule_text.set("")
            self.review_cost_options = []
            self.review_cost_option_map = {}
            if hasattr(self, "review_cost_rule_combo"):
                self.review_cost_rule_combo.configure(values=())
                self.review_cost_rule_combo.configure(state="disabled")
            self._cancel_review_table_refresh()
            _tree_show_dataframe(self.review_table, pd.DataFrame())
            self.review_loaded_raw = {}
            self.review_loaded_display = {}
            self.review_loaded_truncated = {}
            self.review_cost_options_loaded_for_sku = ""
            self.review_status_text.set("No products available yet. Run Processing first.")
            return
        self.review_index = max(0, min(self.review_index, total - 1))
        self._load_review_product(self.review_index)
        _tree_show_dataframe(self.review_table, pd.DataFrame())
        self.review_status_text.set("Review form loaded. Click 'Load All Products Preview' if needed.")

    def _load_review_product(self, index: int) -> None:
        if not self.session.products:
            return
        index = max(0, min(index, len(self.session.products) - 1))
        self.review_index = index
        product = self.session.products[index]
        row = product.to_row()
        self.review_loaded_raw = {}
        self.review_loaded_display = {}
        self.review_loaded_truncated = {}
        for field_name, var in self.review_fields.items():
            raw = str(row.get(field_name, ""))
            display, truncated = self._display_field_value(field_name, raw)
            self.review_loaded_raw[field_name] = raw
            self.review_loaded_display[field_name] = display
            self.review_loaded_truncated[field_name] = truncated
            var.set(display)
        self.review_cost_option_map = {}
        self.review_cost_options = []
        self.review_cost_options_loaded_for_sku = ""
        vendor_label = str(product.vendor or "").strip() or "Select discount rule"
        if hasattr(self, "review_cost_rule_combo"):
            self.review_cost_rule_combo.configure(values=())
            self.review_cost_rule_combo.configure(state="readonly")
        self.review_cost_rule_text.set(vendor_label)
        self.review_index_text.set(f"Product {index + 1} / {len(self.session.products)}")
        self._highlight_review_table_current_product()

    def _save_current_review_product(self) -> None:
        if not self.session.products:
            return
        product = self.session.products[self.review_index]
        product.title = self._read_review_field("title")
        product.description_html = self._read_review_field("description_html")
        media_text = self._read_review_field("media_urls")
        product.media_urls = [part.strip() for part in re.split(r"[|,\n]+", media_text) if part.strip()]
        product.price = self._read_review_field("price")
        product.map_price = self._read_review_field("map_price")
        product.msrp_price = self._read_review_field("msrp_price")
        product.jobber_price = self._read_review_field("jobber_price")
        product.cost = self._read_review_field("cost")
        product.dealer_cost = self._read_review_field("dealer_cost")
        default_inventory = int(self.session.inventory_default or 3000000)
        try:
            product.inventory = int(float(self._read_review_field("inventory") or str(default_inventory)))
        except Exception:
            product.inventory = default_inventory
        product.sku = self._read_review_field("sku").upper()
        product.barcode = self._read_review_field("barcode")
        product.weight = self._read_review_field("weight")
        product.vendor = self._read_review_field("vendor")
        product.type = self._read_review_field("type")
        product.google_product_type = self._read_review_field("google_product_type")
        product.category_code = self._read_review_field("category_code")
        product.product_subtype = self._read_review_field("product_subtype")
        product.mpn = self._read_review_field("mpn")
        product.brand = self._read_review_field("brand")
        product.application = self._read_review_field("application")
        product.core_charge_product_code = self._read_review_field("core_charge_product_code")
        product.finalize_defaults()
        self.review_status_text.set(f"Saved product {self.review_index + 1}.")

    def _review_prev(self) -> None:
        if not self.session.products:
            return
        self._save_current_review_product()
        self._load_review_product(self.review_index - 1)

    def _review_next(self) -> None:
        if not self.session.products:
            return
        self._save_current_review_product()
        self._load_review_product(self.review_index + 1)

    def _export_review_products(self) -> None:
        export_products = [product for product in (self.session.products or []) if not bool(getattr(product, "excluded", False))]
        df = products_to_dataframe(export_products)
        if df.empty:
            messagebox.showinfo(APP_TITLE, "No processed products to export.")
            return
        for column in PRODUCT_EXPORT_COLUMNS:
            if column not in df.columns:
                df[column] = ""
        self._export_dataframe(df[PRODUCT_EXPORT_COLUMNS], "product_prospector_products.csv")

    def _set_shopify_push_busy(self, busy: bool) -> None:
        self.shopify_push_inflight = bool(busy)
        self._refresh_push_button_state()
        if not busy:
            self._hide_review_busy_overlay()

    def _refresh_push_button_state(self) -> None:
        if not hasattr(self, "push_shopify_btn"):
            return
        if self.shopify_push_inflight:
            self.push_shopify_btn.configure(state="disabled")
            return
        if self.session.mode != MODE_NEW:
            self.push_shopify_btn.configure(state="disabled")
            return
        eligible_count = sum(
            1
            for product in (self.session.products or [])
            if normalize_sku(getattr(product, "sku", "")) and not bool(getattr(product, "excluded", False))
        )
        if eligible_count <= 0:
            self.push_shopify_btn.configure(state="disabled")
            return
        self.push_shopify_btn.configure(state="normal")

    def _push_to_shopify_clicked(self) -> None:
        if self.shopify_push_inflight:
            return
        if self.session.mode != MODE_NEW:
            messagebox.showwarning(
                APP_TITLE,
                "Push to Shopify is create-only right now.\nUse Create New Products mode.",
            )
            return
        if not self.session.products:
            messagebox.showwarning(APP_TITLE, "No products available to push.")
            return

        self._save_current_review_product()
        selected = {normalize_sku(sku) for sku in self.push_selected_skus if normalize_sku(sku)}
        products = [
            product
            for product in list(self.session.products or [])
            if normalize_sku(getattr(product, "sku", "")) in selected and not bool(getattr(product, "excluded", False))
        ]
        if not products:
            messagebox.showwarning(APP_TITLE, "No selected rows to push. Check [x] next to at least one product.")
            return

        confirmed = messagebox.askyesno(
            APP_TITLE,
            (
                f"Create {len(products)} Shopify products as DRAFT?\n\n"
                "Safety checks:\n"
                "- Existing SKUs are skipped.\n"
                "- No existing products are updated.\n"
                "- Writes are create-only in the Products area.\n\n"
                "Continue?"
            ),
        )
        if not confirmed:
            return

        include_images_choice = messagebox.askyesnocancel(
            APP_TITLE,
            "Include images in this Shopify push?\n\nYes = Upload images\nNo = Skip images\nCancel = Abort push",
        )
        if include_images_choice is None:
            return
        include_images = bool(include_images_choice)

        config = load_shopify_config()
        if config is None:
            messagebox.showerror(APP_TITLE, "Invalid config/shopify.json. Cannot push to Shopify.")
            return
        token = load_shopify_token()
        if token is None:
            self._connect_shopify_worker(allow_handshake=False)
            token = load_shopify_token()
        if token is None:
            messagebox.showerror(APP_TITLE, "Shopify is not connected. Connect first and retry.")
            return

        image_mode_text = "with images" if include_images else "without images"
        self.review_status_text.set(f"Pushing {len(products)} products to Shopify drafts ({image_mode_text})...")
        self.review_busy_text.set("Checking existing SKUs in Shopify...")
        self._show_review_busy_overlay("Checking existing SKUs in Shopify...")
        self._set_shopify_push_busy(True)

        worker = threading.Thread(
            target=self._run_shopify_push_worker,
            kwargs={
                "config": config,
                "access_token": token.access_token,
                "products": products,
                "include_images": include_images,
            },
            daemon=True,
        )
        worker.start()

    def _run_shopify_push_worker(self, config, access_token: str, products: list, include_images: bool) -> None:
        requested_skus = [normalize_sku(getattr(product, "sku", "")) for product in products if normalize_sku(getattr(product, "sku", ""))]
        existing_df: pd.DataFrame | None = None
        existing_skus: set[str] = set()
        push_error: str | None = None
        summary = ShopifyDraftPushSummary(requested=len(products))

        def on_existing_progress(done: int, total: int) -> None:
            self._run_on_ui_thread(self.review_busy_text.set, f"Checking existing SKUs in Shopify... {done}/{total}")

        def on_push_progress(done: int, total: int, sku: str) -> None:
            label = f"Pushing draft products to Shopify... {done}/{total}"
            sku_text = normalize_sku(sku)
            if sku_text:
                label += f" | {sku_text}"
            self._run_on_ui_thread(self.review_busy_text.set, label)

        try:
            existing_skus, existing_error, existing_df = self._fetch_existing_shopify_skus(
                requested_skus,
                progress_callback=on_existing_progress,
                refresh_on_cache_miss=True,
            )
            if existing_error:
                push_error = f"Could not verify existing Shopify SKUs: {existing_error}"
            else:
                summary = push_new_products_as_drafts(
                    config=config,
                    access_token=access_token,
                    products=products,
                    existing_skus=existing_skus,
                    include_images=include_images,
                    image_root=self.runtime_output_root / "images",
                    required_root=self.required_root,
                    progress_callback=on_push_progress,
                )
        except Exception as exc:
            push_error = str(exc)

        def apply() -> None:
            self._set_shopify_push_busy(False)
            if isinstance(existing_df, pd.DataFrame) and not existing_df.empty:
                self.shopify_df_raw = existing_df
                self._refresh_input_metrics()

            if push_error:
                self.review_status_text.set(f"Shopify push failed: {push_error}")
                messagebox.showerror(APP_TITLE, f"Shopify push failed:\n{push_error}")
                return

            created = len(summary.created_skus)
            skipped = len(summary.skipped_existing_skus)
            failed = len(summary.failed_by_sku)
            warnings_count = len(summary.warnings)
            self.review_status_text.set(
                f"Shopify draft push complete: created {created}, skipped existing {skipped}, failed {failed}."
            )

            image_mode_text = "Yes" if include_images else "No"
            details: list[str] = [
                f"Requested: {summary.requested}",
                f"Created (draft): {created}",
                f"Skipped (already exists): {skipped}",
                f"Failed: {failed}",
                f"Images included: {image_mode_text}",
            ]
            if warnings_count:
                details.append(f"Warnings: {warnings_count}")

            if summary.failed_by_sku:
                details.append("")
                details.append("Failure samples:")
                for sku, error in list(summary.failed_by_sku.items())[:6]:
                    details.append(f"- {sku}: {error}")

            if summary.warnings:
                details.append("")
                details.append("Warning samples:")
                for item in summary.warnings[:5]:
                    details.append(f"- {item}")

            messagebox.showinfo(APP_TITLE, "\n".join(details))

        self._run_on_ui_thread(apply)

    def _select_all_for_push(self) -> None:
        products = self.session.products or []
        self.push_selected_skus = {
            normalize_sku(getattr(product, "sku", ""))
            for product in products
            if normalize_sku(getattr(product, "sku", "")) and not bool(getattr(product, "excluded", False))
        }
        self._refresh_review_table_async()

    def _clear_push_selection(self) -> None:
        self.push_selected_skus = set()
        self._refresh_review_table_async()

    def _on_review_table_click(self, event) -> None:
        if not self.session.products:
            return
        tree = self.review_table
        region = tree.identify_region(event.x, event.y)
        if region == "heading":
            column_id = tree.identify_column(event.x)
            if column_id == "#1":
                self._toggle_review_table_push_selection()
                return "break"
            return

        row_id = tree.identify_row(event.y)
        column_id = tree.identify_column(event.x)
        if not row_id:
            return
        sku_value = normalize_sku(tree.set(row_id, "sku"))
        if not sku_value:
            return
        if column_id != "#1":
            target_index = self._find_product_index_by_sku(sku_value)
            if target_index is None:
                return
            if target_index != self.review_index:
                self._save_current_review_product()
                self._load_review_product(target_index)
            self.review_status_text.set(f"Loaded {sku_value} in Product Review.")
            return

        selected_product = None
        for product in self.session.products:
            if normalize_sku(getattr(product, "sku", "")) == sku_value:
                selected_product = product
                break
        if selected_product is not None and bool(getattr(selected_product, "excluded", False)):
            self.review_status_text.set(str(getattr(selected_product, "exclusion_reason", "") or "Excluded from push/export"))
            return "break"
        if sku_value in self.push_selected_skus:
            self.push_selected_skus.remove(sku_value)
        else:
            self.push_selected_skus.add(sku_value)
        self._refresh_review_table_async()

    def _start_background_api_bootstrap(self) -> None:
        if self._background_connect_running or self._shutdown_requested:
            return
        self._background_connect_running = True
        worker = threading.Thread(target=self._background_api_bootstrap, daemon=True)
        worker.start()

    def _cached_shopify_sku_count(self) -> int:
        try:
            return int(len(load_shopify_sku_cache()))
        except Exception:
            return 0

    def _initialize_shopify_cache_state(self) -> None:
        cached_df = load_shopify_sku_cache()
        self.shopify_cache_ready = cached_df is not None and not cached_df.empty
        if self.shopify_cache_ready and (self.shopify_df_raw is None or self.shopify_df_raw.empty):
            self.shopify_df_raw = cached_df
        self._set_shopify_cache_api_busy(False)
        self._refresh_shopify_cache_action_buttons()
        self._refresh_new_mode_check_controls()

    def _refresh_shopify_cache_action_buttons(self) -> None:
        if not hasattr(self, "shopify_cache_newest_button"):
            return
        can_run = self.shopify_connected and not self.shopify_connecting and not self.shopify_cache_warmup_inflight
        state = "normal" if can_run else "disabled"
        self.shopify_cache_newest_button.configure(state=state)
        self.shopify_cache_redownload_button.configure(state=state)

    def _download_newest_shopify_skus_clicked(self) -> None:
        if self.shopify_connecting or self.shopify_cache_warmup_inflight:
            return
        if not self.shopify_connected:
            self._connect_shopify_clicked()
            if not self.shopify_connected:
                return
        cached_df = load_shopify_sku_cache()
        if cached_df is None or cached_df.empty:
            self._start_background_shopify_cache_warmup(force_refresh=True)
            return
        self._start_background_shopify_cache_newest_refresh()

    def _redownload_shopify_sku_cache_clicked(self) -> None:
        if self.shopify_connecting or self.shopify_cache_warmup_inflight:
            return
        if not self.shopify_connected:
            self._connect_shopify_clicked()
            if not self.shopify_connected:
                return
        self._start_background_shopify_cache_warmup(force_refresh=True)

    def _start_background_shopify_cache_newest_refresh(self) -> None:
        if self.shopify_cache_warmup_inflight:
            return
        worker = threading.Thread(target=self._background_shopify_cache_newest_refresh, daemon=True)
        self.shopify_cache_warmup_inflight = True
        self._run_on_ui_thread(self._refresh_shopify_cache_action_buttons)
        worker.start()

    def _background_shopify_cache_newest_refresh(self) -> None:
        try:
            cached = load_shopify_sku_cache()
            if cached is None:
                cached = pd.DataFrame()
            if cached.empty:
                self._background_shopify_cache_warmup(force_refresh=True)
                return

            config = load_shopify_config()
            if config is None:
                return

            token = load_shopify_token()
            if token is None:
                return

            cache_path = get_shopify_sku_cache_path()
            try:
                last_sync_utc = datetime.fromtimestamp(cache_path.stat().st_mtime, tz=timezone.utc)
                created_since = last_sync_utc.date().isoformat()
            except Exception:
                created_since = datetime.now(timezone.utc).date().isoformat()
            search_query = f"created_at:>={created_since}"
            start_count = int(len(cached))
            cached_skus = set(
                cached["sku"]
                .astype(str)
                .map(normalize_sku)
                .replace("", pd.NA)
                .dropna()
                .tolist()
            )
            overlap_state = {"known_streak": 0}
            discovered_new_skus: set[str] = set()
            self._set_shopify_cache_api_busy(
                True,
                f"Downloading newest Shopify SKUs since {created_since}... {start_count:,} cached",
            )

            def on_catalog_progress(page_count: int, row_count: int) -> None:
                self._set_shopify_cache_api_busy(
                    True,
                    (
                        f"Scanning newest Shopify SKUs since {created_since}... "
                        f"pages {page_count} | scanned {row_count:,} | new {len(discovered_new_skus):,}"
                    ),
                )

            def stop_when_page(page_rows: list[dict[str, str]], _page_count: int, _row_count: int) -> bool:
                page_skus = {
                    normalize_sku(str((row or {}).get("sku", "")))
                    for row in page_rows
                    if normalize_sku(str((row or {}).get("sku", "")))
                }
                if not page_skus:
                    overlap_state["known_streak"] += 1
                    return overlap_state["known_streak"] >= 2

                unseen = {sku for sku in page_skus if sku not in cached_skus and sku not in discovered_new_skus}
                if unseen:
                    discovered_new_skus.update(unseen)
                    overlap_state["known_streak"] = 0
                    return False

                overlap_state["known_streak"] += 1
                # Once we hit two consecutive pages that contain only already-cached SKUs,
                # we've likely reached the overlap boundary and can stop early.
                return overlap_state["known_streak"] >= 2

            df, error = fetch_shopify_catalog_dataframe(
                config=config,
                access_token=token.access_token,
                search_query=search_query,
                sort_key="CREATED_AT",
                reverse=True,
                stop_when_page=stop_when_page,
                progress_callback=on_catalog_progress,
            )
            if error:
                df, error = fetch_shopify_catalog_dataframe(
                    config=config,
                    access_token=token.access_token,
                    sort_key="CREATED_AT",
                    reverse=True,
                    stop_when_page=stop_when_page,
                    progress_callback=on_catalog_progress,
                )
                if error:
                    return
            if df is None or df.empty:
                self.shopify_cache_ready = not cached.empty
                self.shopify_df_raw = cached
                return

            if "sku" not in df.columns:
                return

            df = df.copy()
            df["sku"] = df["sku"].astype(str).str.strip()
            df = df[df["sku"] != ""].copy()
            df["sku_norm"] = df["sku"].astype(str).map(normalize_sku)
            df = df[df["sku_norm"] != ""].copy()
            df = df.drop_duplicates(subset=["sku_norm"], keep="first")
            new_rows = df[~df["sku_norm"].isin(cached_skus)].copy()
            if new_rows.empty:
                self.shopify_cache_ready = not cached.empty
                self.shopify_df_raw = cached
                return
            new_rows = new_rows.drop(columns=["sku_norm"], errors="ignore")
            merged = pd.concat([new_rows, cached], ignore_index=True)
            merged["sku_norm"] = merged["sku"].astype(str).map(normalize_sku)
            merged = merged[merged["sku_norm"] != ""].copy()
            merged = merged.drop_duplicates(subset=["sku_norm"], keep="first")
            merged = merged.drop(columns=["sku_norm"], errors="ignore").reset_index(drop=True)

            save_shopify_sku_cache(merged)
            self.shopify_cache_ready = True
            self.shopify_df_raw = merged
        finally:
            self.shopify_cache_warmup_inflight = False
            self._set_shopify_cache_api_busy(False)
            self._run_on_ui_thread(self._refresh_shopify_cache_action_buttons)
            self._run_on_ui_thread(self._refresh_new_mode_check_controls)
            self._run_on_ui_thread(self._refresh_input_metrics)

    def _start_background_shopify_cache_warmup(self, force_refresh: bool = False) -> None:
        if self.shopify_cache_warmup_inflight:
            return
        worker = threading.Thread(
            target=self._background_shopify_cache_warmup,
            kwargs={"force_refresh": bool(force_refresh)},
            daemon=True,
        )
        self.shopify_cache_warmup_inflight = True
        self._run_on_ui_thread(self._refresh_shopify_cache_action_buttons)
        worker.start()

    def _background_shopify_cache_warmup(self, force_refresh: bool = False) -> None:
        try:
            cached = load_shopify_sku_cache()
            if cached is not None and not cached.empty:
                self.shopify_cache_ready = True
                if self.shopify_df_raw is None or self.shopify_df_raw.empty:
                    self.shopify_df_raw = cached
                if not force_refresh:
                    return

            config = load_shopify_config()
            if config is None:
                return

            token = load_shopify_token()
            if token is None:
                return

            start_count = 0
            if cached is not None and not cached.empty:
                start_count = int(len(cached))
            self._set_shopify_cache_api_busy(True, f"Downloading Shopify SKU cache... {start_count:,} SKUs")

            def on_catalog_progress(_page_count: int, row_count: int) -> None:
                self._set_shopify_cache_api_busy(True, f"Downloading Shopify SKU cache... {row_count:,} SKUs")

            df, error = fetch_shopify_catalog_dataframe(
                config=config,
                access_token=token.access_token,
                progress_callback=on_catalog_progress,
            )
            if error or df is None or df.empty:
                return
            save_shopify_sku_cache(df)
            self.shopify_cache_ready = True
            self.shopify_df_raw = df
        finally:
            self.shopify_cache_warmup_inflight = False
            self._set_shopify_cache_api_busy(False)
            self._run_on_ui_thread(self._refresh_shopify_cache_action_buttons)
            self._run_on_ui_thread(self._refresh_new_mode_check_controls)
            self._run_on_ui_thread(self._refresh_input_metrics)

    def _set_shopify_cache_api_busy(self, busy: bool, text: str = "") -> None:
        def apply() -> None:
            if busy:
                self.shopify_cache_api_text.set(text or "Downloading Shopify SKU cache...")
                if not self.shopify_cache_api_spinner.winfo_ismapped():
                    self.shopify_cache_api_spinner.pack(side=LEFT, padx=(10, 0))
                if not self.shopify_cache_api_label.winfo_ismapped():
                    self.shopify_cache_api_label.pack(side=LEFT, padx=(6, 0))
                self._start_shopify_cache_spinner()
                return
            self._stop_shopify_cache_spinner()
            if self.shopify_cache_api_spinner.winfo_ismapped():
                self.shopify_cache_api_spinner.pack_forget()
            if self.shopify_cache_api_label.winfo_ismapped():
                self.shopify_cache_api_label.pack_forget()
            self.shopify_cache_api_text.set("")

        self._run_on_ui_thread(apply)

    def _draw_shopify_cache_spinner_frame(self) -> None:
        canvas = self.shopify_cache_api_spinner
        canvas.delete("all")
        canvas.create_oval(2, 2, 14, 14, outline="#B8C2CC", width=1)
        canvas.create_arc(
            2,
            2,
            14,
            14,
            start=self.shopify_cache_spinner_angle,
            extent=92,
            style="arc",
            outline="#0FA34A",
            width=2,
        )

    def _animate_shopify_cache_spinner(self) -> None:
        self.shopify_cache_spinner_job = None
        if not self.shopify_cache_api_spinner.winfo_exists() or not self.shopify_cache_api_spinner.winfo_ismapped():
            return
        self._draw_shopify_cache_spinner_frame()
        self.shopify_cache_spinner_angle = (self.shopify_cache_spinner_angle + 28) % 360
        try:
            self.shopify_cache_spinner_job = self.root.after(85, self._animate_shopify_cache_spinner)
        except RuntimeError:
            self.shopify_cache_spinner_job = None

    def _start_shopify_cache_spinner(self) -> None:
        if self.shopify_cache_spinner_job is not None:
            return
        self._draw_shopify_cache_spinner_frame()
        try:
            self.shopify_cache_spinner_job = self.root.after(85, self._animate_shopify_cache_spinner)
        except RuntimeError:
            self.shopify_cache_spinner_job = None

    def _stop_shopify_cache_spinner(self) -> None:
        if self.shopify_cache_spinner_job is not None:
            try:
                self.root.after_cancel(self.shopify_cache_spinner_job)
            except Exception:
                pass
            self.shopify_cache_spinner_job = None
        if self.shopify_cache_api_spinner.winfo_exists():
            self.shopify_cache_api_spinner.delete("all")

    def _draw_review_busy_spinner_frame(self) -> None:
        if not hasattr(self, "review_busy_spinner") or not self.review_busy_spinner.winfo_exists():
            return
        canvas = self.review_busy_spinner
        canvas.delete("all")
        canvas.create_oval(2, 2, 16, 16, outline="#C4C9D1", width=1)
        canvas.create_arc(
            2,
            2,
            16,
            16,
            start=self.review_busy_spinner_angle,
            extent=98,
            style="arc",
            outline="#1F4E79",
            width=2,
        )

    def _animate_review_busy_spinner(self) -> None:
        self.review_busy_spinner_job = None
        if not self.review_busy_active:
            return
        if not hasattr(self, "review_busy_spinner") or not self.review_busy_spinner.winfo_exists():
            return
        self._draw_review_busy_spinner_frame()
        self.review_busy_spinner_angle = (self.review_busy_spinner_angle + 24) % 360
        try:
            self.review_busy_spinner_job = self.root.after(85, self._animate_review_busy_spinner)
        except RuntimeError:
            self.review_busy_spinner_job = None

    def _show_review_busy_overlay(self, message: str) -> None:
        if not hasattr(self, "review_busy_overlay"):
            return
        self.review_busy_text.set(message or "Reprocessing...")
        self.review_busy_active = True
        self.review_busy_overlay.place(relx=0, rely=0, relwidth=1, relheight=1)
        self.review_busy_overlay.lift()
        self._draw_review_busy_spinner_frame()
        if self.review_busy_spinner_job is None:
            try:
                self.review_busy_spinner_job = self.root.after(85, self._animate_review_busy_spinner)
            except RuntimeError:
                self.review_busy_spinner_job = None

    def _hide_review_busy_overlay(self) -> None:
        self.review_busy_active = False
        if self.review_busy_spinner_job is not None:
            try:
                self.root.after_cancel(self.review_busy_spinner_job)
            except Exception:
                pass
            self.review_busy_spinner_job = None
        if hasattr(self, "review_busy_spinner") and self.review_busy_spinner.winfo_exists():
            self.review_busy_spinner.delete("all")
        if hasattr(self, "review_busy_overlay"):
            self.review_busy_overlay.place_forget()

    def _draw_shopify_dot(self, state: str) -> None:
        self.shopify_dot.delete("all")
        if state == "connected":
            self.shopify_dot.create_oval(1, 1, 15, 15, fill="#9CFFD6", outline="")
            self.shopify_dot.create_oval(3, 3, 13, 13, fill="#34D058", outline="")
            self.shopify_dot.create_oval(5, 5, 11, 11, fill="#0FA34A", outline="")
            return
        if state == "connecting":
            self.shopify_dot.create_oval(2, 2, 14, 14, fill="#FFD66E", outline="")
            self.shopify_dot.create_oval(4, 4, 12, 12, fill="#D9A527", outline="")
            return
        self.shopify_dot.create_oval(3, 3, 13, 13, fill="#D9534F", outline="")

    def _set_shopify_status(self, connected: bool, connecting: bool = False) -> None:
        def apply() -> None:
            self.shopify_connecting = connecting
            if connecting:
                self.shopify_status_label.configure(text="Shopify - Connecting...")
                self._draw_shopify_dot(state="connecting")
                self.shopify_connect_button.configure(text="Connecting...", state="disabled")
            elif connected:
                self.shopify_status_label.configure(text="Shopify - Connected")
                self._draw_shopify_dot(state="connected")
                # Match Windows behavior: reconnect action is disabled when already connected.
                self.shopify_connect_button.configure(text="Reconnect", state="disabled")
            else:
                self.shopify_status_label.configure(text="Shopify - Not Connected")
                self._draw_shopify_dot(state="disconnected")
                prior_auth = self.shopify_ever_connected or (load_shopify_token() is not None)
                self.shopify_connect_button.configure(text="Reconnect" if prior_auth else "Connect", state="normal")
            self.shopify_connected = connected
            if connected:
                self.shopify_ever_connected = True
            self._refresh_shopify_cache_action_buttons()
            if connected:
                # Avoid re-downloading full catalog on every reconnect.
                # If cache is already present, warmup uses cached rows and returns quickly.
                self._start_background_shopify_cache_warmup(force_refresh=not self.shopify_cache_ready)
            else:
                self._set_shopify_cache_api_busy(False)
                if self.shopify_ever_connected and not connecting and not self._shutdown_requested:
                    self._start_background_api_bootstrap()

        self._run_on_ui_thread(apply)

    def _connect_shopify_clicked(self) -> None:
        if self.shopify_connecting:
            return
        if self.shopify_connected:
            return
        # Run connect flow on the UI thread so status updates and prompts cannot silently fail.
        # Background bootstrap/check flows still use worker threads.
        try:
            self._connect_shopify_worker(allow_handshake=True)
        except Exception as exc:
            self._set_shopify_status(connected=False)
            messagebox.showerror(APP_TITLE, f"Shopify connect failed:\n{exc}")

    def _set_shopify_auto_retrying(self) -> None:
        def apply() -> None:
            prior_auth = self.shopify_ever_connected or (load_shopify_token() is not None)
            self.shopify_connecting = False
            self.shopify_connected = False
            if prior_auth:
                self.shopify_status_label.configure(text="Shopify - Reconnecting...")
                self._draw_shopify_dot(state="connected")
            else:
                self.shopify_status_label.configure(text="Shopify - Not Connected (auto retrying...)")
                self._draw_shopify_dot(state="disconnected")
            self.shopify_connect_button.configure(text="Reconnect" if prior_auth else "Connect", state="disabled")
            self._refresh_shopify_cache_action_buttons()

        self._run_on_ui_thread(apply)

    def _background_api_bootstrap(self) -> None:
        attempt = 0
        try:
            while not self._shutdown_requested:
                if self.shopify_connected:
                    return
                self._set_shopify_auto_retrying()
                try:
                    connected = bool(self._connect_shopify_worker(allow_handshake=False))
                except Exception:
                    connected = False
                    self._set_shopify_status(connected=False)
                if connected:
                    return
                attempt += 1
                sleep_seconds = min(60.0, 2.0 + (attempt * 3.0))
                end_at = time.monotonic() + sleep_seconds
                while time.monotonic() < end_at:
                    if self._shutdown_requested or self.shopify_connected:
                        return
                    time.sleep(0.2)
        finally:
            self._background_connect_running = False

    def _connect_shopify_worker(self, allow_handshake: bool) -> bool:
        try:
            self._set_shopify_status(connected=False, connecting=True)
            config = load_shopify_config()
            if config is None:
                self._set_shopify_status(connected=False)
                if allow_handshake:
                    self._run_on_ui_thread(messagebox.showerror, APP_TITLE, "Invalid config/shopify.json")
                return False

            if config.admin_api_access_token:
                token_valid, reason = validate_access_token(config=config, access_token=config.admin_api_access_token)
                if token_valid:
                    save_shopify_token(access_token=config.admin_api_access_token, scope="admin_api_access_token")
                    self._set_shopify_status(connected=True)
                    return True
                self._set_shopify_status(connected=False)
                if allow_handshake:
                    self._run_on_ui_thread(
                        messagebox.showerror,
                        APP_TITLE,
                        "Configured admin_api_access_token is invalid.\n"
                        f"Validation error: {reason}",
                    )
                return False

            existing_token = load_shopify_token()
            if existing_token:
                valid, _ = validate_access_token(config=config, access_token=existing_token.access_token)
                if valid:
                    self._set_shopify_status(connected=True)
                    return True

            if config.auth_mode in {"auto", "client_credentials"}:
                cc_result = exchange_client_credentials_for_token(config=config)
                if cc_result.success:
                    save_shopify_token(access_token=cc_result.access_token, scope=cc_result.scope or "client_credentials")
                    self._set_shopify_status(connected=True)
                    return True
                if config.auth_mode == "client_credentials":
                    self._set_shopify_status(connected=False)
                    if allow_handshake:
                        self._run_on_ui_thread(
                            messagebox.showerror,
                            APP_TITLE,
                            "Shopify client_credentials failed.\n"
                            f"{cc_result.error}",
                        )
                    return False

            if not allow_handshake:
                self._set_shopify_status(connected=False)
                return False

            if config.auth_mode not in {"auto", "oauth"}:
                self._set_shopify_status(connected=False)
                return False

            handshake = perform_oauth_handshake(config)
            if not handshake.success:
                self._set_shopify_status(connected=False)
                self._run_on_ui_thread(messagebox.showerror, APP_TITLE, f"Shopify connect failed:\n{handshake.error}")
                return False

            save_shopify_token(access_token=handshake.access_token, scope=handshake.scope)
            self._set_shopify_status(connected=True)
            return True
        except Exception as exc:
            self._set_shopify_status(connected=False)
            if allow_handshake:
                self._run_on_ui_thread(messagebox.showerror, APP_TITLE, f"Shopify connect failed:\n{exc}")
            return False

    def _toggle_advanced_mode(self) -> None:
        return

    def _on_run_mode_changed(self, *_args) -> None:
        mode = self.run_mode.get().strip()
        if not mode:
            self.run_mode_locked.set(False)
            self._set_setup_workflow_visible(False)
            self.setup_status_text.set("Select a Run Mode to begin.")
            self.sku_scope_help_text.set("Enter SKUs that need to be updated or added.")
            self.create_existing_skus = set()
            self.create_duplicate_scope = ()
            self.duplicate_check_text.set("")
            self._set_duplicate_check_busy(False)
            self._set_setup_mode_widgets_enabled(False)
            self.update_fields_wrap.pack_forget()
            self.session.mode = ""
            self._refresh_mode_lock_ui()
            self._update_tab_access()
            self._refresh_sku_action_labels()
            return

        if self._mode_initialized and not self.run_mode_locked.get():
            self.run_mode_locked.set(True)

        self._set_setup_workflow_visible(True)
        if mode == RUN_MODE_UPDATE:
            self.session.mode = MODE_UPDATE
            self.sku_scope_help_text.set("Enter SKUs that need to be updated.")
            self.create_existing_skus = set()
            self.create_duplicate_scope = ()
            self.duplicate_check_text.set("")
            self._set_duplicate_check_busy(False)
            self.mode_help_text.set(
                "Update Existing Products: Finds matching SKUs and proposes field changes (fitment/title/etc). "
                "SKUs not found are skipped."
            )
            self.update_fields_wrap.pack(fill=X, pady=(0, 8), before=self.setup_status)
            self.setup_continue_btn.configure(text="Save & Continue to Scraping")
        elif mode == RUN_MODE_CREATE:
            self.session.mode = MODE_NEW
            self.sku_scope_help_text.set("Enter SKUs that need to be added as new products.")
            cached_count = 0
            try:
                cached_count = int(len(load_shopify_sku_cache()))
            except Exception:
                cached_count = 0
            if cached_count:
                self.duplicate_check_text.set(f"Shopify SKU cache ready: {cached_count} SKU(s).")
            else:
                self.duplicate_check_text.set("Shopify SKU cache is downloading. SKU check buttons enable when ready.")
            self._set_duplicate_check_busy(False)
            self.mode_help_text.set(
                "Create New Products: Treats input as candidates for new product creation. "
                "If Shopify export is loaded, already-existing SKUs can be excluded."
            )
            self.update_fields_wrap.pack_forget()
            self.setup_continue_btn.configure(text="Save & Continue to Scraping")
        else:
            self.session.mode = ""
            self.mode_help_text.set("Select a valid run mode.")
        self._set_setup_mode_widgets_enabled(True)
        self.setup_status_text.set(f"Run Mode selected: {merge_mode_label(self.session.mode)}")
        self._refresh_mode_lock_ui()
        self._refresh_sku_action_labels()
        self._refresh_new_mode_check_controls()
        self._refresh_input_metrics()
        self._refresh_push_button_state()

    def _set_setup_mode_widgets_enabled(self, enabled: bool) -> None:
        self.setup_widgets_enabled = enabled
        state = "normal" if enabled else "disabled"
        for widget in getattr(self, "setup_mode_widgets", []):
            try:
                widget.configure(state=state)
            except Exception:
                continue
        self.sku_text_widget.configure(state=state)
        if not enabled:
            self._set_duplicate_check_busy(False)
            self.setup_continue_btn.configure(state="disabled")
            self._refresh_new_mode_check_controls()
            return
        self.setup_continue_btn.configure(state="normal")
        self._refresh_new_mode_check_controls()

    def _refresh_new_mode_check_controls(self) -> None:
        if not hasattr(self, "load_pasted_btn"):
            return

        base_state = "normal" if self.setup_widgets_enabled else "disabled"
        if base_state == "disabled":
            self.load_pasted_btn.configure(state="disabled")
            self.use_all_sheet_check.configure(state="disabled")
            return

        if self.session.mode != MODE_NEW:
            self.load_pasted_btn.configure(state=base_state)
            self.use_all_sheet_check.configure(state=base_state)
            return

        check_state = "normal" if self.shopify_cache_ready else "disabled"
        self.load_pasted_btn.configure(state=check_state)
        self.use_all_sheet_check.configure(state=check_state)

    def _refresh_sku_action_labels(self) -> None:
        if self.session.mode == MODE_NEW:
            self.load_pasted_btn.configure(text="Load and Check SKUs")
            self.load_sheet_btn.configure(text="Load Price Sheet and Check SKUs")
            self.use_all_sheet_check.configure(text="Use all SKUs from uploaded spreadsheet (and check Shopify)")
            return
        self.load_pasted_btn.configure(text="Load Pasted SKUs")
        self.load_sheet_btn.configure(text="Load Vendor Price Sheet (CSV/XLSX)")
        self.use_all_sheet_check.configure(text="Use all SKUs from uploaded spreadsheet")

    def _set_duplicate_check_busy(self, busy: bool) -> None:
        if busy:
            self._duplicate_check_inflight = True
            self._duplicate_check_started_at = time.monotonic()
            if not self.duplicate_check_progress.winfo_ismapped():
                self.duplicate_check_progress.pack(anchor=W, fill=X, pady=(4, 0))
            self.duplicate_check_progress.configure(mode="determinate", maximum=100, value=0)
            return
        self._duplicate_check_inflight = False
        self._duplicate_check_active_workers = 0
        self._duplicate_check_pending_scope = ()
        self._duplicate_check_started_at = 0.0
        if self.duplicate_check_progress.winfo_ismapped():
            self.duplicate_check_progress.pack_forget()

    def _set_duplicate_check_progress(self, current: int, total: int, text: str | None = None) -> None:
        def apply() -> None:
            if not self._duplicate_check_inflight:
                return
            safe_total = max(1, int(total))
            safe_current = max(0, min(int(current), safe_total))
            if not self.duplicate_check_progress.winfo_ismapped():
                self.duplicate_check_progress.pack(anchor=W, fill=X, pady=(4, 0))
            self.duplicate_check_progress.configure(mode="determinate", maximum=safe_total, value=safe_current)
            if text is not None:
                self.duplicate_check_text.set(text)

        if threading.current_thread() is threading.main_thread():
            apply()
            return
        self._run_on_ui_thread(apply)

    def _on_use_all_sheet_toggle(self) -> None:
        self._refresh_input_metrics()
        if self.session.mode != MODE_NEW:
            return
        if not self.shopify_cache_ready:
            self.duplicate_check_text.set("Shopify SKU cache is downloading. SKU check buttons enable when ready.")
            return
        scope_skus = self._create_scope_skus_for_duplicate_check()
        if not scope_skus:
            if self.use_all_sheet_skus.get():
                self.duplicate_check_text.set("Load a spreadsheet and map SKU to run duplicate check.")
            return
        self._queue_create_duplicate_check(scope_skus)

    def _on_vendor_sku_mapping_changed(self) -> None:
        self._refresh_input_metrics()
        if self.session.mode != MODE_NEW or not self.use_all_sheet_skus.get():
            return
        if not self.shopify_cache_ready:
            self.duplicate_check_text.set("Shopify SKU cache is downloading. SKU check buttons enable when ready.")
            return
        scope_skus = self._sheet_scope_skus()
        if not scope_skus:
            self.duplicate_check_text.set("Map SKU column to run duplicate check for spreadsheet scope.")
            return
        self._queue_create_duplicate_check(scope_skus)

    def _sheet_scope_skus(self) -> list[str]:
        if not self.vendor_source_is_sheet or self.vendor_df_raw is None or self.vendor_df_raw.empty:
            return []
        sku_col = self.vendor_sku_column.get().strip()
        if not sku_col or sku_col not in self.vendor_df_raw.columns:
            return []
        return (
            self.vendor_df_raw[sku_col]
            .astype(str)
            .map(normalize_sku)
            .replace("", pd.NA)
            .dropna()
            .drop_duplicates()
            .tolist()
        )

    def _create_scope_skus_for_duplicate_check(self) -> list[str]:
        if self.session.mode != MODE_NEW:
            return []
        if self.use_all_sheet_skus.get():
            return self._sheet_scope_skus()
        return self._parse_sku_text(self.sku_text_widget.get("1.0", END))

    def _compact_sku_for_partial_match(self, value: str) -> str:
        return re.sub(r"[^A-Z0-9]", "", normalize_sku(value))

    def _match_requested_skus_against_shopify_skus(
        self,
        requested_skus: list[str],
        shopify_skus: list[str],
        progress_callback=None,
    ) -> set[str]:
        requested_ordered = list(
            dict.fromkeys(normalize_sku(sku) for sku in requested_skus if normalize_sku(sku))
        )
        if not requested_ordered:
            return set()

        shopify_norm = [normalize_sku(sku) for sku in shopify_skus if normalize_sku(sku)]
        if not shopify_norm:
            if progress_callback is not None:
                try:
                    progress_callback(len(requested_ordered), len(requested_ordered))
                except Exception:
                    pass
            return set()

        shopify_set = set(shopify_norm)
        shopify_compact = [self._compact_sku_for_partial_match(sku) for sku in shopify_norm]
        shopify_compact = [value for value in shopify_compact if value]
        shopify_compact_set = set(shopify_compact)
        requested_compact = {sku: self._compact_sku_for_partial_match(sku) for sku in requested_ordered}

        lengths = sorted({len(value) for value in requested_compact.values() if value})
        suffix_lookup: dict[int, set[str]] = {}
        if shopify_compact and lengths:
            compact_candidates = list(dict.fromkeys(shopify_compact))
            for length in lengths:
                suffix_lookup[length] = {candidate[-length:] for candidate in compact_candidates if len(candidate) >= length}

        matched_requested: set[str] = set()
        total = len(requested_ordered)
        for index, requested in enumerate(requested_ordered, start=1):
            matched = requested in shopify_set
            if not matched:
                compact = requested_compact.get(requested, "")
                if compact and compact in shopify_compact_set:
                    matched = True
                elif compact:
                    matched = compact in suffix_lookup.get(len(compact), set())
            if matched:
                matched_requested.add(requested)
            if progress_callback is not None:
                try:
                    progress_callback(index, total)
                except Exception:
                    pass
        return matched_requested

    def _fetch_existing_shopify_skus(
        self,
        skus: list[str],
        progress_callback=None,
        refresh_on_cache_miss: bool = False,
    ) -> tuple[set[str], str | None, pd.DataFrame | None]:
        normalized_skus = [normalize_sku(sku) for sku in skus if normalize_sku(sku)]
        if not normalized_skus:
            return set(), None, pd.DataFrame(columns=["sku"])

        config = load_shopify_config()
        if config is None:
            return set(), "Invalid config/shopify.json.", None

        token = load_shopify_token()
        if token is None:
            self._connect_shopify_worker(allow_handshake=False)
            token = load_shopify_token()
        if token is None:
            return set(), "Shopify is not connected.", None

        # Fast path: use in-memory catalog if already available, then local cache.
        df = self.shopify_df_raw.copy() if self.shopify_df_raw is not None and not self.shopify_df_raw.empty else None
        used_cached_catalog = False
        if df is None or df.empty:
            cached_df = load_shopify_sku_cache()
            if cached_df is not None and not cached_df.empty:
                df = cached_df
                used_cached_catalog = True

        # First-time or missing cache: full read-only catalog download and local save.
        if df is None or df.empty:
            fallback_df, fallback_error = fetch_shopify_catalog_dataframe(config=config, access_token=token.access_token)
            if fallback_error:
                return set(), fallback_error, None
            df = fallback_df if fallback_df is not None else pd.DataFrame(columns=["sku"])
            if not df.empty:
                save_shopify_sku_cache(df)
            used_cached_catalog = False

        if "sku" not in df.columns:
            return set(), None, df

        shopify_skus = df["sku"].astype(str).tolist()
        existing = self._match_requested_skus_against_shopify_skus(
            requested_skus=normalized_skus,
            shopify_skus=shopify_skus,
            progress_callback=progress_callback,
        )

        # Optional refresh path for stale cache. Off by default to keep duplicate checks fast.
        if not existing and used_cached_catalog and refresh_on_cache_miss:
            fallback_df, fallback_error = fetch_shopify_catalog_dataframe(config=config, access_token=token.access_token)
            if fallback_error:
                return set(), fallback_error, None
            if fallback_df is not None:
                df = fallback_df
                if not df.empty:
                    save_shopify_sku_cache(df)
                if "sku" in df.columns:
                    existing = self._match_requested_skus_against_shopify_skus(
                        requested_skus=normalized_skus,
                        shopify_skus=df["sku"].astype(str).tolist(),
                        progress_callback=progress_callback,
                    )
        return existing, None, df

    def _queue_create_duplicate_check(self, skus: list[str]) -> None:
        if self.session.mode != MODE_NEW:
            return
        normalized = [normalize_sku(sku) for sku in skus if normalize_sku(sku)]
        if not normalized:
            self.create_existing_skus = set()
            self.create_duplicate_scope = ()
            self.duplicate_check_text.set("No scoped SKUs to check.")
            self._set_duplicate_check_busy(False)
            return

        self._duplicate_check_request_id += 1
        request_id = self._duplicate_check_request_id
        has_memory_catalog = self.shopify_df_raw is not None and not self.shopify_df_raw.empty
        has_cached_catalog = False
        if not has_memory_catalog:
            try:
                has_cached_catalog = not load_shopify_sku_cache().empty
            except Exception:
                has_cached_catalog = False
        if has_memory_catalog or has_cached_catalog:
            self.duplicate_check_text.set(f"Checking Shopify for {len(normalized)} scoped SKU(s)...")
        else:
            self.duplicate_check_text.set(
                f"Building local Shopify SKU cache (one-time read-only sync), then checking {len(normalized)} SKU(s)..."
            )
        self._set_duplicate_check_busy(True)
        self._duplicate_check_pending_scope = tuple(normalized)
        self._duplicate_check_active_workers += 1
        self._set_duplicate_check_progress(
            0,
            len(normalized),
            f"Checking Shopify for {len(normalized)} scoped SKU(s)... 0/{len(normalized)}",
        )

        def worker() -> None:
            update_step = max(1, len(normalized) // 100)
            last_progress = {"value": 0}

            def on_progress(done: int, total: int) -> None:
                if total <= 0:
                    return
                if done < total and done - last_progress["value"] < update_step:
                    return
                last_progress["value"] = done
                self._set_duplicate_check_progress(
                    done,
                    total,
                    f"Checking Shopify for {total} scoped SKU(s)... {done}/{total}",
                )

            existing: set[str] = set()
            error: str | None = None
            df: pd.DataFrame | None = None
            try:
                existing, error, df = self._fetch_existing_shopify_skus(normalized, progress_callback=on_progress)
            except Exception as exc:
                error = str(exc)

            def apply() -> None:
                self._duplicate_check_active_workers = max(0, self._duplicate_check_active_workers - 1)
                if request_id != self._duplicate_check_request_id:
                    if self._duplicate_check_active_workers <= 0:
                        self._set_duplicate_check_busy(False)
                    return
                self._set_duplicate_check_busy(False)
                if error:
                    self.create_existing_skus = set()
                    self.create_duplicate_scope = ()
                    self.duplicate_check_text.set(f"Shopify duplicate check unavailable: {error}")
                    return

                if df is not None:
                    self.shopify_df_raw = df
                    self._refresh_input_metrics()
                self.create_existing_skus = set(existing)
                self.create_duplicate_scope = tuple(normalized)
                if not existing:
                    self.duplicate_check_text.set(
                        f"Shopify duplicate check complete: none found across {len(normalized)} scoped SKU(s)."
                    )
                    return

                existing_ordered = [sku for sku in normalized if sku in existing]
                preview = ", ".join(existing_ordered[:12])
                more = "" if len(existing_ordered) <= 12 else f" (+{len(existing_ordered) - 12} more)"
                self.duplicate_check_text.set(
                    f"{len(existing_ordered)} SKU(s) already exist and will be excluded: {preview}{more}"
                )

            self._run_on_ui_thread(apply)

        threading.Thread(target=worker, daemon=True).start()

    def _ensure_create_duplicate_check(self, skus: list[str]) -> tuple[set[str], str | None]:
        normalized = [normalize_sku(sku) for sku in skus if normalize_sku(sku)]
        if not normalized:
            return set(), None

        if tuple(normalized) == self.create_duplicate_scope:
            return set(self.create_existing_skus), None

        has_memory_catalog = self.shopify_df_raw is not None and not self.shopify_df_raw.empty
        has_cached_catalog = False
        if not has_memory_catalog:
            try:
                has_cached_catalog = not load_shopify_sku_cache().empty
            except Exception:
                has_cached_catalog = False
        if has_memory_catalog or has_cached_catalog:
            self.duplicate_check_text.set(f"Checking Shopify for {len(normalized)} scoped SKU(s)...")
        else:
            self.duplicate_check_text.set(
                f"Building local Shopify SKU cache (one-time read-only sync), then checking {len(normalized)} SKU(s)..."
            )
        self._set_duplicate_check_busy(True)
        self._set_duplicate_check_progress(
            0,
            len(normalized),
            f"Checking Shopify for {len(normalized)} scoped SKU(s)... 0/{len(normalized)}",
        )
        update_step = max(1, len(normalized) // 100)
        last_progress = 0

        def on_progress(done: int, total: int) -> None:
            nonlocal last_progress
            if total <= 0:
                return
            if done < total and done - last_progress < update_step:
                return
            last_progress = done
            self._set_duplicate_check_progress(
                done,
                total,
                f"Checking Shopify for {total} scoped SKU(s)... {done}/{total}",
            )
            self.root.update_idletasks()

        self.root.update_idletasks()
        existing, error, df = self._fetch_existing_shopify_skus(normalized, progress_callback=on_progress)
        self._set_duplicate_check_busy(False)
        if error:
            return set(), error

        self.create_existing_skus = set(existing)
        self.create_duplicate_scope = tuple(normalized)
        if df is not None:
            self.shopify_df_raw = df
            self._refresh_input_metrics()

        if not existing:
            self.duplicate_check_text.set(f"Shopify duplicate check complete: none found across {len(normalized)} scoped SKU(s).")
        else:
            existing_ordered = [sku for sku in normalized if sku in existing]
            preview = ", ".join(existing_ordered[:12])
            more = "" if len(existing_ordered) <= 12 else f" (+{len(existing_ordered) - 12} more)"
            self.duplicate_check_text.set(
                f"{len(existing_ordered)} SKU(s) already exist and will be excluded: {preview}{more}"
            )
        return existing, None

    def _selected_update_fields(self) -> list[str]:
        selected: list[str] = []
        if self.update_title.get():
            selected.append("title")
        if self.update_price.get():
            selected.append("price")
        if self.update_cost.get():
            selected.append("cost")
        if self.update_description.get():
            selected.append("description_html")
        if self.update_images.get():
            selected.append("media_urls")
        if self.update_category_fields.get():
            selected.extend(["type", "google_product_type", "category_code", "product_subtype"])
        if self.update_vendor.get():
            selected.append("vendor")
        if self.update_weight.get():
            selected.append("weight")
        if self.update_barcode.get():
            selected.append("barcode")
        if self.update_application.get():
            selected.append("application")
        return selected

    def _update_tab_access(self) -> None:
        if not hasattr(self, "notebook"):
            return
        if not self.session.setup_complete:
            self.notebook.tab(1, state="disabled")
            self.notebook.tab(2, state="disabled")
            return
        self.notebook.tab(1, state="normal")
        if self.session.processing_complete and self.review_tab_unlocked:
            self.notebook.tab(2, state="normal")
            return
        self.notebook.tab(2, state="disabled")

    def _capture_setup_to_session(
        self,
        show_messages: bool = True,
        preserve_review_state: bool = False,
    ) -> bool:
        if not self.session.mode:
            if show_messages:
                messagebox.showwarning(APP_TITLE, "Select a Run Mode first.")
            return False

        pasted_skus = self._parse_sku_text(self.sku_text_widget.get("1.0", END))
        has_sheet = self.vendor_source_is_sheet and self.vendor_df_raw is not None and not self.vendor_df_raw.empty
        use_all_sheet_skus = bool(self.use_all_sheet_skus.get())

        if use_all_sheet_skus and not has_sheet:
            if show_messages:
                messagebox.showwarning(APP_TITLE, "Load a vendor spreadsheet before using all spreadsheet SKUs.")
            return False
        self._enforce_unique_vendor_mappings()

        self.session.source_mapping.vendor = self.vendor_vendor_column.get().strip()
        self.session.source_mapping.title = self.vendor_title_column.get().strip()
        self.session.source_mapping.description = self.vendor_description_column.get().strip()
        # Always keep media unmapped so image URLs are sourced from scraping.
        self.session.source_mapping.media = ""
        self.session.source_mapping.price = self.vendor_price_column.get().strip()
        # Price source precedence comes from required/rules/pricing_priority_rules.json.
        self.session.source_mapping.map_price = ""
        self.session.source_mapping.msrp_price = ""
        self.session.source_mapping.jobber_price = ""
        self.session.source_mapping.cost = self.vendor_cost_column.get().strip()
        self.session.source_mapping.dealer_cost = ""
        self.session.source_mapping.core_charge_product_code = self.vendor_core_charge_column.get().strip()
        self.session.source_mapping.sku = self.vendor_sku_column.get().strip()
        self.session.source_mapping.barcode = self.vendor_barcode_column.get().strip()
        self.session.source_mapping.weight = self.vendor_weight_column.get().strip()
        self.session.source_mapping.application = self.vendor_fitment_column.get().strip()
        self.session.inventory_default = _inventory_for_owner(self.inventory_owner.get())
        if not has_sheet:
            self.session.source_mapping.sku = "sku"

        sheet_scope_skus: list[str] = []
        filtered_vendor_df: pd.DataFrame | None = None
        sheet_rows_matched = 0
        if has_sheet:
            sku_column = self.session.source_mapping.sku
            if not sku_column:
                if show_messages:
                    messagebox.showwarning(APP_TITLE, "Map the SKU column before continuing.")
                return False
            if sku_column not in self.vendor_df_raw.columns:
                if show_messages:
                    messagebox.showwarning(APP_TITLE, "Mapped SKU column is not in the loaded spreadsheet.")
                return False

            sheet_scope_skus = (
                self.vendor_df_raw[sku_column]
                .astype(str)
                .map(normalize_sku)
                .replace("", pd.NA)
                .dropna()
                .drop_duplicates()
                .tolist()
            )
            if not sheet_scope_skus:
                if show_messages:
                    messagebox.showwarning(
                        APP_TITLE,
                        "Mapped SKU column has no valid SKU values.\n\nSelect the correct SKU column before continuing.",
                    )
                return False

        if use_all_sheet_skus:
            target_skus = sheet_scope_skus
        else:
            target_skus = pasted_skus

        if not target_skus:
            if show_messages:
                if has_sheet and not use_all_sheet_skus:
                    messagebox.showwarning(
                        APP_TITLE,
                        "Paste SKUs or enable 'Use all SKUs from uploaded spreadsheet'.",
                    )
                else:
                    messagebox.showwarning(APP_TITLE, "Provide at least one valid SKU before continuing.")
            return False

        existing_create_skus: set[str] = set()
        if self.session.mode == MODE_NEW:
            target_scope = tuple(target_skus)
            if not self.shopify_cache_ready:
                # Do not block setup/processing when cache is not ready.
                # Keep cache warmup running in background (when token/config allow).
                self.duplicate_check_text.set(
                    "Continuing without Shopify duplicate check. SKU cache sync will continue in background."
                )
                if target_scope == self.create_duplicate_scope:
                    existing_create_skus = set(self.create_existing_skus)
                self._start_background_shopify_cache_warmup()
                threading.Thread(
                    target=self._connect_shopify_worker,
                    kwargs={"allow_handshake": False},
                    daemon=True,
                ).start()
            else:
                if self._duplicate_check_inflight and self._duplicate_check_active_workers <= 0:
                    self._set_duplicate_check_busy(False)
                if self._duplicate_check_inflight:
                    elapsed = 0.0
                    if self._duplicate_check_started_at > 0:
                        elapsed = time.monotonic() - self._duplicate_check_started_at
                    if elapsed > 120.0:
                        self._set_duplicate_check_busy(False)
                        self.duplicate_check_text.set("Previous duplicate check timed out and was reset.")
                    elif self._duplicate_check_pending_scope and self._duplicate_check_pending_scope != target_scope:
                        self._queue_create_duplicate_check(target_skus)
                        if show_messages:
                            messagebox.showinfo(
                                APP_TITLE,
                                "A previous duplicate check was running for a different SKU scope.\n\nStarted a new check for your current scope. Press Save & Continue again when it completes.",
                            )
                        return False
                    else:
                        if show_messages:
                            messagebox.showinfo(
                                APP_TITLE,
                                "Shopify duplicate check is still running in background.\n\nWait for status to finish, then press Save & Continue again.",
                            )
                        return False

                if target_scope != self.create_duplicate_scope:
                    self._queue_create_duplicate_check(target_skus)
                    if show_messages:
                        messagebox.showinfo(
                            APP_TITLE,
                            "Running Shopify duplicate check in background for your current SKU scope.\n\nPress Save & Continue again when it completes.",
                        )
                    return False

                existing_create_skus = set(self.create_existing_skus)
            if existing_create_skus:
                target_skus = [sku for sku in target_skus if sku not in existing_create_skus]
                if not target_skus:
                    if show_messages:
                        messagebox.showinfo(APP_TITLE, "All scoped SKUs already exist in Shopify. Nothing to create.")
                    self.setup_status_text.set("All scoped SKUs already exist in Shopify.")
                    return False

        if has_sheet:
            sku_column = self.session.source_mapping.sku
            working = self.vendor_df_raw.copy()
            working["_norm_sku"] = working[sku_column].astype(str).map(normalize_sku)
            working = working[working["_norm_sku"] != ""].copy()
            target_set = set(target_skus)
            filtered_vendor_df = working[working["_norm_sku"].isin(target_set)].copy()
            sheet_rows_matched = len(filtered_vendor_df)
            if sheet_rows_matched == 0:
                if show_messages:
                    messagebox.showwarning(
                        APP_TITLE,
                        "No scoped SKUs matched the loaded spreadsheet using the mapped SKU column.\n\nCheck SKU mapping before continuing.",
                    )
                return False
            filtered_vendor_df.drop(columns=["_norm_sku"], inplace=True, errors="ignore")

        self.session.vendor_df = filtered_vendor_df if has_sheet else None
        self.session.pasted_skus = target_skus
        self.session.target_skus = target_skus

        if self.session.mode == MODE_UPDATE:
            selected = self._selected_update_fields()
            if not selected:
                if show_messages:
                    messagebox.showwarning(APP_TITLE, "Select at least one update field for Update mode.")
                return False
            self.session.update_fields = selected
        else:
            self.session.update_fields = []

        self.session.missing_fields = detect_missing_required_fields(self.session, required_root=self.required_root)
        self.session.setup_complete = True
        if not preserve_review_state:
            self.session.processing_complete = False
            self.session.products = []
            self.review_tab_unlocked = False
        self.review_refresh_pending = False
        self.review_refresh_inflight = False
        if not preserve_review_state:
            self._cancel_review_table_refresh()
            self._hide_review_busy_overlay()
            self.review_loaded_raw = {}
            self.review_loaded_display = {}
            self.review_loaded_truncated = {}
            self.review_cost_options_loaded_for_sku = ""
        if has_sheet:
            self.source_status_text.set(
                f"SKU scope: {len(target_skus)} | Spreadsheet rows matched to scope: {sheet_rows_matched}"
            )
        else:
            self.source_status_text.set(f"SKU scope: {len(target_skus)} | Spreadsheet: not loaded")
        if self.session.mode == MODE_NEW and existing_create_skus:
            self.rules_status.configure(
                text=f"Excluded existing Shopify SKUs from Create scope: {len(existing_create_skus)}"
            )
        return True

    def _continue_from_setup(self) -> None:
        if not self._capture_setup_to_session(show_messages=True):
            return

        missing_text = ", ".join(self.session.missing_fields) or "none"
        self.processing_status_text.set(
            f"Setup saved for {merge_mode_label(self.session.mode)}. Missing fields: {missing_text}"
        )
        self.review_status_text.set("")
        self._update_tab_access()
        if self.session.missing_fields:
            self.setup_status_text.set("Setup saved. Missing fields found; continue in Processing to scrape.")
            self.notebook.select(1)
            return

        self.setup_status_text.set("Setup saved. All required fields mapped; generating review data now.")
        self.notebook.select(1)
        self._start_processing_clicked(auto_open_review=True)

    def _reprocess_from_review(self) -> None:
        if not self._capture_setup_to_session(show_messages=True, preserve_review_state=True):
            return
        self.setup_status_text.set("Remap applied. Reprocessing now...")
        self.processing_status_text.set("Reprocessing with updated mappings...")
        self.review_status_text.set("Reprocessing in background...")
        self._show_review_busy_overlay("Reprocessing remapped products...")
        started = self._start_processing_clicked(auto_open_review=False)
        if not started:
            self._hide_review_busy_overlay()

    def _refresh_vendor_sheet_ui(self) -> None:
        has_vendor_sheet_rows = self.vendor_source_is_sheet and self.vendor_df_raw is not None and not self.vendor_df_raw.empty

        self.vendor_preview_wrap.pack_forget()
        self.vendor_mapping_wrap.pack_forget()
        if has_vendor_sheet_rows:
            self.vendor_mapping_wrap.pack(fill=X, pady=(0, 8))
            self.vendor_preview_wrap.pack(fill=X, pady=(0, 8))

    def _parse_sku_text(self, raw_text: str) -> list[str]:
        tokens = re.split(r"[\s,;|]+", raw_text or "")
        seen: set[str] = set()
        values: list[str] = []
        for token in tokens:
            sku = normalize_sku(token)
            if not sku or sku in seen:
                continue
            seen.add(sku)
            values.append(sku)
        return values

    def _clear_pasted_skus(self) -> None:
        self.sku_text_widget.delete("1.0", END)
        self.sku_text_status.set("")
        if self.session.mode == MODE_NEW and not self.use_all_sheet_skus.get():
            self.create_existing_skus = set()
            self.create_duplicate_scope = ()
            self.duplicate_check_text.set("Shopify duplicate check runs when SKU scope is loaded.")
        self._refresh_input_metrics()

    def _set_vendor_dataframe(self, df: pd.DataFrame, path_text: str, source_is_sheet: bool) -> None:
        normalized_df = _sanitize_dataframe_columns(df)
        self.vendor_df_raw = normalized_df
        self.vendor_df_stitched = None
        self.plan_df = None
        self.vendor_source_is_sheet = source_is_sheet
        self.vendor_path.set(path_text)
        _tree_show_dataframe(self.vendor_preview, _safe_head(normalized_df))
        self._bind_vendor_columns(list(normalized_df.columns))
        self._auto_suggest_vendor()
        sample_columns = ", ".join(list(normalized_df.columns)[:8])
        if len(normalized_df.columns) > 8:
            sample_columns += ", ..."
        self.source_status_text.set(
            f"Vendor input loaded: {len(normalized_df)} rows, {len(normalized_df.columns)} columns. {sample_columns}"
        )
        self._refresh_input_metrics()
        self._refresh_vendor_sheet_ui()
        if self.session.mode == MODE_NEW and self.use_all_sheet_skus.get():
            scope_skus = self._sheet_scope_skus()
            if scope_skus:
                self._queue_create_duplicate_check(scope_skus)

    def _load_vendor_file(self) -> None:
        path = filedialog.askopenfilename(
            title="Select Vendor File",
            filetypes=[("Spreadsheet", "*.csv *.xlsx *.xls"), ("All Files", "*.*")],
        )
        if not path:
            return
        try:
            df = read_table_from_path(path)
        except Exception as exc:
            messagebox.showerror(APP_TITLE, f"Could not read vendor file:\n{exc}")
            return
        if len(df.columns) == 0:
            messagebox.showerror(
                APP_TITLE,
                "This file did not produce any columns. Confirm the file has a header row and values.",
            )
            return

        self._set_vendor_dataframe(df=df, path_text=path, source_is_sheet=True)

    def _load_pasted_skus(self) -> None:
        raw_text = self.sku_text_widget.get("1.0", END)
        skus = self._parse_sku_text(raw_text)
        if not skus:
            messagebox.showwarning(APP_TITLE, "No valid SKUs found in pasted text.")
            return

        self.sku_text_status.set(f"Found {len(skus)} unique SKUs in text scope")
        self.source_status_text.set(f"SKU text scope ready: {len(skus)} SKUs.")
        self._refresh_input_metrics()
        if self.session.mode == MODE_NEW and not self.use_all_sheet_skus.get():
            self._queue_create_duplicate_check(skus)

    def _refresh_input_metrics(self) -> None:
        vendor_skus = 0
        if self.vendor_df_raw is not None and not self.vendor_df_raw.empty:
            sku_col = self.vendor_sku_column.get().strip()
            if sku_col and sku_col in self.vendor_df_raw.columns:
                vendor_skus = int(self.vendor_df_raw[sku_col].astype(str).map(normalize_sku).replace("", pd.NA).dropna().nunique())
            else:
                vendor_skus = int(len(self.vendor_df_raw))

        scoped_skus = len(self._parse_sku_text(self.sku_text_widget.get("1.0", END)))
        if self.use_all_sheet_skus.get() and vendor_skus:
            scoped_skus = vendor_skus

        shopify_rows = int(len(self.shopify_df_raw)) if self.shopify_df_raw is not None else 0
        self.input_metrics_text.set(
            f"Vendor SKUs: {vendor_skus} | Scoped SKUs: {scoped_skus} | Shopify Catalog SKUs: {shopify_rows}"
        )

    def _vendor_mapping_var_pairs(self) -> list[tuple[str, StringVar]]:
        return [
            ("sku", self.vendor_sku_column),
            ("title", self.vendor_title_column),
            ("description", self.vendor_description_column),
            ("price", self.vendor_price_column),
            ("cost", self.vendor_cost_column),
            ("fitment", self.vendor_fitment_column),
            ("media", self.vendor_image_column),
            ("vendor", self.vendor_vendor_column),
            ("core_charge", self.vendor_core_charge_column),
            ("barcode", self.vendor_barcode_column),
            ("weight", self.vendor_weight_column),
        ]

    def _vendor_mapping_priority_order(self, preferred_field: str | None = None) -> list[str]:
        base_order = [
            "sku",
            "title",
            "description",
            "price",
            "cost",
            "fitment",
            "media",
            "vendor",
            "core_charge",
            "barcode",
            "weight",
        ]
        if not preferred_field or preferred_field not in base_order:
            return base_order
        if preferred_field == "sku":
            return base_order
        return ["sku", preferred_field, *[field for field in base_order if field not in {"sku", preferred_field}]]

    def _attach_vendor_mapping_traces(self) -> None:
        if self._vendor_mapping_trace_ready:
            return
        for field_name, variable in self._vendor_mapping_var_pairs():
            variable.trace_add("write", lambda *_args, field_name=field_name: self._on_vendor_mapping_var_changed(field_name))
        self._vendor_mapping_trace_ready = True

    def _on_vendor_mapping_var_changed(self, field_name: str) -> None:
        if self._vendor_mapping_enforcement_suspended > 0:
            return
        self._enforce_unique_vendor_mappings(preferred_field=field_name)

    def _enforce_unique_vendor_mappings(self, preferred_field: str | None = None) -> None:
        if self._vendor_mapping_enforcement_suspended > 0 or self._vendor_mapping_enforce_inflight:
            return

        mapping_vars = {name: var for name, var in self._vendor_mapping_var_pairs()}
        order = self._vendor_mapping_priority_order(preferred_field=preferred_field)
        if not order:
            return

        sku_before = mapping_vars["sku"].get().strip() if "sku" in mapping_vars else ""
        cleared = 0

        self._vendor_mapping_enforce_inflight = True
        try:
            seen: dict[str, str] = {}
            for field_name in order:
                variable = mapping_vars.get(field_name)
                if variable is None:
                    continue
                value = variable.get().strip()
                if not value:
                    continue
                if value in seen:
                    variable.set("")
                    cleared += 1
                    continue
                seen[value] = field_name
        finally:
            self._vendor_mapping_enforce_inflight = False

        sku_after = mapping_vars["sku"].get().strip() if "sku" in mapping_vars else ""
        if sku_before != sku_after:
            self._on_vendor_sku_mapping_changed()
        elif cleared > 0 and hasattr(self, "rules_status"):
            self.rules_status.configure(text=f"Removed {cleared} duplicate vendor mapping(s).")

    def _bind_vendor_columns(self, columns: list[str]) -> None:
        normalized = [str(column) for column in columns]
        required = normalized
        optional = [""] + normalized
        optional_widgets = [
            self.vendor_vendor_combo,
            self.vendor_title_combo,
            self.vendor_desc_combo,
            self.vendor_fitment_combo,
            self.vendor_image_combo,
            self.vendor_price_combo,
            self.vendor_cost_combo,
            self.vendor_core_charge_combo,
            self.vendor_barcode_combo,
            self.vendor_weight_combo,
            getattr(self, "remap_vendor_combo", None),
            getattr(self, "remap_title_combo", None),
            getattr(self, "remap_desc_combo", None),
            getattr(self, "remap_media_combo", None),
            getattr(self, "remap_price_combo", None),
            getattr(self, "remap_cost_combo", None),
            getattr(self, "remap_core_charge_combo", None),
            getattr(self, "remap_barcode_combo", None),
            getattr(self, "remap_weight_combo", None),
            getattr(self, "remap_application_combo", None),
        ]
        required_widgets = [self.vendor_sku_combo, getattr(self, "remap_sku_combo", None)]

        for widget in optional_widgets:
            if widget is None:
                continue
            _combobox_set_values(widget, optional)
        for widget in required_widgets:
            if widget is None:
                continue
            _combobox_set_values(widget, required)

    def _suggest_vendor_column_by_keywords(
        self,
        keywords: list[str],
        excluded_columns: set[str] | None = None,
    ) -> str | None:
        if self.vendor_df_raw is None:
            return None
        excluded = {str(column).strip() for column in (excluded_columns or set()) if str(column).strip()}
        lowered_keywords = [keyword.lower() for keyword in keywords]
        for column in self.vendor_df_raw.columns:
            name = str(column).lower()
            column_text = str(column).strip()
            if column_text and column_text in excluded:
                continue
            if any(keyword in name for keyword in lowered_keywords):
                return column_text
        return None

    def _suggest_barcode_column(self, excluded_columns: set[str] | None = None) -> str | None:
        if self.vendor_df_raw is None:
            return None
        excluded = {str(column).strip() for column in (excluded_columns or set()) if str(column).strip()}
        columns = [str(column).strip() for column in self.vendor_df_raw.columns]
        exact_aliases = {
            "upc",
            "upc code",
            "barcode",
            "bar code",
            "ean",
            "gtin",
            "upc ean",
            "ean upc",
            "gtin upc",
        }
        contains_aliases = ["upc", "barcode", "ean", "gtin"]

        def normalize_header(text: str) -> str:
            return re.sub(r"[^a-z0-9]+", " ", text.lower()).strip()

        for column in columns:
            if not column or column in excluded:
                continue
            if normalize_header(column) in exact_aliases:
                return column

        for column in columns:
            if not column or column in excluded:
                continue
            normalized = normalize_header(column)
            if any(alias in normalized for alias in contains_aliases):
                return column

        suggestion = suggest_column_for_field(self.vendor_df_raw, field_name="barcode", excluded_columns=excluded)
        picked = (suggestion.column or "").strip()
        return picked or None

    def _auto_suggest_vendor(self) -> None:
        if self.vendor_df_raw is None or self.vendor_df_raw.empty:
            return

        suggestion_order = [
            "sku",
            "title",
            "description",
            "fitment",
            "image_url",
            "map_price",
            "price",
            "cost",
            "core_charge_product_code",
        ]
        suggestions: dict[str, str] = {}
        used_columns: set[str] = set()
        # Reserve UPC/Barcode-style columns for Barcode mapping first so they
        # cannot be consumed by other fields.
        barcode_column = self._suggest_barcode_column(excluded_columns=used_columns) or ""
        if barcode_column:
            used_columns.add(barcode_column)
        for field_name in suggestion_order:
            suggestion = suggest_column_for_field(self.vendor_df_raw, field_name=field_name, excluded_columns=used_columns)
            column_name = (suggestion.column or "").strip()
            suggestions[field_name] = column_name
            if column_name:
                used_columns.add(column_name)
        if not barcode_column:
            barcode_column = self._suggest_barcode_column(excluded_columns=used_columns) or ""

        vendor_column = self._suggest_vendor_column_by_keywords(
            ["vendor", "brand", "manufacturer"],
            excluded_columns=used_columns,
        )
        if vendor_column:
            used_columns.add(vendor_column)
        weight_column = self._suggest_vendor_column_by_keywords(
            ["weight", "lbs", "pounds"],
            excluded_columns=used_columns,
        )

        valid_columns = {str(column).strip() for column in self.vendor_df_raw.columns}

        def _existing_or_blank(variable: StringVar) -> str:
            value = variable.get().strip()
            if not value or value not in valid_columns:
                return ""
            return value

        self._vendor_mapping_enforcement_suspended += 1
        try:
            self.vendor_vendor_column.set(vendor_column or _existing_or_blank(self.vendor_vendor_column))
            sku_column = suggestions["sku"] or _existing_or_blank(self.vendor_sku_column)
            if barcode_column and sku_column == barcode_column:
                sku_column = ""
            self.vendor_sku_column.set(sku_column)
            self.vendor_title_column.set(suggestions["title"] or _existing_or_blank(self.vendor_title_column))
            self.vendor_description_column.set(suggestions["description"] or _existing_or_blank(self.vendor_description_column))
            self.vendor_fitment_column.set(suggestions["fitment"] or _existing_or_blank(self.vendor_fitment_column))
            # Intentionally keep Media mapping blank so images are always scrape-driven.
            self.vendor_image_column.set("")
            # Pricing rule uses MAP first, so keep Price aligned to MAP when we can detect it.
            suggested_price = suggestions["map_price"] or suggestions["price"]
            self.vendor_price_column.set(suggested_price or _existing_or_blank(self.vendor_price_column))
            self.vendor_cost_column.set(suggestions["cost"] or _existing_or_blank(self.vendor_cost_column))
            self.vendor_core_charge_column.set(
                suggestions["core_charge_product_code"] or _existing_or_blank(self.vendor_core_charge_column)
            )
            self.vendor_barcode_column.set(barcode_column or _existing_or_blank(self.vendor_barcode_column))
            self.vendor_weight_column.set(weight_column or _existing_or_blank(self.vendor_weight_column))
        finally:
            self._vendor_mapping_enforcement_suspended = max(0, self._vendor_mapping_enforcement_suspended - 1)

        self._enforce_unique_vendor_mappings()

        self.rules_status.configure(text="Vendor auto-suggestions applied. Media stays unmapped to force image scraping.")
        self._refresh_input_metrics()
        self._on_vendor_sku_mapping_changed()

    def _stitch_vendor_rows(self) -> None:
        if self.vendor_df_raw is None or self.vendor_df_raw.empty:
            messagebox.showwarning(APP_TITLE, "Load vendor input first (spreadsheet or pasted SKUs).")
            return

        sku_column = self.vendor_sku_column.get().strip()
        if not sku_column:
            messagebox.showwarning(APP_TITLE, "Select the Vendor SKU column first.")
            return

        try:
            stitched = stitch_rows_by_sku(self.vendor_df_raw, sku_column, carry_down_sku=self.carry_down_sku.get())
        except Exception as exc:
            messagebox.showerror(APP_TITLE, f"Could not stitch rows:\n{exc}")
            return

        self.vendor_df_stitched = stitched
        self.plan_df = None
        _tree_show_dataframe(self.vendor_preview, _safe_head(stitched))
        self.rules_status.configure(
            text=f"Stitched vendor rows: {len(stitched)} SKU records from {len(self.vendor_df_raw)} source rows."
        )
        self._refresh_input_metrics()

    def _sync_shopify_catalog_for_matching(self, show_errors: bool, target_skus: list[str] | None = None) -> bool:
        config = load_shopify_config()
        if config is None:
            if show_errors:
                messagebox.showerror(APP_TITLE, "Invalid config/shopify.json. Cannot sync Shopify catalog.")
            return False

        existing_token = load_shopify_token()
        if existing_token is None:
            self._connect_shopify_worker(allow_handshake=False)
            existing_token = load_shopify_token()
        if existing_token is None:
            if show_errors:
                messagebox.showerror(
                    APP_TITLE,
                    "Shopify is not connected. Connect Shopify first, then try processing again.",
                )
            return False

        use_targeted = bool(target_skus)
        if use_targeted:
            df, error = fetch_shopify_catalog_for_skus(
                config=config,
                access_token=existing_token.access_token,
                skus=target_skus or [],
            )
            if not error and (df is None or df.empty):
                # Fall back when search syntax or index misses a subset.
                df, error = fetch_shopify_catalog_dataframe(config=config, access_token=existing_token.access_token)
        else:
            df, error = fetch_shopify_catalog_dataframe(config=config, access_token=existing_token.access_token)

        if error:
            if show_errors:
                messagebox.showerror(APP_TITLE, f"Shopify catalog sync failed:\n{error}")
            return False

        self.shopify_df_raw = df
        self._refresh_input_metrics()
        if use_targeted:
            self.rules_status.configure(text=f"Shopify targeted sync complete: {len(df)} SKU rows loaded.")
        else:
            self.rules_status.configure(text=f"Shopify catalog synced in background: {len(df)} SKU rows.")
        return True

    def _build_action_plan(self) -> None:
        if self.vendor_df_raw is None:
            messagebox.showwarning(APP_TITLE, "Load vendor input first.")
            return

        if self.vendor_df_stitched is None:
            self._stitch_vendor_rows()
            if self.vendor_df_stitched is None:
                return

        if self.run_mode.get() in {RUN_MODE_UPDATE, RUN_MODE_UPSERT} and (
            self.shopify_df_raw is None or self.shopify_df_raw.empty
        ):
            synced = self._sync_shopify_catalog_for_matching(show_errors=True)
            if not synced:
                return

        shopify_df_for_plan = self.shopify_df_raw

        vendor_year_columns = [
            column
            for column in [
                self.vendor_fitment_column.get().strip(),
                self.vendor_title_column.get().strip(),
                self.vendor_description_column.get().strip(),
            ]
            if column
        ]
        config = PlanningConfig(
            run_mode=self.run_mode.get(),
            year_policy=self.year_policy.get(),
            vendor_sku_column=self.vendor_sku_column.get().strip(),
            vendor_title_column=self.vendor_title_column.get().strip() or None,
            vendor_description_column=self.vendor_description_column.get().strip() or None,
            vendor_fitment_column=self.vendor_fitment_column.get().strip() or None,
            vendor_year_columns=vendor_year_columns,
            shopify_sku_column=self.shopify_sku_column.get().strip() or None,
            shopify_title_column=self.shopify_title_column.get().strip() or None,
            shopify_description_column=self.shopify_description_column.get().strip() or None,
            shopify_fitment_column=self.shopify_fitment_column.get().strip() or None,
            propose_title_year_update=self.propose_title_year_update.get(),
            only_rows_with_year_changes=self.only_rows_with_year_changes.get(),
        )

        try:
            plan = build_action_plan(vendor_df=self.vendor_df_stitched, shopify_df=shopify_df_for_plan, config=config)
        except Exception as exc:
            messagebox.showerror(APP_TITLE, f"Could not build action plan:\n{exc}")
            return

        self.plan_df = plan
        self._refresh_plan_preview()
        self.plan_status.configure(text=f"Action plan built with {len(plan)} rows.")

    def _filtered_plan(self) -> pd.DataFrame:
        if self.plan_df is None or self.plan_df.empty:
            return pd.DataFrame()

        allowed: list[str] = []
        if self.filter_update.get():
            allowed.append("update")
        if self.filter_create.get():
            allowed.append("create")
        if self.filter_skip.get():
            allowed.append("skip")
        if not allowed:
            return self.plan_df.iloc[0:0].copy()
        return self.plan_df[self.plan_df["row_action"].isin(allowed)].copy()

    def _refresh_plan_preview(self) -> None:
        plan = self._filtered_plan()
        _tree_show_dataframe(self.plan_preview, _safe_head(plan, rows=100))

        full = self.plan_df if self.plan_df is not None else pd.DataFrame()
        updates = int((full.get("row_action", pd.Series(dtype=str)) == "update").sum()) if not full.empty else 0
        creates = int((full.get("row_action", pd.Series(dtype=str)) == "create").sum()) if not full.empty else 0
        skips = int((full.get("row_action", pd.Series(dtype=str)) == "skip").sum()) if not full.empty else 0
        rows = int(len(full))

        self.plan_metrics.configure(
            text=f"Rows: {rows} | Update: {updates} | Create: {creates} | Skip: {skips}"
        )

    def _export_dataframe(self, df: pd.DataFrame, suggested_name: str) -> None:
        if df.empty:
            messagebox.showinfo(APP_TITLE, "There is no data to export.")
            return
        suggested_path = Path(suggested_name)
        default_name = (
            suggested_path.with_suffix(".xlsx").name
            if suggested_path.suffix.lower() == ".csv"
            else suggested_path.name
        )
        path = filedialog.asksaveasfilename(
            title="Save Export",
            defaultextension=".xlsx",
            initialfile=default_name,
            filetypes=[("Excel Workbook", "*.xlsx"), ("CSV", "*.csv")],
        )
        if not path:
            return
        export_df = df.copy()
        for column in export_df.columns:
            export_df[column] = (
                export_df[column]
                .fillna("")
                .map(lambda value: str(value).replace("\x00", "").replace("\r\n", " ").replace("\r", " ").replace("\n", " ").strip())
            )
        output_path = Path(path)
        try:
            if output_path.suffix.lower() == ".xlsx":
                with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
                    sheet_name = "Export"
                    export_df.to_excel(writer, index=False, sheet_name=sheet_name)
                    worksheet = writer.sheets.get(sheet_name)
                    if worksheet is not None:
                        header_to_column: dict[str, int] = {}
                        for header_cell in worksheet[1]:
                            header_text = str(header_cell.value).strip().lower() if header_cell.value is not None else ""
                            if header_text:
                                header_to_column[header_text] = int(header_cell.column)
                        for column_name in ("sku", "barcode", "mpn"):
                            column_index = header_to_column.get(column_name)
                            if not column_index:
                                continue
                            for row in worksheet.iter_rows(
                                min_row=2,
                                max_row=worksheet.max_row,
                                min_col=column_index,
                                max_col=column_index,
                            ):
                                cell = row[0]
                                if cell.value is None:
                                    continue
                                cell.value = str(cell.value).strip()
                                cell.number_format = "@"
            else:
                export_df.to_csv(
                    output_path,
                    index=False,
                    encoding="utf-8-sig",
                    quoting=csv.QUOTE_ALL,
                    lineterminator="\n",
                )
        except Exception as exc:
            messagebox.showerror(APP_TITLE, f"Could not save file:\n{exc}")
            return
        if output_path.suffix.lower() == ".csv":
            self.export_status.configure(
                text=f"Exported: {path} (Excel may auto-format long IDs; save as .xlsx to keep SKU/Barcode/MPN exact)."
            )
        else:
            self.export_status.configure(text=f"Exported: {path}")

    def _export_full_plan(self) -> None:
        if self.plan_df is None:
            messagebox.showinfo(APP_TITLE, "Build an action plan first.")
            return
        self._export_dataframe(self.plan_df, "product_prospector_action_plan.csv")

    def _export_unmatched(self) -> None:
        if self.plan_df is None:
            messagebox.showinfo(APP_TITLE, "Build an action plan first.")
            return
        unmatched = self.plan_df[self.plan_df["match_status"] == "unmatched"].copy()
        self._export_dataframe(unmatched, "product_prospector_unmatched_skus.csv")

    def _export_filtered(self) -> None:
        if self.plan_df is None:
            messagebox.showinfo(APP_TITLE, "Build an action plan first.")
            return
        filtered = self._filtered_plan()
        self._export_dataframe(filtered, "product_prospector_filtered_view.csv")

    def _export_create_template(self) -> None:
        if self.plan_df is None:
            messagebox.showinfo(APP_TITLE, "Build an action plan first.")
            return
        template = build_create_product_output(self.plan_df)
        if template.empty:
            messagebox.showinfo(APP_TITLE, "No create rows found in the current action plan.")
            return
        self._export_dataframe(template, "product_prospector_create_product_template.csv")

    def _load_settings(self) -> None:
        settings = load_app_settings()
        self.run_mode.set("")
        self.year_policy.set(settings.year_policy)
        self.carry_down_sku.set(settings.carry_down_sku)
        self.propose_title_year_update.set(settings.propose_title_year_update)
        self.only_rows_with_year_changes.set(settings.only_rows_with_year_changes)
        owner = str(getattr(settings, "inventory_owner", DEFAULT_INVENTORY_OWNER) or "").strip()
        if owner not in INVENTORY_BY_OWNER:
            owner = DEFAULT_INVENTORY_OWNER
        self.inventory_owner.set(owner)
        self._on_inventory_owner_changed()

    def _on_close(self) -> None:
        self._shutdown_requested = True
        self._cancel_header_logo_animation()
        if self._ui_task_pump_job is not None:
            try:
                self.root.after_cancel(self._ui_task_pump_job)
            except Exception:
                pass
            self._ui_task_pump_job = None
        self._stop_shopify_cache_spinner()
        self._hide_review_busy_overlay()
        settings = AppSettings(
            run_mode=self.run_mode.get(),
            year_policy=self.year_policy.get(),
            carry_down_sku=self.carry_down_sku.get(),
            propose_title_year_update=self.propose_title_year_update.get(),
            only_rows_with_year_changes=self.only_rows_with_year_changes.get(),
            inventory_owner=self.inventory_owner.get().strip() or DEFAULT_INVENTORY_OWNER,
        )
        try:
            save_app_settings(settings)
        except Exception:
            pass
        self.root.destroy()

    def run(self) -> None:
        self.root.mainloop()


def main() -> int:
    mutex_handle = _acquire_single_instance_mutex()
    if mutex_handle is None:
        _show_already_running_message()
        return 1
    try:
        app = ProductProspectorDesktopApp()
        app.run()
        return 0
    finally:
        _release_single_instance_mutex(mutex_handle)


if __name__ == "__main__":
    raise SystemExit(main())
