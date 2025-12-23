from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import List, Optional, Dict, Any

from fastapi import FastAPI, Request, Header, HTTPException
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel, Field

from server_db import ServerDB

app = FastAPI(title="Kiosk Sync Server", version="2.1.0")

# ---- server DB (Postgres if DATABASE_URL exists; else SQLite) ----
db = ServerDB()
db.init_schema()

# ---- templates/static ----
templates = Jinja2Templates(directory="templates")
app.mount("/static", StaticFiles(directory="static"), name="static")

# ---- demo menu ----
MENU_VERSION = 1
MENU_ITEMS = [
    {"id": "coffee_01", "name": "Americano", "price": 3000},
    {"id": "coffee_02", "name": "Latte", "price": 3500},
    {"id": "dessert_01", "name": "Cheesecake", "price": 4500},
]


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


# ---------- API Models ----------
class HealthResponse(BaseModel):
    ok: bool = True
    time_utc: str


class MenuItem(BaseModel):
    id: str
    name: str
    price: int


class MenuResponse(BaseModel):
    version: int
    items: List[MenuItem]
    updated_at_utc: str


class OrderItemIn(BaseModel):
    item_id: str
    name: str
    unit_price: int
    qty: int
    line_total: int


class OrderIn(BaseModel):
    order_id: str
    created_at_utc: str
    total: int
    items: List[OrderItemIn]
    payment_method: str = "MOCK"
    payment_status: str = "PAID"
    extra: Dict[str, Any] = Field(default_factory=dict)


class UploadOrdersRequest(BaseModel):
    kiosk_id: str
    orders: List[OrderIn]


class UploadOrdersResponse(BaseModel):
    accepted: int
    received_at_utc: str
    duplicates: int = 0


class ConfigResponse(BaseModel):
    kiosk_id: str
    sync_interval_sec: int = 10
    idle_timeout_sec: int = 45
    server_time_utc: str


# ---------- Auth ----------
def verify_api_key(x_api_key: str | None) -> None:
    expected = os.getenv("KIOSK_API_KEY")
    if not expected:
        # 서버 설정 실수 방지
        raise HTTPException(status_code=500, detail="API key not configured")

    if x_api_key != expected:
        raise HTTPException(status_code=401, detail="Invalid API key")


# ---------- API ----------
@app.get("/health", response_model=HealthResponse)
def health():
    return HealthResponse(ok=True, time_utc=now_iso())


@app.get("/menu", response_model=MenuResponse)
def menu(since_version: Optional[int] = None):
    return MenuResponse(
        version=MENU_VERSION,
        items=[MenuItem(**x) for x in MENU_ITEMS],
        updated_at_utc=now_iso(),
    )


@app.post("/orders/upload", response_model=UploadOrdersResponse)
def upload_orders(
    req: UploadOrdersRequest,
    x_api_key: str | None = Header(default=None),
):
    verify_api_key(x_api_key)

    accepted = 0
    duplicates = 0
    received = now_iso()

    for o in req.orders:
        inserted = db.upsert_order(
            order_id=o.order_id,
            kiosk_id=req.kiosk_id,
            created_at_utc=o.created_at_utc,
            total=o.total,
            payment_method=o.payment_method,
            payment_status=o.payment_status,
            received_at_utc=received,
            items=[i.model_dump() for i in o.items],
        )
        if inserted:
            accepted += 1
        else:
            duplicates += 1

    return UploadOrdersResponse(accepted=accepted, duplicates=duplicates, received_at_utc=received)


@app.get("/config", response_model=ConfigResponse)
def config(kiosk_id: str = "KIOSK-001"):
    return ConfigResponse(
        kiosk_id=kiosk_id,
        sync_interval_sec=10,
        idle_timeout_sec=45,
        server_time_utc=now_iso(),
    )


# ---------- Admin Web ----------
@app.get("/", include_in_schema=False)
def root():
    return RedirectResponse(url="/admin")


@app.get("/admin", response_class=HTMLResponse, include_in_schema=False)
def admin_dashboard(request: Request):
    rows = db.sales_by_day(last_days=14)
    labels = [r["day"] for r in rows]
    order_counts = [int(r["order_count"] or 0) for r in rows]
    revenues = [int(r["revenue"] or 0) for r in rows]

    today_prefix = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    today_orders = db.list_orders(date_prefix=today_prefix, limit=999999)
    today_count = len(today_orders)
    today_revenue = sum(int(r["total"]) for r in today_orders)

    return templates.TemplateResponse(
        "admin_dashboard.html",
        {
            "request": request,
            "today_count": today_count,
            "today_revenue": today_revenue,
            "labels": labels,
            "order_counts": order_counts,
            "revenues": revenues,
        },
    )


@app.get("/admin/orders", response_class=HTMLResponse, include_in_schema=False)
def admin_orders(request: Request, date: Optional[str] = None, kiosk_id: Optional[str] = None):
    rows = db.list_orders(date_prefix=date, kiosk_id=kiosk_id, limit=500)
    return templates.TemplateResponse(
        "admin_orders.html",
        {
            "request": request,
            "rows": rows,
            "date": date or "",
            "kiosk_id": kiosk_id or "",
        },
    )


@app.get("/admin/orders/{order_id}", response_class=HTMLResponse, include_in_schema=False)
def admin_order_detail(request: Request, order_id: str):
    data = db.get_order(order_id)
    if data is None:
        return HTMLResponse("Order not found", status_code=404)
    o, items = data
    return templates.TemplateResponse(
        "admin_order_detail.html",
        {"request": request, "o": o, "items": items},
    )


@app.get("/admin/export.json", include_in_schema=False)
def admin_export_json(date: Optional[str] = None, kiosk_id: Optional[str] = None):
    rows = db.list_orders(date_prefix=date, kiosk_id=kiosk_id, limit=2000)
    out = []
    for r in rows:
        data = db.get_order(r["order_id"])
        if data is None:
            continue
        o, items = data
        out.append(
            {
                "order": dict(o),
                "items": [dict(i) for i in items],
            }
        )
    return JSONResponse(out)
