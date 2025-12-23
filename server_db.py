from __future__ import annotations

import os
from typing import Any, Dict, List, Optional

from sqlalchemy import (
    create_engine,
    MetaData,
    Table,
    Column,
    Integer,
    String,
    ForeignKey,
    select,
    insert,
    func,
)
from sqlalchemy.engine import Engine
from sqlalchemy.exc import IntegrityError


def _normalize_db_url(url: str) -> str:
    if url.startswith("postgres://"):
        return "postgresql+psycopg://" + url[len("postgres://") :]
    if url.startswith("postgresql://"):
        return "postgresql+psycopg://" + url[len("postgresql://") :]
    return url


class ServerDB:
    def __init__(self):
        raw = os.getenv("DATABASE_URL", "").strip()
        if raw:
            self.db_url = _normalize_db_url(raw)
        else:
            self.db_url = "sqlite+pysqlite:///server.db"

        connect_args = {}
        if self.db_url.startswith("sqlite"):
            connect_args = {"check_same_thread": False}

        self.engine: Engine = create_engine(self.db_url, future=True, connect_args=connect_args)
        self.md = MetaData()

        self.orders = Table(
            "orders",
            self.md,
            Column("order_id", String(64), primary_key=True),
            Column("kiosk_id", String(64), nullable=False, index=True),
            Column("created_at_utc", String(40), nullable=False, index=True),
            Column("total", Integer, nullable=False),
            Column("payment_method", String(32), nullable=False),
            Column("payment_status", String(32), nullable=False),
            Column("received_at_utc", String(40), nullable=False),
        )

        self.order_items = Table(
            "order_items",
            self.md,
            Column("id", Integer, primary_key=True, autoincrement=True),
            Column("order_id", String(64), ForeignKey("orders.order_id", ondelete="CASCADE"), nullable=False, index=True),
            Column("item_id", String(64), nullable=False),
            Column("name", String(200), nullable=False),
            Column("unit_price", Integer, nullable=False),
            Column("qty", Integer, nullable=False),
            Column("line_total", Integer, nullable=False),
        )

    def init_schema(self) -> None:
        self.md.create_all(self.engine)

    def upsert_order(
        self,
        order_id: str,
        kiosk_id: str,
        created_at_utc: str,
        total: int,
        payment_method: str,
        payment_status: str,
        received_at_utc: str,
        items: List[Dict[str, Any]],
    ) -> bool:
        with self.engine.begin() as conn:
            try:
                conn.execute(
                    insert(self.orders).values(
                        order_id=order_id,
                        kiosk_id=kiosk_id,
                        created_at_utc=created_at_utc,
                        total=int(total),
                        payment_method=payment_method,
                        payment_status=payment_status,
                        received_at_utc=received_at_utc,
                    )
                )
            except IntegrityError:
                return False

            if items:
                conn.execute(
                    insert(self.order_items),
                    [
                        {
                            "order_id": order_id,
                            "item_id": i["item_id"],
                            "name": i["name"],
                            "unit_price": int(i["unit_price"]),
                            "qty": int(i["qty"]),
                            "line_total": int(i["line_total"]),
                        }
                        for i in items
                    ],
                )
        return True

    def list_orders(self, date_prefix: Optional[str] = None, kiosk_id: Optional[str] = None, limit: int = 500):
        stmt = select(
            self.orders.c.order_id,
            self.orders.c.kiosk_id,
            self.orders.c.created_at_utc,
            self.orders.c.total,
            self.orders.c.payment_method,
            self.orders.c.payment_status,
            self.orders.c.received_at_utc,
        )

        if date_prefix:
            stmt = stmt.where(self.orders.c.created_at_utc.like(f"{date_prefix}%"))
        if kiosk_id:
            stmt = stmt.where(self.orders.c.kiosk_id == kiosk_id)

        stmt = stmt.order_by(self.orders.c.created_at_utc.desc()).limit(int(limit))

        with self.engine.connect() as conn:
            return conn.execute(stmt).mappings().all()

    def get_order(self, order_id: str):
        with self.engine.connect() as conn:
            o = conn.execute(
                select(
                    self.orders.c.order_id,
                    self.orders.c.kiosk_id,
                    self.orders.c.created_at_utc,
                    self.orders.c.total,
                    self.orders.c.payment_method,
                    self.orders.c.payment_status,
                    self.orders.c.received_at_utc,
                ).where(self.orders.c.order_id == order_id)
            ).mappings().first()

            if not o:
                return None

            items = conn.execute(
                select(
                    self.order_items.c.item_id,
                    self.order_items.c.name,
                    self.order_items.c.unit_price,
                    self.order_items.c.qty,
                    self.order_items.c.line_total,
                ).where(self.order_items.c.order_id == order_id)
            ).mappings().all()

        return o, items

    def sales_by_day(self, last_days: int = 14):
        day_expr = func.substr(self.orders.c.created_at_utc, 1, 10)

        stmt = (
            select(
                day_expr.label("day"),
                func.count().label("order_count"),
                func.coalesce(func.sum(self.orders.c.total), 0).label("revenue"),
            )
            .group_by(day_expr)
            .order_by(day_expr.desc())
            .limit(int(last_days))
        )

        with self.engine.connect() as conn:
            rows = conn.execute(stmt).mappings().all()

        return list(reversed(rows))
