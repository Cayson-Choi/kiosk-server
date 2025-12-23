from __future__ import annotations

import sqlite3
from typing import Any, Dict, List, Optional, Tuple


SCHEMA_SQL = """
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS orders (
  order_id TEXT PRIMARY KEY,
  kiosk_id TEXT NOT NULL,
  created_at_utc TEXT NOT NULL,
  total INTEGER NOT NULL,
  payment_method TEXT NOT NULL,
  payment_status TEXT NOT NULL,
  received_at_utc TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS order_items (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  order_id TEXT NOT NULL,
  item_id TEXT NOT NULL,
  name TEXT NOT NULL,
  unit_price INTEGER NOT NULL,
  qty INTEGER NOT NULL,
  line_total INTEGER NOT NULL,
  FOREIGN KEY(order_id) REFERENCES orders(order_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_orders_created_at ON orders(created_at_utc);
CREATE INDEX IF NOT EXISTS idx_orders_kiosk_id ON orders(kiosk_id);
"""


class ServerDB:
    def __init__(self, path: str):
        self.path = path
        self._conn: Optional[sqlite3.Connection] = None

    def connect(self) -> sqlite3.Connection:
        if self._conn is None:
            self._conn = sqlite3.connect(self.path, check_same_thread=False)
            self._conn.row_factory = sqlite3.Row
        return self._conn

    def init_schema(self) -> None:
        conn = self.connect()
        conn.executescript(SCHEMA_SQL)
        conn.commit()

    def execute(self, sql: str, params: Tuple[Any, ...] = ()) -> None:
        conn = self.connect()
        conn.execute(sql, params)
        conn.commit()

    def executemany(self, sql: str, seq_params: List[Tuple[Any, ...]]) -> None:
        conn = self.connect()
        conn.executemany(sql, seq_params)
        conn.commit()

    def query_all(self, sql: str, params: Tuple[Any, ...] = ()) -> List[sqlite3.Row]:
        conn = self.connect()
        cur = conn.execute(sql, params)
        return cur.fetchall()

    def query_one(self, sql: str, params: Tuple[Any, ...] = ()) -> Optional[sqlite3.Row]:
        conn = self.connect()
        cur = conn.execute(sql, params)
        return cur.fetchone()

    # ---- domain queries ----
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
        """
        returns True if inserted, False if duplicate (already existed)
        """
        exists = self.query_one("SELECT order_id FROM orders WHERE order_id=?", (order_id,))
        if exists is not None:
            return False

        self.execute(
            "INSERT INTO orders(order_id,kiosk_id,created_at_utc,total,payment_method,payment_status,received_at_utc) "
            "VALUES(?,?,?,?,?,?,?)",
            (order_id, kiosk_id, created_at_utc, int(total), payment_method, payment_status, received_at_utc),
        )

        self.executemany(
            "INSERT INTO order_items(order_id,item_id,name,unit_price,qty,line_total) VALUES(?,?,?,?,?,?)",
            [
                (
                    order_id,
                    i["item_id"],
                    i["name"],
                    int(i["unit_price"]),
                    int(i["qty"]),
                    int(i["line_total"]),
                )
                for i in items
            ],
        )
        return True

    def list_orders(self, date_prefix: Optional[str] = None, kiosk_id: Optional[str] = None, limit: int = 200):
        where = []
        params: List[Any] = []
        if date_prefix:
            where.append("created_at_utc LIKE ?")
            params.append(f"{date_prefix}%")
        if kiosk_id:
            where.append("kiosk_id = ?")
            params.append(kiosk_id)

        where_sql = ("WHERE " + " AND ".join(where)) if where else ""
        sql = (
            "SELECT order_id,kiosk_id,created_at_utc,total,payment_method,payment_status,received_at_utc "
            f"FROM orders {where_sql} ORDER BY created_at_utc DESC LIMIT ?"
        )
        params.append(int(limit))
        return self.query_all(sql, tuple(params))

    def get_order(self, order_id: str):
        o = self.query_one(
            "SELECT order_id,kiosk_id,created_at_utc,total,payment_method,payment_status,received_at_utc "
            "FROM orders WHERE order_id=?",
            (order_id,),
        )
        if o is None:
            return None
        items = self.query_all(
            "SELECT item_id,name,unit_price,qty,line_total FROM order_items WHERE order_id=?",
            (order_id,),
        )
        return o, items

    def sales_by_day(self, last_days: int = 14):
        # created_at_utc is ISO; group by first 10 chars YYYY-MM-DD
        rows = self.query_all(
            """
            SELECT substr(created_at_utc,1,10) AS day,
                   COUNT(*) AS order_count,
                   SUM(total) AS revenue
            FROM orders
            WHERE created_at_utc >= datetime('now','-30 day')
            GROUP BY substr(created_at_utc,1,10)
            ORDER BY day DESC
            LIMIT ?
            """,
            (int(last_days),),
        )
        # return ascending for chart
        return list(reversed(rows))
