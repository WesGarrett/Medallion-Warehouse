"""
WarehouseManager  ── schema lifecycle + Snowflake-style credit tracking.

Credits model: 1 credit = 60 wall-clock seconds of query execution time.
"""
from __future__ import annotations

import time
from pathlib import Path
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Engine
from tabulate import tabulate

CREDITS_PER_SECOND: float = 1.0 / 60.0   # 1 credit = 60 s


class WarehouseManager:
    """Manages DDL lifecycle and tracks query performance credits."""

    def __init__(self, engine: Engine) -> None:
        self.engine = engine
        self._credit_log: list[dict[str, Any]] = []

    # ------------------------------------------------------------------
    # Schema management
    # ------------------------------------------------------------------

    def setup_schemas(self, schema_dir: Path | None = None) -> None:
        """
        Execute every .sql file in *schema_dir* in alphabetical order.
        Splits each file on ';' so multi-statement files work correctly.
        """
        if schema_dir is None:
            from src.config import SCHEMA_DIR
            schema_dir = SCHEMA_DIR

        sql_files = sorted(schema_dir.glob("*.sql"))
        if not sql_files:
            raise FileNotFoundError(f"No .sql files found in {schema_dir}")

        with self.engine.begin() as conn:
            for sql_file in sql_files:
                statements = sql_file.read_text(encoding="utf-8").split(";")
                for stmt in statements:
                    stmt = stmt.strip()
                    if stmt:
                        conn.execute(text(stmt))

        print(f"  Schemas created from: {[f.name for f in sql_files]}")

    def drop_all_schemas(self, confirm: bool = False) -> None:
        """Drop bronze/silver/gold schemas (CASCADE).  Requires confirm=True."""
        if not confirm:
            raise RuntimeError(
                "Pass confirm=True to drop_all_schemas() — this is destructive."
            )
        with self.engine.begin() as conn:
            for schema in ("gold", "silver", "bronze"):
                conn.execute(text(f"DROP SCHEMA IF EXISTS {schema} CASCADE"))
        print("  Dropped schemas: bronze, silver, gold")

    # ------------------------------------------------------------------
    # Tracked query execution
    # ------------------------------------------------------------------

    def execute_tracked_query(
        self,
        sql: str,
        label: str,
        params: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        """
        Execute *sql*, measure wall-clock time, log credits consumed.

        Returns
        -------
        list[dict]
            Result rows as dicts (empty list for DML/DDL).
        """
        t0 = time.perf_counter()
        rows: list[dict[str, Any]] = []

        with self.engine.connect() as conn:
            result = conn.execute(text(sql), params or {})
            if result.returns_rows:
                rows = [dict(r._mapping) for r in result]

        elapsed = time.perf_counter() - t0
        credits = elapsed * CREDITS_PER_SECOND

        self._credit_log.append(
            {
                "label": label,
                "elapsed_s": round(elapsed, 6),
                "credits": round(credits, 8),
            }
        )
        return rows

    # ------------------------------------------------------------------
    # Credit reporting
    # ------------------------------------------------------------------

    def get_credit_report(self) -> dict[str, Any]:
        """Return summary stats over all tracked queries."""
        if not self._credit_log:
            return {"entries": [], "total_credits": 0.0, "total_elapsed_s": 0.0}

        total_elapsed = sum(e["elapsed_s"] for e in self._credit_log)
        total_credits = sum(e["credits"] for e in self._credit_log)
        return {
            "entries": list(self._credit_log),
            "total_elapsed_s": round(total_elapsed, 6),
            "total_credits": round(total_credits, 8),
        }

    def print_credit_report(self) -> None:
        """Pretty-print the credit log using tabulate (grid style)."""
        report = self.get_credit_report()
        if not report["entries"]:
            print("  No queries tracked yet.")
            return

        rows = [
            [e["label"], f"{e['elapsed_s']:.4f}", f"{e['credits']:.6f}"]
            for e in report["entries"]
        ]
        # Totals row
        rows.append([
            "─── TOTAL ───",
            f"{report['total_elapsed_s']:.4f}",
            f"{report['total_credits']:.6f}",
        ])

        print(
            tabulate(
                rows,
                headers=["Query Label", "Time (s)", "Credits"],
                tablefmt="grid",
            )
        )
