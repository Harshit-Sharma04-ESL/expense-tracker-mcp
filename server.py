"""Expense Tracker MCP server.

Track expenses, attach descriptions/notes, set monthly budgets, and produce
monthly summaries and reports. Built with FastMCP so it can be deployed to
Prefect Horizon (entrypoint: ``server.py:mcp``).

Storage:
  * ``DATABASE_URL`` set to a postgres:// URL -> Postgres (recommended when hosted)
  * otherwise -> SQLite at ``EXPENSE_DB_PATH`` (default ~/.expense_tracker/expenses.db)
"""

from __future__ import annotations

import calendar
import csv
import io
import os
import sqlite3
import tempfile
from contextlib import contextmanager
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Annotated, Any, Literal
from zoneinfo import ZoneInfo

from fastmcp import FastMCP
from fastmcp.exceptions import ToolError
from mcp.types import ToolAnnotations
from pydantic import Field

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

CURRENCY = os.getenv("EXPENSE_CURRENCY", "USD").upper()
TIMEZONE = ZoneInfo(os.getenv("EXPENSE_TIMEZONE", "UTC"))

CURRENCY_SYMBOLS = {"USD": "$", "INR": "₹", "EUR": "€", "GBP": "£", "JPY": "¥", "AUD": "A$", "CAD": "C$"}
SYMBOL = CURRENCY_SYMBOLS.get(CURRENCY, f"{CURRENCY} ")

SUGGESTED_CATEGORIES = [
    "Food & Dining", "Groceries", "Transport", "Housing", "Utilities", "Shopping",
    "Entertainment", "Health", "Travel", "Education", "Subscriptions", "Bills", "Other",
]


def today() -> date:
    return datetime.now(TIMEZONE).date()


def fmt_money(cents: int) -> str:
    sign = "-" if cents < 0 else ""
    return f"{sign}{SYMBOL}{abs(cents) / 100:,.2f}"


def to_cents(amount: float) -> int:
    return int(round(amount * 100))


# ---------------------------------------------------------------------------
# Database layer (SQLite or Postgres)
# ---------------------------------------------------------------------------


def _default_sqlite_path() -> Path:
    if path := os.getenv("EXPENSE_DB_PATH"):
        return Path(path)
    home_dir = Path.home() / ".expense_tracker"
    try:
        home_dir.mkdir(parents=True, exist_ok=True)
        return home_dir / "expenses.db"
    except OSError:
        return Path(tempfile.gettempdir()) / "expenses.db"


def _clean_database_url(raw: str) -> str:
    """Tolerate common paste mistakes: whitespace, quotes, or a leading 'DATABASE_URL='."""
    url = raw.strip().strip("\"'").strip()
    if url.upper().startswith("DATABASE_URL="):
        url = url.split("=", 1)[1].strip().strip("\"'").strip()
    return url


class Database:
    def __init__(self) -> None:
        url = _clean_database_url(os.getenv("DATABASE_URL", ""))
        self.is_postgres = url.startswith(("postgres://", "postgresql://"))
        if url and not self.is_postgres:
            raise RuntimeError(
                "DATABASE_URL is set but does not start with postgres:// or postgresql://. "
                "Fix it or remove it to use SQLite."
            )
        self.url = url
        self.sqlite_path = None if self.is_postgres else _default_sqlite_path()
        if self.sqlite_path:
            self.sqlite_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_schema()

    @property
    def backend(self) -> str:
        return "postgres" if self.is_postgres else f"sqlite ({self.sqlite_path})"

    @contextmanager
    def _connect(self):
        if self.is_postgres:
            import psycopg
            from psycopg.rows import dict_row

            conn = psycopg.connect(self.url, row_factory=dict_row)
        else:
            conn = sqlite3.connect(self.sqlite_path)
            conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def query(self, sql: str, params: tuple | list = ()) -> list[dict[str, Any]]:
        if self.is_postgres:
            sql = sql.replace("?", "%s")
        with self._connect() as conn:
            cur = conn.execute(sql, tuple(params))
            rows = cur.fetchall() if cur.description else []
            return [dict(r) for r in rows]

    def _init_schema(self) -> None:
        id_col = "SERIAL PRIMARY KEY" if self.is_postgres else "INTEGER PRIMARY KEY AUTOINCREMENT"
        statements = [
            f"""CREATE TABLE IF NOT EXISTS expenses (
                id {id_col},
                expense_date TEXT NOT NULL,
                amount_cents INTEGER NOT NULL,
                category TEXT NOT NULL,
                description TEXT NOT NULL,
                notes TEXT,
                payment_method TEXT,
                created_at TEXT NOT NULL
            )""",
            "CREATE INDEX IF NOT EXISTS idx_expenses_date ON expenses (expense_date)",
            "CREATE INDEX IF NOT EXISTS idx_expenses_category ON expenses (category)",
            """CREATE TABLE IF NOT EXISTS budgets (
                category TEXT PRIMARY KEY,
                monthly_limit_cents INTEGER NOT NULL
            )""",
        ]
        for stmt in statements:
            self.query(stmt)


db = Database()

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def parse_date(value: str | None) -> date:
    if value is None or not value.strip():
        return today()
    v = value.strip().lower()
    if v == "today":
        return today()
    if v == "yesterday":
        return today() - timedelta(days=1)
    try:
        return date.fromisoformat(v)
    except ValueError:
        raise ToolError(f"Invalid date '{value}'. Use YYYY-MM-DD, 'today' or 'yesterday'.")


def parse_month(value: str | None) -> tuple[date, date]:
    """Return (first_day, first_day_of_next_month) for a YYYY-MM string."""
    if value is None or not value.strip():
        d = today()
        year, month = d.year, d.month
    else:
        try:
            year, month = (int(p) for p in value.strip().split("-")[:2])
            if not 1 <= month <= 12:
                raise ValueError
        except ValueError:
            raise ToolError(f"Invalid month '{value}'. Use YYYY-MM, e.g. 2026-10.")
    start = date(year, month, 1)
    end = date(year + (month == 12), month % 12 + 1, 1)
    return start, end


def previous_month(start: date) -> str:
    prev = start - timedelta(days=1)
    return f"{prev.year:04d}-{prev.month:02d}"


def normalize_category(category: str) -> str:
    cleaned = " ".join(category.split())
    if not cleaned:
        raise ToolError("Category cannot be empty.")
    for known in SUGGESTED_CATEGORIES:
        if known.lower() == cleaned.lower():
            return known
    return cleaned[0].upper() + cleaned[1:]


def serialize(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": row["id"],
        "date": row["expense_date"],
        "amount": row["amount_cents"] / 100,
        "amount_formatted": fmt_money(row["amount_cents"]),
        "category": row["category"],
        "description": row["description"],
        "notes": row.get("notes"),
        "payment_method": row.get("payment_method"),
    }


def get_row(expense_id: int) -> dict[str, Any]:
    rows = db.query("SELECT * FROM expenses WHERE id = ?", (expense_id,))
    if not rows:
        raise ToolError(f"Expense #{expense_id} not found.")
    return rows[0]


def month_rows(start: date, end: date) -> list[dict[str, Any]]:
    return db.query(
        "SELECT * FROM expenses WHERE expense_date >= ? AND expense_date < ? ORDER BY expense_date, id",
        (start.isoformat(), end.isoformat()),
    )


def budgets() -> dict[str, int]:
    return {r["category"]: r["monthly_limit_cents"] for r in db.query("SELECT * FROM budgets")}


def build_summary(month: str | None) -> dict[str, Any]:
    start, end = parse_month(month)
    rows = month_rows(start, end)
    month_label = start.strftime("%B %Y")
    month_key = f"{start.year:04d}-{start.month:02d}"

    total = sum(r["amount_cents"] for r in rows)
    days_in_month = calendar.monthrange(start.year, start.month)[1]
    t = today()
    if start <= t < end:
        days_counted = t.day
    elif t < start:
        days_counted = 0
    else:
        days_counted = days_in_month

    by_cat: dict[str, dict[str, int]] = {}
    for r in rows:
        c = by_cat.setdefault(r["category"], {"cents": 0, "count": 0})
        c["cents"] += r["amount_cents"]
        c["count"] += 1

    prev_start, prev_end = parse_month(previous_month(start))
    prev_rows = month_rows(prev_start, prev_end)
    prev_total = sum(r["amount_cents"] for r in prev_rows)
    prev_by_cat: dict[str, int] = {}
    for r in prev_rows:
        prev_by_cat[r["category"]] = prev_by_cat.get(r["category"], 0) + r["amount_cents"]

    budget_map = budgets()
    categories = []
    for name, c in sorted(by_cat.items(), key=lambda kv: -kv[1]["cents"]):
        limit = budget_map.get(name)
        categories.append(
            {
                "category": name,
                "total": c["cents"] / 100,
                "total_formatted": fmt_money(c["cents"]),
                "count": c["count"],
                "share_pct": round(c["cents"] / total * 100, 1) if total else 0.0,
                "previous_month_total": prev_by_cat.get(name, 0) / 100,
                "budget": limit / 100 if limit is not None else None,
                "budget_used_pct": round(c["cents"] / limit * 100, 1) if limit else None,
                "over_budget": bool(limit is not None and c["cents"] > limit),
            }
        )
    # Budgeted categories with no spending yet still deserve a line.
    for name, limit in budget_map.items():
        if name not in by_cat:
            categories.append(
                {
                    "category": name, "total": 0.0, "total_formatted": fmt_money(0), "count": 0,
                    "share_pct": 0.0, "previous_month_total": prev_by_cat.get(name, 0) / 100,
                    "budget": limit / 100, "budget_used_pct": 0.0, "over_budget": False,
                }
            )

    weekly: list[dict[str, Any]] = []
    for wk_start_day in range(1, days_in_month + 1, 7):
        wk_end_day = min(wk_start_day + 6, days_in_month)
        cents = sum(
            r["amount_cents"] for r in rows
            if wk_start_day <= int(r["expense_date"][8:10]) <= wk_end_day
        )
        weekly.append({"days": f"{wk_start_day}-{wk_end_day}", "total": cents / 100, "total_formatted": fmt_money(cents)})

    change = total - prev_total
    return {
        "month": month_key,
        "month_label": month_label,
        "currency": CURRENCY,
        "total": total / 100,
        "total_formatted": fmt_money(total),
        "transaction_count": len(rows),
        "daily_average": round(total / days_counted / 100, 2) if days_counted else 0.0,
        "days_counted": days_counted,
        "previous_month": {
            "month": previous_month(start),
            "total": prev_total / 100,
            "change": change / 100,
            "change_pct": round(change / prev_total * 100, 1) if prev_total else None,
        },
        "by_category": categories,
        "weekly": weekly,
        "top_expenses": [serialize(r) for r in sorted(rows, key=lambda r: -r["amount_cents"])[:5]],
        "_rows": rows,  # stripped before returning from tools
    }


def public(summary: dict[str, Any]) -> dict[str, Any]:
    return {k: v for k, v in summary.items() if not k.startswith("_")}


def bar(pct: float, width: int = 20) -> str:
    filled = int(round(pct / 100 * width))
    return "█" * filled + "░" * (width - filled)


def render_markdown(s: dict[str, Any]) -> str:
    rows = s["_rows"]
    prev = s["previous_month"]
    lines = [
        f"# Expense Report — {s['month_label']}",
        f"_Generated {today().isoformat()} · Currency: {s['currency']}_",
        "",
        "## Overview",
        "",
        "| Metric | Value |",
        "|---|---|",
        f"| Total spent | **{s['total_formatted']}** |",
        f"| Transactions | {s['transaction_count']} |",
        f"| Daily average | {fmt_money(to_cents(s['daily_average']))} (over {s['days_counted']} days) |",
    ]
    if s["top_expenses"]:
        top = s["top_expenses"][0]
        lines.append(f"| Largest expense | {top['amount_formatted']} — {top['description']} ({top['date']}) |")
    if prev["change_pct"] is not None:
        arrow = "▲" if prev["change"] > 0 else "▼" if prev["change"] < 0 else "■"
        lines.append(
            f"| vs {prev['month']} | {arrow} {abs(prev['change_pct'])}% "
            f"({'+' if prev['change'] >= 0 else '-'}{fmt_money(abs(to_cents(prev['change'])))}) |"
        )
    else:
        lines.append(f"| vs {prev['month']} | no spending recorded last month |")

    if not rows:
        lines += ["", "_No expenses recorded for this month._"]
        return "\n".join(lines)

    lines += ["", "## Spending by category", "", "| Category | Amount | Share | Count | Budget |", "|---|---:|---|---:|---|"]
    for c in s["by_category"]:
        if c["budget"] is None:
            budget = "—"
        else:
            flag = " ⚠️ over" if c["over_budget"] else ""
            budget = f"{c['budget_used_pct']}% of {fmt_money(to_cents(c['budget']))}{flag}"
        lines.append(
            f"| {c['category']} | {c['total_formatted']} | `{bar(c['share_pct'])}` {c['share_pct']}% | {c['count']} | {budget} |"
        )

    lines += ["", "## Weekly breakdown", "", "| Days | Amount |", "|---|---:|"]
    lines += [f"| {w['days']} | {w['total_formatted']} |" for w in s["weekly"]]

    lines += ["", "## Top expenses", ""]
    for i, e in enumerate(s["top_expenses"], 1):
        lines.append(f"{i}. **{e['amount_formatted']}** — {e['description']} · {e['category']} · {e['date']}")

    insights = []
    biggest = s["by_category"][0]
    insights.append(f"Largest category: **{biggest['category']}** at {biggest['share_pct']}% of spending.")
    over = [c["category"] for c in s["by_category"] if c["over_budget"]]
    if over:
        insights.append(f"Over budget: **{', '.join(over)}**.")
    movers = [
        (c["category"], c["total"] - c["previous_month_total"])
        for c in s["by_category"] if c["previous_month_total"] or c["total"]
    ]
    if prev["total"] and movers:
        up = max(movers, key=lambda m: m[1])
        down = min(movers, key=lambda m: m[1])
        if up[1] > 0:
            insights.append(f"Biggest increase vs last month: **{up[0]}** (+{fmt_money(to_cents(up[1]))}).")
        if down[1] < 0:
            insights.append(f"Biggest decrease vs last month: **{down[0]}** ({fmt_money(to_cents(down[1]))}).")
    lines += ["", "## Insights", ""] + [f"- {i}" for i in insights]

    lines += ["", "## All transactions", "", "| # | Date | Category | Description | Amount | Payment |", "|---:|---|---|---|---:|---|"]
    for r in rows:
        desc = r["description"] + (f" — _{r['notes']}_" if r.get("notes") else "")
        lines.append(
            f"| {r['id']} | {r['expense_date']} | {r['category']} | {desc.replace('|', '/')} | "
            f"{fmt_money(r['amount_cents'])} | {r.get('payment_method') or ''} |"
        )
    return "\n".join(lines)


def render_csv(rows: list[dict[str, Any]]) -> str:
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(["id", "date", "category", "description", "notes", "payment_method", f"amount_{CURRENCY}"])
    for r in rows:
        w.writerow([r["id"], r["expense_date"], r["category"], r["description"], r.get("notes") or "",
                    r.get("payment_method") or "", f"{r['amount_cents'] / 100:.2f}"])
    return buf.getvalue()


# ---------------------------------------------------------------------------
# MCP server
# ---------------------------------------------------------------------------

mcp = FastMCP(
    name="Expense Tracker",
    instructions=(
        "Personal expense tracker. Use add_expense to record spending (amount, category, description). "
        "Use update_expense to change or add a description/notes. Use monthly_summary for structured "
        "numbers and generate_monthly_report for a full Markdown or CSV report. Months are YYYY-MM; "
        f"dates are YYYY-MM-DD. All amounts are in {CURRENCY}."
    ),
)

READ_ONLY = ToolAnnotations(readOnlyHint=True)

Amount = Annotated[float, Field(gt=0, le=10_000_000, description=f"Amount spent in {CURRENCY}, e.g. 12.50")]
MonthArg = Annotated[str | None, Field(description="Month as YYYY-MM. Defaults to the current month.")]


@mcp.tool
def add_expense(
    amount: Amount,
    category: Annotated[str, Field(description=f"Spending category, e.g. {', '.join(SUGGESTED_CATEGORIES[:5])}")],
    description: Annotated[str, Field(min_length=1, max_length=500, description="What the expense was for")],
    date: Annotated[str | None, Field(description="YYYY-MM-DD, 'today' or 'yesterday'. Defaults to today.")] = None,
    payment_method: Annotated[str | None, Field(description="e.g. cash, credit card, UPI")] = None,
    notes: Annotated[str | None, Field(max_length=2000, description="Optional longer notes")] = None,
) -> dict[str, Any]:
    """Record a new expense."""
    expense_date = parse_date(date)
    rows = db.query(
        """INSERT INTO expenses (expense_date, amount_cents, category, description, notes, payment_method, created_at)
           VALUES (?, ?, ?, ?, ?, ?, ?) RETURNING *""",
        (expense_date.isoformat(), to_cents(amount), normalize_category(category), description.strip(),
         notes, payment_method, datetime.now(TIMEZONE).isoformat()),
    )
    return {"status": "added", "expense": serialize(rows[0])}


@mcp.tool
def update_expense(
    expense_id: int,
    amount: Amount | None = None,
    category: str | None = None,
    description: Annotated[str | None, Field(max_length=500)] = None,
    date: str | None = None,
    payment_method: str | None = None,
    notes: Annotated[str | None, Field(max_length=2000, description="Replaces existing notes")] = None,
) -> dict[str, Any]:
    """Edit an existing expense — change its amount, category, date, or describe it with a new description/notes."""
    get_row(expense_id)
    changes: dict[str, Any] = {}
    if amount is not None:
        changes["amount_cents"] = to_cents(amount)
    if category is not None:
        changes["category"] = normalize_category(category)
    if description is not None:
        if not description.strip():
            raise ToolError("Description cannot be empty.")
        changes["description"] = description.strip()
    if date is not None:
        changes["expense_date"] = parse_date(date).isoformat()
    if payment_method is not None:
        changes["payment_method"] = payment_method
    if notes is not None:
        changes["notes"] = notes
    if not changes:
        raise ToolError("Nothing to update — pass at least one field.")
    assignments = ", ".join(f"{col} = ?" for col in changes)
    rows = db.query(f"UPDATE expenses SET {assignments} WHERE id = ? RETURNING *", (*changes.values(), expense_id))
    return {"status": "updated", "expense": serialize(rows[0])}


@mcp.tool(annotations=ToolAnnotations(destructiveHint=True))
def delete_expense(expense_id: int) -> dict[str, Any]:
    """Permanently delete an expense."""
    row = get_row(expense_id)
    db.query("DELETE FROM expenses WHERE id = ?", (expense_id,))
    return {"status": "deleted", "expense": serialize(row)}


@mcp.tool(annotations=READ_ONLY)
def get_expense(expense_id: int) -> dict[str, Any]:
    """Get full details of one expense, including its notes."""
    return serialize(get_row(expense_id))


@mcp.tool(annotations=READ_ONLY)
def list_expenses(
    month: MonthArg = None,
    category: str | None = None,
    search: Annotated[str | None, Field(description="Text to find in description or notes")] = None,
    limit: Annotated[int, Field(ge=1, le=500)] = 100,
) -> dict[str, Any]:
    """List expenses for a month, optionally filtered by category or search text."""
    start, end = parse_month(month)
    sql = "SELECT * FROM expenses WHERE expense_date >= ? AND expense_date < ?"
    params: list[Any] = [start.isoformat(), end.isoformat()]
    if category:
        sql += " AND LOWER(category) = ?"
        params.append(normalize_category(category).lower())
    if search:
        sql += " AND (LOWER(description) LIKE ? OR LOWER(COALESCE(notes, '')) LIKE ?)"
        params += [f"%{search.lower()}%"] * 2
    rows = db.query(sql + " ORDER BY expense_date DESC, id DESC", params)
    total = sum(r["amount_cents"] for r in rows)
    return {
        "month": f"{start.year:04d}-{start.month:02d}",
        "count": len(rows),
        "total_formatted": fmt_money(total),
        "expenses": [serialize(r) for r in rows[:limit]],
        "truncated": len(rows) > limit,
    }


@mcp.tool(annotations=READ_ONLY)
def list_categories() -> dict[str, Any]:
    """List categories already used (with all-time totals) plus suggested categories."""
    rows = db.query(
        "SELECT category, COUNT(*) AS n, SUM(amount_cents) AS cents FROM expenses GROUP BY category ORDER BY cents DESC"
    )
    return {
        "used": [{"category": r["category"], "count": r["n"], "total_formatted": fmt_money(int(r["cents"]))} for r in rows],
        "suggested": SUGGESTED_CATEGORIES,
    }


@mcp.tool
def set_budget(
    category: str,
    monthly_limit: Annotated[float, Field(ge=0, description="Monthly limit; 0 removes the budget")],
) -> dict[str, Any]:
    """Set (or remove, with 0) a monthly budget for a category."""
    name = normalize_category(category)
    if monthly_limit == 0:
        db.query("DELETE FROM budgets WHERE category = ?", (name,))
        return {"status": "removed", "category": name}
    db.query(
        """INSERT INTO budgets (category, monthly_limit_cents) VALUES (?, ?)
           ON CONFLICT (category) DO UPDATE SET monthly_limit_cents = excluded.monthly_limit_cents""",
        (name, to_cents(monthly_limit)),
    )
    return {"status": "set", "category": name, "monthly_limit": fmt_money(to_cents(monthly_limit))}


@mcp.tool(annotations=READ_ONLY)
def monthly_summary(month: MonthArg = None) -> dict[str, Any]:
    """Structured summary for a month: totals, per-category breakdown with budgets, weekly totals,
    top expenses, and comparison with the previous month."""
    return public(build_summary(month))


@mcp.tool(annotations=READ_ONLY)
def generate_monthly_report(month: MonthArg = None, format: Literal["markdown", "csv"] = "markdown") -> str:
    """Generate a complete monthly expense report. 'markdown' gives a formatted report with overview,
    category breakdown, budgets, insights and every transaction; 'csv' gives raw rows for spreadsheets."""
    summary = build_summary(month)
    return render_csv(summary["_rows"]) if format == "csv" else render_markdown(summary)


@mcp.resource("expenses://categories")
def categories_resource() -> dict[str, Any]:
    """Categories in use and suggested categories."""
    return list_categories()


@mcp.resource("expenses://report/{month}")
def report_resource(month: str) -> str:
    """Markdown expense report for a month (YYYY-MM)."""
    return render_markdown(build_summary(month))


@mcp.prompt
def monthly_review(month: str | None = None) -> str:
    """Ask the assistant to review a month's spending and suggest improvements."""
    target = month or "the current month"
    return (
        f"Call generate_monthly_report for {target}. Then write a short review: overall spending, "
        "the 2-3 categories driving it, any budgets exceeded, notable changes from last month, and "
        "three concrete, specific suggestions to save money next month."
    )


@mcp.tool(annotations=READ_ONLY)
def server_info() -> dict[str, str]:
    """Show storage backend, currency and timezone in use."""
    info = {"storage": db.backend, "currency": CURRENCY, "timezone": str(TIMEZONE), "today": today().isoformat()}
    if not db.is_postgres:
        info["warning"] = (
            "DATABASE_URL is not set, so data is in a local SQLite file that may be lost on restart "
            "or redeploy. Set DATABASE_URL to a Postgres URL for permanent storage."
        )
    return info


if __name__ == "__main__":
    mcp.run()
