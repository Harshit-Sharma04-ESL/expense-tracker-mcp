"""Smoke test: exercises every tool against a throwaway SQLite DB."""
import asyncio, os, tempfile

os.environ["EXPENSE_DB_PATH"] = os.path.join(tempfile.mkdtemp(), "test.db")

from fastmcp import Client
from server import mcp


async def main():
    async with Client(mcp) as c:
        call = lambda name, **kw: c.call_tool(name, kw)
        print("tools:", [t.name for t in await c.list_tools()])
        await call("add_expense", amount=1200, category="housing", description="Rent", date="2026-09-01")
        await call("add_expense", amount=90.5, category="food & dining", description="Dinner", date="2026-09-12")
        await call("add_expense", amount=1200, category="Housing", description="Rent", date="2026-10-01")
        r = await call("add_expense", amount=45.25, category="Groceries", description="Weekly shop", date="2026-10-03", payment_method="card")
        eid = r.data["expense"]["id"]
        await call("add_expense", amount=180, category="Food & Dining", description="Team lunch", date="2026-10-08")
        await call("add_expense", amount=15.99, category="subscriptions", description="Streaming", date="2026-10-10")
        await call("update_expense", expense_id=eid, notes="Included snacks for the week")
        await call("set_budget", category="Food & Dining", monthly_limit=150)
        await call("set_budget", category="Transport", monthly_limit=100)
        print("search:", (await call("list_expenses", month="2026-10", search="SNACKS")).data["count"])
        s = (await call("monthly_summary", month="2026-10")).data
        print("summary total:", s["total_formatted"], "prev change %:", s["previous_month"]["change_pct"])
        print((await call("generate_monthly_report", month="2026-10")).data)
        print((await call("generate_monthly_report", month="2026-10", format="csv")).data)
        await call("delete_expense", expense_id=eid)
        try:
            await call("get_expense", expense_id=eid)
        except Exception as e:
            print("expected error:", e)
        try:
            await call("add_expense", amount=-5, category="x", description="bad")
        except Exception as e:
            print("expected validation error:", type(e).__name__)
        print("resource:", (await c.read_resource("expenses://report/2026-12"))[0].text[:80])
        print("prompt:", (await c.get_prompt("monthly_review", {"month": "2026-10"})).messages[0].content.text[:60])

asyncio.run(main())
