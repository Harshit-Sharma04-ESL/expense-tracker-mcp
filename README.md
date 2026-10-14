# Expense Tracker MCP Server

An MCP server (built with [FastMCP](https://gofastmcp.com)) for tracking expenses, describing them, summarizing each month and generating reports.

## Tools

| Tool | What it does |
|---|---|
| `add_expense` | Record an expense: amount, category, description, optional date / payment method / notes |
| `update_expense` | Edit an expense, or describe it with a new description or notes |
| `get_expense` | Full details of one expense |
| `delete_expense` | Delete an expense |
| `list_expenses` | List a month's expenses, filtered by category or search text |
| `list_categories` | Categories you've used, plus suggested ones |
| `set_budget` | Monthly budget per category (0 removes it) |
| `monthly_summary` | Structured JSON: totals, category breakdown, budgets, weekly totals, top expenses, change from last month |
| `generate_monthly_report` | Full Markdown report, or a CSV export |
| `server_info` | Storage backend, currency and timezone in use |

There is also a resource, `expenses://report/{YYYY-MM}`, and a prompt, `monthly_review`, that asks the assistant to analyze a month and suggest savings.

## Configuration (environment variables)

| Variable | Default | Purpose |
|---|---|---|
| `DATABASE_URL` | unset | `postgresql://...` URL; when set, data is stored in Postgres |
| `EXPENSE_DB_PATH` | `~/.expense_tracker/expenses.db` | SQLite file, used when `DATABASE_URL` is unset |
| `EXPENSE_CURRENCY` | `USD` | e.g. `INR`, `EUR`, `GBP` |
| `EXPENSE_TIMEZONE` | `UTC` | e.g. `Asia/Kolkata`; decides what "today" means |

> **Persistence when hosted:** a hosted container's filesystem may be wiped when the server redeploys or restarts, and the SQLite data with it. For Horizon, set `DATABASE_URL` to a hosted Postgres database (e.g. Neon or Supabase, both have free tiers).

## Run locally

```bash
pip install -r requirements.txt
python test_server.py                                    # smoke test
fastmcp run server.py:mcp --transport http --port 8000   # http://127.0.0.1:8000/mcp
```

## Deploy to Prefect Horizon

1. Push this folder to a GitHub repository:
   ```bash
   git init && git add . && git commit -m "Expense tracker MCP server"
   git remote add origin https://github.com/<you>/expense-tracker-mcp.git
   git push -u origin main
   ```
2. Sign in at https://horizon.prefect.io/ with GitHub and create a new server from that repo.
3. Set the **entrypoint** to `server.py:mcp`. Horizon installs dependencies from `requirements.txt`.
4. Add the environment variables (`DATABASE_URL`, `EXPENSE_CURRENCY`, `EXPENSE_TIMEZONE`) in the server settings, then deploy.
5. Copy the server URL Horizon gives you (it ends in `/mcp`) and add it to your MCP client, for example:
   ```bash
   claude mcp add --transport http expenses https://<your-server>/mcp
   ```
   In Claude.ai / Claude Desktop, add it as a custom connector.

Keep Horizon's authentication enabled. This server keeps a single shared ledger, so anyone who can reach the URL can read and change your expenses.

## Example prompts

- "Add 450 for groceries at DMart yesterday, paid by UPI"
- "Set a 5000 monthly budget for Food & Dining"
- "Summarize my spending for September 2026"
- "Generate my expense report for this month and tell me where I can cut back"
