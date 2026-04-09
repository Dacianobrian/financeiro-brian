import sqlite3
import os

# Em produção (Railway/Render), defina DB_PATH=/data/financeiro.db com volume persistente
DB_PATH = os.environ.get('DB_PATH', os.path.join(os.path.dirname(__file__), 'financeiro.db'))


def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn = get_conn()
    c = conn.cursor()

    c.executescript("""
        CREATE TABLE IF NOT EXISTS transactions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT NOT NULL,
            type TEXT NOT NULL,
            amount REAL NOT NULL,
            description TEXT,
            category TEXT,
            created_at TEXT DEFAULT (datetime('now','localtime'))
        );

        CREATE TABLE IF NOT EXISTS mei_invoices (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ref TEXT NOT NULL,
            issue_date TEXT,
            amount REAL NOT NULL,
            year INTEGER NOT NULL
        );

        CREATE TABLE IF NOT EXISTS das_payments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            month_name TEXT NOT NULL,
            year INTEGER NOT NULL,
            amount REAL NOT NULL,
            payment_date TEXT
        );

        CREATE TABLE IF NOT EXISTS fii_portfolio (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT UNIQUE NOT NULL,
            shares INTEGER NOT NULL DEFAULT 0,
            price REAL DEFAULT 0,
            sector TEXT,
            fund_type TEXT,
            pvp REAL,
            dy REAL,
            variation_12m REAL,
            dividends_per_share REAL DEFAULT 0.13,
            vacancy REAL,
            aum TEXT
        );

        CREATE TABLE IF NOT EXISTS fixed_costs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            description TEXT NOT NULL,
            amount REAL DEFAULT 0,
            due_day INTEGER,
            type TEXT DEFAULT 'saida'
        );

        CREATE TABLE IF NOT EXISTS goals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            year INTEGER,
            month INTEGER,
            meta_diario REAL DEFAULT 70,
            meta_reserva REAL DEFAULT 10000,
            meta_mensal REAL DEFAULT 10000
        );

        CREATE TABLE IF NOT EXISTS seeded (
            id INTEGER PRIMARY KEY
        );
    """)
    conn.commit()

    # Check if already seeded
    row = c.execute("SELECT id FROM seeded WHERE id = 1").fetchone()
    if row:
        conn.close()
        return

    # ── Fixed Costs ──────────────────────────────────────────────────────────
    fixed_costs = [
        ("Internet",          80.00,   5, "saida"),
        ("Celular Claro",     60.00,  15, "saida"),
        ("Saúde Unimed",     220.00,  15, "saida"),
        ("Financiamento",   2899.00,  17, "saida"),
        ("DAS MEI",            0.00,  20, "saida"),
        ("Cartão Bradesco", 4000.00,  25, "saida"),
        ("Capitalização",    100.00,  26, "saida"),
        ("Casa Line",       2090.00,  20, "saida"),
    ]
    c.executemany(
        "INSERT INTO fixed_costs (description, amount, due_day, type) VALUES (?,?,?,?)",
        fixed_costs
    )

    # ── FII Portfolio ─────────────────────────────────────────────────────────
    fiis = [
        ("VGIR11", 306,  9.63, "Títulos Mobiliários", "Papel", 0.98, 15.05, 21.13, 0.13, None, None),
        ("BTCI11", 264,  9.52, "Títulos Mobiliários", "Papel", 0.93, 12.31, 21.27, 0.10, None, None),
        ("MXRF11", 309,  9.07, "Híbrido",             "Papel", 1.01, 12.30, 17.41, 0.10, None, None),
        ("GARE11", 300,  9.33, "Híbrido",             "Tijolo",0.96,  6.19, 22.27, 0.08, None, None),
        ("BRCR11",  33, 43.82, "Híbrido",             "Tijolo",0.51, 12.24, 18.31, 0.41, 10.8, None),
        ("XPML11",  13,107.85, "Shopping e Varejo",   "Tijolo",0.99, 12.96, 27.37, 0.92,  4.10,None),
    ]
    c.executemany(
        """INSERT INTO fii_portfolio
           (ticker, shares, price, sector, fund_type, pvp, dy, variation_12m, dividends_per_share, vacancy, aum)
           VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
        fiis
    )

    # ── 2025 Monthly Transactions ─────────────────────────────────────────────
    monthly_2025 = [
        ("2025-01-31", "entrada",  948.50,  "Receita Janeiro 2025",    None),
        ("2025-02-28", "entrada",  791.11,  "Receita Fevereiro 2025",  None),
        ("2025-03-31", "entrada", 5800.31,  "Receita Março 2025",      None),
        ("2025-04-30", "entrada",13824.88,  "Receita Abril 2025",      None),
        ("2025-05-31", "entrada",14616.05,  "Receita Maio 2025",       None),
        ("2025-06-30", "entrada",22152.97,  "Receita Junho 2025",      None),
        ("2025-06-30", "investido",  100.0, "Investimento Junho 2025", None),
        ("2025-07-31", "entrada",29450.83,  "Receita Julho 2025",      None),
        ("2025-07-31", "investido", 5100.0, "Investimento Julho 2025", None),
        ("2025-08-31", "entrada",46280.00,  "Receita Agosto 2025",     None),
        ("2025-08-31", "investido",15100.0, "Investimento Agosto 2025",None),
        ("2025-09-30", "entrada",22889.18,  "Receita Setembro 2025",   None),
        ("2025-09-30", "investido", 5100.0, "Investimento Setembro 2025",None),
        ("2025-10-31", "entrada",21349.89,  "Receita Outubro 2025",    None),
        ("2025-10-31", "investido", 5100.0, "Investimento Outubro 2025",None),
        ("2025-11-30", "entrada",18825.80,  "Receita Novembro 2025",   None),
        ("2025-11-30", "investido", 5100.0, "Investimento Novembro 2025",None),
        ("2025-12-31", "entrada",24623.33,  "Receita Dezembro 2025",   None),
        ("2025-12-31", "investido",10100.0, "Investimento Dezembro 2025",None),
    ]
    c.executemany(
        "INSERT INTO transactions (date, type, amount, description, category) VALUES (?,?,?,?,?)",
        monthly_2025
    )

    # ── Default Goal ──────────────────────────────────────────────────────────
    c.execute(
        "INSERT INTO goals (year, month, meta_diario, meta_reserva, meta_mensal) VALUES (?,?,?,?,?)",
        (2025, None, 70, 10000, 10000)
    )

    c.execute("INSERT INTO seeded (id) VALUES (1)")
    conn.commit()
    conn.close()


# ── Query helpers ─────────────────────────────────────────────────────────────

def add_transaction(date, ttype, amount, description, category=None):
    conn = get_conn()
    c = conn.cursor()
    c.execute(
        "INSERT INTO transactions (date, type, amount, description, category) VALUES (?,?,?,?,?)",
        (date, ttype, amount, description, category)
    )
    conn.commit()
    conn.close()


def add_mei_invoice(ref, amount, year, issue_date=None):
    conn = get_conn()
    c = conn.cursor()
    c.execute(
        "INSERT INTO mei_invoices (ref, issue_date, amount, year) VALUES (?,?,?,?)",
        (ref, issue_date, amount, year)
    )
    conn.commit()
    conn.close()


def add_das_payment(month_name, year, amount, payment_date=None):
    conn = get_conn()
    c = conn.cursor()
    # Upsert: if same month+year exists, update it
    existing = c.execute(
        "SELECT id FROM das_payments WHERE month_name=? AND year=?",
        (month_name, year)
    ).fetchone()
    if existing:
        c.execute(
            "UPDATE das_payments SET amount=?, payment_date=? WHERE id=?",
            (amount, payment_date, existing['id'])
        )
    else:
        c.execute(
            "INSERT INTO das_payments (month_name, year, amount, payment_date) VALUES (?,?,?,?)",
            (month_name, year, amount, payment_date)
        )
    conn.commit()
    conn.close()


def upsert_fii(ticker, shares, price):
    conn = get_conn()
    c = conn.cursor()
    existing = c.execute("SELECT * FROM fii_portfolio WHERE ticker=?", (ticker,)).fetchone()
    if existing:
        c.execute(
            "UPDATE fii_portfolio SET shares=?, price=? WHERE ticker=?",
            (shares, price, ticker)
        )
    else:
        c.execute(
            "INSERT INTO fii_portfolio (ticker, shares, price) VALUES (?,?,?)",
            (ticker, shares, price)
        )
    conn.commit()
    conn.close()


def update_fii_price(ticker, price):
    conn = get_conn()
    c = conn.cursor()
    c.execute("UPDATE fii_portfolio SET price=? WHERE ticker=?", (price, ticker))
    conn.commit()
    conn.close()


def add_fii_shares(ticker, extra_shares, price):
    conn = get_conn()
    c = conn.cursor()
    existing = c.execute("SELECT shares FROM fii_portfolio WHERE ticker=?", (ticker,)).fetchone()
    if existing:
        new_shares = existing['shares'] + extra_shares
        c.execute(
            "UPDATE fii_portfolio SET shares=?, price=? WHERE ticker=?",
            (new_shares, price, ticker)
        )
    else:
        c.execute(
            "INSERT INTO fii_portfolio (ticker, shares, price) VALUES (?,?,?)",
            (ticker, extra_shares, price)
        )
    conn.commit()
    conn.close()


def get_recent_transactions(limit=10):
    conn = get_conn()
    c = conn.cursor()
    rows = c.execute(
        "SELECT * FROM transactions ORDER BY date DESC, id DESC LIMIT ?", (limit,)
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_monthly_summary(year: int = 2026):
    conn = get_conn()
    c = conn.cursor()
    months = [
        ("Jan",f"{year}-01"),("Fev",f"{year}-02"),("Mar",f"{year}-03"),
        ("Abr",f"{year}-04"),("Mai",f"{year}-05"),("Jun",f"{year}-06"),
        ("Jul",f"{year}-07"),("Ago",f"{year}-08"),("Set",f"{year}-09"),
        ("Out",f"{year}-10"),("Nov",f"{year}-11"),("Dez",f"{year}-12"),
    ]
    result = []
    for label, ym in months:
        row = c.execute("""
            SELECT
                COALESCE(SUM(CASE WHEN type='entrada'   THEN amount ELSE 0 END),0) AS entradas,
                COALESCE(SUM(CASE WHEN type='saida'     THEN amount ELSE 0 END),0) AS saidas,
                COALESCE(SUM(CASE WHEN type='investido' THEN amount ELSE 0 END),0) AS investido
            FROM transactions WHERE strftime('%Y-%m', date) = ?
        """, (ym,)).fetchone()
        result.append({"month": label, "entradas": row["entradas"],
                        "saidas": row["saidas"], "investido": row["investido"]})
    conn.close()
    return result


def get_expense_by_category(year: int = 2026):
    conn = get_conn()
    c = conn.cursor()
    rows = c.execute("""
        SELECT COALESCE(category,'Outros') AS cat,
               SUM(amount) AS total
        FROM transactions
        WHERE type='saida' AND strftime('%Y', date) = ?
        GROUP BY cat ORDER BY total DESC
    """, (str(year),)).fetchall()
    conn.close()
    return {r["cat"]: round(r["total"], 2) for r in rows}


def get_savings_rate(year: int = 2026):
    conn = get_conn()
    c = conn.cursor()
    row = c.execute("""
        SELECT
            COALESCE(SUM(CASE WHEN type='entrada'   THEN amount ELSE 0 END),0) AS e,
            COALESCE(SUM(CASE WHEN type='investido' THEN amount ELSE 0 END),0) AS i
        FROM transactions WHERE strftime('%Y', date) = ?
    """, (str(year),)).fetchone()
    conn.close()
    if row["e"] > 0:
        return round(row["i"] / row["e"] * 100, 1), round(row["i"], 2)
    return 0.0, 0.0


def get_monthly_summary_2025():
    conn = get_conn()
    c = conn.cursor()
    months = [
        ("Jan", "2025-01"), ("Fev", "2025-02"), ("Mar", "2025-03"),
        ("Abr", "2025-04"), ("Mai", "2025-05"), ("Jun", "2025-06"),
        ("Jul", "2025-07"), ("Ago", "2025-08"), ("Set", "2025-09"),
        ("Out", "2025-10"), ("Nov", "2025-11"), ("Dez", "2025-12"),
    ]
    result = []
    for label, ym in months:
        row = c.execute("""
            SELECT
                COALESCE(SUM(CASE WHEN type='entrada'  THEN amount ELSE 0 END), 0) AS entradas,
                COALESCE(SUM(CASE WHEN type='saida'    THEN amount ELSE 0 END), 0) AS saidas,
                COALESCE(SUM(CASE WHEN type='investido' THEN amount ELSE 0 END), 0) AS investido
            FROM transactions
            WHERE strftime('%Y-%m', date) = ?
        """, (ym,)).fetchone()
        result.append({
            "month": label,
            "entradas": row["entradas"],
            "saidas": row["saidas"],
            "investido": row["investido"],
        })
    conn.close()
    return result


def get_current_month_data():
    from datetime import date
    today = date.today()
    ym = today.strftime("%Y-%m")
    conn = get_conn()
    c = conn.cursor()
    row = c.execute("""
        SELECT
            COALESCE(SUM(CASE WHEN type='entrada'   THEN amount ELSE 0 END), 0) AS entradas,
            COALESCE(SUM(CASE WHEN type='saida'     THEN amount ELSE 0 END), 0) AS saidas,
            COALESCE(SUM(CASE WHEN type='investido' THEN amount ELSE 0 END), 0) AS investido
        FROM transactions
        WHERE strftime('%Y-%m', date) = ?
    """, (ym,)).fetchone()
    goal = c.execute(
        "SELECT * FROM goals ORDER BY id DESC LIMIT 1"
    ).fetchone()
    conn.close()
    return {
        "entradas": row["entradas"],
        "saidas": row["saidas"],
        "investido": row["investido"],
        "saldo": row["entradas"] - row["saidas"] - row["investido"],
        "meta_diario": goal["meta_diario"] if goal else 70,
        "meta_reserva": goal["meta_reserva"] if goal else 10000,
        "meta_mensal": goal["meta_mensal"] if goal else 10000,
    }


def get_fii_portfolio():
    conn = get_conn()
    c = conn.cursor()
    rows = c.execute("SELECT * FROM fii_portfolio ORDER BY ticker").fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_mei_invoices(limit=10):
    conn = get_conn()
    c = conn.cursor()
    rows = c.execute(
        "SELECT * FROM mei_invoices ORDER BY id DESC LIMIT ?", (limit,)
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_das_payments():
    conn = get_conn()
    c = conn.cursor()
    rows = c.execute(
        "SELECT * FROM das_payments ORDER BY year DESC, id DESC"
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_fixed_costs():
    conn = get_conn()
    c = conn.cursor()
    rows = c.execute("SELECT * FROM fixed_costs ORDER BY due_day").fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_patrimonio_total():
    conn = get_conn()
    c = conn.cursor()
    row = c.execute(
        "SELECT COALESCE(SUM(shares * price), 0) AS total FROM fii_portfolio"
    ).fetchone()
    conn.close()
    return row["total"]


def get_accounts():
    conn = get_conn()
    c = conn.cursor()
    rows = c.execute("SELECT * FROM accounts ORDER BY bank, type").fetchall()
    conn.close()
    return [dict(r) for r in rows]


def update_account(name: str, balance: float):
    conn = get_conn()
    c = conn.cursor()
    existing = c.execute("SELECT id FROM accounts WHERE LOWER(name) LIKE ?", (f"%{name.lower()}%",)).fetchone()
    if existing:
        c.execute("UPDATE accounts SET balance=?, updated_at=datetime('now','localtime') WHERE id=?",
                  (balance, existing["id"]))
        conn.commit()
        conn.close()
        return True
    conn.close()
    return False


def get_patrimonio_breakdown():
    conn = get_conn()
    c = conn.cursor()
    accounts   = c.execute("SELECT SUM(balance) FROM accounts").fetchone()[0] or 0
    fii_total  = c.execute("SELECT SUM(shares*price) FROM fii_portfolio").fetchone()[0] or 0
    conn.close()
    return {"accounts": accounts, "fii": fii_total, "total": accounts + fii_total}


def get_savings_rate_2025():
    conn = get_conn()
    c = conn.cursor()
    row = c.execute("""
        SELECT
            COALESCE(SUM(CASE WHEN type='entrada'   THEN amount ELSE 0 END), 0) AS total_entrada,
            COALESCE(SUM(CASE WHEN type='investido' THEN amount ELSE 0 END), 0) AS total_investido
        FROM transactions
        WHERE strftime('%Y', date) = '2025'
    """).fetchone()
    conn.close()
    if row["total_entrada"] > 0:
        return round((row["total_investido"] / row["total_entrada"]) * 100, 1)
    return 0.0


def get_all_transactions():
    conn = get_conn()
    c = conn.cursor()
    rows = c.execute("SELECT * FROM transactions ORDER BY date DESC, id DESC").fetchall()
    conn.close()
    return [dict(r) for r in rows]
