import os

# ── Conexão: PostgreSQL (Supabase) em produção, SQLite local ─────────────────
DATABASE_URL = os.environ.get('DATABASE_URL')  # postgres://user:pass@host:5432/db

if DATABASE_URL:
    import psycopg2
    import psycopg2.extras

    def get_conn():
        conn = psycopg2.connect(DATABASE_URL)
        return conn

    def _fetch(cursor):
        cols = [d[0] for d in cursor.description]
        return [dict(zip(cols, row)) for row in cursor.fetchall()]

    def _fetchone(cursor):
        cols = [d[0] for d in cursor.description]
        row = cursor.fetchone()
        return dict(zip(cols, row)) if row else None

    def _q(sql):
        """Convert ? placeholders to %s for psycopg2."""
        return sql.replace('?', '%s')

    def _strftime_year(col, val):
        return f"to_char({col}, 'YYYY') = %s", (val,)

    def _strftime_ym(col, val):
        return f"to_char({col}::date, 'YYYY-MM') = %s", (val,)

    def _now():
        return "to_char(now(), 'YYYY-MM-DD HH24:MI:SS')"

else:
    import sqlite3
    DB_PATH = os.environ.get('DB_PATH', os.path.join(os.path.dirname(__file__), 'financeiro.db'))

    def get_conn():
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        return conn

    def _fetch(cursor):
        return [dict(r) for r in cursor.fetchall()]

    def _fetchone(cursor):
        r = cursor.fetchone()
        return dict(r) if r else None

    def _q(sql):
        return sql

    def _strftime_year(col, val):
        return f"strftime('%Y', {col}) = ?", (str(val),)

    def _strftime_ym(col, val):
        return f"strftime('%Y-%m', {col}) = ?", (val,)

    def _now():
        return "datetime('now','localtime')"


def init_db():
    """Only needed for SQLite local dev. Supabase schema is pre-created."""
    if DATABASE_URL:
        return  # Supabase already has all tables

    conn = get_conn()
    c = conn.cursor()
    c.executescript("""
        CREATE TABLE IF NOT EXISTS transactions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT NOT NULL, type TEXT NOT NULL, amount REAL NOT NULL,
            description TEXT, category TEXT, account TEXT,
            created_at TEXT DEFAULT (datetime('now','localtime'))
        );
        CREATE TABLE IF NOT EXISTS mei_invoices (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ref TEXT NOT NULL, issue_date TEXT, amount REAL NOT NULL, year INTEGER NOT NULL
        );
        CREATE TABLE IF NOT EXISTS das_payments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            month_name TEXT NOT NULL, year INTEGER NOT NULL, amount REAL NOT NULL, payment_date TEXT
        );
        CREATE TABLE IF NOT EXISTS fii_portfolio (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT UNIQUE NOT NULL, shares INTEGER NOT NULL DEFAULT 0, price REAL DEFAULT 0,
            sector TEXT, fund_type TEXT, pvp REAL, dy REAL, variation_12m REAL,
            dividends_per_share REAL DEFAULT 0.13, vacancy REAL, aum TEXT
        );
        CREATE TABLE IF NOT EXISTS fixed_costs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            description TEXT NOT NULL, amount REAL DEFAULT 0, due_day INTEGER, type TEXT DEFAULT 'saida'
        );
        CREATE TABLE IF NOT EXISTS goals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            year INTEGER, month INTEGER, meta_diario REAL DEFAULT 70,
            meta_reserva REAL DEFAULT 10000, meta_mensal REAL DEFAULT 10000
        );
        CREATE TABLE IF NOT EXISTS accounts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE NOT NULL, bank TEXT NOT NULL, type TEXT NOT NULL,
            balance REAL DEFAULT 0, color TEXT DEFAULT '#6366f1',
            icon TEXT DEFAULT '🏦', updated_at TEXT
        );
        CREATE TABLE IF NOT EXISTS income_categories (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE NOT NULL, color TEXT DEFAULT '#22c55e', icon TEXT DEFAULT '💵'
        );
        CREATE TABLE IF NOT EXISTS expense_categories (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE NOT NULL, color TEXT DEFAULT '#94a3b8', icon TEXT DEFAULT '💰'
        );
        CREATE TABLE IF NOT EXISTS seeded (id INTEGER PRIMARY KEY);
    """)
    conn.commit()

    row = c.execute("SELECT id FROM seeded WHERE id = 1").fetchone()
    if row:
        conn.close()
        return

    # Seed default data
    c.executemany("INSERT INTO fixed_costs (description, amount, due_day, type) VALUES (?,?,?,?)", [
        ("Internet", 80.00, 5, "saida"), ("Celular Claro", 60.00, 15, "saida"),
        ("Saúde Unimed", 220.00, 15, "saida"), ("Financiamento", 2899.00, 17, "saida"),
        ("DAS MEI", 0.00, 20, "saida"), ("Cartão Bradesco", 4000.00, 25, "saida"),
        ("Capitalização", 100.00, 26, "saida"), ("Casa Line", 2090.00, 20, "saida"),
    ])
    c.executemany("""INSERT INTO accounts (name, bank, type, balance, color, icon) VALUES (?,?,?,?,?,?)""", [
        ("Nubank Conta Corrente", "nubank", "corrente", 0, "#820ad1", "💜"),
        ("Nubank Caixinha (RDB)", "nubank", "investimento", 0, "#a855f7", "📦"),
        ("Bradesco Conta Corrente", "bradesco", "corrente", 0, "#cc092f", "❤️"),
    ])
    c.execute("INSERT INTO goals (year, meta_diario, meta_reserva, meta_mensal) VALUES (2025, 70, 10000, 10000)")
    c.execute("INSERT INTO seeded (id) VALUES (1)")
    conn.commit()
    conn.close()


# ── Helpers ───────────────────────────────────────────────────────────────────

def add_transaction(date, ttype, amount, description, category=None, account=None):
    conn = get_conn()
    c = conn.cursor()
    if DATABASE_URL:
        c.execute(
            "INSERT INTO transactions (date, type, amount, description, category, account) VALUES (%s,%s,%s,%s,%s,%s)",
            (date, ttype, amount, description, category, account)
        )
    else:
        c.execute(
            "INSERT INTO transactions (date, type, amount, description, category, account) VALUES (?,?,?,?,?,?)",
            (date, ttype, amount, description, category, account)
        )
    conn.commit()
    conn.close()


def add_mei_invoice(ref, amount, year, issue_date=None):
    conn = get_conn()
    c = conn.cursor()
    if DATABASE_URL:
        c.execute("INSERT INTO mei_invoices (ref, issue_date, amount, year) VALUES (%s,%s,%s,%s)",
                  (ref, issue_date, amount, year))
    else:
        c.execute("INSERT INTO mei_invoices (ref, issue_date, amount, year) VALUES (?,?,?,?)",
                  (ref, issue_date, amount, year))
    conn.commit()
    conn.close()


def add_das_payment(month_name, year, amount, payment_date=None):
    conn = get_conn()
    c = conn.cursor()
    if DATABASE_URL:
        c.execute("SELECT id FROM das_payments WHERE month_name=%s AND year=%s", (month_name, year))
        existing = c.fetchone()
        if existing:
            c.execute("UPDATE das_payments SET amount=%s, payment_date=%s WHERE id=%s",
                      (amount, payment_date, existing[0]))
        else:
            c.execute("INSERT INTO das_payments (month_name, year, amount, payment_date) VALUES (%s,%s,%s,%s)",
                      (month_name, year, amount, payment_date))
    else:
        existing = c.execute("SELECT id FROM das_payments WHERE month_name=? AND year=?",
                             (month_name, year)).fetchone()
        if existing:
            c.execute("UPDATE das_payments SET amount=?, payment_date=? WHERE id=?",
                      (amount, payment_date, existing[0]))
        else:
            c.execute("INSERT INTO das_payments (month_name, year, amount, payment_date) VALUES (?,?,?,?)",
                      (month_name, year, amount, payment_date))
    conn.commit()
    conn.close()


def upsert_fii(ticker, shares, price):
    conn = get_conn()
    c = conn.cursor()
    if DATABASE_URL:
        c.execute("SELECT id FROM fii_portfolio WHERE ticker=%s", (ticker,))
        existing = c.fetchone()
        if existing:
            c.execute("UPDATE fii_portfolio SET shares=%s, price=%s WHERE ticker=%s", (shares, price, ticker))
        else:
            c.execute("INSERT INTO fii_portfolio (ticker, shares, price) VALUES (%s,%s,%s)", (ticker, shares, price))
    else:
        existing = c.execute("SELECT id FROM fii_portfolio WHERE ticker=?", (ticker,)).fetchone()
        if existing:
            c.execute("UPDATE fii_portfolio SET shares=?, price=? WHERE ticker=?", (shares, price, ticker))
        else:
            c.execute("INSERT INTO fii_portfolio (ticker, shares, price) VALUES (?,?,?)", (ticker, shares, price))
    conn.commit()
    conn.close()


def update_fii_price(ticker, price):
    conn = get_conn()
    c = conn.cursor()
    if DATABASE_URL:
        c.execute("UPDATE fii_portfolio SET price=%s WHERE ticker=%s", (price, ticker))
    else:
        c.execute("UPDATE fii_portfolio SET price=? WHERE ticker=?", (price, ticker))
    conn.commit()
    conn.close()


def add_fii_shares(ticker, extra_shares, price):
    conn = get_conn()
    c = conn.cursor()
    if DATABASE_URL:
        c.execute("SELECT shares FROM fii_portfolio WHERE ticker=%s", (ticker,))
        row = c.fetchone()
        if row:
            c.execute("UPDATE fii_portfolio SET shares=%s, price=%s WHERE ticker=%s",
                      (row[0] + extra_shares, price, ticker))
        else:
            c.execute("INSERT INTO fii_portfolio (ticker, shares, price) VALUES (%s,%s,%s)",
                      (ticker, extra_shares, price))
    else:
        existing = c.execute("SELECT shares FROM fii_portfolio WHERE ticker=?", (ticker,)).fetchone()
        if existing:
            c.execute("UPDATE fii_portfolio SET shares=?, price=? WHERE ticker=?",
                      (existing['shares'] + extra_shares, price, ticker))
        else:
            c.execute("INSERT INTO fii_portfolio (ticker, shares, price) VALUES (?,?,?)",
                      (ticker, extra_shares, price))
    conn.commit()
    conn.close()


def get_recent_transactions(limit=10):
    conn = get_conn()
    c = conn.cursor()
    if DATABASE_URL:
        c.execute("SELECT * FROM transactions ORDER BY date DESC, id DESC LIMIT %s", (limit,))
        rows = _fetch(c)
    else:
        rows = _fetch(c.execute("SELECT * FROM transactions ORDER BY date DESC, id DESC LIMIT ?", (limit,)))
    conn.close()
    return rows


def get_monthly_summary(year: int = 2026):
    conn = get_conn()
    c = conn.cursor()
    months = [
        ("Jan", f"{year}-01"), ("Fev", f"{year}-02"), ("Mar", f"{year}-03"),
        ("Abr", f"{year}-04"), ("Mai", f"{year}-05"), ("Jun", f"{year}-06"),
        ("Jul", f"{year}-07"), ("Ago", f"{year}-08"), ("Set", f"{year}-09"),
        ("Out", f"{year}-10"), ("Nov", f"{year}-11"), ("Dez", f"{year}-12"),
    ]
    result = []
    for label, ym in months:
        if DATABASE_URL:
            c.execute("""
                SELECT
                    COALESCE(SUM(CASE WHEN type='entrada'   THEN amount ELSE 0 END),0) AS entradas,
                    COALESCE(SUM(CASE WHEN type='saida'     THEN amount ELSE 0 END),0) AS saidas,
                    COALESCE(SUM(CASE WHEN type='investido' THEN amount ELSE 0 END),0) AS investido
                FROM transactions WHERE to_char(date::date, 'YYYY-MM') = %s
            """, (ym,))
            row = _fetchone(c)
        else:
            row = _fetchone(c.execute("""
                SELECT
                    COALESCE(SUM(CASE WHEN type='entrada'   THEN amount ELSE 0 END),0) AS entradas,
                    COALESCE(SUM(CASE WHEN type='saida'     THEN amount ELSE 0 END),0) AS saidas,
                    COALESCE(SUM(CASE WHEN type='investido' THEN amount ELSE 0 END),0) AS investido
                FROM transactions WHERE strftime('%Y-%m', date) = ?
            """, (ym,)))
        result.append({"month": label, "entradas": row["entradas"],
                       "saidas": row["saidas"], "investido": row["investido"]})
    conn.close()
    return result


def get_expense_by_category(year: int = 2026):
    conn = get_conn()
    c = conn.cursor()
    if DATABASE_URL:
        c.execute("""
            SELECT COALESCE(category,'Outros') AS cat, SUM(amount) AS total
            FROM transactions
            WHERE type='saida' AND to_char(date::date, 'YYYY') = %s
            GROUP BY cat ORDER BY total DESC
        """, (str(year),))
        rows = _fetch(c)
    else:
        rows = _fetch(c.execute("""
            SELECT COALESCE(category,'Outros') AS cat, SUM(amount) AS total
            FROM transactions
            WHERE type='saida' AND strftime('%Y', date) = ?
            GROUP BY cat ORDER BY total DESC
        """, (str(year),)))
    conn.close()
    return {r["cat"]: round(r["total"], 2) for r in rows}


def get_savings_rate(year: int = 2026):
    conn = get_conn()
    c = conn.cursor()
    if DATABASE_URL:
        c.execute("""
            SELECT
                COALESCE(SUM(CASE WHEN type='entrada'   THEN amount ELSE 0 END),0) AS e,
                COALESCE(SUM(CASE WHEN type='investido' THEN amount ELSE 0 END),0) AS i
            FROM transactions WHERE to_char(date::date, 'YYYY') = %s
        """, (str(year),))
        row = _fetchone(c)
    else:
        row = _fetchone(c.execute("""
            SELECT
                COALESCE(SUM(CASE WHEN type='entrada'   THEN amount ELSE 0 END),0) AS e,
                COALESCE(SUM(CASE WHEN type='investido' THEN amount ELSE 0 END),0) AS i
            FROM transactions WHERE strftime('%Y', date) = ?
        """, (str(year),)))
    conn.close()
    if row and row["e"] > 0:
        return round(row["i"] / row["e"] * 100, 1), round(row["i"], 2)
    return 0.0, 0.0


def get_monthly_summary_2025():
    return get_monthly_summary(2025)


def get_current_month_data():
    from datetime import date
    today = date.today()
    ym = today.strftime("%Y-%m")
    conn = get_conn()
    c = conn.cursor()
    if DATABASE_URL:
        c.execute("""
            SELECT
                COALESCE(SUM(CASE WHEN type='entrada'   THEN amount ELSE 0 END), 0) AS entradas,
                COALESCE(SUM(CASE WHEN type='saida'     THEN amount ELSE 0 END), 0) AS saidas,
                COALESCE(SUM(CASE WHEN type='investido' THEN amount ELSE 0 END), 0) AS investido
            FROM transactions WHERE to_char(date::date, 'YYYY-MM') = %s
        """, (ym,))
        row = _fetchone(c)
        c.execute("SELECT * FROM goals ORDER BY id DESC LIMIT 1")
        goal = _fetchone(c)
    else:
        row = _fetchone(c.execute("""
            SELECT
                COALESCE(SUM(CASE WHEN type='entrada'   THEN amount ELSE 0 END), 0) AS entradas,
                COALESCE(SUM(CASE WHEN type='saida'     THEN amount ELSE 0 END), 0) AS saidas,
                COALESCE(SUM(CASE WHEN type='investido' THEN amount ELSE 0 END), 0) AS investido
            FROM transactions WHERE strftime('%Y-%m', date) = ?
        """, (ym,)))
        goal = _fetchone(c.execute("SELECT * FROM goals ORDER BY id DESC LIMIT 1"))
    conn.close()
    return {
        "entradas": row["entradas"], "saidas": row["saidas"], "investido": row["investido"],
        "saldo": row["entradas"] - row["saidas"] - row["investido"],
        "meta_diario": goal["meta_diario"] if goal else 70,
        "meta_reserva": goal["meta_reserva"] if goal else 10000,
        "meta_mensal": goal["meta_mensal"] if goal else 10000,
    }


def get_fii_portfolio():
    conn = get_conn()
    c = conn.cursor()
    if DATABASE_URL:
        c.execute("SELECT * FROM fii_portfolio ORDER BY ticker")
        rows = _fetch(c)
    else:
        rows = _fetch(c.execute("SELECT * FROM fii_portfolio ORDER BY ticker"))
    conn.close()
    return rows


def get_mei_invoices(limit=10):
    conn = get_conn()
    c = conn.cursor()
    if DATABASE_URL:
        c.execute("SELECT * FROM mei_invoices ORDER BY id DESC LIMIT %s", (limit,))
        rows = _fetch(c)
    else:
        rows = _fetch(c.execute("SELECT * FROM mei_invoices ORDER BY id DESC LIMIT ?", (limit,)))
    conn.close()
    return rows


def get_das_payments():
    conn = get_conn()
    c = conn.cursor()
    if DATABASE_URL:
        c.execute("SELECT * FROM das_payments ORDER BY year DESC, id DESC")
        rows = _fetch(c)
    else:
        rows = _fetch(c.execute("SELECT * FROM das_payments ORDER BY year DESC, id DESC"))
    conn.close()
    return rows


def get_fixed_costs():
    conn = get_conn()
    c = conn.cursor()
    if DATABASE_URL:
        c.execute("SELECT * FROM fixed_costs ORDER BY due_day NULLS LAST, id")
        rows = _fetch(c)
    else:
        rows = _fetch(c.execute("SELECT * FROM fixed_costs ORDER BY due_day, id"))
    conn.close()
    return rows


def add_fixed_cost(description, amount, due_day):
    conn = get_conn()
    c = conn.cursor()
    if DATABASE_URL:
        c.execute(
            "INSERT INTO fixed_costs (description, amount, due_day, type) VALUES (%s,%s,%s,'saida') RETURNING id",
            (description, amount, due_day)
        )
        row_id = c.fetchone()[0]
    else:
        c.execute("INSERT INTO fixed_costs (description, amount, due_day, type) VALUES (?,?,?,'saida')",
                  (description, amount, due_day))
        row_id = c.lastrowid
    conn.commit()
    conn.close()
    return row_id


def update_fixed_cost(cost_id, description, amount, due_day):
    conn = get_conn()
    c = conn.cursor()
    if DATABASE_URL:
        c.execute("UPDATE fixed_costs SET description=%s, amount=%s, due_day=%s WHERE id=%s",
                  (description, amount, due_day, cost_id))
    else:
        c.execute("UPDATE fixed_costs SET description=?, amount=?, due_day=? WHERE id=?",
                  (description, amount, due_day, cost_id))
    conn.commit()
    conn.close()


def delete_fixed_cost(cost_id):
    conn = get_conn()
    c = conn.cursor()
    if DATABASE_URL:
        c.execute("DELETE FROM fixed_costs WHERE id=%s", (cost_id,))
    else:
        c.execute("DELETE FROM fixed_costs WHERE id=?", (cost_id,))
    conn.commit()
    conn.close()


def get_patrimonio_total():
    conn = get_conn()
    c = conn.cursor()
    if DATABASE_URL:
        c.execute("SELECT COALESCE(SUM(shares * price), 0) AS total FROM fii_portfolio")
        row = _fetchone(c)
    else:
        row = _fetchone(c.execute("SELECT COALESCE(SUM(shares * price), 0) AS total FROM fii_portfolio"))
    conn.close()
    return row["total"]


def get_accounts():
    conn = get_conn()
    c = conn.cursor()
    if DATABASE_URL:
        c.execute("SELECT * FROM accounts ORDER BY bank, type")
        rows = _fetch(c)
    else:
        rows = _fetch(c.execute("SELECT * FROM accounts ORDER BY bank, type"))
    conn.close()
    return rows


def update_account(name: str, balance: float):
    conn = get_conn()
    c = conn.cursor()
    if DATABASE_URL:
        c.execute("SELECT id FROM accounts WHERE LOWER(name) LIKE %s", (f"%{name.lower()}%",))
        existing = c.fetchone()
        if existing:
            c.execute("UPDATE accounts SET balance=%s, updated_at=to_char(now(),'YYYY-MM-DD HH24:MI:SS') WHERE id=%s",
                      (balance, existing[0]))
            conn.commit()
            conn.close()
            return True
    else:
        existing = c.execute("SELECT id FROM accounts WHERE LOWER(name) LIKE ?",
                             (f"%{name.lower()}%",)).fetchone()
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
    if DATABASE_URL:
        c.execute("SELECT COALESCE(SUM(balance),0) AS total FROM accounts")
        accounts = c.fetchone()[0]
        c.execute("SELECT COALESCE(SUM(shares*price),0) AS total FROM fii_portfolio")
        fii_total = c.fetchone()[0]
    else:
        accounts = c.execute("SELECT SUM(balance) FROM accounts").fetchone()[0] or 0
        fii_total = c.execute("SELECT SUM(shares*price) FROM fii_portfolio").fetchone()[0] or 0
    conn.close()
    return {"accounts": float(accounts), "fii": float(fii_total), "total": float(accounts) + float(fii_total)}


def get_savings_rate_2025():
    rate, _ = get_savings_rate(2025)
    return rate


def get_all_transactions():
    conn = get_conn()
    c = conn.cursor()
    if DATABASE_URL:
        c.execute("SELECT * FROM transactions ORDER BY date DESC, id DESC")
        rows = _fetch(c)
    else:
        rows = _fetch(c.execute("SELECT * FROM transactions ORDER BY date DESC, id DESC"))
    conn.close()
    return rows
