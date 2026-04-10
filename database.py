import os

# ── Conexão: Supabase REST API em produção, SQLite local ─────────────────────
SUPABASE_URL = os.environ.get('SUPABASE_URL')
SUPABASE_KEY = os.environ.get('SUPABASE_KEY')

if SUPABASE_URL and SUPABASE_KEY:
    import requests as _req

    _REST = f"{SUPABASE_URL}/rest/v1"
    _HDR = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
    }

    def _get(table, params=None):
        r = _req.get(f"{_REST}/{table}", headers=_HDR, params=params)
        r.raise_for_status()
        return r.json()

    def _post(table, data, returning=False):
        h = dict(_HDR)
        if returning:
            h["Prefer"] = "return=representation"
        r = _req.post(f"{_REST}/{table}", headers=h, json=data)
        r.raise_for_status()
        return r.json() if returning else None

    def _patch(table, params, data):
        r = _req.patch(f"{_REST}/{table}", headers=_HDR, params=params, json=data)
        r.raise_for_status()

    def _delete(table, params):
        r = _req.delete(f"{_REST}/{table}", headers=_HDR, params=params)
        r.raise_for_status()

    def _rpc(func, params=None):
        r = _req.post(f"{_REST}/rpc/{func}", headers=_HDR, json=params or {})
        r.raise_for_status()
        return r.json()

    USE_SUPABASE = True

else:
    import sqlite3
    DB_PATH = os.environ.get('DB_PATH', os.path.join(os.path.dirname(__file__), 'financeiro.db'))

    def _sqlite():
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        return conn

    USE_SUPABASE = False


# ── Init (SQLite local only) ──────────────────────────────────────────────────

def init_db():
    if USE_SUPABASE:
        return

    conn = _sqlite()
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

    c.executemany("INSERT INTO fixed_costs (description, amount, due_day, type) VALUES (?,?,?,?)", [
        ("Internet", 80.00, 5, "saida"), ("Celular Claro", 60.00, 15, "saida"),
        ("Saúde Unimed", 220.00, 15, "saida"), ("Financiamento", 2899.00, 17, "saida"),
        ("DAS MEI", 0.00, 20, "saida"), ("Cartão Bradesco", 4000.00, 25, "saida"),
        ("Capitalização", 100.00, 26, "saida"), ("Casa Line", 2090.00, 20, "saida"),
    ])
    c.executemany("INSERT INTO accounts (name, bank, type, balance, color, icon) VALUES (?,?,?,?,?,?)", [
        ("Nubank Conta Corrente", "nubank", "corrente", 0, "#820ad1", "💜"),
        ("Nubank Caixinha (RDB)", "nubank", "investimento", 0, "#a855f7", "📦"),
        ("Bradesco Conta Corrente", "bradesco", "corrente", 0, "#cc092f", "❤️"),
    ])
    c.execute("INSERT INTO goals (year, meta_diario, meta_reserva, meta_mensal) VALUES (2025, 70, 10000, 10000)")
    c.execute("INSERT INTO seeded (id) VALUES (1)")
    conn.commit()
    conn.close()


# ── Write functions ───────────────────────────────────────────────────────────

def add_transaction(date, ttype, amount, description, category=None, account=None):
    if USE_SUPABASE:
        _post('transactions', {
            'date': date, 'type': ttype, 'amount': amount,
            'description': description, 'category': category, 'account': account
        })
    else:
        conn = _sqlite()
        conn.execute(
            "INSERT INTO transactions (date,type,amount,description,category,account) VALUES (?,?,?,?,?,?)",
            (date, ttype, amount, description, category, account)
        )
        conn.commit(); conn.close()


def add_mei_invoice(ref, amount, year, issue_date=None):
    if USE_SUPABASE:
        _post('mei_invoices', {'ref': ref, 'issue_date': issue_date, 'amount': amount, 'year': year})
    else:
        conn = _sqlite()
        conn.execute("INSERT INTO mei_invoices (ref,issue_date,amount,year) VALUES (?,?,?,?)",
                     (ref, issue_date, amount, year))
        conn.commit(); conn.close()


def add_das_payment(month_name, year, amount, payment_date=None):
    if USE_SUPABASE:
        existing = _get('das_payments', {'month_name': f'eq.{month_name}', 'year': f'eq.{year}', 'select': 'id'})
        if existing:
            _patch('das_payments', {'id': f'eq.{existing[0]["id"]}'}, {'amount': amount, 'payment_date': payment_date})
        else:
            _post('das_payments', {'month_name': month_name, 'year': year, 'amount': amount, 'payment_date': payment_date})
    else:
        conn = _sqlite()
        c = conn.cursor()
        row = c.execute("SELECT id FROM das_payments WHERE month_name=? AND year=?", (month_name, year)).fetchone()
        if row:
            c.execute("UPDATE das_payments SET amount=?, payment_date=? WHERE id=?", (amount, payment_date, row[0]))
        else:
            c.execute("INSERT INTO das_payments (month_name,year,amount,payment_date) VALUES (?,?,?,?)",
                      (month_name, year, amount, payment_date))
        conn.commit(); conn.close()


def upsert_fii(ticker, shares, price):
    if USE_SUPABASE:
        existing = _get('fii_portfolio', {'ticker': f'eq.{ticker}', 'select': 'id'})
        if existing:
            _patch('fii_portfolio', {'ticker': f'eq.{ticker}'}, {'shares': shares, 'price': price})
        else:
            _post('fii_portfolio', {'ticker': ticker, 'shares': shares, 'price': price})
    else:
        conn = _sqlite()
        c = conn.cursor()
        row = c.execute("SELECT id FROM fii_portfolio WHERE ticker=?", (ticker,)).fetchone()
        if row:
            c.execute("UPDATE fii_portfolio SET shares=?,price=? WHERE ticker=?", (shares, price, ticker))
        else:
            c.execute("INSERT INTO fii_portfolio (ticker,shares,price) VALUES (?,?,?)", (ticker, shares, price))
        conn.commit(); conn.close()


def update_fii_price(ticker, price):
    if USE_SUPABASE:
        _patch('fii_portfolio', {'ticker': f'eq.{ticker}'}, {'price': price})
    else:
        conn = _sqlite()
        conn.execute("UPDATE fii_portfolio SET price=? WHERE ticker=?", (price, ticker))
        conn.commit(); conn.close()


def add_fii_shares(ticker, extra_shares, price):
    if USE_SUPABASE:
        existing = _get('fii_portfolio', {'ticker': f'eq.{ticker}', 'select': 'shares'})
        if existing:
            _patch('fii_portfolio', {'ticker': f'eq.{ticker}'},
                   {'shares': existing[0]['shares'] + extra_shares, 'price': price})
        else:
            _post('fii_portfolio', {'ticker': ticker, 'shares': extra_shares, 'price': price})
    else:
        conn = _sqlite()
        c = conn.cursor()
        row = c.execute("SELECT shares FROM fii_portfolio WHERE ticker=?", (ticker,)).fetchone()
        if row:
            c.execute("UPDATE fii_portfolio SET shares=?,price=? WHERE ticker=?",
                      (dict(row)['shares'] + extra_shares, price, ticker))
        else:
            c.execute("INSERT INTO fii_portfolio (ticker,shares,price) VALUES (?,?,?)", (ticker, extra_shares, price))
        conn.commit(); conn.close()


def add_fixed_cost(description, amount, due_day):
    if USE_SUPABASE:
        rows = _post('fixed_costs',
                     {'description': description, 'amount': amount, 'due_day': due_day, 'type': 'saida'},
                     returning=True)
        return rows[0]['id'] if rows else None
    else:
        conn = _sqlite()
        c = conn.cursor()
        c.execute("INSERT INTO fixed_costs (description,amount,due_day,type) VALUES (?,?,?,'saida')",
                  (description, amount, due_day))
        row_id = c.lastrowid
        conn.commit(); conn.close()
        return row_id


def update_fixed_cost(cost_id, description, amount, due_day):
    if USE_SUPABASE:
        _patch('fixed_costs', {'id': f'eq.{cost_id}'}, {'description': description, 'amount': amount, 'due_day': due_day})
    else:
        conn = _sqlite()
        conn.execute("UPDATE fixed_costs SET description=?,amount=?,due_day=? WHERE id=?",
                     (description, amount, due_day, cost_id))
        conn.commit(); conn.close()


def delete_fixed_cost(cost_id):
    if USE_SUPABASE:
        _delete('fixed_costs', {'id': f'eq.{cost_id}'})
    else:
        conn = _sqlite()
        conn.execute("DELETE FROM fixed_costs WHERE id=?", (cost_id,))
        conn.commit(); conn.close()


def update_account(name: str, balance: float):
    if USE_SUPABASE:
        rows = _get('accounts', {'name': f'ilike.*{name}*', 'select': 'id'})
        if rows:
            _patch('accounts', {'id': f'eq.{rows[0]["id"]}'}, {'balance': balance})
            return True
        return False
    else:
        conn = _sqlite()
        c = conn.cursor()
        row = c.execute("SELECT id FROM accounts WHERE LOWER(name) LIKE ?", (f"%{name.lower()}%",)).fetchone()
        if row:
            c.execute("UPDATE accounts SET balance=?,updated_at=datetime('now','localtime') WHERE id=?",
                      (balance, row[0]))
            conn.commit(); conn.close()
            return True
        conn.close()
        return False


# ── Read functions ────────────────────────────────────────────────────────────

def get_recent_transactions(limit=10):
    if USE_SUPABASE:
        return _get('transactions', {'select': '*', 'order': 'date.desc,id.desc', 'limit': limit})
    else:
        conn = _sqlite()
        rows = [dict(r) for r in conn.execute(
            "SELECT * FROM transactions ORDER BY date DESC, id DESC LIMIT ?", (limit,)
        ).fetchall()]
        conn.close()
        return rows


def get_all_transactions():
    if USE_SUPABASE:
        return _get('transactions', {'select': '*', 'order': 'date.desc,id.desc', 'limit': 2000})
    else:
        conn = _sqlite()
        rows = [dict(r) for r in conn.execute(
            "SELECT * FROM transactions ORDER BY date DESC, id DESC"
        ).fetchall()]
        conn.close()
        return rows


def get_fii_portfolio():
    if USE_SUPABASE:
        return _get('fii_portfolio', {'select': '*', 'order': 'ticker.asc'})
    else:
        conn = _sqlite()
        rows = [dict(r) for r in conn.execute("SELECT * FROM fii_portfolio ORDER BY ticker").fetchall()]
        conn.close()
        return rows


def get_mei_invoices(limit=10):
    if USE_SUPABASE:
        return _get('mei_invoices', {'select': '*', 'order': 'id.desc', 'limit': limit})
    else:
        conn = _sqlite()
        rows = [dict(r) for r in conn.execute(
            "SELECT * FROM mei_invoices ORDER BY id DESC LIMIT ?", (limit,)
        ).fetchall()]
        conn.close()
        return rows


def get_das_payments():
    if USE_SUPABASE:
        return _get('das_payments', {'select': '*', 'order': 'year.desc,id.desc'})
    else:
        conn = _sqlite()
        rows = [dict(r) for r in conn.execute(
            "SELECT * FROM das_payments ORDER BY year DESC, id DESC"
        ).fetchall()]
        conn.close()
        return rows


def get_fixed_costs():
    if USE_SUPABASE:
        return _get('fixed_costs', {'select': '*', 'order': 'due_day.asc.nullslast,id.asc'})
    else:
        conn = _sqlite()
        rows = [dict(r) for r in conn.execute(
            "SELECT * FROM fixed_costs ORDER BY due_day, id"
        ).fetchall()]
        conn.close()
        return rows


def get_accounts():
    if USE_SUPABASE:
        return _get('accounts', {'select': '*', 'order': 'bank.asc,type.asc'})
    else:
        conn = _sqlite()
        rows = [dict(r) for r in conn.execute("SELECT * FROM accounts ORDER BY bank, type").fetchall()]
        conn.close()
        return rows


def get_patrimonio_total():
    if USE_SUPABASE:
        fiis = _get('fii_portfolio', {'select': 'shares,price'})
        return sum(f['shares'] * f['price'] for f in fiis)
    else:
        conn = _sqlite()
        row = conn.execute("SELECT COALESCE(SUM(shares*price),0) FROM fii_portfolio").fetchone()
        conn.close()
        return row[0]


# ── Aggregate functions (use RPC in Supabase) ─────────────────────────────────

def get_monthly_summary(year: int = 2026):
    if USE_SUPABASE:
        rows = _rpc('get_monthly_summary', {'year_val': year})
        # Translate month names from English to PT-BR
        _en_pt = {'Jan':'Jan','Feb':'Fev','Mar':'Mar','Apr':'Abr','May':'Mai','Jun':'Jun',
                  'Jul':'Jul','Aug':'Ago','Sep':'Set','Oct':'Out','Nov':'Nov','Dec':'Dez'}
        result = []
        for r in rows:
            m = r.get('month', '')
            result.append({
                'month': _en_pt.get(m, m),
                'entradas': float(r['entradas']),
                'saidas': float(r['saidas']),
                'investido': float(r['investido']),
            })
        return result
    else:
        conn = _sqlite()
        months = [
            ("Jan", f"{year}-01"), ("Fev", f"{year}-02"), ("Mar", f"{year}-03"),
            ("Abr", f"{year}-04"), ("Mai", f"{year}-05"), ("Jun", f"{year}-06"),
            ("Jul", f"{year}-07"), ("Ago", f"{year}-08"), ("Set", f"{year}-09"),
            ("Out", f"{year}-10"), ("Nov", f"{year}-11"), ("Dez", f"{year}-12"),
        ]
        result = []
        for label, ym in months:
            row = conn.execute("""
                SELECT
                    COALESCE(SUM(CASE WHEN type='entrada'   THEN amount ELSE 0 END),0) AS entradas,
                    COALESCE(SUM(CASE WHEN type='saida'     THEN amount ELSE 0 END),0) AS saidas,
                    COALESCE(SUM(CASE WHEN type='investido' THEN amount ELSE 0 END),0) AS investido
                FROM transactions WHERE strftime('%Y-%m', date) = ?
            """, (ym,)).fetchone()
            result.append({'month': label, 'entradas': row[0], 'saidas': row[1], 'investido': row[2]})
        conn.close()
        return result


def get_monthly_summary_2025():
    return get_monthly_summary(2025)


def get_expense_by_category(year: int = 2026):
    if USE_SUPABASE:
        rows = _rpc('get_expense_by_category', {'year_val': year})
        return {r['cat']: round(float(r['total']), 2) for r in rows}
    else:
        conn = _sqlite()
        rows = conn.execute("""
            SELECT COALESCE(category,'Outros') AS cat, SUM(amount) AS total
            FROM transactions WHERE type='saida' AND strftime('%Y', date) = ?
            GROUP BY cat ORDER BY total DESC
        """, (str(year),)).fetchall()
        conn.close()
        return {r[0]: round(r[1], 2) for r in rows}


def get_savings_rate(year: int = 2026):
    if USE_SUPABASE:
        rows = _rpc('get_savings_rate_agg', {'year_val': year})
        if rows:
            e, i = float(rows[0]['entradas']), float(rows[0]['investido'])
            if e > 0:
                return round(i / e * 100, 1), round(i, 2)
        return 0.0, 0.0
    else:
        conn = _sqlite()
        row = conn.execute("""
            SELECT
                COALESCE(SUM(CASE WHEN type='entrada'   THEN amount ELSE 0 END),0),
                COALESCE(SUM(CASE WHEN type='investido' THEN amount ELSE 0 END),0)
            FROM transactions WHERE strftime('%Y', date) = ?
        """, (str(year),)).fetchone()
        conn.close()
        e, i = row[0], row[1]
        if e > 0:
            return round(i / e * 100, 1), round(i, 2)
        return 0.0, 0.0


def get_savings_rate_2025():
    rate, _ = get_savings_rate(2025)
    return rate


def get_current_month_data():
    if USE_SUPABASE:
        rows = _rpc('get_current_month_data')
        if rows:
            r = rows[0]
            e, s, i = float(r['entradas']), float(r['saidas']), float(r['investido'])
            return {
                'entradas': e, 'saidas': s, 'investido': i,
                'saldo': e - s - i,
                'meta_diario': float(r['meta_diario']),
                'meta_reserva': float(r['meta_reserva']),
                'meta_mensal': float(r['meta_mensal']),
            }
        return {'entradas': 0, 'saidas': 0, 'investido': 0, 'saldo': 0,
                'meta_diario': 70, 'meta_reserva': 10000, 'meta_mensal': 10000}
    else:
        from datetime import date
        ym = date.today().strftime("%Y-%m")
        conn = _sqlite()
        row = conn.execute("""
            SELECT
                COALESCE(SUM(CASE WHEN type='entrada'   THEN amount ELSE 0 END),0),
                COALESCE(SUM(CASE WHEN type='saida'     THEN amount ELSE 0 END),0),
                COALESCE(SUM(CASE WHEN type='investido' THEN amount ELSE 0 END),0)
            FROM transactions WHERE strftime('%Y-%m', date) = ?
        """, (ym,)).fetchone()
        goal = conn.execute("SELECT * FROM goals ORDER BY id DESC LIMIT 1").fetchone()
        conn.close()
        e, s, i = row[0], row[1], row[2]
        g = dict(goal) if goal else {}
        return {
            'entradas': e, 'saidas': s, 'investido': i, 'saldo': e - s - i,
            'meta_diario': g.get('meta_diario', 70),
            'meta_reserva': g.get('meta_reserva', 10000),
            'meta_mensal': g.get('meta_mensal', 10000),
        }


def get_patrimonio_breakdown():
    if USE_SUPABASE:
        rows = _rpc('get_patrimonio_breakdown')
        if rows:
            r = rows[0]
            return {'accounts': float(r['accounts']), 'fii': float(r['fii']), 'total': float(r['total'])}
        return {'accounts': 0, 'fii': 0, 'total': 0}
    else:
        conn = _sqlite()
        acc = conn.execute("SELECT SUM(balance) FROM accounts").fetchone()[0] or 0
        fii = conn.execute("SELECT SUM(shares*price) FROM fii_portfolio").fetchone()[0] or 0
        conn.close()
        return {'accounts': float(acc), 'fii': float(fii), 'total': float(acc) + float(fii)}
