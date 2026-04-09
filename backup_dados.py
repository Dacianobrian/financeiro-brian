"""
Exporta todos os dados do financeiro.db para um arquivo SQL.
Use para migrar dados entre ambientes (local → nuvem ou vice-versa).

Uso:
  python3 backup_dados.py              → salva backup_AAAA-MM-DD.sql
  python3 backup_dados.py restaurar    → restaura do arquivo backup mais recente
"""
import sqlite3, os, sys, glob
from datetime import date

DB_PATH = os.environ.get('DB_PATH', os.path.join(os.path.dirname(__file__), 'financeiro.db'))

def exportar():
    conn = sqlite3.connect(DB_PATH)
    filename = f"backup_{date.today()}.sql"
    with open(filename, 'w', encoding='utf-8') as f:
        for line in conn.iterdump():
            f.write(line + '\n')
    conn.close()
    print(f"✓ Backup salvo em: {filename}")
    return filename

def restaurar(filename=None):
    if not filename:
        files = sorted(glob.glob("backup_*.sql"))
        if not files:
            print("✗ Nenhum arquivo backup_*.sql encontrado.")
            return
        filename = files[-1]
    with open(filename, 'r', encoding='utf-8') as f:
        sql = f.read()
    conn = sqlite3.connect(DB_PATH)
    conn.executescript(sql)
    conn.close()
    print(f"✓ Dados restaurados de: {filename}")

if __name__ == '__main__':
    if len(sys.argv) > 1 and sys.argv[1] == 'restaurar':
        restaurar(sys.argv[2] if len(sys.argv) > 2 else None)
    else:
        exportar()
