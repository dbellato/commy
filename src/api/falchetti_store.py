import pyodbc
import re

VIEW = "dbo.ArticoliFalchettiModelli"

FILTERABLE_COLS = [
    "Codice","Bloccato","Tipo","Sicurezza","Prolunga","Pieghevole",
    "InterasseFori","Puleggia","Fune1","Fune2","Fune3","Fune4",
    "Argano","TiroSpinta", "Modelli"
]
ALL_COLS = FILTERABLE_COLS + ["Note"]

_ROWS = None
_DISTINCTS = None

_SPLIT_RE = re.compile(r"\s*,\s*")

def _split_modelli(cell) -> list[str]:
    """Trasforma 'A, B, C' -> ['A','B','C'] (puliti, senza vuoti)."""
    if cell is None:
        return []
    s = str(cell).strip()
    if not s:
        return []
    parts = _SPLIT_RE.split(s)
    return [p.strip() for p in parts if p and p.strip()]

def connect(conn_str: str):
    """
    Apre una connessione a SQL Server usando una connection string ODBC.
    Esempio conn_str:
    'DRIVER={ODBC Driver 17 for SQL Server};SERVER=localhost;DATABASE=NomeDB;Trusted_Connection=yes;'
    """
    conn = pyodbc.connect(conn_str)
    return conn

def warmup(db_path: str,force: bool = False):
    global _ROWS, _DISTINCTS
    if _ROWS is not None and not force:
        return

    q = f"SELECT {', '.join(ALL_COLS)} FROM {VIEW};"
    with connect(db_path) as cn:
        cur = cn.cursor()
        cur.execute(q)
        cols = [d[0] for d in cur.description]
        rows = [dict(zip(cols, r)) for r in cur.fetchall()]

    # trim stringhe
    for r in rows:
        for k, v in list(r.items()):
            if isinstance(v, str):
                r[k] = v.strip()

    distincts = {}
    for col in FILTERABLE_COLS:
        if col == "Modelli":
            continue
        s = set()
        for r in rows:
            v = r.get(col)
            if v is None:
                continue
            if isinstance(v, str) and v.strip() == "":
                continue
            s.add(v)
        distincts[col] = sorted(s, key=lambda x: str(x))

    # ✅ DISTINCT DERIVATI PER "Modelli"
    mod_set = set()
    for r in rows:
        for m in _split_modelli(r.get("Modelli")):
            mod_set.add(m)

    # ordinati per nome (case-insensitive ma preserva originali)
    distincts["Modelli"] = sorted(mod_set, key=lambda x: x.casefold())

    _ROWS = rows
    _DISTINCTS = distincts

def get_state(conn_str: str):
    warmup(conn_str)
    return {
        "all_cols": ALL_COLS,
        "filterable_cols": FILTERABLE_COLS,
        "distincts": _DISTINCTS,
        "rows": _ROWS
    }

def clear():
    global _ROWS, _DISTINCTS
    _ROWS = None
    _DISTINCTS = None
