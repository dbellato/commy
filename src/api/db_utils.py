from __future__ import annotations
import re, logging
from typing import Any, Dict, List, Optional, Tuple, DefaultDict
from collections import defaultdict
import difflib
import os
import pyodbc

FUZZY_THRESHOLD = 0.60
logger = logging.getLogger("commy.sql")

def connect(conn_str: str):
    """
    Apre una connessione a SQL Server usando una connection string ODBC.
    Esempio conn_str:
    'DRIVER={ODBC Driver 17 for SQL Server};SERVER=localhost;DATABASE=NomeDB;Trusted_Connection=yes;'
    """
    safe = re.sub(r"PWD=[^;]*", "PWD=***", conn_str, flags=re.I)
    logger.warning("DB connect using: %s", safe)
    return pyodbc.connect(conn_str)

def _fetch_dicts(cursor) -> List[Dict[str, Any]]:
    """
    Converte il result set pyodbc in una lista di dict {colonna: valore}
    """
    cols = [c[0] for c in cursor.description]
    return [dict(zip(cols, row)) for row in cursor.fetchall()]

def search_configurazioni(
    db_path: str,  # ATTENZIONE: qui db_path è in realtà la CONNECTION STRING
    *,
    rpm: float,
    torque: float,
    flow: float,
    rotary_type: Optional[str] = None,
    motor_type: Optional[str] = None,
    machine_model: Optional[str] = None,
) -> List[Dict[str, Any]]:


    sql_lines = [
        "WITH base AS ",
        "(SELECT DISTINCT InfoRotary, Rotary, RotaryID, Gruppo, Descrizione, Codice, Motore, ConfigurazioneID, Mode, " # Keywords, Modello, 
        "Portata, Pressione, VgMotore, CoppiaNetta, RapportoTesta, RapportoRiduttore, RapportoCambio, CoppiaNom, GiriNom "
        "FROM ConfigurazioniRotariesModelli "
    ]

    params: List[Any] = []
    conditions = []

    if flow is not None:
        conditions.append("Portata = ?")
        params.append(float(flow))
    if motor_type:
        conditions.append("Motore LIKE ?")
        params.append("%"+motor_type+"%")
    if rotary_type:
        conditions.append("COALESCE(Rotary, InfoRotary) LIKE ?")
        params.append("%"+rotary_type+"%")

    if conditions:
        sql_lines.append(" WHERE " + " AND ".join(conditions))

    
    sql_lines.append(
        "), ranges AS "
        "(SELECT InfoRotary, Motore, Gruppo, "
        "MIN(CoppiaNom) AS MinCoppiaNom, MAX(CoppiaNom) AS MaxCoppiaNom, MIN(GiriNom) AS MinGiriNom, MAX(GiriNom) AS MaxGiriNom "
        "FROM base "
        "GROUP BY InfoRotary, Motore, Gruppo), "
        "matching_groups AS "
        "(SELECT * "
        "FROM ranges WHERE ? = MaxCoppiaNom "
    )
    params.append(float(torque))

    if rpm:
        sql_lines.append( 
            " AND ? = MaxGiriNom" 
        )
        params.append(float(rpm))

    sql_lines.append( 
        ") "
        "SELECT " 
        "b.InfoRotary, b.Rotary, b.RotaryID, b.Gruppo, b.Descrizione, b.Codice, "
        "b.Motore, b.ConfigurazioneID, "
        "b.Mode AS Mode, "         # <-- the one you want
        "b.Portata, b.Pressione, b.VgMotore, b.CoppiaNetta, "
        "b.RapportoTesta, b.RapportoRiduttore, b.RapportoCambio, "
        "b.CoppiaNom, b.GiriNom "
        "FROM  base b "
        "JOIN matching_groups mg "
        "ON mg.InfoRotary = b.InfoRotary "
        "AND mg.Motore = b.Motore "
        "AND mg.Gruppo = b.Gruppo "
    )
        

    if machine_model:
        # Normalizzazione spazi e trattini su entrambi i lati (SQL Server)
        sql_lines.append(
            "WHERE REPLACE(REPLACE(LOWER(Modello), ' ', ''), '-', '') = "
            "REPLACE(REPLACE(LOWER(?), ' ', ''), '-', '') "
        )
        params.append(machine_model)

    sql_lines.append(
        "ORDER BY b.InfoRotary, b.RotaryID, b.Gruppo, b.Motore, b.CoppiaNom, b.GiriNom "
    )

    sql = "\n".join(sql_lines)

    with connect(db_path) as conn:
        cur = conn.cursor()
        cur.execute(sql, params)
        rows = _fetch_dicts(cur)
        if not rows:
            sql = sql.replace("? = MaxGiriNom", "? BETWEEN MinGiriNom AND MaxGiriNom")
            cur = conn.cursor()
            cur.execute(sql, params)
            rows = _fetch_dicts(cur)

        return rows

def row_contains_any(rotary, targets):
        if not isinstance(rotary, str):
            return False
        return any(target in rotary for target in targets)

def config_for_rotary(
    db_path: str,  # ATTENZIONE: qui db_path è in realtà la CONNECTION STRING
    cid: int,
    flow_rate: Optional[str] = None,
    volume: Optional[str] = None,
) -> List[Dict[str, Any]]:

    sql = "SELECT c.*, a.Descrizione AS Motore FROM Configurazioni c INNER JOIN Articoli a ON a.ArticoloID = c.MotoreID WHERE ConfigurazioneID = ? "

    with connect(db_path) as conn:
        cur = conn.cursor()
        cur.execute(sql, [cid])
        row = cur.fetchone()   # one row, many columns

        if not row:
            # no result
            return

        columns = [col[0] for col in cur.description]
        row_dict = dict(zip(columns, row))

    rotary = row_dict["InfoRotary"]
    motor = row_dict["Motore"]
    mode = row_dict["Mode"]
    if flow_rate is None: 
        flow_rate_ = float(row_dict["Portata"])
    else:
        flow_rate_=float(flow_rate.strip().replace(",", "."))
    if volume is None: 
        volume_ = float(row_dict["VgMotore"])
    else:
        volume_ = float(volume.strip().replace(",", "."))
    pressure = row_dict["Pressione"]  
    RRotary = row_dict["RapportoTesta"]  
    if RRotary is None: 
        RRotary_ = float(1)
    else:
        RRotary_ = float(RRotary)
    RGear = row_dict["RapportoCambio"]  
    if RGear is None: 
        RGear_ = float(1)
    else:
        RGear_ = float(RGear)
    RReducer = row_dict["RapportoRiduttore"]  
    if RReducer is None: 
        RReducer_ = float(1)
    else:
        RReducer_ = float(RReducer)   
    NetTorque = row_dict["CoppiaNetta"]  
    MecEff = row_dict["RendimentoMec"] 
    VolEff = row_dict["RendimentoVol"] 

    targetSet1 = ['R300', 'R450', 'R900', 'R1600', 'R2400'] # con serie/parallelo/parallelo 2/parallelo 3
    targetSet2 = ['R350', 'R600', 'R700', 'R750', 'R1000', 'R1300', 'R1450', 'R1500', 'R1700', 'R3000'] # Rexroth normale
    targetR3200 = ['R3200'] # con serie 2 /parallelo 4
    targetSet4 = ['R1400', 'R2000', 'R2500', 'R2900', 'R3500', 'R4000', 'R5000'] # Rexroth 2
    targetR400 = ['R400']

    if row_contains_any(rotary, targetSet1):
        if mode == 'parallelo 2':
            rpm = round(VolEff * flow_rate_ * 1000 / (volume_ * RRotary_ * 2), 0)
            torque = round(NetTorque * RRotary_ * MecEff * 2, 0)
        elif mode == 'parallelo 3':
            rpm = round(VolEff * flow_rate_ * 1000 / (volume_ * RRotary_ * 3), 0)
            torque = round(NetTorque * RRotary_ * MecEff * 3, 0)
        elif mode and 'serie' in mode:
            rpm = round(VolEff * flow_rate_ * 1000 / (volume_ * RRotary_), 0)
            torque = round(NetTorque * RRotary_ * MecEff, 0)
        elif mode and 'parallelo' in mode:
            rpm = round(VolEff * flow_rate_ * 1000 / (volume_ * RRotary_ * 2), 0)
            torque = round(NetTorque * RRotary_ * MecEff * 2, 0)
        else:
            raise ValueError(f"No matching mode '{mode}' for rotary '{rotary}' (targetSet1)")

    elif row_contains_any(rotary, targetSet2):
        rpm = round(VolEff * flow_rate_ * 1000 / (volume_ * RRotary_ * RReducer_ * RGear_), 0)
        torque = round(pressure * volume_ * RRotary_ * RReducer_ * RGear_ * MecEff / 628, 0)

    elif row_contains_any(rotary, targetR3200):
        if mode and 'serie' in mode:
            rpm = round(VolEff * flow_rate_ * 1000 / (volume_ * RRotary_ * 2), 0)
            torque = round(NetTorque * RRotary_ * MecEff * 2, 0)
        elif mode and 'parallelo' in mode:
            rpm = round(VolEff * flow_rate_ * 1000 / (volume_ * RRotary_ * 4), 0)
            torque = round(NetTorque * RRotary_ * MecEff * 4, 0)
        else:
            raise ValueError(f"No matching mode '{mode}' for rotary '{rotary}' (targetR3200)")

    elif row_contains_any(rotary, targetSet4):
        rpm = round(VolEff * flow_rate_ * 1000 / (2 * volume_ * RRotary_ * RReducer_ * RGear_), 0)
        torque = round(2* pressure * volume_ * RRotary_ * RReducer_ * RGear_ * MecEff / 628, 0)

    elif row_contains_any(rotary, targetR400):
        if mode is not None:
            if 'serie' in mode:
                rpm = round(VolEff * flow_rate_ * 1000 / (volume_ * RRotary_), 0)
                torque = round(NetTorque * RRotary_ * MecEff, 0)
            elif 'parallelo' in mode:
                rpm = round(VolEff * flow_rate_ * 1000 / (volume_ * RRotary_ * 2), 0)
                torque = round(NetTorque * RRotary_ * MecEff * 2, 0)
            else:
                raise ValueError(f"No matching mode '{mode}' for rotary '{rotary}' (targetR400)")
        else:
            rpm = round(VolEff * flow_rate_ * 1000 / (volume_ * RRotary_ * RReducer_ * RGear_), 0)
            torque = round(pressure * volume_ * RRotary_ * RReducer_ * RGear_ * MecEff / 628, 0)
    else:
        raise ValueError(f"No matching rotary target for '{rotary}', mode='{mode}'")            

    results = {
        "Rotary": rotary,
        "Motore": motor,
        "Mode": mode,
        "Portata (l/min)": flow_rate_,
        "Pressione (bar)": pressure,
        "Vg Motore (cc)": volume_,
        "Coppia netta (daNm)": NetTorque,
        "R. riduttore": RReducer,
        "R. testa": RRotary,
        "R. cambio": RGear,
        "Rendimento mec.": MecEff,
        "Rendimento vol.": VolEff,
        "Coppia (daNm)": torque,
        "Giri (rpm)": rpm
    }

    return results

def machines_for_rotary(db_path: str, rotary_id: Any) -> List[str]:
    """
    Dalla vista RotariesMotoriModelli: lista di Modello per una RotaryID.
    """
    sql = (
        "SELECT DISTINCT Modello "
        "FROM RotariesMotoriModelli "   # eventualmente dbo.RotariesMotoriModelli
        "WHERE RotaryID = ? "
        "ORDER BY Modello"
    )
    with connect(db_path) as conn:
        cur = conn.cursor()
        cur.execute(sql, [rotary_id])
        return [row[0] for row in cur.fetchall()]

def search_torque(db_path: str, rotary_type: Any) -> List[str]:
    # Find max torque if rotary type is provided.
    sql = (
        "WITH m AS (SELECT InfoRotary, Motore, Gruppo, MAX(CoppiaNom) AS MaxCoppiaNom "
        "FROM ConfigurazioniRotariesModelli "
        "WHERE InfoRotary LIKE ? "
        "GROUP BY InfoRotary, Motore, Gruppo) "
        "SELECT DISTINCT m.MaxCoppiaNom AS CoppiaNom FROM m"
    )
    with connect(db_path) as conn:
        cur = conn.cursor()
        cur.execute(sql, ["%"+rotary_type+"%"])
        return [row[0] for row in cur.fetchall()]

def machines_for_configuration(db_path: str, configurazione_id: Any) -> List[str]:
    """
    Dalla vista ConfigurazioniRotariesModelli:
    elenco dei Modello per una certa ConfigurazioneID.
    """
    sql = (
        "SELECT DISTINCT Modello "
        "FROM ConfigurazioniRotariesModelli "
        "WHERE ConfigurazioneID = ? "
        "ORDER BY Modello"
    )
    with connect(db_path) as conn:
        cur = conn.cursor()
        cur.execute(sql, [configurazione_id])
        return [row[0] for row in cur.fetchall()]

def normalize_model_name(s: str) -> str:
    """
    Normalize a model string: lowercase + remove spaces and hyphens.
    """
    if s is None:
        return ""
    s = str(s).lower()
    s = re.sub(r"[\s\-]+", "", s)
    return s

def load_existing_matricole(cursor):
    """
    Load existing matricole from destination table as a Python set.
    """
    sql = f"SELECT Matricola FROM Commesse"
    cursor.execute(sql)
    rows = cursor.fetchall()
    existing = {row[0] for row in rows if row[0] is not None}
    return existing

def load_macchine_models(cursor):
    """
    Load MacchinaID and Modello from Macchine table.
    Return a list of (MacchinaID, Modello_original, Modello_normalized)
    """
    sql = f"SELECT ModelloID, Modello FROM Modelli"
    cursor.execute(sql)
    rows = cursor.fetchall()

    modelli = []
    for modello_id, modello in rows:
        modelli.append(
            (
                modello_id,
                modello,
                normalize_model_name(modello)
            )
        )
    return modelli

def load_commesse(cursor):
    sql = f"SELECT Tipo + '-' + Commessa AS Commessa FROM Commesse"
    cursor.execute(sql)
    rows = cursor.fetchall()

    commesse = []
    for commessa in rows:
        commesse.append(
            (
                commessa
            )
        )
    return commesse

def remove_any_ending(s: str, endings) -> str:
    """
    Remove one matching ending from `s` if it ends with
    any of the strings in `endings`. If none match, return s unchanged.
    """
    # Check longer endings first to avoid partial overlaps
    for end in sorted(endings, key=len, reverse=True):
        if s.endswith(end):
            return s[:-len(end)]
    return s

def find_best_modello_match(modello_excel, modelli_list):
    """
    Given an Excel 'Descrizione 2' and the list of models,
    find the best matching ModelloID using fuzzy matching.

    Returns:
        (best_modello_id, best_modello_original, best_score, perfect_match: bool)
    """
    endings = ("(la4)", "portogallo", "revisionatacompletamenteespeditail05/12/02", 
               "(cancellata)", "singapore", "rhcity", "pto", "suskid", "revisionata", 
               "sutrattore", "elettrica", "diesel", "dieseltcd2.9", "dx140lcr-7", "dx140lcr7", "dx245")

    modello_norm = remove_any_ending(normalize_model_name(modello_excel),endings)

    if modello_norm == 'nan':
        modello_norm = None
    elif modello_norm == 'escavatoremc-e235' or modello_norm == 'escavatoremce235':
        modello_norm = 'mce235'
    elif modello_norm == 'centralek75elettrica':
        modello_norm = 'k75'
    elif modello_norm == 'mc-e/dx140' or modello_norm == 'mce/dx140' or modello_norm == 'mcedx140':
        modello_norm = 'mce15'

    # all Commesse for which there are no Models are not considered
    if not modello_norm or bool(re.fullmatch(r"R\d+", modello_norm)):
        return None, None, 0.0, False

    best_score = -1.0
    best_entry = None

    # Pre-build list of normalized modello strings for difflib
    normalized_modelli = [m[2] for m in modelli_list]
    matches = difflib.get_close_matches(modello_norm, normalized_modelli, n=1, cutoff=0.0)

    if not matches:
        return None, None, 0.0, False

    best_norm = matches[0]

    # Find original entry corresponding to best_norm
    for modello_id, modello_original, modello_norm in modelli_list:
        if modello_norm == best_norm:
            # Compute similarity ratio
            score = difflib.SequenceMatcher(None, modello_norm, modello_norm).ratio()
            best_score = score
            best_entry = (modello_id, modello_original, modello_norm)
            break

    if best_entry is None:
        return None, None, 0.0, False

    modello_id, modello_original, _ = best_entry

    perfect_match = (modello_norm == best_norm)

    return modello_id, modello_original, best_score, perfect_match

def log_mismatch(f, matricola, descrizione_excel, modello_db, score, note):
    """
    Write a line in the log file.
    """
    f.write(
        f"Matricola={matricola} | DescrizioneExcel='{descrizione_excel}' | "
        f"ModelloDB='{modello_db}' | Score={score:.3f} | Note={note}\n"
    )



def extract_value(cell_text: str, label: str) -> str:
    """
    From the cell text containing something like 'CODICE TESTA: ABC123'
    returns 'ABC123'.
    If the label is not found, returns cleaned cell text.
    """
    if not cell_text:
        return ""

    text = " ".join(cell_text.split())  # normalize spaces/newlines

    if label in text:
        after = text.split(label, 1)[1]
        after = after.lstrip(" :-\t")
        return after.strip()
    else:
        return text.strip()


def extract_codice_matricola_commessa_from_controls(fields, file_path):
    errors = []

    # --- 1) Commessa & Matricola from filename (unchanged) ---
    base = os.path.basename(file_path)
    name_no_ext, _ = os.path.splitext(base)

    commessa = None
    matricola = None

    if "-" in name_no_ext:
        left, right = name_no_ext.split("-", 1)
        commessa = left.strip()
        matricola = right.strip()
    else:
        parts = name_no_ext.split()
        if len(parts) >= 2:
            commessa = parts[0].strip()
            matricola = parts[1].strip()
        else:
            errors.append(
                f"Filename '{base}' does not contain '-' or enough space-separated parts "
                "to get Commessa/Matricola."
            )

    # --- 2) Codice: handle split fragments like '6-6700.' + '310.3' ---
    codice = None

    CODICE_PREFIX = "6-6700"
    CODICE_COMPLETE = re.compile(r"^6-6700\.[0-9]+[A-Za-z]?\.[0-9]+[A-Za-z]?$")
    CODICE_FRAGMENT = re.compile(r"[0-9A-Za-z.]+")  # same as your original

    for i, f in enumerate(fields):
        text = (f.get("Value") or "").strip()

        # skip if does not contain prefix
        if CODICE_PREFIX not in text:
            continue

        # 1. If text is already a complete codice → use it and skip joining
        if CODICE_COMPLETE.fullmatch(text):
            codice = text
            break   # or break depending on your logic

        # 2. Otherwise begin joining procedure
        combined = text

        j = i + 1
        while j < len(fields):
            nxt = (fields[j].get("Value") or "").strip()
            # join ONLY pure digit/dot fragments
            if not re.fullmatch(r"[0-9.]+", nxt):
                break
            combined += nxt
            j += 1

        # Extract codice inside the combined text
        m = CODICE_FRAGMENT.search(combined)
        if m:
            codice = m.group(0)

    if not codice:
        errors.append(
            "Codice not found in document text (no '6-6700' sequence, "
            "even after combining adjacent numeric fragments)."
        )

    return {
        "Codice": codice,
        "Matricola": matricola,
        "Commessa": commessa,
        "Errors": errors or None,
    }

def write_inventario_and_commesse_inventario(db_path: str, df):
    """
    df columns: Codice, Matricola, Commessa, Errors, SourceFile

    Writes:
      - Inventario(Descrizione, Matricola, ArticoloID)
      - CommesseInventario(CommessaID, InventarioID)

    Mapping:
      Inventario.Descrizione <- df.Codice
      Inventario.Matricola   <- df.Matricola
      Inventario.ArticoloID  <- Articoli.ArticoloID where Articoli.Codice = df.Codice

      CommesseInventario.CommessaID  <- Commesse.CommessaID where
                                        df.Commessa = (Commesse.Tipo || '-' || Commesse.Commessa)
      CommesseInventario.InventarioID <- Inventario.InventarioID for that Matricola
    """
    # 2. Connect to DB
    with connect(db_path) as conn:
        cur = conn.cursor()

    for _, row in df.iterrows():
        codice = str(row["Codice"]).strip()
        matricola = str(row["Matricola"]).strip()
        commessa = str(row["Commessa"]).strip()
        commessa_type=commessa[0]
        commessa_nr=commessa[1:]

        if not codice or not matricola or not commessa:
            continue

        # --- 1) Get ArticoloID from Articoli by Codice ---
        cur.execute("SELECT ArticoloID FROM Articoli WHERE Codice = ?", (codice))
        res = cur.fetchone()
        if not res:
            logger.warning("Codice '%s' not found in Articoli; skipping row.", codice)
            continue
        articolo_id = res[0]

        # --- 2) Ensure row exists in Inventario, get InventarioID ---
        cur.execute("SELECT InventarioID FROM Inventario WHERE Matricola = ?", (matricola))
        inv_row = cur.fetchone()

        if inv_row:
            inventario_id = inv_row[0]
            # optional: keep Inventario in sync
            cur.execute(
                """
                UPDATE Inventario
                SET Descrizione = ?, ArticoloID = ?
                WHERE InventarioID = ?
                """,
                (codice, articolo_id, inventario_id)
            )
        else:
            # SQL Server: use OUTPUT INSERTED to get identity
            cur.execute("""
                INSERT INTO Inventario (Descrizione, Matricola, ArticoloID)
                OUTPUT INSERTED.InventarioID
                VALUES (?, ?, ?)
            """, (codice, matricola, articolo_id))
            inventario_id = cur.fetchone()[0]

        # --- 3) Get CommessaID from Commesse ---
        # df.Commessa is matched against Tipo || '-' || Commessa
        cur.execute(
            """
            SELECT CommessaID
            FROM Commesse
            WHERE (Tipo + Commessa) = ?
            """,
            (commessa)
        )
        com_row = cur.fetchone()
        if not com_row:
            # check if there is at least the number
            cur.execute(
                """
                SELECT CommessaID, Matricola, ModelloID, PesoTot, PesoSonda, PesoCentrale
                FROM Commesse
                WHERE Commessa = ?
                """,
                (commessa_nr)
            )
            com_row2 = cur.fetchone()
            if not com_row2:
                logger.warning("Commessa '%s' not found in Commesse; skipping CommesseInventario.", commessa)
                continue
            else:
                # Add special commessa
                matricola_ = com_row2[1]
                modello_ = com_row2[2]
                pesotot_ = com_row2[3]
                pesosonda_ = com_row2[4]
                pesocentrale_ = com_row2[5]
                cur.execute(
                    """
                    INSERT INTO Commesse (Commessa, Type, Matricola, ModelloID, PesoTot, PesoSonda, PesoCentrale)
                    VALUES (?, ?)
                    """,
                    (commessa_nr, commessa_type, matricola_, modello_, pesotot_, pesosonda_, pesocentrale_)
                )
                # get the special commessa
                cur.execute(
                    """
                    SELECT CommessaID
                    FROM Commesse
                    WHERE (Tipo + Commessa) = ?
                    """,
                    (commessa)
                )
                com_row = cur.fetchone()
        commessa_id = com_row[0]

        # --- 4) Insert into CommesseInventario if not already present ---
        cur.execute(
            """
            SELECT 1
            FROM CommesseInventario
            WHERE CommessaID = ? AND InventarioID = ?
            """,
            (commessa_id, inventario_id)
        )
        if not cur.fetchone():
            cur.execute(
                """
                INSERT INTO CommesseInventario (CommessaID, InventarioID)
                VALUES (?, ?)
                """,
                (commessa_id, inventario_id)
            )
        else:
            logger.warning("CommesseInventario already inside.")

    conn.commit()
    conn.close()


def _build_kits_result(
    rotary_meta: Dict[str, Optional[str]],
    kit_rows: "DefaultDict[str, List[Tuple[str, str]]]",
    kit_meta: Dict[str, Dict[str, Optional[str]]],
    selected_kits: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """Assembla il dizionario di risultato arricchito con metadati immagini/PDF."""
    keys = selected_kits if selected_kits is not None else list(kit_rows.keys())
    return {
        **rotary_meta,
        "kits": {
            kit: {"items": kit_rows[kit], **kit_meta.get(kit, {})}
            for kit in keys
        },
    }


def kits_for_rotary(
    db_path: str,
    rotary_id: Any,
    keywords: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """
    Vista RotariesKitsArticoli -> dizionario arricchito con:
      - immagine_rotary, scheda_rotary  (per l'intera rotary)
      - kits: {Kit: {items: [(Accessorio, CodiceAccessorio), ...],
                     immagine_kit, scheda_kit}}

    Se `keywords` è specificato:
    - se esistono kit che matchano *tutte* le keywords -> ritorna solo quei kit
    - altrimenti, ritorna i kit che matchano il maggior numero di keywords
    - se nessun kit matcha alcuna keyword -> solleva ValueError
    (match substring, case-insensitive).
    """
    # normalizza keywords (lowercase, rimuovi spazi vuoti)
    kw = [k.strip().lower() for k in (keywords or []) if k.strip()]

    base_sql = (
        "SELECT Kit, Accessorio, CodiceAccessorio, "
        " LOWER(ISNULL(Keywords, '')) AS kw, ImmagineRotary, SchedaRotary, ImmagineKit, SchedaKit, Descrizione "
        "FROM RotariesKitsArticoli "
        "WHERE RotaryID = ?"
    )

    with connect(db_path) as conn:
        cur = conn.cursor()
        cur.execute(base_sql, [rotary_id])
        rows = _fetch_dicts(cur)  # assumo che ritorni una lista di dict

    # Raggruppo righe per kit
    kit_rows: DefaultDict[str, List[Tuple[str, str]]] = defaultdict(list)
    kit_kw_text: DefaultDict[str, str] = defaultdict(str)
    kit_meta: Dict[str, Dict[str, Optional[str]]] = {}
    rotary_meta: Dict[str, Optional[str]] = {"immagine_rotary": None, "scheda_rotary": None, "descrizione": None}

    for r in rows:
        kit = r["Kit"]
        acc = r["Accessorio"]
        code = r["CodiceAccessorio"]
        keyw = (r.get("kw") or "").lower()

        kit_rows[kit].append((acc, code))

        # Metadati a livello rotary (uguale per tutte le righe)
        if rotary_meta["immagine_rotary"] is None:
            rotary_meta["immagine_rotary"] = r.get("ImmagineRotary") or None
            rotary_meta["scheda_rotary"] = r.get("SchedaRotary") or None
            rotary_meta["descrizione"] = r.get("Descrizione") or None

        # Metadati a livello kit
        if kit not in kit_meta:
            kit_meta[kit] = {
                "immagine_kit": r.get("ImmagineKit") or None,
                "scheda_kit": r.get("SchedaKit") or None,
            }

        # Accumulo il testo delle keywords del kit (nel caso ci siano più righe)
        if keyw:
            if kit_kw_text[kit]:
                kit_kw_text[kit] += " " + keyw
            else:
                kit_kw_text[kit] = keyw

    # Se non ci sono keywords fornite -> ritorna tutto
    if not kw:
        return _build_kits_result(rotary_meta, kit_rows, kit_meta)

    # Calcola quante keywords matchano per ciascun kit
    kit_match_counts: Dict[str, int] = {}
    for kit, text in kit_kw_text.items():
        count = sum(1 for k in kw if k in text)
        kit_match_counts[kit] = count

    # Considera anche kit che non hanno testo keywords (text == "")
    for kit in kit_rows.keys():
        kit_match_counts.setdefault(kit, 0)

    # Se nessun kit matcha alcuna keyword -> errore
    max_matches = max(kit_match_counts.values()) if kit_match_counts else 0
    if max_matches == 0:
        raise ValueError("No kits match the provided keywords")

    # Kit che matchano *tutte* le keywords
    full_match_kits = [
        kit for kit, count in kit_match_counts.items() if count == len(kw)
    ]

    if full_match_kits:
        return _build_kits_result(rotary_meta, kit_rows, kit_meta, full_match_kits)

    # Nessun kit matcha tutte le keywords -> prendo quelli col massimo numero di match
    best_kits = [kit for kit, count in kit_match_counts.items() if count == max_matches]

    return _build_kits_result(rotary_meta, kit_rows, kit_meta, best_kits)

def kits_for_configuration(
    db_path: str,
    configuration_id: Any,
    keywords: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """
    Vista RotariesKitsArticoli -> dizionario arricchito con:
      - immagine_rotary, scheda_rotary  (per l'intera rotary)
      - kits: {Kit: {items: [(Accessorio, CodiceAccessorio), ...],
                     immagine_kit, scheda_kit}}

    Se `keywords` è specificato:
    includi solo i kit per cui la colonna Keywords contiene almeno 2 parole-chiave
    (match substring, case-insensitive).
    """
    kw = [k.strip().lower() for k in (keywords or []) if k.strip()]

    base_sql = (
        "SELECT rka.Kit, rka.Accessorio, rka.CodiceAccessorio, "
        "LOWER(ISNULL(rka.Keywords, '')) AS kw, ImmagineRotary, SchedaRotary, ImmagineKit, SchedaKit, rka.Descrizione "
        "FROM RotariesKitsArticoli rka "
        "INNER JOIN Configurazioni c ON c.InfoRotary = rka.Rotary "
        "WHERE c.ConfigurazioneID = ?"
    )

    with connect(db_path) as conn:
        cur = conn.cursor()
        cur.execute(base_sql, [configuration_id])
        rows = _fetch_dicts(cur)  # assumo che ritorni una lista di dict

    # Raggruppo righe per kit
    kit_rows: DefaultDict[str, List[Tuple[str, str]]] = defaultdict(list)
    kit_kw_text: DefaultDict[str, str] = defaultdict(str)
    kit_meta: Dict[str, Dict[str, Optional[str]]] = {}
    rotary_meta: Dict[str, Optional[str]] = {"immagine_rotary": None, "scheda_rotary": None, "descrizione": None}

    for r in rows:
        kit = r["Kit"]
        acc = r["Accessorio"]
        code = r["CodiceAccessorio"]
        keyw = (r.get("kw") or "").lower()

        kit_rows[kit].append((acc, code))

        # Metadati a livello rotary (uguale per tutte le righe)
        if rotary_meta["immagine_rotary"] is None:
            rotary_meta["immagine_rotary"] = r.get("ImmagineRotary") or None
            rotary_meta["scheda_rotary"] = r.get("SchedaRotary") or None
            rotary_meta["descrizione"] = r.get("Descrizione") or None

        # Metadati a livello kit
        if kit not in kit_meta:
            kit_meta[kit] = {
                "immagine_kit": r.get("ImmagineKit") or None,
                "scheda_kit": r.get("SchedaKit") or None,
            }

        # Accumulo il testo delle keywords del kit (nel caso ci siano più righe)
        if keyw:
            if kit_kw_text[kit]:
                kit_kw_text[kit] += " " + keyw
            else:
                kit_kw_text[kit] = keyw

    # Se non ci sono keywords fornite -> ritorna tutto
    if not kw:
        return _build_kits_result(rotary_meta, kit_rows, kit_meta)

    # Calcola quante keywords matchano per ciascun kit
    kit_match_counts: Dict[str, int] = {}
    for kit, text in kit_kw_text.items():
        count = sum(1 for k in kw if k in text)
        kit_match_counts[kit] = count

    # Considera anche kit che non hanno testo keywords (text == "")
    for kit in kit_rows.keys():
        kit_match_counts.setdefault(kit, 0)

    # Se nessun kit matcha alcuna keyword -> errore
    max_matches = max(kit_match_counts.values()) if kit_match_counts else 0
    if max_matches == 0:
        raise ValueError("No kits match the provided keywords")

    # Kit che matchano *tutte* le keywords
    full_match_kits = [
        kit for kit, count in kit_match_counts.items() if count == len(kw)
    ]

    if full_match_kits:
        return _build_kits_result(rotary_meta, kit_rows, kit_meta, full_match_kits)

    # Nessun kit matcha tutte le keywords -> prendo quelli col massimo numero di match
    best_kits = [kit for kit, count in kit_match_counts.items() if count == max_matches]

    return _build_kits_result(rotary_meta, kit_rows, kit_meta, best_kits)

def check_rotaryID(
        db_path: str,
        rotary_id: Any
    ):
    """
    Reads all rotaryIDs.
    """
    with connect(db_path) as conn:
        sql = f"SELECT ArticoloID FROM Articoli WHERE Articolo IS NOT NULL AND ArticoloID = ?"
        cur = conn.cursor()
        cur.execute(sql,(rotary_id))
        return cur.fetchone()

def analyis_of_machine_data(
    db_path: str,
    intent: Any,
    field: Any,
    id: Any
) -> Dict[str, List[Tuple[str, str]]]:
    
    """
    Vista CommesseArticoliInventario 

    """

    sql = (
        # "CREATE FUNCTION dbo.NormalizeString (@input NVARCHAR(4000)) "
        # "RETURNS NVARCHAR(4000) "
        # "AS "
        # "BEGIN "
        #     "DECLARE @s NVARCHAR(4000); "
        #     "SET @s = LOWER(@input); "
        #     "SET @s = REPLACE(@s, ' ', ''); "
        #     "SET @s = REPLACE(@s, '-', ''); "
        #     "SET @s = REPLACE(@s, CHAR(9),  ''); "
        #     "SET @s = REPLACE(@s, CHAR(10), ''); "
        #     "SET @s = REPLACE(@s, CHAR(13), ''); "
        #     "RETURN @s; "
        # "END; "
        # "GO "
        "SELECT  * "
        "FROM    dbo.CommesseArticoliInventario "
    )

    id = normalize_model_name(id)

    if intent == 'peso':
        sql = sql + " WHERE Attuale = 1 AND dbo.NormalizeString(Modello) LIKE '%' + ? + '%'"
    else:
        if field == "modello":
            sql = sql + " WHERE Attuale = 1 AND dbo.NormalizeString(Modello) LIKE '%' + ? + '%'"
        elif field == "rotaryid":
            sql = "SELECT * FROM CommesseArticoliInventario WHERE Attuale = 1 AND RotaryID = ?"
        elif field == "testa":
            sql += """
                WHERE Attuale = 1
                AND (
                    dbo.NormalizeString([Tipo rotary]) LIKE '%' + ? + '%'
                    OR dbo.NormalizeString([Codice rotary]) LIKE '%' + ? + '%'
                )
            """
        elif field == "matricola":
            sql = sql + " WHERE Attuale = 1 AND dbo.NormalizeString([Matricola testa]) LIKE '%' + ? + '%'"
    sql = sql + " ORDER BY Commessa DESC"

    with connect(db_path) as conn:
        cur = conn.cursor()
        if intent != "peso" and field == "testa":
            cur.execute(sql, [id, id])
        else:
            cur.execute(sql, [id])
        return _fetch_dicts(cur)
