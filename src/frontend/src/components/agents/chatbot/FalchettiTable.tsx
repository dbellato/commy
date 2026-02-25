import React, { useMemo, useState } from "react";

type FalchettiPayload = {
  all_cols: string[];
  filterable_cols: string[];
  distincts: Record<string, string[]>;
  rows: Record<string, any>[];
  max_rows: number;
};

type Props = { payload: FalchettiPayload };

export const FalchettiTable: React.FC<Props> = ({ payload }) => {
  const { all_cols, filterable_cols, distincts, rows, max_rows } = payload;

  const [filters, setFilters] = useState<Record<string, string[]>>({});

  const filtered = useMemo(() => {
    if (!filters || Object.keys(filters).length === 0) return rows;

    return rows.filter((r) => {
      for (const col of Object.keys(filters)) {
        const values = filters[col] || [];
        if (values.length === 0) continue;

        if (col === "Modelli") {
          const cell = String(r[col] ?? "");
          const mods = cell.split(",").map((x) => x.trim()).filter(Boolean);
          const ok = mods.some((m) => values.includes(m));
          if (!ok) return false;
        } else {
          const cell = String(r[col] ?? "").trim();
          if (!values.includes(cell)) return false;
        }
      }
      return true;
    });
  }, [rows, filters]);

  const shown = filtered.slice(0, max_rows);

  const clearFilters = () => setFilters({});

  return (
    <div style={{ marginTop: 8 }}>
      <div style={{ display: "flex", alignItems: "center", gap: 12 }}>
        <strong>Falchetti</strong>
        <span style={{ opacity: 0.8 }}>
          {Object.keys(filters).length ? `Filtrati: ${filtered.length}` : `Totale: ${rows.length}`}
        </span>
        <button onClick={clearFilters} disabled={Object.keys(filters).length === 0}>
          Reset filtri
        </button>
      </div>

      {/* Filters */}
      <div style={{ display: "flex", gap: 12, flexWrap: "wrap", marginTop: 10 }}>
        {filterable_cols.map((col) => (
          <div key={col} style={{ minWidth: col.startsWith("Fune") ? 120 : 180 }}>
            <div style={{ fontSize: 12, opacity: 0.8 }}>{col}</div>
            <select
              multiple
              value={filters[col] ?? []}
              onChange={(e) => {
                const vals = Array.from(e.target.selectedOptions).map((o) => o.value);
                setFilters((prev) => {
                  const next = { ...prev };
                  if (vals.length) next[col] = vals;
                  else delete next[col];
                  return next;
                });
              }}
              style={{ width: "100%", minHeight: 80 }}
            >
              {(distincts[col] ?? []).map((v) => (
                <option key={v} value={v}>
                  {v}
                </option>
              ))}
            </select>
          </div>
        ))}
      </div>

      {/* Table */}
      <div style={{ overflowX: "auto", marginTop: 12 }}>
        <table style={{ borderCollapse: "collapse", width: "100%" }}>
          <thead>
            <tr>
              {all_cols.map((c) => (
                <th key={c} style={{ border: "1px solid #ddd", padding: 6, textAlign: "left" }}>
                  {c}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {shown.map((r, i) => (
              <tr key={i}>
                {all_cols.map((c) => (
                  <td key={c} style={{ border: "1px solid #ddd", padding: 6, verticalAlign: "top" }}>
                    {String(r[c] ?? "")}
                  </td>
                ))}
              </tr>
            ))}
            {shown.length === 0 && (
              <tr>
                <td colSpan={all_cols.length} style={{ padding: 10 }}>
                  Nessun risultato con questi filtri.
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>

      {filtered.length > max_rows && (
        <div style={{ marginTop: 8, opacity: 0.8 }}>
          Mostro {max_rows} righe su {filtered.length}. Raffina i filtri.
        </div>
      )}
    </div>
  );
};
