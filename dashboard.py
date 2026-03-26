"""
Camada de banco de dados — SQLite local (sem instalação necessária).
O arquivo radar_ma.db é criado automaticamente na primeira execução.
"""

import sqlite3
import os
from datetime import datetime
from pathlib import Path

DB_PATH = Path(__file__).parent.parent / "data" / "radar_ma.db"


def get_conn() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def init_db():
    """Cria todas as tabelas se não existirem."""
    with get_conn() as conn:
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS companies (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            cnpj            TEXT UNIQUE,
            razao_social    TEXT NOT NULL,
            uf              TEXT,
            municipio       TEXT,
            setor           TEXT,
            tipo_sociedade  TEXT,
            is_b3_listed    INTEGER DEFAULT 0,
            created_at      TEXT DEFAULT (datetime('now')),
            updated_at      TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS financial_statements (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            company_id          INTEGER REFERENCES companies(id),
            ano_referencia      INTEGER,
            receita_liquida     REAL,
            ebitda              REAL,
            ebit                REAL,
            lucro_liquido       REAL,
            depreciacao_amort   REAL,
            ativo_total         REAL,
            divida_liquida      REAL,
            margem_ebitda       REAL,
            fonte_url           TEXT,
            fonte_tipo          TEXT,
            fonte_uf            TEXT,
            confianca_extracao  REAL DEFAULT 1.0,
            created_at          TEXT DEFAULT (datetime('now')),
            UNIQUE(company_id, ano_referencia)
        );

        CREATE TABLE IF NOT EXISTS pipeline_runs (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            fonte       TEXT,
            uf          TEXT,
            status      TEXT,
            docs_found  INTEGER DEFAULT 0,
            docs_parsed INTEGER DEFAULT 0,
            empresas_novas INTEGER DEFAULT 0,
            started_at  TEXT,
            finished_at TEXT,
            log_text    TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_ebitda
            ON financial_statements(ebitda DESC);
        CREATE INDEX IF NOT EXISTS idx_company_ano
            ON financial_statements(company_id, ano_referencia);
        """)


# ── Queries ──────────────────────────────────────────────────────────────────

def query_companies(
    ebitda_min: float = 40_000_000,
    ebitda_max: float | None = None,
    margem_min: float | None = None,
    receita_min: float | None = None,
    uf: str | None = None,
    setor: str | None = None,
    search: str | None = None,
    order_by: str = "ebitda",
    order_dir: str = "DESC",
    limit: int = 500,
) -> list[dict]:
    allowed_cols = {"ebitda", "receita_liquida", "margem_ebitda", "ano_referencia", "razao_social"}
    order_col = order_by if order_by in allowed_cols else "ebitda"
    order_dir = "DESC" if order_dir.upper() == "DESC" else "ASC"

    filters = ["c.is_b3_listed = 0", "fs.ebitda >= ?"]
    params: list = [ebitda_min]

    if ebitda_max:
        filters.append("fs.ebitda <= ?"); params.append(ebitda_max)
    if margem_min:
        filters.append("fs.margem_ebitda >= ?"); params.append(margem_min / 100)
    if receita_min:
        filters.append("fs.receita_liquida >= ?"); params.append(receita_min)
    if uf and uf != "Todos":
        filters.append("c.uf = ?"); params.append(uf)
    if setor and setor != "Todos":
        filters.append("c.setor LIKE ?"); params.append(f"%{setor}%")
    if search:
        filters.append("c.razao_social LIKE ?"); params.append(f"%{search}%")

    where = " AND ".join(filters)
    sql = f"""
        SELECT
            c.id, c.razao_social, c.cnpj, c.uf, c.municipio,
            c.setor, c.tipo_sociedade,
            fs.receita_liquida, fs.ebitda, fs.margem_ebitda,
            fs.lucro_liquido, fs.ano_referencia,
            fs.fonte_url, fs.fonte_tipo, fs.confianca_extracao
        FROM companies c
        JOIN financial_statements fs ON fs.company_id = c.id
        WHERE {where}
        ORDER BY fs.{order_col} {order_dir}
        LIMIT {limit}
    """
    with get_conn() as conn:
        rows = conn.execute(sql, params).fetchall()
    return [dict(r) for r in rows]


def get_summary_stats() -> dict:
    sql = """
        SELECT
            COUNT(DISTINCT c.id)                AS total_empresas,
            SUM(fs.ebitda)                      AS ebitda_total,
            AVG(fs.margem_ebitda)               AS margem_media,
            MAX(fs.ebitda)                      AS maior_ebitda,
            COUNT(DISTINCT c.uf)                AS estados_cobertos,
            MAX(fs.ano_referencia)              AS ano_mais_recente
        FROM companies c
        JOIN financial_statements fs ON fs.company_id = c.id
        WHERE c.is_b3_listed = 0 AND fs.ebitda >= 40000000
    """
    with get_conn() as conn:
        row = conn.execute(sql).fetchone()
    return dict(row) if row else {}


def get_distribution_by_uf() -> list[dict]:
    sql = """
        SELECT c.uf, COUNT(*) as total, SUM(fs.ebitda) as ebitda_soma
        FROM companies c
        JOIN financial_statements fs ON fs.company_id = c.id
        WHERE c.is_b3_listed = 0 AND fs.ebitda >= 40000000
        GROUP BY c.uf ORDER BY total DESC
    """
    with get_conn() as conn:
        rows = conn.execute(sql).fetchall()
    return [dict(r) for r in rows]


def get_distribution_by_setor() -> list[dict]:
    sql = """
        SELECT c.setor, COUNT(*) as total, AVG(fs.margem_ebitda)*100 as margem_media
        FROM companies c
        JOIN financial_statements fs ON fs.company_id = c.id
        WHERE c.is_b3_listed = 0 AND fs.ebitda >= 40000000 AND c.setor IS NOT NULL
        GROUP BY c.setor ORDER BY total DESC LIMIT 15
    """
    with get_conn() as conn:
        rows = conn.execute(sql).fetchall()
    return [dict(r) for r in rows]


def upsert_company(data: dict) -> int:
    with get_conn() as conn:
        conn.execute("""
            INSERT INTO companies (cnpj, razao_social, uf, municipio, setor, tipo_sociedade)
            VALUES (:cnpj, :razao_social, :uf, :municipio, :setor, :tipo_sociedade)
            ON CONFLICT(cnpj) DO UPDATE SET
                razao_social = excluded.razao_social,
                uf           = excluded.uf,
                setor        = excluded.setor,
                updated_at   = datetime('now')
        """, data)
        row = conn.execute(
            "SELECT id FROM companies WHERE cnpj = ?", (data["cnpj"],)
        ).fetchone()
        return row["id"]


def upsert_statement(company_id: int, data: dict):
    margem = (
        data["ebitda"] / data["receita_liquida"]
        if data.get("receita_liquida") and data["receita_liquida"] > 0
        else None
    )
    with get_conn() as conn:
        conn.execute("""
            INSERT INTO financial_statements
                (company_id, ano_referencia, receita_liquida, ebitda,
                 lucro_liquido, depreciacao_amort, margem_ebitda,
                 fonte_url, fonte_tipo, fonte_uf, confianca_extracao)
            VALUES (?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(company_id, ano_referencia) DO UPDATE SET
                receita_liquida   = excluded.receita_liquida,
                ebitda            = excluded.ebitda,
                margem_ebitda     = excluded.margem_ebitda,
                fonte_url         = excluded.fonte_url
        """, (
            company_id,
            data.get("ano_referencia"),
            data.get("receita_liquida"),
            data.get("ebitda"),
            data.get("lucro_liquido"),
            data.get("depreciacao_amort"),
            margem,
            data.get("fonte_url"),
            data.get("fonte_tipo"),
            data.get("fonte_uf"),
            data.get("confianca_extracao", 1.0),
        ))


def log_pipeline_run(fonte: str, uf: str, status: str,
                     docs_found=0, docs_parsed=0, empresas_novas=0,
                     started_at=None, log_text="") -> int:
    with get_conn() as conn:
        cur = conn.execute("""
            INSERT INTO pipeline_runs
                (fonte, uf, status, docs_found, docs_parsed,
                 empresas_novas, started_at, finished_at, log_text)
            VALUES (?,?,?,?,?,?,?,datetime('now'),?)
        """, (fonte, uf, status, docs_found, docs_parsed,
              empresas_novas, started_at or datetime.utcnow().isoformat(), log_text))
        return cur.lastrowid


def get_recent_runs(limit=20) -> list[dict]:
    with get_conn() as conn:
        rows = conn.execute("""
            SELECT * FROM pipeline_runs
            ORDER BY id DESC LIMIT ?
        """, (limit,)).fetchall()
    return [dict(r) for r in rows]


def get_all_ufs() -> list[str]:
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT DISTINCT uf FROM companies WHERE uf IS NOT NULL ORDER BY uf"
        ).fetchall()
    return [r["uf"] for r in rows]


def get_all_setores() -> list[str]:
    with get_conn() as conn:
        rows = conn.execute("""
            SELECT DISTINCT setor FROM companies
            WHERE setor IS NOT NULL ORDER BY setor
        """).fetchall()
    return [r["setor"] for r in rows]
