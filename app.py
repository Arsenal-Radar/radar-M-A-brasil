"""Página: Exportar e importar dados em Excel — backup e restauração completa."""

import streamlit as st
import pandas as pd
import io
from datetime import datetime
from core.database import (
    init_db, query_companies, get_summary_stats,
    upsert_company, upsert_statement
)


# ── Colunas do Excel exportado ───────────────────────────────────────────────
EXPORT_COLS = {
    "razao_social":       "Empresa",
    "cnpj":               "CNPJ",
    "uf":                 "UF",
    "municipio":          "Município",
    "setor":              "Setor",
    "tipo_sociedade":     "Tipo",
    "ano_referencia":     "Ano Ref.",
    "receita_liquida":    "Receita Líquida (R$)",
    "ebitda":             "EBITDA (R$)",
    "margem_ebitda_pct":  "Margem EBITDA (%)",
    "lucro_liquido":      "Lucro Líquido (R$)",
    "depreciacao_amort":  "Depreciação/Amort (R$)",
    "fonte_tipo":         "Fonte",
    "fonte_url":          "Link da Fonte",
    "confianca_extracao": "Confiança Extração",
}

# Colunas obrigatórias para importação
IMPORT_REQUIRED = ["Empresa", "CNPJ", "UF", "Ano Ref.", "EBITDA (R$)"]


def render():
    init_db()

    st.title("📤 Exportar / Importar Excel")
    st.caption("Salve seus dados localmente e restaure quando precisar — nunca perca uma coleta.")

    # ── Resumo atual ──────────────────────────────────────────────────────────
    stats = get_summary_stats()
    total = stats.get("total_empresas", 0)

    if total:
        st.success(f"✅ **{total} empresas** na base atual · Ano mais recente: {stats.get('ano_mais_recente','—')}")
    else:
        st.warning("⚠️ Base vazia. Faça uma coleta ou importe um Excel salvo anteriormente.")

    st.markdown("---")

    # ════════════════════════════════════════════════════════════════════════
    # EXPORTAR
    # ════════════════════════════════════════════════════════════════════════
    st.subheader("⬇️ Exportar para Excel")
    st.write("Baixa **todas as empresas da base** em uma planilha formatada, pronta para abrir no Excel ou Google Sheets.")

    col_f1, col_f2, col_f3 = st.columns(3)
    with col_f1:
        ebitda_min_exp = st.number_input(
            "EBITDA mínimo (R$ M)", value=40.0, min_value=0.0, step=10.0,
            key="exp_ebitda_min"
        )
    with col_f2:
        ufs_exp = ["Todos"] + _get_ufs()
        uf_exp = st.selectbox("Estado", ufs_exp, key="exp_uf")
    with col_f3:
        setores_exp = ["Todos"] + _get_setores()
        setor_exp = st.selectbox("Setor", setores_exp, key="exp_setor")

    col_btn_exp, col_info = st.columns([1, 2])
    with col_btn_exp:
        gerar = st.button("📊 Gerar Excel", type="primary", use_container_width=True)

    if gerar:
        with st.spinner("Gerando planilha..."):
            dados = query_companies(
                ebitda_min=ebitda_min_exp * 1_000_000,
                uf=uf_exp if uf_exp != "Todos" else None,
                setor=setor_exp if setor_exp != "Todos" else None,
                limit=10_000,
            )

        if not dados:
            st.warning("Nenhuma empresa encontrada com esses filtros.")
        else:
            df = pd.DataFrame(dados)

            # Calcular coluna de margem em %
            df["margem_ebitda_pct"] = (df["margem_ebitda"] * 100).round(2)

            # Selecionar e renomear colunas
            cols_presentes = [c for c in EXPORT_COLS if c in df.columns]
            df_exp = df[cols_presentes].rename(columns=EXPORT_COLS)

            # Gerar Excel em memória
            buffer = io.BytesIO()
            with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
                df_exp.to_excel(writer, index=False, sheet_name="Empresas")

                # Aba de resumo
                resumo = pd.DataFrame([{
                    "Total de empresas":    len(df_exp),
                    "EBITDA total (R$)":    df["ebitda"].sum(),
                    "EBITDA médio (R$)":    df["ebitda"].mean().round(0),
                    "Margem média (%)":     df["margem_ebitda_pct"].mean().round(2),
                    "Maior EBITDA (R$)":    df["ebitda"].max(),
                    "Gerado em":            datetime.now().strftime("%d/%m/%Y %H:%M"),
                }])
                resumo.to_excel(writer, index=False, sheet_name="Resumo")

                # Formatação básica
                _format_excel(writer, df_exp)

            nome_arquivo = f"radar_ma_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx"
            st.download_button(
                label=f"⬇️ Baixar Excel ({len(df_exp)} empresas)",
                data=buffer.getvalue(),
                file_name=nome_arquivo,
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                type="primary",
                use_container_width=False,
            )
            st.success(f"✅ Planilha com **{len(df_exp)} empresas** pronta para download.")

            # Preview
            with st.expander("👀 Preview da planilha"):
                st.dataframe(df_exp.head(10), use_container_width=True)

    st.markdown("---")

    # ════════════════════════════════════════════════════════════════════════
    # IMPORTAR
    # ════════════════════════════════════════════════════════════════════════
    st.subheader("⬆️ Importar Excel salvo")
    st.write(
        "Restaura dados de uma planilha exportada anteriormente. "
        "Útil para recuperar dados após reinicialização do Streamlit Cloud."
    )

    st.info(
        "💡 **Fluxo recomendado:** Coletou dados → Exportou Excel → guardou no computador. "
        "Da próxima vez que o app reiniciar, importe o Excel aqui e seus dados voltam."
    )

    arquivo = st.file_uploader(
        "Selecione o arquivo Excel exportado por este sistema",
        type=["xlsx", "xls"],
        key="importar_excel",
    )

    if arquivo:
        try:
            df_import = pd.read_excel(arquivo, sheet_name="Empresas")
        except Exception:
            try:
                df_import = pd.read_excel(arquivo)
            except Exception as e:
                st.error(f"❌ Não consegui ler o arquivo: {e}")
                return

        st.markdown(f"**{len(df_import)} linhas encontradas** na planilha.")

        # Verificar colunas obrigatórias
        faltando = [c for c in IMPORT_REQUIRED if c not in df_import.columns]
        if faltando:
            st.error(
                f"❌ Colunas obrigatórias não encontradas: **{', '.join(faltando)}**\n\n"
                "Use apenas planilhas exportadas por este sistema."
            )
            return

        # Preview
        st.dataframe(df_import.head(5), use_container_width=True)

        col_imp, col_modo = st.columns([1, 2])
        with col_modo:
            modo = st.radio(
                "Modo de importação",
                ["Adicionar aos dados existentes", "Substituir tudo (apaga base atual)"],
                key="modo_import",
            )
        with col_imp:
            st.markdown("&nbsp;")
            confirmar = st.button("⬆️ Importar agora", type="primary", use_container_width=True)

        if confirmar:
            if "Substituir" in modo:
                _limpar_base()

            progresso = st.progress(0, text="Importando...")
            erros = 0
            importadas = 0

            # Mapa reverso: nome Excel → nome interno
            rev_map = {v: k for k, v in EXPORT_COLS.items()}
            df_imp_int = df_import.rename(columns=rev_map)

            for i, row in df_imp_int.iterrows():
                try:
                    cnpj = str(row.get("cnpj", "") or "").strip()
                    nome = str(row.get("razao_social", "") or "").strip()
                    if not nome:
                        continue

                    company_data = {
                        "cnpj":          cnpj if cnpj else f"IMP_{i:05d}",
                        "razao_social":  nome,
                        "uf":            _safe(row, "uf"),
                        "municipio":     _safe(row, "municipio"),
                        "setor":         _safe(row, "setor"),
                        "tipo_sociedade": _safe(row, "tipo_sociedade"),
                    }
                    cid = upsert_company(company_data)

                    ebitda = _safe_float(row, "ebitda")
                    if not ebitda:
                        continue

                    stmt_data = {
                        "ano_referencia":    _safe_int(row, "ano_referencia") or 2024,
                        "receita_liquida":   _safe_float(row, "receita_liquida"),
                        "ebitda":            ebitda,
                        "lucro_liquido":     _safe_float(row, "lucro_liquido"),
                        "depreciacao_amort": _safe_float(row, "depreciacao_amort"),
                        "fonte_url":         _safe(row, "fonte_url"),
                        "fonte_tipo":        _safe(row, "fonte_tipo") or "IMPORTADO",
                        "fonte_uf":          _safe(row, "uf"),
                        "confianca_extracao": _safe_float(row, "confianca_extracao") or 1.0,
                    }
                    upsert_statement(cid, stmt_data)
                    importadas += 1

                except Exception as e:
                    erros += 1

                progresso.progress(
                    int((i + 1) / len(df_imp_int) * 100),
                    text=f"Importando... {importadas} empresas"
                )

            progresso.progress(100, text="Concluído!")
            if erros:
                st.warning(f"⚠️ {importadas} empresas importadas · {erros} linhas com erro (ignoradas)")
            else:
                st.success(f"✅ **{importadas} empresas importadas com sucesso!**")
            st.rerun()

    st.markdown("---")

    # ── Dica de uso ──────────────────────────────────────────────────────────
    with st.expander("📖 Como usar o Excel para não perder dados"):
        st.markdown("""
        **O Streamlit Cloud gratuito reinicia o servidor periodicamente**, o que apaga o banco SQLite.
        O Excel é sua solução de backup:

        **Rotina recomendada:**
        1. Depois de cada coleta → **Exportar Excel** → salvar no seu computador (ou Google Drive)
        2. Se o app reiniciar e os dados sumirem → **Importar Excel** aqui
        3. Os dados voltam em segundos e você continua de onde parou

        **A planilha exportada contém:**
        - Aba **Empresas** — todos os dados para reimportação e análise
        - Aba **Resumo** — métricas consolidadas (total, médias, data de geração)

        **Dica extra:** o Excel exportado já está formatado para abrir direto no Excel ou Google Sheets,
        com as colunas de valor em reais prontas para filtros e tabelas dinâmicas.
        """)


# ── Helpers ──────────────────────────────────────────────────────────────────

def _format_excel(writer, df: pd.DataFrame):
    """Aplica largura de colunas e formatação básica."""
    try:
        from openpyxl.styles import Font, PatternFill, Alignment
        from openpyxl.utils import get_column_letter

        ws = writer.sheets["Empresas"]

        # Header
        header_fill = PatternFill("solid", fgColor="0F3460")
        header_font = Font(bold=True, color="FFFFFF")
        for cell in ws[1]:
            cell.fill = header_fill
            cell.font = header_font
            cell.alignment = Alignment(horizontal="center")

        # Largura das colunas
        col_widths = {
            "Empresa": 45, "CNPJ": 20, "UF": 6, "Município": 20,
            "Setor": 18, "Tipo": 14, "Ano Ref.": 10,
            "Receita Líquida (R$)": 22, "EBITDA (R$)": 18,
            "Margem EBITDA (%)": 18, "Lucro Líquido (R$)": 20,
            "Depreciação/Amort (R$)": 24, "Fonte": 16,
            "Link da Fonte": 40, "Confiança Extração": 20,
        }
        for i, col in enumerate(df.columns, 1):
            width = col_widths.get(col, 15)
            ws.column_dimensions[get_column_letter(i)].width = width

        # Formato monetário nas colunas de valor
        money_cols = [
            "Receita Líquida (R$)", "EBITDA (R$)",
            "Lucro Líquido (R$)", "Depreciação/Amort (R$)"
        ]
        money_fmt = '#,##0.00'
        for i, col in enumerate(df.columns, 1):
            if col in money_cols:
                for row in ws.iter_rows(min_row=2, min_col=i, max_col=i):
                    for cell in row:
                        cell.number_format = money_fmt

    except Exception:
        pass   # Formatação é cosmética — não quebra se falhar


def _safe(row, col: str) -> str | None:
    val = row.get(col)
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    return str(val).strip() or None


def _safe_float(row, col: str) -> float | None:
    try:
        val = row.get(col)
        if val is None or (isinstance(val, float) and pd.isna(val)):
            return None
        return float(val)
    except Exception:
        return None


def _safe_int(row, col: str) -> int | None:
    try:
        val = row.get(col)
        if val is None or (isinstance(val, float) and pd.isna(val)):
            return None
        return int(float(val))
    except Exception:
        return None


def _limpar_base():
    from core.database import get_conn
    with get_conn() as conn:
        conn.executescript("""
            DELETE FROM financial_statements;
            DELETE FROM companies;
        """)


def _get_ufs() -> list[str]:
    from core.database import get_all_ufs
    return get_all_ufs()


def _get_setores() -> list[str]:
    from core.database import get_all_setores
    return get_all_setores()
