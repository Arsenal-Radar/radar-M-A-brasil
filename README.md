"""Página: Coletor de dados com progresso em tempo real."""

import streamlit as st
import time
import threading
from datetime import datetime
from core.database import init_db, get_recent_runs, get_summary_stats
from core.collector import COLLECTORS, run_collector


def render():
    init_db()

    st.title("⚙️ Coletar Dados")
    st.caption("Busca automática em Diários Oficiais, JUCEs e fontes públicas brasileiras")

    # ── Status atual ─────────────────────────────────────────────────────────
    stats = get_summary_stats()
    c1, c2, c3 = st.columns(3)
    with c1:
        st.metric("Empresas na base", stats.get("total_empresas", 0))
    with c2:
        st.metric("Ano mais recente", stats.get("ano_mais_recente", "—"))
    with c3:
        runs = get_recent_runs(1)
        last_run = runs[0]["finished_at"] if runs else "Nunca"
        if last_run and last_run != "Nunca":
            try:
                dt = datetime.fromisoformat(last_run)
                last_run = dt.strftime("%d/%m/%Y %H:%M")
            except Exception:
                pass
        st.metric("Última coleta", last_run)

    st.markdown("---")

    # ── Dados de demonstração ────────────────────────────────────────────────
    with st.expander("📦 Carregar dados de demonstração (recomendado para começar)", expanded=True):
        st.write(
            "Popula a base com **30 empresas reais** extraídas de publicações oficiais, "
            "prontas para explorar agora."
        )
        col_seed, _ = st.columns([1, 2])
        with col_seed:
            if st.button("⚡ Carregar dados de demonstração", type="primary", use_container_width=True):
                with st.spinner("Carregando empresas..."):
                    from core.seed_data import seed
                    seed()
                st.success("✅ 30 empresas carregadas! Vá para 🔍 Buscar Empresas.")
                st.balloons()

    st.markdown("---")

    # ── Coleta automatizada ──────────────────────────────────────────────────
    st.subheader("🤖 Coleta automatizada nas fontes públicas")

    st.warning(
        "⚠️ **Importante:** A coleta real acessa Diários Oficiais e JUCEs estaduais. "
        "Pode levar de 30 minutos a várias horas dependendo das fontes selecionadas. "
        "Os servidores públicos têm velocidade limitada — o sistema respeita isso automaticamente."
    )

    col_fonte, col_btn = st.columns([2, 1])

    with col_fonte:
        fontes_disponiveis = list(COLLECTORS.keys())
        fontes_sel = st.multiselect(
            "Fontes para coletar",
            fontes_disponiveis,
            default=["DOU (União)", "Diário Oficial SP"],
            help="Selecione as fontes que deseja varrer. Comece pelas mais abrangentes.",
        )

    with col_btn:
        st.markdown("&nbsp;")
        coletar_btn = st.button(
            f"🚀 Iniciar coleta ({len(fontes_sel)} fonte{'s' if len(fontes_sel)!=1 else ''})",
            type="primary",
            use_container_width=True,
            disabled=len(fontes_sel) == 0,
        )

    # ── Execução da coleta ───────────────────────────────────────────────────
    if coletar_btn and fontes_sel:
        log_container = st.empty()
        progress_bar = st.progress(0, text="Iniciando...")
        status_placeholder = st.empty()

        all_logs = []
        total_fontes = len(fontes_sel)

        for i, fonte in enumerate(fontes_sel):
            progress_pct = int((i / total_fontes) * 100)
            progress_bar.progress(progress_pct, text=f"Coletando: {fonte}...")
            status_placeholder.info(f"🔄 Processando fonte {i+1}/{total_fontes}: **{fonte}**")

            logs_fonte = [f"\n{'='*50}", f"🔍 FONTE: {fonte}", f"{'='*50}"]
            docs_count = [0]

            def log_cb(msg, _logs=logs_fonte):
                _logs.append(msg)
                log_container.text_area(
                    "Log em tempo real",
                    "\n".join(all_logs + _logs),
                    height=300,
                    key=f"log_{time.time()}",
                )

            def prog_cb(n, _dc=docs_count):
                _dc[0] = n

            try:
                result = run_collector(fonte, progress_callback=prog_cb, log_callback=log_cb)
                logs_fonte.append(
                    f"✅ Concluído: {result.get('docs_parsed',0)} empresas novas | "
                    f"{result.get('errors',0)} erros"
                )
            except Exception as e:
                logs_fonte.append(f"❌ Erro na fonte {fonte}: {e}")

            all_logs.extend(logs_fonte)
            log_container.text_area("Log em tempo real", "\n".join(all_logs), height=300)

        progress_bar.progress(100, text="Coleta finalizada!")
        status_placeholder.success(
            f"✅ Coleta de {total_fontes} fonte(s) finalizada! "
            "Vá para 🔍 Buscar Empresas para ver os resultados."
        )

    st.markdown("---")

    # ── Histórico de execuções ───────────────────────────────────────────────
    st.subheader("📋 Histórico de coletas")
    runs = get_recent_runs(20)

    if not runs:
        st.info("Nenhuma coleta executada ainda.")
        return

    import pandas as pd
    df_runs = pd.DataFrame(runs)

    # Formatar
    status_map = {"done": "✅ Concluído", "partial": "⚠️ Parcial", "running": "🔄 Rodando", "failed": "❌ Erro"}
    df_runs["Status"] = df_runs["status"].map(status_map).fillna(df_runs["status"])
    df_runs["Início"] = pd.to_datetime(df_runs["started_at"]).dt.strftime("%d/%m %H:%M")
    df_runs["Fim"] = pd.to_datetime(df_runs["finished_at"]).dt.strftime("%d/%m %H:%M")

    cols_show = ["fonte", "uf", "Status", "docs_found", "docs_parsed", "empresas_novas", "Início", "Fim"]
    rename = {
        "fonte": "Fonte", "uf": "UF",
        "docs_found": "Docs encontrados",
        "docs_parsed": "Docs processados",
        "empresas_novas": "Empresas novas",
    }
    st.dataframe(
        df_runs[cols_show].rename(columns=rename),
        use_container_width=True,
        hide_index=True,
    )

    # Detalhe do log
    with st.expander("Ver log detalhado da última execução"):
        if runs[0].get("log_text"):
            st.text(runs[0]["log_text"])
        else:
            st.info("Log não disponível para esta execução.")
