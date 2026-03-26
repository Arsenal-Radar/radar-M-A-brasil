"""Página: Dashboard principal com métricas e gráficos."""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from core.database import init_db, get_summary_stats, get_distribution_by_uf, get_distribution_by_setor, query_companies


def render():
    init_db()

    st.markdown('<div class="radar-header">', unsafe_allow_html=True)
    st.title("🎯 Radar M&A Brasil")
    st.caption("Empresas não listadas · EBITDA > R$ 40M · Dados de Diários Oficiais e JUCEs")
    st.markdown('</div>', unsafe_allow_html=True)

    stats = get_summary_stats()

    if not stats or not stats.get("total_empresas"):
        st.info("⚡ Base ainda sem dados. Vá em **⚙️ Coletar Dados** para iniciar a busca, ou aguarde o carregamento dos dados de demonstração.")
        _seed_prompt()
        return

    # ── Métricas topo ────────────────────────────────────────────────────────
    c1, c2, c3, c4, c5 = st.columns(5)
    with c1:
        st.metric("Empresas mapeadas", f"{stats['total_empresas']:,}".replace(",", "."))
    with c2:
        ebitda_total = stats.get("ebitda_total") or 0
        st.metric("EBITDA agregado", f"R$ {ebitda_total/1e9:.1f}B")
    with c3:
        margem = (stats.get("margem_media") or 0) * 100
        st.metric("Margem EBITDA média", f"{margem:.1f}%")
    with c4:
        maior = stats.get("maior_ebitda") or 0
        st.metric("Maior EBITDA", f"R$ {maior/1e9:.1f}B")
    with c5:
        st.metric("Estados cobertos", stats.get("estados_cobertos", 0))

    st.markdown("---")

    # ── Gráficos ─────────────────────────────────────────────────────────────
    col_esq, col_dir = st.columns([1, 1])

    with col_esq:
        st.subheader("Empresas por estado")
        uf_data = get_distribution_by_uf()
        if uf_data:
            df_uf = pd.DataFrame(uf_data)
            fig = px.bar(
                df_uf.head(12), x="uf", y="total",
                color="total",
                color_continuous_scale=["#0f3460", "#00d4aa"],
                labels={"uf": "Estado", "total": "Empresas"},
                template="plotly_dark",
            )
            fig.update_layout(
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)",
                coloraxis_showscale=False,
                margin=dict(l=0, r=0, t=0, b=0),
                height=300,
            )
            fig.update_traces(marker_line_width=0)
            st.plotly_chart(fig, use_container_width=True)

    with col_dir:
        st.subheader("EBITDA médio por setor")
        setor_data = get_distribution_by_setor()
        if setor_data:
            df_setor = pd.DataFrame(setor_data)
            fig2 = px.bar(
                df_setor.sort_values("margem_media", ascending=True).tail(10),
                x="margem_media", y="setor",
                orientation="h",
                color="margem_media",
                color_continuous_scale=["#0f3460", "#00d4aa"],
                labels={"setor": "", "margem_media": "Margem EBITDA (%)"},
                template="plotly_dark",
            )
            fig2.update_layout(
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)",
                coloraxis_showscale=False,
                margin=dict(l=0, r=0, t=0, b=0),
                height=300,
            )
            st.plotly_chart(fig2, use_container_width=True)

    st.markdown("---")

    # ── Top 10 empresas ──────────────────────────────────────────────────────
    st.subheader("🏆 Top 10 por EBITDA")
    top = query_companies(ebitda_min=40_000_000, limit=10)
    if top:
        for i, emp in enumerate(top, 1):
            ebitda_m = (emp["ebitda"] or 0) / 1e6
            receita_m = (emp["receita_liquida"] or 0) / 1e6
            margem_pct = (emp["margem_ebitda"] or 0) * 100

            badge_color = "badge-green" if margem_pct >= 20 else ("badge-yellow" if margem_pct >= 10 else "badge-red")

            st.markdown(f"""
            <div class="company-card">
                <div style="display:flex; justify-content:space-between; align-items:center;">
                    <div>
                        <span style="color:#555; font-size:0.75rem;">#{i}</span>
                        <span class="company-name"> {emp['razao_social']}</span>
                        <div class="company-meta">
                            {emp.get('uf','?')} &nbsp;·&nbsp;
                            {emp.get('setor','N/D')} &nbsp;·&nbsp;
                            {emp.get('tipo_sociedade','?')} &nbsp;·&nbsp;
                            Ano {emp.get('ano_referencia','?')}
                        </div>
                    </div>
                    <div style="text-align:right;">
                        <div class="ebitda-label">EBITDA</div>
                        <div class="company-ebitda">R$ {ebitda_m:.0f}M</div>
                        <span class="{badge_color}">{margem_pct:.1f}% margem</span>
                    </div>
                </div>
            </div>
            """, unsafe_allow_html=True)

    # ── Scatter: Receita × EBITDA ────────────────────────────────────────────
    st.markdown("---")
    st.subheader("📊 Mapa: Receita × EBITDA")
    all_data = query_companies(ebitda_min=40_000_000, limit=200)
    if all_data:
        df = pd.DataFrame(all_data)
        df["receita_bi"] = df["receita_liquida"] / 1e9
        df["ebitda_m"]   = df["ebitda"] / 1e6
        df["margem_pct"] = df["margem_ebitda"] * 100

        fig3 = px.scatter(
            df,
            x="receita_bi", y="ebitda_m",
            size="ebitda_m", color="setor",
            hover_name="razao_social",
            hover_data={"receita_bi": ":.1f", "ebitda_m": ":.0f", "margem_pct": ":.1f"},
            labels={
                "receita_bi": "Receita Líquida (R$ bi)",
                "ebitda_m": "EBITDA (R$ M)",
                "margem_pct": "Margem EBITDA (%)",
                "setor": "Setor",
            },
            template="plotly_dark",
            height=450,
        )
        fig3.update_layout(
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(15,17,23,0.8)",
            margin=dict(l=0, r=0, t=10, b=0),
        )
        st.plotly_chart(fig3, use_container_width=True)


def _seed_prompt():
    st.markdown("### 🚀 Carregar dados de demonstração")
    st.write("Clique abaixo para popular a base com **30 empresas reais** extraídas de publicações oficiais:")
    if st.button("⚡ Carregar dados de demonstração agora", type="primary"):
        with st.spinner("Carregando..."):
            import sys, os
            sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
            from core.seed_data import seed
            seed()
        st.success("✅ Dados carregados! Recarregue a página.")
        st.rerun()
