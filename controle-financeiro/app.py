import streamlit as st
import pandas as pd
import plotly.express as px

# CONFIGURAÇÃO DA PÁGINA
st.set_page_config(
    page_title="Controle Financeiro",
    page_icon="💰",
    layout="wide"
)

# =========================
# DADOS MOCKADOS (TEMPORÁRIOS)
# =========================

receita = 5200
despesa = 3680
saldo = receita - despesa
comprometimento = (despesa / receita) * 100

parcelas_futuras = 1450
fixos_futuros = 2100
reserva = 7800

# =========================
# FUNÇÃO STATUS FINANCEIRO
# =========================

def status_financeiro(comp):
    if comp < 70:
        return "🟢 Saudável"
    elif comp < 90:
        return "🟡 Atenção"
    else:
        return "🔴 Crítico"

status = status_financeiro(comprometimento)

# =========================
# TÍTULO
# =========================

st.title("💰 Controle Financeiro")
st.caption("Visão executiva da sua saúde financeira")

st.divider()

# =========================
# BLOCO 1 — FLUXO ATUAL
# =========================

st.subheader("Fluxo Atual")

col1, col2, col3, col4 = st.columns(4)

col1.metric(
    "Receita",
    f"R$ {receita:,.0f}".replace(",", ".")
)

col2.metric(
    "Despesa",
    f"R$ {despesa:,.0f}".replace(",", ".")
)

col3.metric(
    "Saldo",
    f"R$ {saldo:,.0f}".replace(",", ".")
)

col4.metric(
    "% Comprometido",
    f"{comprometimento:.1f}%"
)

st.divider()

# =========================
# BLOCO 2 — COMPROMETIMENTO FUTURO
# =========================

st.subheader("Comprometimento Futuro")

col5, col6, col7 = st.columns(3)

col5.metric(
    "Parcelas Futuras",
    f"R$ {parcelas_futuras:,.0f}".replace(",", ".")
)

col6.metric(
    "Fixos Futuros",
    f"R$ {fixos_futuros:,.0f}".replace(",", ".")
)

col7.metric(
    "Reserva",
    f"R$ {reserva:,.0f}".replace(",", ".")
)

st.divider()

# =========================
# STATUS FINANCEIRO
# =========================

st.subheader("Status Financeiro")

st.markdown(f"## {status}")

if comprometimento < 70:
    st.success(
        "Seu comprometimento financeiro está dentro de uma faixa saudável."
    )

elif comprometimento < 90:
    st.warning(
        "Seu comprometimento financeiro está elevado. "
        "Novas despesas fixas exigem atenção."
    )

else:
    st.error(
        "Seu comprometimento financeiro está crítico. "
        "Evite assumir novos compromissos."
    )

st.divider()

# =========================
# GRÁFICO SIMPLES
# =========================

st.subheader("Resumo do Mês")

grafico_df = pd.DataFrame({
    "Categoria": ["Receita", "Despesa", "Saldo"],
    "Valor": [receita, despesa, saldo]
})

fig = px.bar(
    grafico_df,
    x="Categoria",
    y="Valor",
    text_auto=True
)

fig.update_layout(
    height=400,
    showlegend=False
)

st.plotly_chart(fig, use_container_width=True)
