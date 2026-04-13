import sqlite3
from pathlib import Path
from datetime import datetime
from decimal import Decimal, InvalidOperation
import re
import streamlit as st
import pandas as pd

DB_PATH = Path("financeiro.db")


# =========================
# BANCO DE DADOS
# =========================
def get_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    with get_connection() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS transacoes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                fitid TEXT UNIQUE,
                data TEXT NOT NULL,
                valor REAL NOT NULL,
                tipo TEXT NOT NULL CHECK (tipo IN ('entrada', 'saida')),
                descricao TEXT NOT NULL,
                conta TEXT,
                origem TEXT NOT NULL CHECK (origem IN ('ofx', 'manual')),
                categoria TEXT,
                subcategoria TEXT,
                status TEXT NOT NULL DEFAULT 'pendente' CHECK (status IN ('pendente', 'revisado')),
                data_importacao TEXT NOT NULL,
                arquivo_origem TEXT,
                observacao TEXT
            )
            """
        )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS importacoes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                nome_arquivo TEXT NOT NULL,
                hash_arquivo TEXT,
                origem TEXT NOT NULL,
                data_importacao TEXT NOT NULL,
                total_linhas INTEGER NOT NULL,
                novas_linhas INTEGER NOT NULL,
                duplicadas INTEGER NOT NULL
            )
            """
        )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS orcamentos (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                categoria TEXT NOT NULL UNIQUE,
                valor_orcado REAL NOT NULL,
                data_criacao TEXT NOT NULL,
                data_atualizacao TEXT NOT NULL
            )
            """
        )


# =========================
# CATEGORIZAÇÃO INICIAL
# =========================
CATEGORIZATION_RULES = {
    "uber": ("Transporte", "Mobilidade"),
    "uber* trip": ("Transporte", "Mobilidade"),
    "uberrides": ("Transporte", "Mobilidade"),
    "spotify": ("Assinaturas", "Música"),
    "netflix": ("Assinaturas", "Streaming"),
    "amazonprime": ("Assinaturas", "Streaming"),
    "google one": ("Assinaturas", "Nuvem"),
    "giga atacado": ("Alimentação", "Mercado"),
    "emporio": ("Alimentação", "Mercado"),
    "minuto": ("Alimentação", "Conveniência"),
    "kabum": ("Compras", "Tecnologia"),
    "terabyteshop": ("Compras", "Tecnologia"),
}


def suggest_category(description: str) -> tuple[str | None, str | None]:
    text = (description or "").strip().lower()
    for keyword, result in CATEGORIZATION_RULES.items():
        if keyword in text:
            return result
    return None, None


# =========================
# PARSER OFX
# =========================
def extract_tag_value(block: str, tag: str) -> str | None:
    pattern = rf"<{tag}>([^<\r\n]+)"
    match = re.search(pattern, block, flags=re.IGNORECASE)
    return match.group(1).strip() if match else None


def parse_ofx_date(raw_date: str | None) -> str:
    if not raw_date:
        raise ValueError("Data OFX ausente.")

    clean = raw_date.strip()
    # Exemplo comum: 20260325120000[-3:BRT]
    clean = re.split(r"\[", clean)[0]
    clean = clean[:14] if len(clean) >= 14 else clean[:8]

    if len(clean) >= 14:
        dt = datetime.strptime(clean[:14], "%Y%m%d%H%M%S")
    else:
        dt = datetime.strptime(clean[:8], "%Y%m%d")

    return dt.strftime("%Y-%m-%d")


def parse_amount(raw_amount: str | None) -> float:
    if raw_amount is None:
        raise ValueError("Valor OFX ausente.")

    text = raw_amount.strip().replace(",", ".")
    try:
        return float(Decimal(text))
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"Valor inválido no OFX: {raw_amount}") from exc


def parse_ofx(content: str) -> list[dict]:
    transactions = []
    blocks = re.findall(r"<STMTTRN>(.*?)</STMTTRN>", content, flags=re.IGNORECASE | re.DOTALL)

    account_id = extract_tag_value(content, "ACCTID")

    for block in blocks:
        trn_type = extract_tag_value(block, "TRNTYPE") or ""
        posted = extract_tag_value(block, "DTPOSTED")
        amount = extract_tag_value(block, "TRNAMT")
        fitid = extract_tag_value(block, "FITID")
        memo = extract_tag_value(block, "MEMO") or "Sem descrição"

        parsed_amount = parse_amount(amount)
        tipo = "entrada" if parsed_amount > 0 else "saida"
        categoria, subcategoria = suggest_category(memo)

        transactions.append(
            {
                "fitid": fitid,
                "data": parse_ofx_date(posted),
                "valor": abs(parsed_amount),
                "tipo": tipo,
                "descricao": memo,
                "conta": account_id,
                "origem": "ofx",
                "categoria": categoria,
                "subcategoria": subcategoria,
                "status": "pendente",
            }
        )

    if not transactions:
        raise ValueError("Nenhuma transação foi encontrada no OFX.")

    return transactions


# =========================
# GRAVAÇÃO E CONSULTA
# =========================
def insert_transactions(transactions: list[dict], file_name: str) -> tuple[int, int]:
    now = datetime.now().isoformat(timespec="seconds")
    inserted = 0
    duplicates = 0

    with get_connection() as conn:
        for tr in transactions:
            try:
                conn.execute(
                    """
                    INSERT INTO transacoes (
                        fitid, data, valor, tipo, descricao, conta, origem,
                        categoria, subcategoria, status, data_importacao, arquivo_origem
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        tr["fitid"],
                        tr["data"],
                        tr["valor"],
                        tr["tipo"],
                        tr["descricao"],
                        tr["conta"],
                        tr["origem"],
                        tr["categoria"],
                        tr["subcategoria"],
                        tr["status"],
                        now,
                        file_name,
                    ),
                )
                inserted += 1
            except sqlite3.IntegrityError:
                duplicates += 1

        conn.execute(
            """
            INSERT INTO importacoes (
                nome_arquivo, origem, data_importacao, total_linhas, novas_linhas, duplicadas
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (file_name, "ofx", now, len(transactions), inserted, duplicates),
        )

    return inserted, duplicates


def add_manual_transaction(
    data: str,
    valor: float,
    tipo: str,
    descricao: str,
    categoria: str | None,
    subcategoria: str | None,
    observacao: str | None,
) -> None:
    now = datetime.now().isoformat(timespec="seconds")

    with get_connection() as conn:
        conn.execute(
            """
            INSERT INTO transacoes (
                fitid, data, valor, tipo, descricao, conta, origem,
                categoria, subcategoria, status, data_importacao, arquivo_origem, observacao
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                None,
                data,
                valor,
                tipo,
                descricao,
                None,
                "manual",
                categoria,
                subcategoria,
                "revisado",
                now,
                None,
                observacao,
            ),
        )


def load_transactions() -> pd.DataFrame:
    with get_connection() as conn:
        df = pd.read_sql_query(
            """
            SELECT
                id, fitid, data, valor, tipo, descricao, conta, origem,
                categoria, subcategoria, status, data_importacao, arquivo_origem, observacao
            FROM transacoes
            ORDER BY data DESC, id DESC
            """,
            conn,
        )

    if not df.empty:
        df["data"] = pd.to_datetime(df["data"])
    return df


def load_imports() -> pd.DataFrame:
    with get_connection() as conn:
        return pd.read_sql_query(
            """
            SELECT id, nome_arquivo, origem, data_importacao, total_linhas, novas_linhas, duplicadas
            FROM importacoes
            ORDER BY id DESC
            """,
            conn,
        )


def upsert_budget(categoria: str, valor_orcado: float) -> None:
    now = datetime.now().isoformat(timespec="seconds")
    with get_connection() as conn:
        conn.execute(
            """
            INSERT INTO orcamentos (categoria, valor_orcado, data_criacao, data_atualizacao)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(categoria) DO UPDATE SET
                valor_orcado = excluded.valor_orcado,
                data_atualizacao = excluded.data_atualizacao
            """,
            (categoria, valor_orcado, now, now),
        )


def delete_budget(categoria: str) -> None:
    with get_connection() as conn:
        conn.execute("DELETE FROM orcamentos WHERE categoria = ?", (categoria,))


def load_budgets() -> pd.DataFrame:
    with get_connection() as conn:
        return pd.read_sql_query(
            """
            SELECT categoria, valor_orcado, data_criacao, data_atualizacao
            FROM orcamentos
            ORDER BY categoria
            """,
            conn,
        )


def build_recurring_analysis(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()

    base = df.copy()
    base = base[base["tipo"] == "saida"].copy()
    if base.empty:
        return pd.DataFrame()

    base["ano_mes"] = base["data"].dt.to_period("M").astype(str)

    grouped = (
        base.groupby(["descricao", "categoria"], dropna=False)
        .agg(
            ocorrencias=("id", "count"),
            total_gasto=("valor", "sum"),
            media_valor=("valor", "mean"),
            meses_distintos=("ano_mes", "nunique"),
            primeira_data=("data", "min"),
            ultima_data=("data", "max"),
        )
        .reset_index()
    )

    def classify_pattern(row) -> str:
        if row["meses_distintos"] >= 2:
            return "Recorrente"
        if row["ocorrencias"] >= 3:
            return "Frequente"
        return "Pontual"

    grouped["categoria"] = grouped["categoria"].fillna("Sem categoria")
    grouped["classificacao"] = grouped.apply(classify_pattern, axis=1)
    grouped["custo_anual_estimado"] = grouped["media_valor"] * 12
    grouped["custo_mensal_estimado"] = grouped.apply(
        lambda row: row["media_valor"] if row["classificacao"] == "Recorrente" else row["total_gasto"] / max(row["meses_distintos"], 1),
        axis=1,
    )

    grouped = grouped.sort_values(
        by=["classificacao", "custo_mensal_estimado", "total_gasto"],
        ascending=[True, False, False],
    )
    return grouped


# =========================
# MÉTRICAS
# =========================
def format_brl(value: float) -> str:
    return f"R$ {value:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


def build_metrics(df: pd.DataFrame) -> dict:
    if df.empty:
        return {
            "entradas": 0.0,
            "saidas": 0.0,
            "saldo": 0.0,
            "qtd": 0,
            "ticket_medio_despesa": 0.0,
        }

    entradas = df.loc[df["tipo"] == "entrada", "valor"].sum()
    saidas = df.loc[df["tipo"] == "saida", "valor"].sum()
    saldo = entradas - saidas
    despesas = df.loc[df["tipo"] == "saida", "valor"]
    ticket_medio_despesa = float(despesas.mean()) if not despesas.empty else 0.0

    return {
        "entradas": float(entradas),
        "saidas": float(saidas),
        "saldo": float(saldo),
        "qtd": int(len(df)),
        "ticket_medio_despesa": ticket_medio_despesa,
    }


def apply_period_filter(df: pd.DataFrame, start_date, end_date) -> pd.DataFrame:
    if df.empty:
        return df

    filtered = df.copy()
    filtered["data_dia"] = filtered["data"].dt.date
    filtered = filtered[
        (filtered["data_dia"] >= start_date) &
        (filtered["data_dia"] <= end_date)
    ].copy()
    filtered.drop(columns=["data_dia"], inplace=True, errors="ignore")
    return filtered


def build_previous_period(df: pd.DataFrame, start_date, end_date) -> pd.DataFrame:
    if df.empty:
        return df

    start_date = pd.to_datetime(start_date)
    end_date = pd.to_datetime(end_date)

    period_days = (end_date - start_date).days + 1
    previous_end = start_date - pd.Timedelta(days=1)
    previous_start = previous_end - pd.Timedelta(days=period_days - 1)

    filtered = df.copy()
    filtered["data_dia"] = pd.to_datetime(filtered["data"]).dt.normalize()
    prev = filtered[
        (filtered["data_dia"] >= previous_start.normalize()) &
        (filtered["data_dia"] <= previous_end.normalize())
    ].copy()
    prev.drop(columns=["data_dia"], inplace=True, errors="ignore")
    return prev


# =========================
# INTERFACE
# =========================
def page_dashboard(df: pd.DataFrame, start_date, end_date) -> None:
    st.subheader("Visão geral")
    df_periodo = apply_period_filter(df, start_date, end_date)
    metrics = build_metrics(df_periodo)
    df_periodo_anterior = build_previous_period(df, start_date, end_date)
    metrics_prev = build_metrics(df_periodo_anterior)

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Entradas", format_brl(metrics["entradas"]))
    c2.metric("Saídas", format_brl(metrics["saidas"]))
    c3.metric("Saldo", format_brl(metrics["saldo"]))
    c4.metric("Transações", metrics["qtd"])
    c5.metric("Ticket médio despesa", format_brl(metrics["ticket_medio_despesa"]))

    if df_periodo.empty:
        st.info("Não há transações no período selecionado.")
        return

    st.caption(
        f"Período analisado: {start_date.strftime('%d/%m/%Y')} até {end_date.strftime('%d/%m/%Y')}"
    )

    comp1, comp2, comp3 = st.columns(3)
    delta_entradas = metrics["entradas"] - metrics_prev["entradas"]
    delta_saidas = metrics["saidas"] - metrics_prev["saidas"]
    delta_saldo = metrics["saldo"] - metrics_prev["saldo"]
    comp1.metric("Variação entradas vs período anterior", format_brl(metrics["entradas"]), delta=format_brl(delta_entradas))
    comp2.metric("Variação saídas vs período anterior", format_brl(metrics["saidas"]), delta=format_brl(delta_saidas))
    comp3.metric("Variação saldo vs período anterior", format_brl(metrics["saldo"]), delta=format_brl(delta_saldo))

    df_plot = df.copy()
    df_plot["mes"] = df_plot["data"].dt.to_period("M").astype(str)
    resumo_mes = (
        df_plot.groupby(["mes", "tipo"], as_index=False)["valor"]
        .sum()
        .pivot(index="mes", columns="tipo", values="valor")
        .fillna(0)
        .reset_index()
    )

    st.write("**Resumo mensal do histórico**")
    st.bar_chart(resumo_mes.set_index("mes"))

    cat = df_periodo[df_periodo["tipo"] == "saida"].copy()
    if not cat.empty:
        categoria_resumo = cat.groupby("categoria", dropna=False, as_index=False)["valor"].sum()
        categoria_resumo["categoria"] = categoria_resumo["categoria"].fillna("Sem categoria")
        categoria_resumo = categoria_resumo.sort_values("valor", ascending=False)
        total_despesas = categoria_resumo["valor"].sum()
        categoria_resumo["percentual"] = categoria_resumo["valor"].apply(
            lambda x: f"{(x / total_despesas * 100):.1f}%" if total_despesas > 0 else "0,0%"
        )
        categoria_resumo["valor"] = categoria_resumo["valor"].map(format_brl)
        st.write("**Despesas por categoria no período**")
        st.dataframe(categoria_resumo, use_container_width=True, hide_index=True)

        top_descricoes = (
            cat.groupby("descricao", as_index=False)["valor"].sum()
            .sort_values("valor", ascending=False)
            .head(10)
        )
        top_descricoes["valor"] = top_descricoes["valor"].map(format_brl)
        st.write("**Top despesas do período por descrição**")
        st.dataframe(top_descricoes, use_container_width=True, hide_index=True)

    qualidade1, qualidade2, qualidade3 = st.columns(3)
    pendentes = int((df_periodo["status"] == "pendente").sum())
    revisadas = int((df_periodo["status"] == "revisado").sum())
    total = len(df_periodo)
    percentual_revisado = (revisadas / total * 100) if total > 0 else 0
    qualidade1.metric("Pendentes no período", pendentes)
    qualidade2.metric("Revisadas no período", revisadas)
    qualidade3.metric("% revisado", f"{percentual_revisado:.1f}%")


def page_import_ofx() -> None:
    st.subheader("Importar OFX")
    uploaded = st.file_uploader("Selecione o arquivo OFX", type=["ofx"])

    if uploaded is None:
        st.caption("Ao importar, o sistema ignora automaticamente transações já existentes pelo FITID.")
        return

    try:
        content = uploaded.getvalue().decode("utf-8", errors="ignore")
        transactions = parse_ofx(content)
    except Exception as exc:
        st.error(f"Erro ao ler o OFX: {exc}")
        return

    preview_df = pd.DataFrame(transactions)
    preview_df["valor"] = preview_df["valor"].map(format_brl)

    st.write("**Prévia da importação**")
    st.dataframe(preview_df, use_container_width=True, hide_index=True)

    if st.button("Confirmar importação", type="primary"):
        inserted, duplicates = insert_transactions(transactions, uploaded.name)
        st.success(
            f"Importação concluída. Novas transações: {inserted}. Duplicadas ignoradas: {duplicates}."
        )


def page_manual_entry() -> None:
    st.subheader("Lançamento manual")

    with st.form("manual_entry_form"):
        col1, col2 = st.columns(2)
        data = col1.date_input("Data", value=datetime.today())
        tipo = col2.selectbox("Tipo", options=["saida", "entrada"])

        col3, col4 = st.columns(2)
        valor = col3.number_input("Valor", min_value=0.01, step=0.01, format="%.2f")
        categoria = col4.selectbox(
            "Categoria",
            options=[
                "",
                "Alimentação",
                "Transporte",
                "Moradia",
                "Assinaturas",
                "Lazer",
                "Saúde",
                "Educação",
                "Compras",
                "Outros",
            ],
        )

        descricao = st.text_input("Descrição")
        subcategoria = st.text_input("Subcategoria")
        observacao = st.text_area("Observação")

        submitted = st.form_submit_button("Salvar lançamento")

        if submitted:
            if not descricao.strip():
                st.error("Descrição é obrigatória.")
                return

            add_manual_transaction(
                data=str(data),
                valor=float(valor),
                tipo=tipo,
                descricao=descricao.strip(),
                categoria=categoria or None,
                subcategoria=subcategoria.strip() or None,
                observacao=observacao.strip() or None,
            )
            st.success("Lançamento manual salvo com sucesso.")


def update_transaction(transaction_id: int, categoria: str | None, subcategoria: str | None, status: str) -> None:
    with get_connection() as conn:
        conn.execute(
            """
            UPDATE transacoes
            SET categoria = ?, subcategoria = ?, status = ?
            WHERE id = ?
            """,
            (categoria, subcategoria, status, transaction_id),
        )


def delete_transaction(transaction_id: int) -> None:
    with get_connection() as conn:
        conn.execute("DELETE FROM transacoes WHERE id = ?", (transaction_id,))


def page_review() -> None:
    st.subheader("Revisão de transações")
    df = load_transactions()

    if df.empty:
        st.info("Nenhuma transação cadastrada ainda.")
        return

    col1, col2 = st.columns(2)
    status_filter = col1.selectbox("Status", ["todos", "pendente", "revisado"], key="review_status")
    origem_filter = col2.selectbox("Origem", ["todas", "ofx", "manual"], key="review_origin")

    filtered = df.copy()
    if status_filter != "todos":
        filtered = filtered[filtered["status"] == status_filter]
    if origem_filter != "todas":
        filtered = filtered[filtered["origem"] == origem_filter]

    if filtered.empty:
        st.info("Nenhuma transação encontrada com os filtros selecionados.")
        return

    categorias_opcoes = [
        "",
        "Alimentação",
        "Transporte",
        "Moradia",
        "Assinaturas",
        "Lazer",
        "Saúde",
        "Educação",
        "Compras",
        "Outros",
    ]

    for _, row in filtered.iterrows():
        with st.container(border=True):
            c1, c2, c3, c4 = st.columns([1.2, 3.5, 1.2, 1.2])
            c1.write(f"**Data:** {row['data'].strftime('%d/%m/%Y')}")
            c2.write(f"**Descrição:** {row['descricao']}")
            c3.write(f"**Tipo:** {row['tipo']}")
            c4.write(f"**Valor:** {format_brl(float(row['valor']))}")

            c5, c6, c7 = st.columns([2, 2, 2])
            categoria_atual = row["categoria"] if pd.notna(row["categoria"]) else ""
            subcategoria_atual = row["subcategoria"] if pd.notna(row["subcategoria"]) else ""
            status_atual = row["status"] if pd.notna(row["status"]) else "pendente"

            categoria = c5.selectbox(
                f"Categoria #{row['id']}",
                options=categorias_opcoes,
                index=categorias_opcoes.index(categoria_atual) if categoria_atual in categorias_opcoes else 0,
                key=f"cat_{row['id']}",
            )
            subcategoria = c6.text_input(
                f"Subcategoria #{row['id']}",
                value=subcategoria_atual,
                key=f"subcat_{row['id']}",
            )
            status = c7.selectbox(
                f"Status #{row['id']}",
                options=["pendente", "revisado"],
                index=0 if status_atual == "pendente" else 1,
                key=f"status_{row['id']}",
            )

            c8, c9, c10 = st.columns([1.4, 1.4, 4])
            if c8.button("Salvar", key=f"save_{row['id']}"):
                update_transaction(
                    transaction_id=int(row["id"]),
                    categoria=categoria or None,
                    subcategoria=subcategoria.strip() or None,
                    status=status,
                )
                st.success(f"Transação {row['id']} atualizada.")
                st.rerun()

            if c9.button("Excluir", key=f"delete_{row['id']}"):
                delete_transaction(int(row["id"]))
                st.warning(f"Transação {row['id']} excluída.")
                st.rerun()

            c10.caption(
                f"Origem: {row['origem']} | Status atual: {row['status']} | Arquivo: {row['arquivo_origem'] or '-'}"
            )


def page_transactions(df: pd.DataFrame, start_date, end_date) -> None:
    st.subheader("Transações")
    if df.empty:
        st.info("Nenhuma transação cadastrada ainda.")
        return

    df = apply_period_filter(df, start_date, end_date)
    if df.empty:
        st.info("Nenhuma transação encontrada no período selecionado.")
        return

    filtro_tipo = st.selectbox("Filtrar por tipo", options=["todos", "entrada", "saida"])
    filtro_origem = st.selectbox("Filtrar por origem", options=["todas", "ofx", "manual"])
    filtro_status = st.selectbox("Filtrar por status", options=["todos", "pendente", "revisado"])
    categorias = ["todas"] + sorted([c for c in df["categoria"].dropna().unique().tolist()])
    filtro_categoria = st.selectbox("Filtrar por categoria", options=categorias)

    filtered = df.copy()
    if filtro_tipo != "todos":
        filtered = filtered[filtered["tipo"] == filtro_tipo]
    if filtro_origem != "todas":
        filtered = filtered[filtered["origem"] == filtro_origem]
    if filtro_status != "todos":
        filtered = filtered[filtered["status"] == filtro_status]
    if filtro_categoria != "todas":
        filtered = filtered[filtered["categoria"] == filtro_categoria]

    filtered = filtered.copy()
    filtered["data"] = filtered["data"].dt.strftime("%d/%m/%Y")
    filtered["valor"] = filtered["valor"].map(format_brl)

    st.dataframe(filtered, use_container_width=True, hide_index=True)


def page_import_history() -> None:
    st.subheader("Histórico de importações")
    imports_df = load_imports()
    if imports_df.empty:
        st.info("Nenhuma importação registrada ainda.")
        return
    st.dataframe(imports_df, use_container_width=True, hide_index=True)


def page_budget(df: pd.DataFrame, start_date, end_date) -> None:
    st.subheader("Orçamento")

    categorias_base = [
        "Alimentação",
        "Transporte",
        "Moradia",
        "Assinaturas",
        "Lazer",
        "Saúde",
        "Educação",
        "Compras",
        "Outros",
    ]

    with st.form("budget_form"):
        col1, col2 = st.columns(2)
        categoria = col1.selectbox("Categoria", categorias_base)
        valor_orcado = col2.number_input("Valor orçado", min_value=0.01, step=0.01, format="%.2f")
        submitted = st.form_submit_button("Salvar orçamento")
        if submitted:
            upsert_budget(categoria, float(valor_orcado))
            st.success(f"Orçamento salvo para {categoria}.")
            st.rerun()

    budgets = load_budgets()
    if budgets.empty:
        st.info("Nenhum orçamento cadastrado ainda.")
        return

    st.write("**Orçamentos cadastrados**")
    exibir_budgets = budgets.copy()
    exibir_budgets["valor_orcado"] = exibir_budgets["valor_orcado"].map(format_brl)
    st.dataframe(exibir_budgets, use_container_width=True, hide_index=True)

    st.write("**Remover orçamento**")
    col_del1, col_del2 = st.columns([2, 1])
    categoria_delete = col_del1.selectbox("Categoria para excluir", budgets["categoria"].tolist(), key="budget_delete")
    if col_del2.button("Excluir orçamento"):
        delete_budget(categoria_delete)
        st.warning(f"Orçamento excluído para {categoria_delete}.")
        st.rerun()

    df_periodo = apply_period_filter(df, start_date, end_date)
    despesas_periodo = df_periodo[df_periodo["tipo"] == "saida"].copy()

    realizado = (
        despesas_periodo.groupby("categoria", dropna=False, as_index=False)["valor"]
        .sum()
        .rename(columns={"valor": "valor_realizado"})
    )
    realizado["categoria"] = realizado["categoria"].fillna("Sem categoria")

    comparativo = budgets[["categoria", "valor_orcado"]].merge(
        realizado,
        on="categoria",
        how="left",
    )
    comparativo["valor_realizado"] = comparativo["valor_realizado"].fillna(0.0)
    comparativo["diferenca"] = comparativo["valor_orcado"] - comparativo["valor_realizado"]
    comparativo["percentual_consumido"] = comparativo.apply(
        lambda row: (row["valor_realizado"] / row["valor_orcado"] * 100) if row["valor_orcado"] > 0 else 0,
        axis=1,
    )

    total_orcado = float(comparativo["valor_orcado"].sum())
    total_realizado = float(comparativo["valor_realizado"].sum())
    saldo_orcamento = total_orcado - total_realizado
    percentual_total = (total_realizado / total_orcado * 100) if total_orcado > 0 else 0

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Orçamento total", format_brl(total_orcado))
    c2.metric("Realizado no período", format_brl(total_realizado))
    c3.metric("Saldo do orçamento", format_brl(saldo_orcamento))
    c4.metric("% consumido", f"{percentual_total:.1f}%")

    comparativo_exibir = comparativo.copy()
    comparativo_exibir["status_alerta"] = comparativo_exibir["percentual_consumido"].apply(
        lambda x: "Estourado" if x > 100 else ("Atenção" if x >= 80 else "Ok")
    )
    comparativo_exibir["valor_orcado"] = comparativo_exibir["valor_orcado"].map(format_brl)
    comparativo_exibir["valor_realizado"] = comparativo_exibir["valor_realizado"].map(format_brl)
    comparativo_exibir["diferenca"] = comparativo_exibir["diferenca"].map(format_brl)
    comparativo_exibir["percentual_consumido"] = comparativo_exibir["percentual_consumido"].apply(lambda x: f"{x:.1f}%")

    st.write("**Comparativo orçado x realizado no período**")
    st.dataframe(comparativo_exibir, use_container_width=True, hide_index=True)

    estourados = comparativo[comparativo["percentual_consumido"] > 100].copy()
    atencao = comparativo[(comparativo["percentual_consumido"] >= 80) & (comparativo["percentual_consumido"] <= 100)].copy()

    st.write("**Alertas**")
    if estourados.empty and atencao.empty:
        st.success("Nenhuma categoria em alerta no período selecionado.")
    else:
        if not estourados.empty:
            for _, row in estourados.iterrows():
                st.error(
                    f"{row['categoria']}: realizado {format_brl(float(row['valor_realizado']))} para orçamento de {format_brl(float(row['valor_orcado']))} ({row['percentual_consumido']:.1f}%)."
                )
        if not atencao.empty:
            for _, row in atencao.iterrows():
                st.warning(
                    f"{row['categoria']}: consumo em {row['percentual_consumido']:.1f}% do orçamento."
                )


def page_patterns(df: pd.DataFrame, start_date, end_date) -> None:
    st.subheader("Padrões e recorrência")

    df_periodo = apply_period_filter(df, start_date, end_date)
    analysis = build_recurring_analysis(df_periodo)

    if analysis.empty:
        st.info("Não há despesas suficientes no período selecionado para analisar padrões.")
        return

    recorrentes = analysis[analysis["classificacao"] == "Recorrente"].copy()
    frequentes = analysis[analysis["classificacao"] == "Frequente"].copy()
    pontuais = analysis[analysis["classificacao"] == "Pontual"].copy()

    custo_recorrente_mensal = float(recorrentes["custo_mensal_estimado"].sum()) if not recorrentes.empty else 0.0
    custo_recorrente_anual = float(recorrentes["custo_anual_estimado"].sum()) if not recorrentes.empty else 0.0
    qtd_recorrentes = int(len(recorrentes))
    qtd_frequentes = int(len(frequentes))

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Itens recorrentes", qtd_recorrentes)
    c2.metric("Itens frequentes", qtd_frequentes)
    c3.metric("Custo recorrente mensal estimado", format_brl(custo_recorrente_mensal))
    c4.metric("Custo recorrente anual estimado", format_brl(custo_recorrente_anual))

    st.write("**Resumo por classificação**")
    resumo_classificacao = (
        analysis.groupby("classificacao", as_index=False)
        .agg(
            itens=("descricao", "count"),
            total_gasto=("total_gasto", "sum"),
            custo_mensal_estimado=("custo_mensal_estimado", "sum"),
        )
        .sort_values("custo_mensal_estimado", ascending=False)
    )
    resumo_classificacao["total_gasto"] = resumo_classificacao["total_gasto"].map(format_brl)
    resumo_classificacao["custo_mensal_estimado"] = resumo_classificacao["custo_mensal_estimado"].map(format_brl)
    st.dataframe(resumo_classificacao, use_container_width=True, hide_index=True)

    st.write("**Recorrências detectadas**")
    if recorrentes.empty:
        st.info("Nenhum item recorrente detectado no período selecionado.")
    else:
        recorrentes_exibir = recorrentes.copy()
        recorrentes_exibir["total_gasto"] = recorrentes_exibir["total_gasto"].map(format_brl)
        recorrentes_exibir["media_valor"] = recorrentes_exibir["media_valor"].map(format_brl)
        recorrentes_exibir["custo_mensal_estimado"] = recorrentes_exibir["custo_mensal_estimado"].map(format_brl)
        recorrentes_exibir["custo_anual_estimado"] = recorrentes_exibir["custo_anual_estimado"].map(format_brl)
        recorrentes_exibir["primeira_data"] = pd.to_datetime(recorrentes_exibir["primeira_data"]).dt.strftime("%d/%m/%Y")
        recorrentes_exibir["ultima_data"] = pd.to_datetime(recorrentes_exibir["ultima_data"]).dt.strftime("%d/%m/%Y")
        st.dataframe(recorrentes_exibir, use_container_width=True, hide_index=True)

    st.write("**Gastos frequentes**")
    if frequentes.empty:
        st.info("Nenhum item frequente detectado no período selecionado.")
    else:
        frequentes_exibir = frequentes.copy()
        frequentes_exibir["total_gasto"] = frequentes_exibir["total_gasto"].map(format_brl)
        frequentes_exibir["media_valor"] = frequentes_exibir["media_valor"].map(format_brl)
        frequentes_exibir["custo_mensal_estimado"] = frequentes_exibir["custo_mensal_estimado"].map(format_brl)
        frequentes_exibir["custo_anual_estimado"] = frequentes_exibir["custo_anual_estimado"].map(format_brl)
        frequentes_exibir["primeira_data"] = pd.to_datetime(frequentes_exibir["primeira_data"]).dt.strftime("%d/%m/%Y")
        frequentes_exibir["ultima_data"] = pd.to_datetime(frequentes_exibir["ultima_data"]).dt.strftime("%d/%m/%Y")
        st.dataframe(frequentes_exibir, use_container_width=True, hide_index=True)

    top_impacto = analysis.sort_values("custo_mensal_estimado", ascending=False).head(10).copy()
    top_impacto["custo_mensal_estimado"] = top_impacto["custo_mensal_estimado"].map(format_brl)
    top_impacto["total_gasto"] = top_impacto["total_gasto"].map(format_brl)
    st.write("**Top 10 impactos estimados no mês**")
    st.dataframe(
        top_impacto[["descricao", "categoria", "classificacao", "ocorrencias", "meses_distintos", "custo_mensal_estimado", "total_gasto"]],
        use_container_width=True,
        hide_index=True,
    )


def main() -> None:
    st.set_page_config(page_title="Controle Financeiro Pessoal", layout="wide")
    init_db()

    st.title("Controle Financeiro Pessoal")
    st.caption("MVP com importação OFX, persistência em SQLite e lançamentos manuais.")

    menu = st.sidebar.radio(
        "Navegação",
        [
            "Dashboard",
            "Importar OFX",
            "Lançamento manual",
            "Revisão",
            "Transações",
            "Orçamento",
            "Padrões",
            "Histórico de importações",
        ],
    )

    df = load_transactions()

    st.sidebar.markdown("---")
    st.sidebar.write("**Filtro de período**")

    if df.empty:
        start_date = datetime.today().date()
        end_date = datetime.today().date()
    else:
        min_date = df["data"].min().date()
        max_date = df["data"].max().date()
        start_date = st.sidebar.date_input("Data inicial", value=min_date, min_value=min_date, max_value=max_date)
        end_date = st.sidebar.date_input("Data final", value=max_date, min_value=min_date, max_value=max_date)

        if start_date > end_date:
            st.sidebar.error("A data inicial não pode ser maior que a data final.")
            return

    if menu == "Dashboard":
        page_dashboard(df, start_date, end_date)
    elif menu == "Importar OFX":
        page_import_ofx()
    elif menu == "Lançamento manual":
        page_manual_entry()
    elif menu == "Revisão":
        page_review()
    elif menu == "Transações":
        page_transactions(df, start_date, end_date)
    elif menu == "Orçamento":
        page_budget(df, start_date, end_date)
    elif menu == "Padrões":
        page_patterns(df, start_date, end_date)
    elif menu == "Histórico de importações":
        page_import_history()


if __name__ == "__main__":
    main()
