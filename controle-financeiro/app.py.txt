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
        }

    entradas = df.loc[df["tipo"] == "entrada", "valor"].sum()
    saidas = df.loc[df["tipo"] == "saida", "valor"].sum()
    saldo = entradas - saidas

    return {
        "entradas": float(entradas),
        "saidas": float(saidas),
        "saldo": float(saldo),
        "qtd": int(len(df)),
    }


# =========================
# INTERFACE
# =========================
def page_dashboard(df: pd.DataFrame) -> None:
    st.subheader("Visão geral")
    metrics = build_metrics(df)

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Entradas", format_brl(metrics["entradas"]))
    c2.metric("Saídas", format_brl(metrics["saidas"]))
    c3.metric("Saldo", format_brl(metrics["saldo"]))
    c4.metric("Transações", metrics["qtd"])

    if df.empty:
        st.info("Ainda não há transações salvas.")
        return

    df_plot = df.copy()
    df_plot["mes"] = df_plot["data"].dt.to_period("M").astype(str)
    resumo_mes = (
        df_plot.groupby(["mes", "tipo"], as_index=False)["valor"]
        .sum()
        .pivot(index="mes", columns="tipo", values="valor")
        .fillna(0)
        .reset_index()
    )

    st.write("**Resumo mensal**")
    st.bar_chart(resumo_mes.set_index("mes"))

    cat = df[df["tipo"] == "saida"].copy()
    if not cat.empty:
        categoria_resumo = cat.groupby("categoria", dropna=False, as_index=False)["valor"].sum()
        categoria_resumo["categoria"] = categoria_resumo["categoria"].fillna("Sem categoria")
        categoria_resumo = categoria_resumo.sort_values("valor", ascending=False)
        st.write("**Despesas por categoria**")
        st.dataframe(categoria_resumo, use_container_width=True, hide_index=True)


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


def page_transactions(df: pd.DataFrame) -> None:
    st.subheader("Transações")
    if df.empty:
        st.info("Nenhuma transação cadastrada ainda.")
        return

    filtro_tipo = st.selectbox("Filtrar por tipo", options=["todos", "entrada", "saida"])
    filtro_origem = st.selectbox("Filtrar por origem", options=["todas", "ofx", "manual"])

    filtered = df.copy()
    if filtro_tipo != "todos":
        filtered = filtered[filtered["tipo"] == filtro_tipo]
    if filtro_origem != "todas":
        filtered = filtered[filtered["origem"] == filtro_origem]

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


# =========================
# APP
# =========================
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
            "Transações",
            "Histórico de importações",
        ],
    )

    df = load_transactions()

    if menu == "Dashboard":
        page_dashboard(df)
    elif menu == "Importar OFX":
        page_import_ofx()
    elif menu == "Lançamento manual":
        page_manual_entry()
    elif menu == "Transações":
        page_transactions(df)
    elif menu == "Histórico de importações":
        page_import_history()


if __name__ == "__main__":
    main()
