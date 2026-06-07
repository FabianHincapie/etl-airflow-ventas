from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import psycopg2
import pandas as pd

# ─────────────────────────
# CONFIGURACIÓN
# ─────────────────────────

DB_SOURCE = {
    "host":     "host.docker.internal",
    "port":     5433,
    "database": "sales_source",
    "user":     "postgres",
    "password": "123456"   # ← cambia esto
}

DB_DEST = {
    "host":     "host.docker.internal",
    "port":     5433,
    "database": "sales_dest",
    "user":     "postgres",
    "password": "123456"   # ← cambia esto
}

# ─────────────────────────
# EXTRACT
# ─────────────────────────
def extract(**context):
    print("📥 Extrayendo datos de sales_source...")

    conn = psycopg2.connect(**DB_SOURCE)

    df = pd.read_sql("""
        SELECT
            o.order_id,
            c.name      AS customer,
            c.email,
            c.city,
            p.name      AS product,
            p.category,
            o.quantity,
            p.price,
            o.discount_pct,
            o.order_date::text,
            o.status
        FROM orders o
        JOIN customers c ON o.customer_id = c.customer_id
        JOIN products  p ON o.product_id  = p.product_id
    """, conn)

    conn.close()

    print(f"✅ Extraídos {len(df)} pedidos")
    context["ti"].xcom_push(key="raw_data", value=df.to_json())

# ─────────────────────────
# TRANSFORM
# ─────────────────────────
def transform(**context):
    print("🔄 Transformando datos...")

    raw = context["ti"].xcom_pull(key="raw_data", task_ids="extract")
    df  = pd.read_json(raw)

    # Calcular total con descuento
    df["subtotal"]       = df["quantity"] * df["price"]
    df["discount_value"] = df["subtotal"] * (df["discount_pct"] / 100)
    df["total"]          = (df["subtotal"] - df["discount_value"]).round(2)

    # Clasificar por valor
    def classify(total):
        if total >= 1000: return "premium"
        if total >= 200:  return "medium"
        return "basic"

    df["segment"]      = df["total"].apply(classify)
    df["order_date"]   = pd.to_datetime(df["order_date"])
    df["month"]        = df["order_date"].dt.month
    df["quarter"]      = df["order_date"].dt.quarter
    df["processed_at"] = pd.Timestamp.now().isoformat()

    print(f"✅ Transformados {len(df)} registros")
    print(f"   Total ventas: ${df['total'].sum():,.2f}")
    context["ti"].xcom_push(key="clean_data", value=df.to_json(date_format="iso"))

# ─────────────────────────
# LOAD
# ─────────────────────────
def load(**context):
    print("📤 Cargando datos en sales_dest...")

    from sqlalchemy import create_engine

    clean = context["ti"].xcom_pull(key="clean_data", task_ids="transform")
    df    = pd.read_json(clean)

    engine = create_engine(
        f"postgresql+psycopg2://{DB_DEST['user']}:{DB_DEST['password']}"
        f"@{DB_DEST['host']}:{DB_DEST['port']}/{DB_DEST['database']}"
    )

    # Tabla principal
    df.to_sql("orders_clean", engine, if_exists="replace", index=False)

    # Resumen por categoría
    summary = df.groupby("category").agg(
        total_orders=("order_id",  "count"),
        revenue=     ("total",     "sum"),
        avg_ticket=  ("total",     "mean")
    ).reset_index()
    summary["revenue"]    = summary["revenue"].round(2)
    summary["avg_ticket"] = summary["avg_ticket"].round(2)
    summary.to_sql("summary_by_category", engine, if_exists="replace", index=False)

    # Resumen por ciudad
    by_city = df.groupby("city").agg(
        total_orders=("order_id", "count"),
        revenue=     ("total",    "sum")
    ).reset_index()
    by_city.to_sql("summary_by_city", engine, if_exists="replace", index=False)

    print("✅ Datos cargados en sales_dest")
    print(f"   Tablas: orders_clean, summary_by_category, summary_by_city")

# ─────────────────────────
# VALIDATE
# ─────────────────────────
def validate(**context):
    print("🔍 Validando datos...")

    conn  = psycopg2.connect(**DB_DEST)
    total = pd.read_sql("SELECT COUNT(*) FROM orders_clean", conn).iloc[0,0]
    conn.close()

    assert total > 0, "❌ La tabla orders_clean está vacía"
    print(f"✅ Validación exitosa: {total} registros en sales_dest")

# ─────────────────────────
# DAG
# ─────────────────────────
default_args = {
    "owner":       "data-team",
    "retries":     1,
    "retry_delay": timedelta(minutes=2),
}

dag = DAG(
    dag_id="etl_postgres",
    default_args=default_args,
    description="ETL: sales_source → sales_dest",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["etl", "postgres"],
)

t1 = PythonOperator(task_id="extract",   python_callable=extract,   provide_context=True, dag=dag)
t2 = PythonOperator(task_id="transform", python_callable=transform, provide_context=True, dag=dag)
t3 = PythonOperator(task_id="load",      python_callable=load,      provide_context=True, dag=dag)
t4 = PythonOperator(task_id="validate",  python_callable=validate,  provide_context=True, dag=dag)

t1 >> t2 >> t3 >> t4
