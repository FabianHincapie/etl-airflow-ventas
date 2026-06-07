from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import pandas as pd
import sqlite3
import os
import random

# ─────────────────────────
# FUNCIONES ETL
# ─────────────────────────
def extract(**context):
    print("📥 Extrayendo datos...")

    posts = requests.get("https://jsonplaceholder.typicode.com/posts").json()
    users = requests.get("https://jsonplaceholder.typicode.com/users").json()

    context["ti"].xcom_push(key="posts", value=posts)
    context["ti"].xcom_push(key="users", value=users)
    print(f"✅ Extraídos {len(posts)} pedidos y {len(users)} usuarios")


def transform(**context):
    print("🔄 Transformando datos...")

    posts = context["ti"].xcom_pull(key="posts", task_ids="extract")
    users = context["ti"].xcom_pull(key="users", task_ids="extract")

    random.seed(42)
    productos = ["Laptop", "Mouse", "Teclado", "Monitor", "Audífonos"]
    categorias = {"Laptop": "Computadores", "Mouse": "Periféricos",
                  "Teclado": "Periféricos", "Monitor": "Pantallas", "Audífonos": "Audio"}

    ordenes = []
    for post in posts:
        producto  = random.choice(productos)
        cantidad  = random.randint(1, 5)
        precio    = round(random.uniform(15.0, 1500.0), 2)
        descuento = random.choice([0, 0, 5, 10, 15])
        total     = round(cantidad * precio * (1 - descuento / 100), 2)
        ordenes.append({
            "order_id":        post["id"],
            "user_id":         post["userId"],
            "producto":        producto,
            "categoria":       categorias[producto],
            "cantidad":        cantidad,
            "precio_unitario": precio,
            "descuento_pct":   descuento,
            "total":           total,
            "estado":          random.choice(["completado", "completado", "pendiente", "cancelado"]),
            "ciudad":          random.choice(["Bogotá", "Medellín", "Cali", "Barranquilla"]),
        })

    df_ordenes  = pd.DataFrame(ordenes)
    df_usuarios = pd.DataFrame([{"user_id": u["id"], "nombre": u["name"], "email": u["email"]} for u in users])
    df_final    = df_ordenes.merge(df_usuarios, on="user_id", how="left")

    context["ti"].xcom_push(key="data", value=df_final.to_json())
    print(f"✅ Transformados {len(df_final)} registros | Total: ${df_final['total'].sum():,.2f}")


def load(**context):
    print("📤 Cargando datos...")

    import json
    data = context["ti"].xcom_pull(key="data", task_ids="transform")
    df   = pd.read_json(data)

    os.makedirs("/opt/airflow/data", exist_ok=True)
    conn = sqlite3.connect("/opt/airflow/data/ventas.db")
    df.to_sql("pedidos", conn, if_exists="replace", index=False)

    resumen = df.groupby("categoria").agg(
        total_pedidos=("order_id", "count"),
        ingresos=("total", "sum"),
    ).reset_index()
    resumen.to_sql("resumen_categoria", conn, if_exists="replace", index=False)

    conn.close()
    print("✅ Datos guardados en /opt/airflow/data/ventas.db")


def validate(**context):
    print("🔍 Validando...")

    conn  = sqlite3.connect("/opt/airflow/data/ventas.db")
    total = conn.execute("SELECT COUNT(*) FROM pedidos").fetchone()[0]
    conn.close()

    assert total > 0, "❌ Tabla vacía"
    print(f"✅ Validación exitosa: {total} registros")


# ─────────────────────────
# DEFINICIÓN DEL DAG
# ─────────────────────────
default_args = {
    "owner": "data-team",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

dag = DAG(
    dag_id="etl_ventas",
    default_args=default_args,
    description="ETL de ventas diario",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["etl", "ventas"],
)

t1 = PythonOperator(task_id="extract",   python_callable=extract,   provide_context=True, dag=dag)
t2 = PythonOperator(task_id="transform", python_callable=transform, provide_context=True, dag=dag)
t3 = PythonOperator(task_id="load",      python_callable=load,      provide_context=True, dag=dag)
t4 = PythonOperator(task_id="validate",  python_callable=validate,  provide_context=True, dag=dag)

# Pipeline: extract → transform → load → validate
t1 >> t2 >> t3 >> t4


