import requests
import pandas as pd
import sqlite3
import os
from datetime import datetime

# ─────────────────────────
# PASO 1: EXTRACT
# ─────────────────────────
def extract():
    print("📥 Extrayendo datos...")

    # Extraer pedidos
    response = requests.get("https://jsonplaceholder.typicode.com/posts")
    posts = response.json()

    # Extraer usuarios
    response2 = requests.get("https://jsonplaceholder.typicode.com/users")
    users = response2.json()

    print(f"✅ Extraídos {len(posts)} pedidos y {len(users)} usuarios")
    return posts, users

# ─────────────────────────
# PASO 2: TRANSFORM
# ─────────────────────────
def transform(posts, users):
    print("🔄 Transformando datos...")

    import random
    random.seed(42)

    productos = ["Laptop", "Mouse", "Teclado", "Monitor", "Audífonos"]
    categorias = {"Laptop": "Computadores", "Mouse": "Periféricos",
                  "Teclado": "Periféricos", "Monitor": "Pantallas", "Audífonos": "Audio"}

    # Construir tabla de pedidos
    ordenes = []
    for post in posts:
        producto = random.choice(productos)
        cantidad = random.randint(1, 5)
        precio   = round(random.uniform(15.0, 1500.0), 2)
        descuento = random.choice([0, 0, 5, 10, 15])
        subtotal  = cantidad * precio
        total     = round(subtotal * (1 - descuento / 100), 2)

        ordenes.append({
            "order_id":       post["id"],
            "user_id":        post["userId"],
            "producto":       producto,
            "categoria":      categorias[producto],
            "cantidad":       cantidad,
            "precio_unitario": precio,
            "descuento_pct":  descuento,
            "total":          total,
            "estado":         random.choice(["completado", "completado", "pendiente", "cancelado"]),
            "ciudad":         random.choice(["Bogotá", "Medellín", "Cali", "Barranquilla"]),
        })

    df_ordenes = pd.DataFrame(ordenes)

    # Construir tabla de usuarios
    df_usuarios = pd.DataFrame([{
        "user_id": u["id"],
        "nombre":  u["name"],
        "email":   u["email"],
    } for u in users])

    # Unir pedidos con usuarios
    df_final = df_ordenes.merge(df_usuarios, on="user_id", how="left")
    df_final["processed_at"] = datetime.now().isoformat()

    print(f"✅ Transformados {len(df_final)} registros")
    print(f"   Total ventas: ${df_final['total'].sum():,.2f}")
    return df_final

# ─────────────────────────
# PASO 3: LOAD
# ─────────────────────────
def load(df):
    print("📤 Cargando datos en la base de datos...")

    os.makedirs("data", exist_ok=True)
    conn = sqlite3.connect("data/ventas.db")

    # Tabla principal
    df.to_sql("pedidos", conn, if_exists="replace", index=False)

    # Resumen por categoría
    resumen = df.groupby("categoria").agg(
        total_pedidos=("order_id", "count"),
        ingresos=("total", "sum"),
        ticket_promedio=("total", "mean")
    ).reset_index()
    resumen.to_sql("resumen_categoria", conn, if_exists="replace", index=False)

    conn.close()
    print("✅ Datos guardados en data/ventas.db")

# ─────────────────────────
# PASO 4: VALIDATE
# ─────────────────────────
def validate():
    print("🔍 Validando datos...")

    conn = sqlite3.connect("data/ventas.db")

    total = conn.execute("SELECT COUNT(*) FROM pedidos").fetchone()[0]
    negativos = conn.execute("SELECT COUNT(*) FROM pedidos WHERE total < 0").fetchone()[0]

    assert total > 0,     "❌ La tabla de pedidos está vacía"
    assert negativos == 0, "❌ Hay pedidos con total negativo"

    conn.close()
    print(f"✅ Validación exitosa: {total} registros, sin errores")

# ─────────────────────────
# EJECUTAR ETL
# ─────────────────────────
if __name__ == "__main__":
    print("\n🚀 Iniciando ETL...\n")
    posts, users = extract()
    df = transform(posts, users)
    load(df)
    validate()
    print("\n🎉 ETL completado exitosamente!\n")