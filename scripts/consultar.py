import sqlite3

conn = sqlite3.connect("data/ventas.db")

print("\n" + "="*50)
print("  📊 RESULTADOS DEL ETL")
print("="*50)

# Resumen general
row = conn.execute("""
    SELECT 
        COUNT(*) as pedidos,
        ROUND(SUM(total), 2) as ingresos,
        ROUND(AVG(total), 2) as ticket_promedio
    FROM pedidos
""").fetchone()

print(f"\n📦 Total pedidos:     {row[0]}")
print(f"💰 Ingresos totales: ${row[1]:,.2f}")
print(f"🎯 Ticket promedio:  ${row[2]:,.2f}")

# Por estado
print("\n📋 PEDIDOS POR ESTADO")
print("-"*30)
for r in conn.execute("SELECT estado, COUNT(*) FROM pedidos GROUP BY estado ORDER BY 2 DESC"):
    print(f"  {r[0]:<15} {r[1]} pedidos")

# Por categoría
print("\n🏷️  VENTAS POR CATEGORÍA")
print("-"*50)
for r in conn.execute("SELECT * FROM resumen_categoria ORDER BY ingresos DESC"):
    print(f"  {r[0]:<15} {r[1]:>5} pedidos   ${r[2]:>10,.2f}")

# Por ciudad
print("\n🏙️  PEDIDOS POR CIUDAD")
print("-"*30)
for r in conn.execute("SELECT ciudad, COUNT(*) FROM pedidos GROUP BY ciudad ORDER BY 2 DESC"):
    print(f"  {r[0]:<15} {r[1]} pedidos")

conn.close()
print("\n" + "="*50 + "\n")