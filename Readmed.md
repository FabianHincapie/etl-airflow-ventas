# 🚀 ETL con Apache Airflow + Docker

Pipeline de datos automatizado construido con Apache Airflow, Docker y Python. Extrae datos de una API REST, los transforma y los carga en SQLite (o PostgreSQL), orquestado con Airflow.

---

## 📁 Estructura del proyecto

```
ETL_Airflow/
│
├── dags/
│   ├── etl_ventas_dag.py        # DAG principal — API → SQLite
│   └── etl_postgres.py          # DAG alternativo — PostgreSQL → PostgreSQL
│
├── data/
│   └── ventas.db                # Base de datos SQLite generada (se crea automáticamente)
│
├── logs/                        # Logs de Airflow (se crea automáticamente)
│
├── etl_ventas.py                # Script ETL standalone (sin Airflow)
├── consultar.py                 # Script para consultar resultados en SQLite
├── docker-compose.yml           # Configuración de contenedores
├── requirements.txt             # Dependencias para ejecución local
└── README.md                    # Este archivo
```

---

## ⚙️ ¿Qué hace este proyecto?

### DAG `etl_ventas` (principal)
Corre diariamente con 4 pasos:

```
Extract → Transform → Load → Validate
```

| Paso | Descripción |
|---|---|
| **Extract** | Consume datos de `jsonplaceholder.typicode.com` (posts + users) |
| **Transform** | Genera órdenes simuladas con productos, precios, descuentos y ciudades colombianas |
| **Load** | Guarda en SQLite: tabla `pedidos` y resumen `resumen_categoria` |
| **Validate** | Verifica que los datos se cargaron correctamente |

### DAG `etl_postgres` (alternativo)
ETL entre dos bases de datos PostgreSQL (`sales_source` → `sales_dest`). Requiere configuración adicional — ver sección correspondiente.

---

## ✅ Requisitos previos

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) instalado y corriendo
- Python 3.9+ (solo si corres los scripts localmente sin Docker)
- Git

> ⚠️ En Windows: verificar que Docker Desktop esté iniciado antes de correr cualquier comando.

---

## 🚀 Instalación y uso con Docker (recomendado)

### Paso 1 — Clonar el repositorio

```bash
git clone https://github.com/tu-usuario/ETL_Airflow.git
cd ETL_Airflow
```

### Paso 2 — Crear las carpetas necesarias

```bash
# Windows (PowerShell)
mkdir dags, logs, data

# Linux / Mac
mkdir -p dags logs data
```

### Paso 3 — Copiar los DAGs a la carpeta dags

```bash
# Windows
copy etl_ventas_dag.py dags\
copy etl_postgres.py dags\

# Linux / Mac
cp etl_ventas_dag.py dags/
cp etl_postgres.py dags/
```

### Paso 4 — Levantar los contenedores

```bash
docker compose up -d
```

Espera aproximadamente 2 minutos mientras inicializa. Verifica que todo esté corriendo:

```bash
docker ps
```

Debes ver 3 contenedores activos:
```
etl_airflow-postgres-1           → Healthy
etl_airflow-airflow-webserver-1  → Up
etl_airflow-airflow-scheduler-1  → Up
```

### Paso 5 — Abrir Airflow

Abre en el navegador:
```
http://localhost:8081
```

| Campo | Valor |
|---|---|
| Username | `admin` |
| Password | `admin` |

### Paso 6 — Ejecutar el DAG

1. En la interfaz busca el DAG **`etl_ventas`**
2. Actívalo con el toggle ▶️
3. Click en **Trigger DAG** para ejecutarlo manualmente
4. Verifica que los 4 pasos queden en verde ✅

---

## 🐍 Ejecución local sin Docker (opcional)

Si quieres correr el ETL directamente sin Airflow:

### Instalar dependencias

```bash
pip install -r requirements.txt
```

### Correr el ETL

```bash
python etl_ventas.py
```

### Consultar resultados

```bash
python consultar.py
```

Verás algo como:
```
==================================================
  📊 RESULTADOS DEL ETL
==================================================

📦 Total pedidos:     100
💰 Ingresos totales: $38,245.50
🎯 Ticket promedio:  $382.45

📋 PEDIDOS POR ESTADO
completado       67 pedidos
pendiente        20 pedidos
cancelado        13 pedidos

🏷️  VENTAS POR CATEGORÍA
Computadores      25 pedidos   $18,432.00
Periféricos       40 pedidos   $12,100.75
...
```

---

## 🗄️ Conectar DBeaver a la base de datos SQLite

Para explorar los datos con DBeaver:

1. Nueva conexión → selecciona **SQLite**
2. En **Path** apunta al archivo:
   ```
   C:\ruta\a\tu\proyecto\ETL_Airflow\data\ventas.db
   ```
3. Click **Test Connection** → **Finish**

Tablas disponibles:
- `pedidos` — todos los registros del ETL
- `resumen_categoria` — agrupado por categoría de producto

---

## 🐘 DAG etl_postgres — Configuración adicional

Este DAG requiere dos bases de datos PostgreSQL externas en el puerto **5433**.

### Crear las bases de datos

```sql
CREATE DATABASE sales_source;
CREATE DATABASE sales_dest;
```

### Crear la tabla fuente en sales_source

```sql
\c sales_source

CREATE TABLE customers (
    customer_id SERIAL PRIMARY KEY,
    name        VARCHAR(100),
    email       VARCHAR(100),
    city        VARCHAR(50)
);

CREATE TABLE products (
    product_id SERIAL PRIMARY KEY,
    name       VARCHAR(100),
    category   VARCHAR(50),
    price      NUMERIC(10,2)
);

CREATE TABLE orders (
    order_id     SERIAL PRIMARY KEY,
    customer_id  INT REFERENCES customers(customer_id),
    product_id   INT REFERENCES products(product_id),
    quantity     INT,
    discount_pct NUMERIC(5,2),
    order_date   DATE,
    status       VARCHAR(20)
);
```

### Actualizar credenciales en etl_postgres.py

```python
DB_SOURCE = {
    "host":     "host.docker.internal",
    "port":     5433,
    "database": "sales_source",
    "user":     "postgres",
    "password": "tu_password"   # ← cambiar
}

DB_DEST = {
    "host":     "host.docker.internal",
    "port":     5433,
    "database": "sales_dest",
    "user":     "postgres",
    "password": "tu_password"   # ← cambiar
}
```

---

## 🛑 Detener los contenedores

```bash
docker compose down
```

Para eliminar también los volúmenes (borra los datos de Airflow):
```bash
docker compose down -v
```

---

## 🛠️ Solución de problemas comunes

| Error | Causa | Solución |
|---|---|---|
| `docker: command not found` | Docker no está en PATH | Abrir Docker Desktop y reiniciar terminal |
| `localhost:8081` no carga | Puerto ocupado por IIS | Cambiar `8081:8080` en docker-compose.yml |
| DAG no aparece en Airflow | Archivo no está en `/dags` | Copiar el archivo a la carpeta `dags/` |
| `ModuleNotFoundError` en DAG | Librería faltante | Agregarla en `_PIP_ADDITIONAL_REQUIREMENTS` del docker-compose.yml |
| Contenedor `airflow-init` en error | Error de inicialización | Correr `docker compose down -v` y volver a `docker compose up -d` |

---

## 🔒 Archivos a ignorar en Git

Crea un `.gitignore` con:

```
logs/
data/ventas.db
__pycache__/
*.pyc
.env
```

---

## 🧰 Tecnologías utilizadas

- [Apache Airflow 2.8.1](https://airflow.apache.org/)
- [Docker](https://www.docker.com/) + [Docker Compose](https://docs.docker.com/compose/)
- [PostgreSQL 13](https://www.postgresql.org/)
- [Python 3.9+](https://www.python.org/)
- [Pandas](https://pandas.pydata.org/)
- [SQLite](https://www.sqlite.org/) — base de datos ligera incluida en Python
- [SQLAlchemy](https://www.sqlalchemy.org/)