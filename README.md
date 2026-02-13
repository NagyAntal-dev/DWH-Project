# 🪙 Crypto Data Warehouse

Kriptovaluta-piaci adattárház — Docker Compose stack élő CoinGecko adattal.

## Architektúra

```
CoinGecko API ──► Airflow DAG ──► PostgreSQL (OLTP) ──► PostgreSQL (DW) ──► Metabase
                   (Extract)       raw schema            star schema         dashboards
                                   │         CDC         │
                                   ├── logical repl. ────┤
                                   └── dbt transforms ───┘
```

## Komponensek

| Szolgáltatás    | Port  | URL                        | Leírás                        |
|-----------------|-------|----------------------------|-------------------------------|
| Airflow Web     | 8080  | http://localhost:8080      | DAG kezelés (admin/admin)     |
| Metabase        | 3000  | http://localhost:3000      | Dashboard / BI                |
| OLTP PostgreSQL | 5435  | `localhost:5435`           | Raw CoinGecko adatok          |
| DW PostgreSQL   | 5436  | `localhost:5436`           | Csillagséma (star schema)     |
| Redis           | 6379  | `localhost:6379`           | Cache / Airflow support       |

## Gyorsindítás

```bash
# 1. Környezeti változók beállítása
cp .env.example .env
# Opcionális: szerkeszd a .env-t és add meg a CoinGecko Demo API kulcsot
# (ingyenes, de ajánlott): https://www.coingecko.com/en/api

# 2. Indítás
docker compose up -d --build

# 3. Várd meg az inicializálást (~90 mp)
docker compose logs -f airflow-init

# 4. Nyisd meg az Airflow UI-t és engedélyezd a crypto_etl_pipeline DAG-ot
# http://localhost:8080 (admin/admin)

# 5. Metabase beállítása → connect to DW
# http://localhost:3000 → Add database:
#   Host: dw-db | Port: 5432 | DB: crypto_dw | User: dw_user | Pass: dw_pass
```

## Projektstruktúra

```
├── docker-compose.yml          # 7 szolgáltatás definíciója
├── Dockerfile                  # Airflow + dbt egyedi image
├── requirements.txt            # Python függőségek
├── .env / .env.example         # Környezeti változók
├── init-scripts/
│   ├── oltp/
│   │   ├── 01_create_databases.sql
│   │   ├── 02_schema.sql       # OLTP séma inicializálás
│   │   └── 03_cdc_setup.sql    # CDC publication
│   └── dw/
│       ├── 01_schema.sql       # DW csillagséma + dim_currency (USD/EUR/HUF/BTC)
│       └── 02_cdc_subscription.sql  # CDC mirror schema
├── dags/
│   ├── crypto_etl_pipeline.py  # Fő ETL DAG (multi-currency)
│   ├── ohlc_backfill_dag.py    # OHLC backfill DAG (manuális)
│   ├── cdc_setup_dag.py        # CDC subscription setup (manuális)
│   ├── crypto_alerting_dag.py  # Áresés alerting DAG
│   └── scripts/
│       └── extract_coingecko.py
└── dbt/
    ├── dbt_project.yml
    ├── profiles.yml
    ├── models/
    │   ├── staging/            # Staging nézetek + tesztek
    │   └── marts/              # Aggregált mart táblák + tesztek
    └── tests/                  # Egyedi dbt tesztek
```

## Adatbázis séma

### OLTP (raw schema)
- `raw.coins` — Coin metaadatok
- `raw.market_snapshots` — Óránkénti piaci pillanatképek (multi-currency)
- `raw.ohlc_daily` — Napi OHLC adatok (multi-currency)
- `raw.trending_coins` — Trendező coinok
- `raw.global_market` — Globális piaci adatok

### DW (star schema)
- **Dimenziók**: `dim_coin` (SCD-2), `dim_date`, `dim_time`, `dim_currency` (USD/EUR/HUF/BTC)
- **Tények**: `fact_market_snapshot`, `fact_ohlc_daily`, `fact_global_market`, `fact_trending`
- **CDC**: `cdc_raw.*` — Logikai replikációs mirror táblák

### dbt Marts
- `mart_daily_coin_summary` — Napi aggregált coin-metrikák dashboardokhoz
- `mart_global_market_daily` — Napi globális piaci összefoglaló
- `mart_trending_history` — Trendező coinok története

## Airflow DAG-ok

| DAG                    | Ütemezés       | Leírás                                         |
|------------------------|----------------|-------------------------------------------------|
| `crypto_etl_pipeline`  | `0 */2 * * *`  | Fő ETL: CoinGecko → OLTP → DW → dbt (4 valuta)|
| `ohlc_backfill`        | Manuális        | Történeti OHLC adat betöltés (365 nap)          |
| `cdc_setup`            | Manuális        | CDC logikai replikáció beállítása                |
| `crypto_alerting`      | `*/30 * * * *` | Áresés monitoring (BTC >5% /1h)                 |

## ETL Pipeline

A `crypto_etl_pipeline` DAG **2 óránként** fut és a következő lépéseket hajtja végre:

1. **Extract** — CoinGecko API hívások (markets, trending, global, OHLC) — **4 valutában**
2. **Load OLTP** — Nyers adatok betöltése az OLTP adatbázisba
3. **Load DW** — Dimenziók és tény táblák feltöltése (SCD-2)
4. **dbt Run** — Mart táblák létrehozása/frissítése
5. **dbt Test** — Automatikus adatminőségi tesztek

## Hasznos parancsok

```bash
# Logok követése
docker compose logs -f airflow-scheduler

# OLTP-hez csatlakozás
docker compose exec oltp-db psql -U oltp_user -d crypto_oltp

# DW-hez csatlakozás
docker compose exec dw-db psql -U dw_user -d crypto_dw

# dbt futtatás kézzel (a scheduler containerből)
docker compose exec airflow-scheduler bash -c "cd /opt/dbt && dbt run --profiles-dir ."

# dbt tesztek futtatása
docker compose exec airflow-scheduler bash -c "cd /opt/dbt && dbt test --profiles-dir ."

# Összes konténer állapota
docker compose ps

# Újraindítás tiszta állapotból
docker compose down -v && docker compose up -d --build
```

## Továbbfejlesztési lehetőségek

- [x] OHLC backfill DAG (történeti adatok betöltése)
- [x] CDC (Change Data Capture) PostgreSQL logical replication-nel
- [x] Több valuta támogatása (EUR, HUF, BTC)
- [x] Alerting Airflow-ban (pl. ha BTC >5% esik 1 óra alatt)
- [x] dbt tesztek és dokumentáció
- [ ] Apache Superset Metabase helyett OLAP-funkcionalitáshoz
