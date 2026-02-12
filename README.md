# 🪙 Crypto Data Warehouse

Kriptovaluta-piaci adattárház — Docker Compose stack élő CoinGecko adattal.

## Architektúra

```
CoinGecko API ──► Airflow DAG ──► PostgreSQL (OLTP) ──► PostgreSQL (DW) ──► Metabase
                   (Extract)       raw schema            star schema         dashboards
                                   │                     │
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
# 1. CoinGecko API kulcs beállítása
cp .env.example .env   # vagy szerkeszd a meglévő .env fájlt
# Szerkeszd a .env-t: COINGECKO_API_KEY=CG-xxxxxxxxxxxx

# 2. Indítás
docker compose up -d

# 3. Várd meg az inicializálást (~60 mp)
docker compose logs -f airflow-init

# 4. Nyisd meg az Airflow UI-t és engedélyezd a crypto_etl_pipeline DAG-ot
# http://localhost:8080 (admin/admin)

# 5. Metabase beállítása → connect to DW
# http://localhost:3000 → Add database:
#   Host: dw-db | Port: 5432 | DB: crypto_dw | User: dw_user | Pass: dw_pass
```

## Adatbázis séma

### OLTP (raw schema)
- `raw.coins` — Coin metaadatok
- `raw.market_snapshots` — Óránkénti piaci pillanatképek
- `raw.ohlc_daily` — Napi OHLC adatok
- `raw.trending_coins` — Trendező coinok
- `raw.global_market` — Globális piaci adatok

### DW (star schema)
- **Dimenziók**: `dim_coin` (SCD-2), `dim_date`, `dim_time`, `dim_currency`
- **Tények**: `fact_market_snapshot`, `fact_ohlc_daily`, `fact_global_market`, `fact_trending`

### dbt Marts
- `mart_daily_coin_summary` — Napi aggregált coin-metrikák dashboardokhoz

## Hasznos parancsok

```bash
# Logok követése
docker compose logs -f airflow-scheduler

# OLTP-hez csatlakozás
psql -h localhost -p 5435 -U oltp_user -d crypto_oltp

# DW-hez csatlakozás
psql -h localhost -p 5436 -U dw_user -d crypto_dw

# dbt futtatás kézzel (a scheduler containerből)
docker compose exec airflow-scheduler bash -c "cd /opt/dbt && dbt run --profiles-dir ."

# Újraindítás tiszta állapotból
docker compose down -v && docker compose up -d
```

## Továbbfejlesztési lehetőségek

- [ ] OHLC backfill DAG (történeti adatok betöltése)
- [ ] CDC (Change Data Capture) PostgreSQL logical replication-nel
- [ ] Több valuta támogatása (EUR, HUF, BTC)
- [ ] Alerting Airflow-ban (pl. ha BTC >5% esik 1 óra alatt)
- [ ] dbt tesztek és dokumentáció
- [ ] Apache Superset Metabase helyett OLAP-funkcionalitáshoz
