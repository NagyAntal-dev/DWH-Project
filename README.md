<div align="center">

# Kriptovaluta Adattárház

**Teljes körű kriptovaluta elemzési platform**
**CoinGecko API &rarr; Airflow &rarr; PostgreSQL &rarr; dbt &rarr; Metabase**

![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-336791?logo=postgresql&logoColor=white)
![Airflow](https://img.shields.io/badge/Apache_Airflow-2.10-017CEE?logo=apacheairflow&logoColor=white)
![dbt](https://img.shields.io/badge/dbt-1.9-FF694B?logo=dbt&logoColor=white)
![Docker](https://img.shields.io/badge/Docker_Compose-ready-2496ED?logo=docker&logoColor=white)
![License](https://img.shields.io/badge/license-MIT-green)

</div>

---

## Áttekintés

Egy teljesen konténerizált adattárház, amely élő kriptovaluta piaci adatokat tölt be a [CoinGecko API](https://www.coingecko.com/en/api)-ból, normalizált OLTP adatbázisba tölti, dimenzionális csillagséma modellbe transzformálja, és interaktív dashboardokon jeleníti meg Metabase-en keresztül — mindezt Apache Airflow vezényli és dbt modellezi.

### Főbb képességek

- **Többvalutás** — az árakat **USD, EUR, HUF és BTC** pénznemekben követi egyidejűleg
- **Change Data Capture** — PostgreSQL logikai replikáció tükrözi a nyers adatokat az adattárházba közel valós időben
- **SCD Type 2** — a lassan változó dimenziók megőrzik az érme metaadatok teljes előzménytörténetét
- **Automatikus riasztás** — figyeli a jelentős árcsökkenéseket (konfigurálható küszöbérték) és naplóz/e-mailt küld
- **Történelmi visszatöltés** — egykattintásos DAG akár 365 napos OHLC gyertya-előzmény betöltéséhez
- **Adatminőség** — beépített dbt tesztek biztosítják a hivatkozási integritást, értéktartományokat és sorszám-elvárásokat

---

## Architektúra

```
                          ┌──────────────────────────────────────────────────────────┐
                          │                   Docker Compose Stack                   │
                          │                                                          │
  CoinGecko API           │  ┌─────────────┐    ┌──────────────┐    ┌────────────┐  │
  (markets, trending,     │  │   Airflow    │    │  PostgreSQL  │    │ PostgreSQL │  │
   global, OHLC)  ───────►│  │  Scheduler   │──► │    (OLTP)    │──► │    (DW)    │  │
                          │  │  + Webserver  │    │  raw séma    │CDC │ csillagséma│  │
                          │  └──────┬───────┘    └──────────────┘    └─────┬──────┘  │
                          │         │                                      │         │
                          │         │  dbt run / dbt test                  │         │
                          │         └──────────────────────────────────────┘         │
                          │                                                          │
                          │  ┌────────────┐    ┌─────────┐                           │
                          │  │  Metabase   │◄───│  Redis  │  (Airflow broker)        │
                          │  │ Dashboardok │    └─────────┘                           │
                          │  └────────────┘                                           │
                          └──────────────────────────────────────────────────────────┘
```

---

## Szolgáltatások

| Szolgáltatás    | Port   | URL / Végpont                 | Leírás                                |
|-----------------|--------|-------------------------------|---------------------------------------|
| Airflow Web     | `8080` | http://localhost:8080         | DAG kezelő felület (admin / admin)    |
| Metabase        | `3000` | http://localhost:3000         | BI dashboardok                        |
| OLTP PostgreSQL | `5435` | `localhost:5435`              | Nyers CoinGecko adatok                |
| DW PostgreSQL   | `5436` | `localhost:5436`              | Dimenzionális csillagséma             |
| Redis           | `6379` | `localhost:6379`              | Airflow Celery broker / gyorsítótár   |

---

## Gyors Indítás

### Előfeltételek

- [Docker](https://docs.docker.com/get-docker/) & [Docker Compose](https://docs.docker.com/compose/install/) v2+
- *(opcionális)* Ingyenes CoinGecko Demo API kulcs — [itt szerezhető be](https://www.coingecko.com/en/api)

### 1 — Környezet konfigurálása

```bash
cp .env.example .env
# Szerkeszd a .env fájlt és add meg a CoinGecko API kulcsodat (opcionális, de ajánlott)
```

### 2 — Build & indítás

```bash
docker compose up -d --build
```

### 3 — Várakozás az inicializálásra (~90 mp)

```bash
docker compose logs -f airflow-init
# Várd meg, amíg megjelenik az "Airflow init complete" üzenet
```

### 4 — Az ETL pipeline engedélyezése

Nyisd meg a http://localhost:8080 oldalt, lépj be az **admin / admin** fiókkal, és kapcsold be a **`crypto_etl_pipeline`** DAG-ot.

### 5 — Metabase csatlakoztatása az adattárházhoz

Nyisd meg a http://localhost:3000 oldalt és adj hozzá egy PostgreSQL adatbázist:

| Mező     | Érték      |
|----------|------------|
| Host     | `dw-db`    |
| Port     | `5432`     |
| Database | `crypto_dw`|
| User     | `dw_user`  |
| Password | `dw_pass`  |

---

## Projekt Struktúra

```
.
├── docker-compose.yml              # 7 szolgáltatásos stack definíció
├── Dockerfile                      # Egyedi Airflow + dbt image
├── requirements.txt                # Python függőségek (dbt, requests, psycopg2)
├── .env / .env.example             # Környezeti változók
│
├── init-scripts/
│   ├── oltp/
│   │   ├── 01_create_databases.sql # Airflow metaadat DB létrehozás
│   │   ├── 02_schema.sql           # OLTP raw séma (5 tábla)
│   │   └── 03_cdc_setup.sql        # CDC publikáció beállítás
│   └── dw/
│       ├── 01_schema.sql           # Csillagséma + seed adatok
│       └── 02_cdc_subscription.sql # CDC tükör séma
│
├── dags/
│   ├── crypto_etl_pipeline.py      # Fő ETL DAG (ütemezett)
│   ├── ohlc_backfill_dag.py        # Történelmi OHLC betöltő (manuális)
│   ├── cdc_setup_dag.py            # CDC replikáció beállítás (manuális)
│   ├── crypto_alerting_dag.py      # Árcsökkenés riasztás (ütemezett)
│   └── scripts/
│       └── extract_coingecko.py    # API kliens újrapróbálkozással & rate-limittel
│
└── dbt/
    ├── dbt_project.yml
    ├── profiles.yml
    ├── models/
    │   ├── staging/                 # Könnyűsúlyú nézetek a nyers táblákon
    │   └── marts/                   # Aggregált elemzési táblák
    └── tests/                       # Egyedi adatminőségi ellenőrzések
```

---

## Adatmodell

### OLTP — Nyers Réteg (`raw` séma)

| Tábla                  | Leírás                                             |
|------------------------|----------------------------------------------------|
| `raw.coins`            | Érme metaadatok (id, szimbólum, név, rang, kép)    |
| `raw.market_snapshots` | Kétóránkénti ár-pillanatképek (többvalutás)        |
| `raw.ohlc_daily`       | Napi OHLC gyertyaadatok (többvalutás)              |
| `raw.trending_coins`   | CoinGecko felkapott érmék pillanatképei            |
| `raw.global_market`    | Aggregált piaci statisztikák (teljes kapitalizáció, forgalom stb.) |

### DW — Csillagséma

**Dimenziók**

| Tábla           | Megjegyzések                                                 |
|-----------------|--------------------------------------------------------------|
| `dim_coin`      | SCD Type 2 — az érme attribútum-változások teljes előzménye  |
| `dim_date`      | Előre feltöltött naptár (2020 – 2030)                        |
| `dim_time`      | Óra szintű felbontás (0 – 23)                                |
| `dim_currency`  | USD, EUR, HUF, BTC                                           |

**Tény táblák**

| Tábla                   | Granularitás                  | Főbb mértékek                            |
|-------------------------|-------------------------------|------------------------------------------|
| `fact_market_snapshot`  | érme × dátum × óra × valuta  | ár, piaci kap., forgalom, 24 órás változás |
| `fact_ohlc_daily`       | érme × dátum × valuta        | nyitó, maximum, minimum, záró            |
| `fact_global_market`    | dátum × óra                   | teljes kap., forgalom, BTC/ETH dominancia |
| `fact_trending`         | érme × dátum × óra            | felkapottsági rang, BTC ár               |

**CDC Tükör** — `cdc_raw.*` táblák PostgreSQL logikai replikációval másolva.

### dbt Mart Modellek

| Modell                       | Leírás                                           |
|------------------------------|--------------------------------------------------|
| `mart_daily_coin_summary`    | Napi aggregált ár & forgalom érmékre lebontva    |
| `mart_global_market_daily`   | Napi globális piaci áttekintés                   |
| `mart_trending_history`      | Történelmi felkapott érmék rangsorolása          |

---

## Airflow DAG-ok

| DAG                      | Ütemezés       | Leírás                                                      |
|--------------------------|----------------|--------------------------------------------------------------|
| `crypto_etl_pipeline`    | `0 */2 * * *`  | Teljes ETL: Kinyerés → OLTP → DW → dbt run → dbt test      |
| `crypto_alerting`        | `*/30 * * * *` | Árcsökkenés figyelés (alapért.: BTC > 5% 1 órán belül)      |
| `ohlc_backfill`          | Manuális       | Akár 365 napos történelmi OHLC adat visszatöltése           |
| `cdc_setup`              | Manuális       | Logikai replikációs előfizetés létrehozása (egyszeri futtatás) |

### ETL Pipeline Folyamat

```
extract_markets ──┐
extract_trending ─┤
extract_global ───┤──► load_to_oltp ──► load_to_dw ──► dbt_run ──► dbt_test
extract_ohlc ─────┘
```

1. **Kinyerés** — párhuzamos CoinGecko API hívások (markets, trending, global, OHLC) 4 valután keresztül
2. **OLTP betöltés** — nyers adatok upsert-je az OLTP adatbázisba
3. **DW betöltés** — dimenziók feltöltése (SCD-2 az érmékhez) és ténytáblák
4. **dbt Run** — staging nézetek és mart táblák materializálása
5. **dbt Test** — automatikus adatminőségi ellenőrzések futtatása

---

## Hasznos Parancsok

```bash
# Ütemező logok követése
docker compose logs -f airflow-scheduler

# Csatlakozás az OLTP adatbázishoz
docker compose exec oltp-db psql -U oltp_user -d crypto_oltp

# Csatlakozás a DW adatbázishoz
docker compose exec dw-db psql -U dw_user -d crypto_dw

# dbt manuális futtatása
docker compose exec airflow-scheduler bash -c "cd /opt/dbt && dbt run --profiles-dir ."

# dbt tesztek futtatása
docker compose exec airflow-scheduler bash -c "cd /opt/dbt && dbt test --profiles-dir ."

# Összes konténer állapotának ellenőrzése
docker compose ps

# Teljes leállítás (kötetekkel együtt) és újraépítés
docker compose down -v && docker compose up -d --build
```

---

## Technológiai Stack

| Réteg            | Technológia                       |
|------------------|-----------------------------------|
| Vezénylés        | Apache Airflow 2.10               |
| Adatforrás       | CoinGecko REST API (ingyenes szint)|
| OLTP Adatbázis   | PostgreSQL 16                     |
| Adattárház       | PostgreSQL 16 (csillagséma)       |
| Transzformáció   | dbt-core 1.9 + dbt-postgres      |
| CDC              | PostgreSQL logikai replikáció     |
| BI / Dashboardok | Metabase                          |
| Konténerizáció   | Docker Compose                    |
| Broker / Cache   | Redis 7                           |
