# ⚡ EV Charging Intelligence Platform — Bangalore Edition (4-Project Portfolio)

**One theme, four roles, one resume story:**
*"I designed and built an end-to-end EV Charging Intelligence Platform for Bangalore — spanning Data Engineering, Data Science, ML Engineering, and Applied AI — using 100% open-source and free-tier tools."*

Scoping everything to **Bangalore** (12.83°N–13.14°N, 77.35°E–77.75°E roughly — Electronic City to Yelahanka, Whitefield to Nelamangala) makes the projects sharper and more defensible: smaller, richer geospatial story ("charging deserts in HSR vs Whitefield"), locally relevant regulatory/tariff data (BESCOM, Karnataka EV Policy), and lighter infra (a single-city OSM extract instead of a planet file).

The 4 projects stay connected but independently demoable, in the same order as before — DE feeds gold data → DS mines it → ML productionizes a model → AI wraps it in an agent.

---

## 🌍 Shared Foundations (Bangalore-specific, all free)

### Datasets & data sources
| Source | What it gives you | Access |
|---|---|---|
| **Open Charge Map (OCM)** — filtered to Bangalore bounding box | Real charging station locations, connector types, power ratings, operators (Tata Power EZ Charge, Statiq, Ather Grid, Kazam, Charzer, etc. all listed here) | Free REST API, query by `boundingbox=12.83,77.35,13.14,77.75` |
| **data.gov.in** | India's open government data portal — search "electric vehicle charging stations Karnataka", BBMP/BMTC datasets | Free, no key for most datasets |
| **Vahan Dashboard (parivahan.gov.in)** | Public EV registration counts by RTO — Bangalore has ~10 RTO codes (KA-01 to KA-05, KA-41 etc.) — use as a proxy for EV adoption density by zone | Free public dashboard, exportable |
| **BESCOM EV charging tariff order** (KERC public tariff filings) | Real ₹/kWh public-charging tariff structure — use for realistic revenue/pricing features | Free public PDF from KERC/BESCOM website |
| **Open-Meteo API** (pinned to Bangalore: lat 12.9716, lon 77.5946) | Free hourly weather (temp, rain) — Bangalore rain strongly affects two-wheeler/EV usage patterns | Free, no key needed |
| **OpenStreetMap Bangalore extract (via Geofabrik)** | Road network, POIs, ward boundaries for Bangalore only | Free download |
| **BBMP ward boundaries** (via OSM/data.gov.in) | ~198 BBMP wards — used for "charging desert" geospatial analysis instead of arbitrary hexagons | Free |
| **Karnataka EV Policy 2021 (PDF, public)** | Policy text — used as a knowledge-base document for the AI project | Free public PDF |

> 💡 **Why not scrape operator apps (Tata Power, Statiq, Ather Grid)?** Their in-app locators aren't public APIs, and scraping them risks ToS issues. OCM already aggregates most of their public station listings under an open (ODbL) license — cite it and you're safe. For anything OCM doesn't cover, generate a **synthetic session simulator** calibrated to Bangalore commute patterns (IT-corridor peaks: Whitefield/ORR/Electronic City 8–10am & 6–9pm, weekend mall/tech-park dips) — a strong "I modeled realistic local behavior" talking point.

### Common free infra (unchanged, all local via Docker — zero cost)
- **Docker + Docker Compose**, **MinIO**, **PostgreSQL/DuckDB**
- **GitHub + GitHub Actions** (free CI/CD minutes)
- **Streamlit Community Cloud** / **Hugging Face Spaces** for free public demo links
- **Self-hosted OSRM routing engine** on the Bangalore-only OSM extract (few hundred MB, runs fine locally/free-tier VM) — gives you unlimited free routing without hitting any external API rate limit, and is a nice "I stood up my own routing service" resume line

---

# 1️⃣ Data Engineering — "Bangalore EV Charging Data Platform"

### Goal
Ingest Bangalore-only station + session + weather/tariff data (batch + streaming), build a medallion lakehouse, orchestrate it, test data quality, and serve curated tables.

### Architecture
```
                       ┌──────────────────────┐
 OCM API (bbox=BLR)   →│ Airflow (batch DAG)  │→ Bronze (MinIO, raw JSON/Parquet)
 data.gov.in / Vahan  →│                      │
 Open-Meteo (BLR)     →│                      │
                       └──────────────────────┘
                                                          │
 Synthetic session          ┌───────────────┐              ▼
 generator (BLR commute  →  │ Spark Struct. │→ Silver (Delta/Iceberg tables)
 patterns) → Kafka topic    │ Streaming     │              │
 (Redpanda, local)          └───────────────┘              ▼
                                                  dbt models → Gold (Postgres/DuckDB)
                                                          │
                                                          ▼
                                       Great Expectations (DQ tests)
                                                          │
                                                          ▼
                                     FastAPI serving layer (Bangalore-only)
                                     → consumed by DS / ML / AI projects
```

### Tools & Libraries
- **Ingestion**: `requests`/`httpx` — OCM (bbox-filtered), data.gov.in datasets, Open-Meteo pinned to Bangalore
- **Orchestration**: Apache Airflow (docker-compose) — daily station-refresh + weather-pull DAGs
- **Streaming**: Redpanda + `kafka-python`; Spark Structured Streaming (PySpark) for windowed aggregation of synthetic sessions
- **Storage**: MinIO + Delta Lake (`deltalake`) or `pyiceberg`
- **Transformation**: dbt-core (duckdb/postgres adapter) — bronze→silver→gold, with tests + auto-generated docs
- **Data quality**: Great Expectations / `soda-core`
- **Serving**: FastAPI + SQLModel over gold Postgres tables
- **CI/CD**: GitHub Actions
- **Monitoring**: Airflow UI + Prometheus/Grafana

### Folder structure
```
blr-ev-data-platform/
├── dags/
│   ├── ingest_station_metadata_blr.py     # OCM bbox query
│   ├── ingest_weather_blr.py              # Open-Meteo, lat 12.9716 lon 77.5946
│   ├── ingest_registration_data.py        # Vahan/data.gov.in
│   └── run_dbt_models.py
├── streaming/
│   ├── producer_synthetic_sessions_blr.py # IT-corridor commute pattern seeded
│   ├── spark_streaming_job.py
│   └── session_simulator.py
├── dbt/
│   ├── models/
│   │   ├── bronze/
│   │   ├── silver/
│   │   └── gold/ (station_utilization.sql, demand_by_hour.sql, revenue_estimate_inr.sql, demand_by_ward.sql)
│   ├── tests/
│   └── dbt_project.yml
├── great_expectations/
├── serving_api/
│   ├── main.py
│   └── models.py
├── docker-compose.yml
├── .github/workflows/ci.yml
└── README.md
```

### Key features
1. Daily batch DAG pulling only Bangalore-bounded station metadata + Karnataka registration proxy data
2. Streaming pipeline simulating live sessions with Bangalore commute-hour seasonality (ORR/Whitefield/Electronic City peaks, monsoon rain dips)
3. dbt gold models: station utilization %, hourly demand curve, **ward-level** demand aggregation, INR revenue estimate using real BESCOM tariff structure
4. Automated data quality gate (nulls, duplicate stations, out-of-Bangalore-bbox rows rejected)
5. FastAPI endpoint `/stations/{id}/utilization`, `/wards/{ward_id}/demand`
6. CI: lint → dbt test → docker build on every push

### Resume line
*"Built a medallion-architecture lakehouse for Bangalore's public EV charging network (Airflow + Kafka/Spark + dbt + Great Expectations), aggregating station, weather, and registration data down to the BBMP-ward level, served via FastAPI."*

---

# 2️⃣ Data Science — "Bangalore EV Demand Analytics & Forecasting"

### Goal
Turn Bangalore gold data into insight: ward-level usage patterns, charging-desert mapping, monsoon/traffic-correlated demand forecasting — packaged as a deployed dashboard.

### Architecture
```
Gold tables (from DE project, or OCM+synthetic if standalone)
        │
        ▼
 EDA (pandas, ydata-profiling) ──► usage-pattern notebooks
        │
        ▼
 Forecasting (Prophet/SARIMA/sklearn, MLflow-tracked)
        │
        ▼
 Geospatial (GeoPandas + BBMP ward shapefile + Folium) ──► charging-desert maps
        │
        ▼
 Streamlit dashboard ──► deployed free on Streamlit Community Cloud
```

### Tools & Libraries
- **EDA**: pandas, `ydata-profiling`, Matplotlib/Seaborn/Plotly
- **Forecasting**: Prophet, `statsmodels` (SARIMA), scikit-learn/XGBoost — with weather (monsoon) as an exogenous regressor
- **Anomaly detection**: STL decomposition + z-score, `PyOD`
- **Geospatial**: GeoPandas + **BBMP ward boundaries** (not generic hexagons) + Folium — map station density vs. EV-registration density per ward to flag "charging deserts" (e.g., under-served wards in outer zones vs. saturated ones like Koramangala/Indiranagar)
- **Experiment tracking**: MLflow (local)
- **Dashboard**: Streamlit → Streamlit Community Cloud (free)

### Folder structure
```
blr-ev-demand-analytics/
├── notebooks/
│   ├── 01_eda_station_usage_blr.ipynb
│   ├── 02_ward_level_coverage_gaps.ipynb
│   ├── 03_demand_forecasting_monsoon.ipynb
│   └── 04_anomaly_detection.ipynb
├── data/
│   └── bbmp_ward_boundaries.geojson
├── src/
│   ├── data_loader.py
│   ├── forecasting.py
│   └── geospatial.py
├── mlruns/
├── dashboard/
│   └── streamlit_app.py
├── reports/
│   └── ev_profiling_report.html
├── requirements.txt
└── README.md
```

### Key features
1. Full EDA: peak charging hours by zone (IT-corridor vs. residential), weekday vs weekend, monsoon-season dip
2. **Ward-level charging-desert map**: overlay station density against Vahan-derived EV registration density per BBMP ward
3. Per-station/per-ward demand forecast (Prophet with rain/temp as regressors), backtested (MAPE/RMSE), tracked in MLflow
4. Anomaly detection flagging stations with abnormal usage drops (possible faults) — cross-referenced by ward
5. Interactive Streamlit dashboard: select a Bangalore zone (Whitefield, Electronic City, Koramangala, Yelahanka…) → trend + forecast + anomaly flags
6. Dynamic-pricing simulation in ₹/kWh using real BESCOM tariff bands

### Resume line
*"Forecasted EV charging demand across Bangalore's BBMP wards using Prophet/SARIMA with monsoon-weather regressors, mapped charging deserts against EV registration density, and shipped a public Streamlit dashboard."*

---

# 3️⃣ ML Engineering — "Productionized Bangalore Demand Prediction & Station Recommendation Service"

### Goal
Productionize the demand/recommendation model with a real MLOps stack, geo-bounded to Bangalore.

### Architecture
```
Bangalore gold data ─► Feature pipeline (Feast/pandas) ─► Feature store
                                                                │
                                                                ▼
                             Training pipeline (Prefect/Airflow) ─► XGBoost/LightGBM
                                                                │
                                                                ▼
                                       MLflow Model Registry (staging→prod)
                                                                │
                                                                ▼
                             Docker image ─► FastAPI (/predict, /recommend — BLR bbox only)
                                                                │
                  ┌──────────────────────────┼──────────────────────────┐
                  ▼                          ▼                          ▼
        Evidently (drift monitor)   Prometheus/Grafana         GitHub Actions CI/CD
                                    (latency, request rate)     (auto retrain + deploy)
```

### Tools & Libraries
- **Feature store**: Feast — features scoped to Bangalore stations/wards only
- **Modeling**: scikit-learn, XGBoost, LightGBM — next-hour demand regression + "recommend top-3 nearby available stations" ranking, using real Bangalore lat/lon (haversine distance, `geopy`)
- **Hyperparameter tuning**: Optuna
- **Tracking + registry**: MLflow
- **Orchestration**: Prefect (or reuse Airflow)
- **Versioning**: DVC (free remote — Google Drive)
- **Serving**: FastAPI + Uvicorn in Docker
- **Testing**: pytest + model-quality validation gate
- **CI/CD**: GitHub Actions
- **Monitoring**: Evidently AI + Prometheus/Grafana
- **Deployment (free)**: Render.com free web service / Hugging Face Spaces (Docker SDK)

### Folder structure
```
blr-ev-ml-service/
├── features/
│   ├── feature_repo/          # Feast, Bangalore station/ward features
│   └── build_features.py
├── training/
│   ├── train.py
│   ├── tune_optuna.py
│   └── evaluate.py
├── serving/
│   ├── main.py     # /predict_demand, /recommend_station (bbox-validated to Bangalore)
│   ├── schemas.py
│   └── Dockerfile
├── monitoring/
│   ├── drift_report.py
│   └── grafana_dashboards/
├── pipelines/
│   └── retrain_flow.py
├── tests/
│   ├── test_features.py
│   └── test_model_quality.py
├── .github/workflows/
│   ├── ci.yml
│   └── cd_deploy.yml
├── dvc.yaml
└── README.md
```

### Key features
1. Feature pipeline with point-in-time-correct features (station history, Bangalore weather, time-of-day, RTO-zone registration density)
2. Optuna-tuned training pipeline, tracked/registered in MLflow
3. Promotion gate: new model must beat current prod model on held-out Bangalore data
4. FastAPI: `/predict_demand` (per station/ward) and `/recommend_station` (given a Bangalore lat/lon, ranks nearby stations by predicted availability + ETA via OSRM)
5. Weekly Evidently drift report → auto-triggers retrain if drift detected
6. Full CI/CD: PR → tests+lint → train → validate → containerize → deploy
7. Load-tested with Locust — report p95 latency for a single-city-scale service (small dataset, so you can honestly claim sub-100ms)

### Resume line
*"Built an MLOps pipeline (Feast + MLflow + Optuna + Evidently) for Bangalore-specific EV demand prediction and station recommendation, with automated CI/CD retraining gated on model-quality checks, served via FastAPI with sub-100ms p95 latency."*

---

# 4️⃣ AI (Applied GenAI) — "Bangalore EV Charging Intelligent Assistant"

### Goal
A conversational, tool-using agent for Bangalore EV drivers: RAG over local policy/tariff docs, plus live tool-calls into the DE/ML APIs and a self-hosted Bangalore router.

### Architecture
```
User (chat UI) ──► Orchestrator (LangGraph / CrewAI)
                          │
        ┌─────────────────┼───────────────────────┬─────────────────┐
        ▼                 ▼                       ▼                 ▼
   RAG tool          DE API tool            ML API tool        Routing tool
 (Chroma + LLM,     (live Bangalore        (demand/recommend   (self-hosted OSRM
  Karnataka EV       station data,          from Project 3)      on Bangalore
  Policy + BESCOM    from Project 1)                             OSM extract)
  tariff PDFs)
        │                 │                       │                 │
        └─────────────────┴───────────┬───────────┴─────────────────┘
                                       ▼
                         LLM (Llama 3.1 via Ollama, local & free
                         OR Groq/Gemini free tier for hosted demo)
                                       │
                                       ▼
                          Gradio/Streamlit chat UI
                          → deployed free on Hugging Face Spaces
```

### Tools & Libraries
- **LLM**: Ollama (Llama 3.1 8B/Mistral) locally, or Groq/Gemini free tier for the hosted demo
- **RAG**: LangChain or LlamaIndex
- **Vector DB**: Chroma (local, free)
- **Embeddings**: `sentence-transformers` (local, free)
- **Agent orchestration**: LangGraph (explicit graph) or CrewAI
- **Tool integration**: function-calling wrappers around DE FastAPI (`/stations`, `/wards`) and ML FastAPI (`/predict_demand`, `/recommend_station`)
- **Routing**: **self-hosted OSRM** on the Bangalore-only OSM extract (no external rate limits, fully free) — fallback to OpenRouteService free tier if you skip self-hosting
- **RAG evaluation**: RAGAS
- **Guardrails**: prompt-constrained + optional NeMo Guardrails
- **UI**: Gradio/Streamlit → Hugging Face Spaces

### Folder structure
```
blr-ev-ai-assistant/
├── knowledge_base/
│   ├── raw_docs/
│   │   ├── karnataka_ev_policy_2021.pdf
│   │   ├── bescom_ev_tariff_order.pdf
│   │   └── connector_type_faq.md
│   └── ingest_to_chroma.py
├── agent/
│   ├── graph.py
│   ├── tools/
│   │   ├── station_lookup_tool.py    # calls DE API
│   │   ├── demand_predict_tool.py    # calls ML API
│   │   └── route_tool.py             # calls self-hosted OSRM
│   └── prompts/
├── eval/
│   └── ragas_eval.py
├── ui/
│   └── app_gradio.py
├── Dockerfile
├── requirements.txt
└── README.md
```

### Key features
1. RAG chatbot grounded in Karnataka EV Policy 2021 + BESCOM tariff order + a small connector-type/FAQ doc — answers "what's the public charging tariff in Bangalore?" correctly and citably
2. Agentic tool-use: "Find me a fast charger near Indiranagar likely free in the next hour" → station-lookup + demand-prediction + OSRM ETA, reasoned into one ranked answer
3. Multi-turn memory (remembers vehicle/connector type across the chat)
4. RAGAS evaluation (faithfulness/relevance scores reported in README)
5. Guardrails: refuses non-EV/non-Bangalore questions, always states which tool/source backed the answer
6. Public demo link on Hugging Face Spaces

### Resume line
*"Built an agentic RAG assistant (LangGraph + Chroma + Llama 3.1) for Bangalore EV drivers, combining local policy/tariff retrieval with live tool-calling into production APIs and a self-hosted OSRM router, evaluated with RAGAS and deployed publicly on Hugging Face Spaces."*

---

## 🗂️ How the 4 repos connect
```
blr-ev-data-platform  ──serves gold data via API──►  blr-ev-ml-service (features)
blr-ev-data-platform  ──serves gold data──────────►  blr-ev-demand-analytics (EDA/dashboard)
blr-ev-ml-service     ──serves predictions API────►  blr-ev-ai-assistant (tool call)
blr-ev-data-platform  ──serves live station API───►  blr-ev-ai-assistant (tool call)
```
Each repo still runs standalone (falls back to Bangalore-bbox OCM data or synthetic data) so it can be evaluated independently.

## ✅ Suggested build order
1. **Data Engineering** — Bangalore-bounded ingestion + gold layer
2. **Data Science** — fastest path to an impressive deployed ward-level dashboard
3. **ML Engineering** — reuses DS modeling, adds production rigor
4. **AI** — capstone tying DE + ML APIs into an agent

## 📄 Resume/README checklist per project
- Live deployed link (Streamlit/HF Spaces/Render)
- Mermaid architecture diagram in the README (renders natively on GitHub)
- Concrete metrics: dbt test pass rate, forecast RMSE/MAPE, API p95 latency, RAGAS faithfulness score
- Explicit note that all data is bounded to Bangalore (bbox, BBMP wards, RTO zones) — signals deliberate scoping, not just "grabbed a Kaggle CSV"
- "Skills demonstrated" bullets mapped to the target job description's keywords
