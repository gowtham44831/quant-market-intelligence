from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import psycopg2
import requests
import os
import logging

DB_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "postgres"),
    "port": int(os.getenv("POSTGRES_PORT", "5432")),
    "dbname": os.getenv("POSTGRES_DB", "stocks"),
    "user": os.getenv("POSTGRES_USER", "postgres"),
    "password": os.getenv("POSTGRES_PASSWORD"),
}

API_KEY = os.getenv("MARKET_API_KEY")
MASSIVE_URL = "https://api.massive.com/v2/aggs/ticker/{ticker}/range/10/minute/{start}/{end}?adjusted=true"

logger = logging.getLogger("airflow.task")

# -------- Fetch & Load ----------
def backfill_selected_tickers(**context):
    tickers = context["params"]["tickers"]
    start = context["params"]["start"]
    end = context["params"]["end"]

    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    for ticker in tickers:
        url = MASSIVE_URL.format(ticker=ticker, start=start, end=end)
        resp = requests.get(url, headers={"Authorization": f"Bearer {API_KEY}"})
        data = resp.json().get("results", [])

        for r in data:
            cur.execute("""
                INSERT INTO intraday_data
                (ticker, ts, open, high, low, close, volume)
                VALUES (%s, to_timestamp(%s/1000), %s,%s,%s,%s,%s)
                ON CONFLICT DO NOTHING
            """, (
                ticker, r["t"], r["o"], r["h"], r["l"], r["c"], r["v"]
            ))

        conn.commit()
        logger.info("Loaded %s rows for %s", len(data), ticker)

    cur.close()
    conn.close()

# -------- DAG ----------
dag = DAG(
    dag_id="backfill_intraday_selected_tickers",
    start_date=datetime(2025, 12, 24),
    schedule=None,   # manual only
    catchup=False
)

PythonOperator(
    task_id="backfill_intraday",
    python_callable=backfill_selected_tickers,
    params={
        "tickers": ["ARM","ASML","ATO","AVB","AVY","AWK","AXP","AZN","AZO","BA","BAC","BAX","BBY","BDX","BEN","BF.B","BG","BK","BLDR","BLK","BMY","BR","BRK.B","BRO","BSX","BXP","C","CAG","CAH","CARR","CAT","CB","CBOE","CBRE","CCI","CCL","CF","CFG","CHD","CHRW","CI","CINF","CL","CLX","CME","CMG","CMI","CMS","CNC","CNP","COF","COIN","COO","COP","COR","CPAY","CPB","CPT","CRL","CRM","CTRA","CTVA","CVS","CVX","D","DAL","DAY","DD","DE","DECK","DELL","DG","DGX","DHI","DHR","DIS","DLR","DLTR","DOC","DOV","DOW","DPZ","DRI","DTE","DUK","DVA","DVN","EBAY","ECL","ED","EFX","EG","EIX","EL","ELV","EME","EMR","EOG","EPAM","EQIX","EQR","EQT","ERIE","ES","ESS","ETN","ETR","EVRG","EW","EXE","EXPD","EXPE","EXR","F","FCX","FDS","FDX","FE","FFIV","FICO","FIS","FISV","FITB","FOX","FOXA","FRT","FSLR","FTV","GD","GDDY","GE","GEN","GEV","GFS","GIS","GL","GLW","GM","GNRC","GPC","GPN","GRMN","GS","GWW","HAL","HAS","HBAN","HCA","HD","HIG","HII","HLT","HOLX","HOOD","HPE","HPQ","HRL","HSIC","HST","HSY","HUBB","HUM","HWM","IBKR","IBM","ICE","IEX","IFF","INCY","INVH","IP","IQV","IR","IRM","IT","ITW","IVZ","J","JBHT","JBL","JCI","JKHY","JNJ","JPM","KEY","KEYS","KIM","KKR","KMB","KMI","KO","KR","KVUE","L","LDOS","LEN","LH","LHX","LII","LKQ","LLY","LMT","LNT","LOW","LUV","LVS","LW","LYB","LYV","MA","MAA","MAS","MCD","MCK","MCO","MDT","MELI","MET","MGM","MHK","MKC","MLM","MMC","MMM","MO","MOH","MOS","MPC","MPWR","MRK","MRNA","MRVL","MS","MSCI","MSI","MSTR","MTB","MTCH","MTD","NCLH","NDAQ","NDSN","NEE","NEM","NI","NKE","NOC","NOW","NRG","NSC","NTAP","NTRS","NUE","NVR","NWS","NWSA","O","OKE","OMC","ORCL","PFG","PH","PNW","PODD","RCL","RF","RJF","RMD","ROK","RSG","RTX","RVTY","SBAC","SCHW","SHOP","SHW","SJM","SLB","SMCI","SNA","SNDK","SO","SOLS","SOLV","SPG","SPGI","SRE","STE","STLD","STT","STX","STZ","SW","SWK","SWKS","SYF","SYK","SYY","T","TAP","TDG","TDY","TEAM","TECH","TEL","TER","TFC","TGT","TJX","TKO","TMO","TPL","TPR","TRGP","TRI","TRMB","TROW","TRV","TSCO","TSN","TT","TXT","TYL","UHS","ULTA","UNH","UNP","UPS","URI","USB","V","VICI","VLO","VLTO","VMC","VRSN","VST","VTR","VTRS","VZ","WAB","WAT","WDC","WEC","WELL","WFC","WM","WMB","WMT","WRB","WSM","WST","WTW","WY","WYNN","XOM","XYL","XYZ","YUM","ZBH","ZBRA","ZS","ZTS"],
        "start": "2025-04-28",
        "end": "2025-12-22"
    },
    dag=dag
)
