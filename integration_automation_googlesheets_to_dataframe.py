import pandas as pd
import re
from datetime import datetime
from google.cloud import bigquery
from sqlalchemy import create_engine, text
client = bigquery.Client(project="lithe-transport-431116-b4")

dataset = "test"
target_tbl = "u_literature"
staging_tbl = "u_literature_staging"

## Postgres
engine = create_engine("postgresql+psycopg2://postgres:root@localhost:5432/assessment")

## Psate CSV link
url = "https://docs.google.com/spreadsheets/d/15OnfUErAK7ETBD4X-pRyAtgeD--PQeYODL9xPPXWPww/export?gid=131054257"

## Fetch sheed id and gid
sheet_match = re.search(r"/d/([a-zA-Z-0-9-_]+)", url)
sheet_id = sheet_match.group(1) if sheet_match else None
gid_match = re.search(r"gid=(\d+)", url)
gid = gid_match.group(1) if gid_match else None

## Load CSV to Dataframe
url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid={gid}"
df = pd.read_csv(url)
try:
    df = pd.read_csv(url, encoding='utf-8')
except UnicodeDecodeError:
    df = pd.read_csv(url, encoding='latin1')

## Map to table structure for loading
datatype_map = {
    "user_id": "int64",
    "title": "string",
    "context_text": "string",
    "photo_url": "string",
    "description": "string",
    "id": "int64",
    "content_html": "string",
    "category": "string",
    "created_at": "datetime64[ns]",
    "updated_at": "datetime64[ns]",
    "date_accessed": "datetime64[ns]",
    "created_date": "datetime64[ns]",
    "random_users_count": "int64",
    "date_loaded": "datetime64[ns]"
    }

for col, dtype in datatype_map.items():
    if col in df.columns:
        if "datetime" in dtype:
            df[col] = pd.to_datetime(df[col], errors="coerce")
            df[col] = df[col].astype("datetime64[us]")
        elif dtype == "string":
            df[col] = df[col].astype("string")
        else:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype(dtype, errors="ignore")

## Validate check if pkey has null values (pkey is not nullable)
df = df.dropna(subset=['user_id'])

print(df.dtypes)
print(len(df))
## deduplicate (user_id) keep newest date_loaded
df = df.sort_values('date_loaded', ascending=False).drop_duplicates(subset=['user_id'], keep='first')
print(len(df))

## UPSERT IS BEING DONE HERE
## upsert data to postgres prod as staging
## GCP is in free mode: workaround (upsert to postgres -> truncate load to google bigquery)
with engine.begin() as conn:
    for _, row in df.iterrows():
        upsert = text("""
        INSERT INTO public.u_literature_prod(
        user_id, title, content_text, photo_url, description,
        id, content_html,category, updated_at, created_at,
        random_users_count, created_date, date_accessed, date_loaded
        ) VALUES (
        :user_id, :title, :content_text, :photo_url, :description,
        :id, :content_html, :category, :updated_at, :created_at,
        :random_users_count, :created_date, :date_accessed, :date_loaded
        )
        ON CONFLICT (user_id) DO UPDATE SET
                        title = EXCLUDED.title,
                        content_text = EXCLUDED.content_text,
                        photo_url = EXCLUDED.photo_url,
                        description = EXCLUDED.description,
                        id = EXCLUDED.id,
                        content_html = EXCLUDED.content_html,
                        category = EXCLUDED.category,
                        updated_at = EXCLUDED.updated_at,
                        created_at = EXCLUDED.created_at,
                        random_users_count = EXCLUDED.random_users_count,
                        created_date = EXCLUDED.created_date,
                        date_accessed = EXCLUDED.date_accessed,
                        date_loaded = EXCLUDED.date_loaded
        
        """)
        conn.execute(upsert, row.to_dict())

## Get data from postgres in preparation for GoogleBigQuery loading
query = "SELECT * FROM public.u_literature_prod ORDER BY date_loaded"
df = pd.read_sql(query, engine)

table_id = f"{client.project}.{dataset}.{target_tbl}"
conf = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
load = client.load_table_from_dataframe(df, table_id, job_config=conf)
query = "SELECT MAX(date_loaded) as date_loaded FROM `lithe-transport-431116-b4.test.u_literature` LIMIT 5"

## Check if date_loaded is same from csv
val_newloaded = client.query(query).to_dataframe()['date_loaded'].iloc[0]
date_loaded = df['date_loaded'].max()
print(f"Postgres latest date_loaded: {date_loaded}")
print(f"Google BigQuery latest date_loaded: {val_newloaded}")

## Validate if Data is same
if pd.to_datetime(date_loaded) == pd.to_datetime(val_newloaded):
    print("Data Loaded successfuly")
else:
    print("Mismatch, check etl")