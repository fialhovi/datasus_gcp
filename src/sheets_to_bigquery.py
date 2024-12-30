import os
from datetime import datetime

import pandas as pd

from classes import GoogleCloud

# secret_project_id = os.getenv("secret_project_id")
# secret_id = os.getenv("secret_id")

df = pd.read_csv("./data/csv_lookup/lookup_municipality.csv", index_col=False)
df = df.astype(str)
df["date_loading"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

google_cloud = GoogleCloud()

sa_json = "config/service_account.json"
# sa_json = google_cloud.access_secret_from_secret_manager(secret_project_id, secret_id)

google_cloud.insert_dataframe_into_bigquery(
    df,
    table_id="lookup.tb_lookup_municipality",
    sa_json=sa_json,
    partition_columns="",
    gcp_project="datasus-prod",
    if_exists="replace"
)
