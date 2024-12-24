from itertools import product
from datetime import datetime
from dateutil.relativedelta import relativedelta
from multiprocessing import Pool
from loguru import logger
import glob
import os
import pandas as pd
from classes.controller.SIHController import SIHController
from classes.cloud.GoogleCloud import GoogleCloud

  # Request report (example in README.md)              OK OK
  # Create bucket, if necessary (example in README.md) OK OK
  # Transform into parquet                             OK OK
  # Insert parquet in storage                          OK OK
  # Read multiple parquet (dataframe) in Storage       OK OK
  # Delete same data in BigQuery                       OK OK
  # Insert data in BigQuery                            OK OK

  # Usar Black e Isort, talvez flake8
  # Pre-commit
  # Adicionar SecretManager
  # Adicionar observabilidade


def main(request):
  logger.info("Starting the extraction of DataSUS SIH RD.")
  data_request = request.get_json()

  uf = data_request.get("uf")
  year = data_request.get("year")
  month = data_request.get("month")

  gcp_project = data_request.get("gcp_project")
  table_id = data_request.get("table_id")
  partition_columns = data_request.get("partition_columns")
  bucket_name_parquet = data_request.get("bucket_name_parquet")

  sa_json="config/service_account.json"

  if not year or year == [""]:
    current_year = datetime.now().strftime("%y")
    year = [current_year]

  if not month or month == [""]:
    last_month_date = datetime.now() - relativedelta(months=1)
    current_month = last_month_date.strftime("%m")
    month = [current_month]

  # Request report
  df = SIHController.request_RD_report_dataframe_format(uf, year, month)

  # Create bucket, if necessary
  # GoogleCloud.create_bucket_in_storage(
  #   bucket_name=bucket_name_parquet,
  #   sa_json=sa_json
  # )

  # # Insert parquet files in Storage    
  # if parquet_files:
  #   GoogleCloud.save_files_in_storage(parquet_files, bucket_name_parquet, sa_json)
  # else:
  #   print("No .parquet files found in the source directory.")

  # # Read multiple parquet files (dataframe) in Storage
  # params = list(product(uf, year, month))
  # file_names = [f"RD{u}{y}{m}.parquet" for u, y, m in params]

  # df = GoogleCloud.read_parquet_files_in_storage(
  #   file_names,
  #   bucket_name_parquet,
  #   sa_json
  # )

  # Insert data in BigQuery
  df = df.astype(str)
  df['insert_loading'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
  google_cloud = GoogleCloud()
  google_cloud.insert_dataframe_into_bigquery(df, table_id, sa_json, partition_columns, gcp_project)

  return logger.info("Extraction of DataSUS SIH RD completed successfully.")



class Request:
    def __init__(self):
        print("")

    def get_json(self):
        parameters = {
          "gcp_project": "datasus-prod",
          "table_id": "raw.tb_sih_rd",
          "partition_columns": ["UF_ZI", "ANO_CMPT", "MES_CMPT"],
          "bucket_name_parquet": "raw_sih_rd_parquet",
          "uf": [
            "RJ",
            "SP",
            "MG",
            "ES"
          ],
          "year": [
            "24"
          ],
          "month": [
            "10"
          ]
        }
        return parameters


if __name__ == "__main__":
    main(Request())
