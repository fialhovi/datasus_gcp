import json
import os
from typing import List

import duckdb
import pandas as pd
import pandas_gbq
from google.cloud import bigquery, secretmanager, storage
from google.oauth2 import service_account
from loguru import logger


class GoogleCloud:
    def __init__(self) -> None:
        self.credentials = None

    def authenticate(self, sa_json: str) -> service_account.Credentials:
        """
        Authenticate using a service account JSON file or JSON string.

        Parameters:
        sa_json (str): Path to the Service Account JSON file or the JSON content as a string.

        Returns:
        google.oauth2.service_account.Credentials: The authenticated credentials object.
        """
        try:
            if os.path.exists(sa_json):
                self.credentials = (
                    service_account.Credentials.from_service_account_file(sa_json)
                )
                logger.info("Authentication successful using service account file.")
            else:
                self.credentials = (
                    service_account.Credentials.from_service_account_info(
                        json.loads(sa_json)
                    )
                )
                logger.info(
                    "Authentication successful using service account JSON content."
                )
        except Exception as e:
            logger.error(f"Authentication error: {e}")
            self.credentials = None

        return self.credentials

    def create_bucket_in_storage(
        self,
        bucket_name: str,
        sa_json: str,
        location: str = "US",
        storage_class: str = "STANDARD",
    ) -> None:
        """
        Creates a bucket in Google Cloud Storage using explicit Service Account credentials.

        Parameters:
        bucket_name (str): The unique name for the bucket.
        sa_json (str): Path to the Service Account JSON file.
        location (str): The geographic location for the bucket (default: "US").
        storage_class (str): The storage class for the bucket (default: "STANDARD").
        """
        credentials = self.authenticate(sa_json)
        client = storage.Client(credentials=credentials)
        bucket = client.bucket(bucket_name)
        bucket.storage_class = storage_class

        new_bucket = client.create_bucket(bucket, location=location)

        logger.info(
            f"Bucket {new_bucket.name} created in {new_bucket.location} with storage class {new_bucket.storage_class}."
        )

        return None

    def save_local_files_in_storage(
        self,
        filenames: List[str],
        source_directory: str,
        bucket_name: str,
        sa_json: str,
    ) -> None:
        """
        Uploads files from a local directory to a specified Google Cloud Storage bucket.

        Parameters:
        filenames (List[str]): Names of files to upload.
        source_directory (str): Local directory containing the files.
        bucket_name (str): Name of the destination GCS bucket.
        sa_json (str): Path to the Service Account JSON file.

        Raises:
        FileNotFoundError: If any file in the specified directory does not exist.
        """
        try:
            self.credentials = self.authenticate(sa_json)
            if not self.credentials:
                logger.error("Failed to authenticate. Cannot proceed.")
                return

            storage_client = storage.Client(credentials=self.credentials)
            bucket = storage_client.bucket(bucket_name)

            for filename in filenames:
                local_path = os.path.join(source_directory, filename)

                if not os.path.exists(local_path):
                    logger.error(f"File {local_path} does not exist. Skipping.")
                    continue

                blob = bucket.blob(filename)
                blob.upload_from_filename(local_path)

                logger.info(f"Uploaded {filename} to bucket {bucket_name}.")

                try:
                    os.remove(local_path)
                    logger.info(f"Removed local file {filename} in {local_path}.")
                except Exception as e:
                    logger.error(
                        f"Failed to remove local file {filename} in {local_path}: {e}"
                    )

        except Exception as e:
            logger.error(f"Error uploading files: {e}")

    def save_files_in_storage(
        self, file_paths: List[str], bucket_name: str, sa_json: str
    ) -> None:
        """
        Uploads a list of files to a specified Google Cloud Storage bucket.

        Parameters:
        file_paths (List[str]): List of file paths to be uploaded.
        bucket_name (str): The name of the GCS bucket.
        sa_json (str): Path to the Service Account JSON file for authentication.

        Raises:
        FileNotFoundError: If any file in the specified directory does not exist.
        """
        try:
            self.credentials = self.authenticate(sa_json)
            if not self.credentials:
                logger.error("Failed to authenticate. Cannot proceed.")
                return

            storage_client = storage.Client(credentials=self.credentials)
            bucket = storage_client.bucket(bucket_name)

            for file_path in file_paths:
                if not os.path.exists(file_path):
                    logger.error(f"File {file_path} does not exist. Skipping.")
                    continue

                file_name = os.path.basename(file_path)
                blob = bucket.blob(file_name)
                blob.upload_from_filename(file_path)

                logger.info(f"Uploaded {file_name} to bucket {bucket_name}.")

        except Exception as e:
            logger.error(f"Error uploading files to storage: {e}")

        return

    def read_parquet_files_in_storage(
        self, file_names: List[str], bucket_name: str, sa_json: str
    ) -> pd.DataFrame:
        """
        Reads multiple Parquet files from a GCS bucket using DuckDB.

        Parameters:
        file_names (List[str]): List of file names to read from the bucket.
        bucket_name (str): The name of the GCS bucket.
        sa_json (str): Path to the Service Account JSON file for authentication.

        Returns:
        pd.DataFrame: DataFrame containing merged data from Parquet files.
                    Returns an empty DataFrame in case of errors.
        """
        try:
            self.credentials = self.authenticate(sa_json)
            if not self.credentials:
                logger.error("Failed to authenticate. Cannot proceed.")
                return pd.DataFrame()

            storage_client = storage.Client(credentials=self.credentials)
            bucket = storage_client.bucket(bucket_name)

            temp_dir = "/tmp/parquet_files"
            os.makedirs(temp_dir, exist_ok=True)

            local_files = []
            for file_name in file_names:
                local_path = os.path.join(temp_dir, file_name)
                blob = bucket.blob(file_name)
                blob.download_to_filename(local_path)
                local_files.append(local_path)
                logger.info(f"Downloaded {file_name} to {local_path}.")

            query = f"SELECT * FROM read_parquet({','.join([f'\'{file}\'' for file in local_files])})"
            logger.info(f"Running DuckDB query: {query}")
            df = duckdb.query(query).to_df()

            for file in local_files:
                os.remove(file)
                logger.info(f"Removed temporary file {file}.")

            logger.info("Successfully read Parquet files into a DataFrame.")
            return df

        except Exception as e:
            logger.error(f"Error reading Parquet files: {e}")
            return pd.DataFrame()

    def insert_dataframe_into_bigquery(
        self,
        df: pd.DataFrame,
        table_id: str,
        sa_json: str,
        partition_columns: List[str],
        gcp_project: str,
        if_exists: str = "append",
    ) -> None:
        """
        Inserts a DataFrame into a BigQuery table with optional partition-based deletion.

        Parameters:
        df (pd.DataFrame): DataFrame to insert.
        table_id (str): Full table ID in the format `dataset.table`.
        sa_json (str): Path to the Service Account JSON file.
        partition_columns (List[str]): Column names for partition deletion (e.g., ["column1", "column2"]).
        gcp_project (str): GCP Project ID.
        if_exists (str): Behavior when the table exists:
                        - "fail": Raise an error.
                        - "replace": Overwrite the table.
                        - "append": Append to the table (default).

        Raises:
        Exception: If an error occurs during insertion.
        """
        try:
            self.credentials = self.authenticate(sa_json)
            client = bigquery.Client(credentials=self.credentials, project=gcp_project)

            table_ref = client.dataset(table_id.split(".")[0]).table(
                table_id.split(".")[1]
            )
            try:
                client.get_table(table_ref)
                table_exists = True
                logger.info(f"Table `{table_id}` exists.")
            except:
                table_exists = False
                logger.info(f"Table `{table_id}` does not exist. It will be created.")

            if table_exists and partition_columns:
                delete_conditions = " AND ".join(
                    [
                        f"{col} IN ({', '.join([f'\'{value}\'' for value in df[col].unique()])})"
                        for col in partition_columns
                    ]
                )
                delete_query = (
                    f"DELETE FROM `{gcp_project}.{table_id}` WHERE {delete_conditions}"
                )

                logger.info(
                    f"Deleting existing data from {gcp_project}.{table_id} for partitions: {delete_conditions}."
                )
                pandas_gbq.read_gbq(delete_query, project_id=gcp_project)

            logger.info(f"Inserting data into {gcp_project}.{table_id}.")
            pandas_gbq.to_gbq(
                df,
                destination_table=table_id,
                project_id=gcp_project,
                if_exists=if_exists,
            )

            logger.info(
                f"Inserted data into {gcp_project}.{table_id} successfully. Rows: {df.shape[0]}, Columns: {df.shape[1]}."
            )

        except Exception as e:
            logger.error(f"Error inserting data into BigQuery: {e}")

        return

    def access_secret_from_secret_manager(
        self, secret_project_id: str, secret_id: str, version_id: str = "latest"
    ) -> str:
        """
        Access a secret's value from GCP Secret Manager.

        Parameters:
        secret_project_id (str): GCP Project ID.
        secret_id (str): Secret ID in Secret Manager.
        version_id (str): Version of the secret (default: 'latest').

        Returns:
        str: The secret value as a string.
        """
        client = secretmanager.SecretManagerServiceClient()

        secret_name = (
            f"projects/{secret_project_id}/secrets/{secret_id}/versions/{version_id}"
        )
        response = client.access_secret_version(request={"name": secret_name})
        secret_value = response.payload.data.decode("UTF-8")

        return secret_value
