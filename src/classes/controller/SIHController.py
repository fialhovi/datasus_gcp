import glob
import os
import urllib.error
import urllib.request
from multiprocessing import Pool
from typing import List, Tuple

import pandas as pd
from loguru import logger
from pysus.ftp.databases.sih import SIH


class SIHController:
    def __init__(self, uf, year, month, params):
        self.uf = uf
        self.year = year
        self.month = month
        self.params = params

    def request_one_RD_report_dbc_format(uf: str, year: str, month: str) -> None:
        """
        Downloads a single RD report file in DBC format for a specific state, year, and month from the SIH-SUS FTP server.

        Parameters:
        uf (str): Two-letter Brazilian state code (e.g., 'SP' for São Paulo, 'RJ' for Rio de Janeiro).
        year (str): Two-digit year (e.g., '24') indicating the year of the report.
        month (str): Two-digit month (e.g., '01' for January, '12' for December).

        Saves:
        A `.dbc` file to the local directory `./data/dbc/` under the name `RD<uf><year><month>.dbc`.

        Raises:
        urllib.error.URLError: If there is a URL-related error during download.
        Exception: For any other unexpected error.
        """
        url = f"ftp://ftp.datasus.gov.br/dissemin/publicos/SIHSUS/200801_/Dados/RD{uf}{year}{month}.dbc"
        file_path = f"./data/dbc/RD{uf}{year}{month}.dbc"

        os.makedirs(os.path.dirname(file_path), exist_ok=True)

        try:
            urllib.request.urlretrieve(url, file_path)
            logger.info(f"Successfully downloaded RD{uf}{year}{month}.dbc")

        except urllib.error.URLError as e:
            logger.error(f"Download error {url}: {e}")

        except Exception as e:
            logger.error(f"Download error {url}: {e}")

        return

    def request_multiple_RD_reports_dbc_format(
        params: List[Tuple[str, str, str]]
    ) -> None:
        """
        Downloads multiple RD report files in DBC format using parallel processing.

        Parameters:
        params (List[Tuple[str, str, str]]):
            A list of tuples where each tuple contains:
            - A two-letter Brazilian state code (e.g., 'SP' for São Paulo).
            - A two-digit year as a string (e.g., '24').
            - A two-digit month as a string (e.g., '01' for January).

        Details:
        - Uses parallel processing with a pool size of 8 for concurrent downloads.
        - Calls `request_one_RD_report_dbc_format` internally for each tuple.

        Raises:
        Exception: If any error occurs during parallel processing.
        """
        with Pool(8) as pool:
            pool.starmap(SIHController.request_one_RD_report_dbc_format, params)

        return

    def request_RD_report_dataframe_format(
        uf: str, year: str, month: str
    ) -> pd.DataFrame:
        """
        Downloads and processes RD report files into a combined DataFrame.

        Parameters:
        uf (str): Two-letter Brazilian state code (e.g., 'SP' for São Paulo).
        year (str): Two-digit year (e.g., '24') indicating the year of the report.
        month (str): Two-digit month (e.g., '01' for January).

        Returns:
        pd.DataFrame:
            A combined DataFrame of all processed RD reports for the specified state, year, and month.
            Returns an empty DataFrame if no files are available or an error occurs.

        Raises:
        Exception: For errors during file fetching, processing, or DataFrame combination.

        Notes:
        - The method relies on the `pysus` library to access SIH-SUS FTP servers.
        - Files are downloaded and converted to Parquet before loading into DataFrames.
        """
        sih = SIH().load()

        try:
            files = sih.get_files("RD", uf=uf, year=year, month=month)
            if not files:
                logger.warning("No files found for the specified parameters.")
                return pd.DataFrame()
        except Exception as e:
            logger.error(f"Error fetching file list for RD reports: {e}")
            return pd.DataFrame()

        dataframes = []

        for file in files:
            try:
                logger.info(f"Processing file: {file}")
                parquet_file = sih.download([file])

                df = parquet_file.to_dataframe()
                dataframes.append(df)
                logger.info(f"Successfully processed {file}")
            except Exception as e:
                logger.error(f"Error reading {file}: {e}")

        if dataframes:
            try:
                combined_df = pd.concat(dataframes, ignore_index=True)
                logger.info("Successfully combined all DataFrames.")
                return combined_df
            except Exception as e:
                logger.error(f"Error combining DataFrames: {e}")
                return pd.DataFrame()
        else:
            logger.warning("No DataFrames were successfully processed.")
            return pd.DataFrame()
