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
        Downloads a single RD report file for a specific Brazilian state, year, and month from the SIH-SUS FTP server.

        Parameters:
        uf (str): The two-letter Brazilian state code (e.g., 'SP' for São Paulo, 'RJ' for Rio de Janeiro).
        year (str): The four-digit year (e.g., '2024') indicating the year of the report.
        month (str): The two-digit month (e.g., '01' for January, '12' for December) indicating the month of the report.

        Returns:
        None: Performs the download operation and logs relevant messages but does not return any value.
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
        Downloads multiple RD report files for specified Brazilian states, years, and months using parallel processing.

        Parameters:
        params (List[Tuple[str, str, str]]):
            A list of tuples, where each tuple contains:
            - A two-letter Brazilian state code (e.g., 'SP' for São Paulo).
            - A four-digit year as a string (e.g., '24').
            - A two-digit month as a string (e.g., '01' for January).

        Returns:
        None: Initiates parallel downloads and does not return any value.
        """
        with Pool(8) as pool:
            pool.starmap(SIHController.request_one_RD_report_dbc_format, params)

        return

    def request_RD_report_dataframe_format(
        uf: str, year: str, month: str
    ) -> pd.DataFrame:
        """
        Downloads and converts RD reports for the specified parameters to a combined DataFrame.

        Parameters:
        uf (str): The two-letter Brazilian state code (e.g., 'SP' for São Paulo).
        year (str): The four-digit year (e.g., '24') indicating the year of the report.
        month (str): The two-digit month (e.g., '01' for January) indicating the month of the report.

        Returns:
        pd.DataFrame: A combined DataFrame of all the processed reports.
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
