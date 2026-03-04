"""Google map reviews worker handle code"""

__author__ = "Thanh Do"
__version__ = "1.0.0"
__task__ = "DA-216"
__maintainer__ = "Thanh Do"
__email__ = "thanh.do@jrgvn.com"
__status__ = "Production"
import os
import re
import sys
import pytz
import pandas as pd
from datetime import datetime
from urllib.parse import quote_plus
from sqlalchemy import create_engine, text
from dateutil.relativedelta import relativedelta
from src.data.selenium.replicate_google_map_reviews.config import (
    is_get_all_review,
)
from config.project_setup import PROJECT_VARIABLES
from models.postgres_credential import PostgresDBCredential
from src.utils.secret_factory.providers.gcp_secret_manager import GCPSecretManager
from src.utils.secret_factory.secret_factory import SecretFactory

vietnamese_to_english = {
    "giờ": "hours",
    "phút": "minutes",
    "giây": "seconds",
    "ngày": "days",
    "tuần": "weeks",
    "tháng": "months",
    "năm": "years",
}


def convert_date(ago):
    """Convert ago in string (for instance: 2 days ago) to a meaningful date (2023-07-01)
    Args:
        ago: Ago date in yyyy-mm-dd format
    Return: Formatted date
    """
    match = re.search(r"(\d+) (\w+) trước", ago)

    if match:
        value, unit = match.groups()
        if unit in vietnamese_to_english:
            unit = vietnamese_to_english[unit]
        if value == "một":
            value = 1
        delta = relativedelta(**{unit: int(value)})
        return datetime.now() - delta
    else:
        match = re.search(r"(\w+) (\w+) trước", ago)
        if match:
            value, unit = match.groups()
            if unit in vietnamese_to_english:
                unit = vietnamese_to_english[unit]
            if value == "một":
                value = 1
            delta = relativedelta(**{unit: int(value)})
            return datetime.now() - delta
        return None


def main():
    """Extracting csv files from data/ folder and upload to bigquery"""
    tz = pytz.timezone("Asia/Ho_Chi_Minh")
    current_date = datetime.now(tz).date()
    current_datetime = datetime.now(tz)
    formatted_date = current_date.strftime("%Y-%m-%d")
    # Set the path to the folder containing your Excel files
    folder_path = f"{PROJECT_VARIABLES['repo_path']}/src/data/selenium/replicate_google_map_reviews/data/{formatted_date}"

    # Get a list of all the Excel files in the folder
    excel_files = [f for f in os.listdir(folder_path) if f.endswith(".xlsx")]

    # Initialize an empty list to store individual DataFrames
    dataframes = []

    # Read each Excel file and append its DataFrame to the list
    for file in excel_files:
        file_path = os.path.join(folder_path, file)
        df = pd.read_excel(file_path, engine="openpyxl")
        dataframes.append(df)

    # Concatenate all DataFrames into a single DataFrame
    combined_df = pd.concat(dataframes, ignore_index=True)
    current_date = datetime.now()
    combined_df["review_date"] = combined_df["review_date"].apply(convert_date)
    combined_df["rating"] = combined_df["rating"].str.split(" ").str[0]
    index_to_drop = 0
    combined_df = combined_df.drop(combined_df.columns[index_to_drop], axis=1)
    combined_df["rating"] = combined_df["rating"].astype(int)
    combined_df["is_reply"] = combined_df["is_reply"].astype(bool)
    combined_df["tags"] = combined_df["tags"].astype(str)
    combined_df["Created_Date"] = current_datetime
    combined_df["Created_Date"] = pd.to_datetime(combined_df["Created_Date"])
    if_exist = "replace"
    if not is_get_all_review:
        if_exist = "append"
    combined_df.to_gbq("sql-server-replicate.Review.GoogleReviews", if_exists=if_exist)
    grouped_df = combined_df.groupby("s_no").size().reset_index(name="No_Rows")
    google_sm: GCPSecretManager = SecretFactory.get_secret_provider('gcp')
    postgres_db_credentials: PostgresDBCredential = google_sm.get_secret_and_parse(
        secret_name="PHVN_Crendetials_POSTGRES",
        json_object_node="POSTGRES_DB",
        dataclass_type=PostgresDBCredential
    )    
    encoded_password = quote_plus(postgres_db_credentials.password)
    engine = create_engine(
        f"postgresql+psycopg2://{postgres_db_credentials.username}:{encoded_password}@{postgres_db_credentials.host}:{postgres_db_credentials.port}/airflow",
        future=True,
    )
    connection = engine.connect()
    for _, row in grouped_df.iterrows():
        s_no = str(row["s_no"])
        no_rows = int(row["No_Rows"])
        print(s_no, no_rows)
        if is_get_all_review:
            dynamic_condition = 'SET "No_Rows" = :no_rows'
        else:
            dynamic_condition = 'SET "No_Rows" = "No_Rows" + :no_rows'
        update_google_review_table_query = f"""
            UPDATE public.google_review_metadata
            {dynamic_condition}
            WHERE "S_No" = :s_no and "Brand_Name" = 'PHVN'
        """
        print(update_google_review_table_query)
        params = {"no_rows": no_rows, "s_no": s_no}
        try:
            # Execute the SQL update statement
            connection.execute(text(update_google_review_table_query), params)
            connection.commit()
            print("Update successful")
        except Exception as e:
            print("Error occurred:", e)
            connection.rollback()
        # finally:
        #     print('Close connection to the database')
        #     engine.dispose()


# if __name__ == "__main__":
# main()
