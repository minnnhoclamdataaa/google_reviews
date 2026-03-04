import pandas as pd
import os
import glob
import pytz
import re
from datetime import datetime, timedelta
from urllib.parse import quote_plus
from google.oauth2 import service_account
from sqlalchemy import create_engine, text
from src.data.selenium.replicate_google_map_reviews.llms.create_labels import (
    create_labels_gpt,
)
from pandas import DataFrame
from dotenv import load_dotenv
from config.project_setup import PROJECT_VARIABLES
from models.postgres_credential import PostgresDBCredential
from src.utils.secret_factory.providers.gcp_secret_manager import GCPSecretManager
from src.utils.secret_factory.secret_factory import SecretFactory

def rename_columns(df: DataFrame) -> DataFrame:
    df["review_time"] = df["review_date"]
    df["review_date"] = df["review_date"].dt.date
    df = df.rename(
        columns={
            "s_no": "store_id",
            "name": "review_user",
            "comment": "review_content",
            "review_time": "review_time",
            "review_date": "review_date",
            "rating": "review_rating",
        }
    )
    return df


def astype_review(df: DataFrame) -> DataFrame:
    df[
        [
            "store_id",
            "review_user",
            "review_content",
            "review_time",
            "review_labels",
            "review_sentiment",
            "review_link",
        ]
    ] = df[
        [
            "store_id",
            "review_user",
            "review_content",
            "review_time",
            "review_labels",
            "review_sentiment",
            "review_link",
        ]
    ].astype(
        str
    )
    df["review_rating"] = df["review_rating"].astype(int)
    return df


def validate_and_process_row(row) -> pd.Series:
    row["is_valid"] = True
    review_content = row["review_content"]
    response = create_labels_gpt(review_content)
    row["review_labels"] = ",".join(response["labels"])
    row["review_sentiment"] = response["sentiment"]
    if not isinstance(row["review_labels"], str) or not row["review_labels"]:
        row["review_labels"] = ""
        row["is_valid"] = False
    if row["review_sentiment"] not in ["Positive", "Negative", "Neutral"]:
        row["review_sentiment"] = ""
        row["is_valid"] = False
    return row


def process_row_step_1(row) -> pd.Series:
    if isinstance(row["review_content"], str) and row["review_content"]:
        row = validate_and_process_row(row)
    return pd.Series(row)


def process_row_step_2_3(row) -> pd.Series:
    if (
        not row["is_valid"]
        and isinstance(row["review_content"], str)
        and row["review_content"]
    ):
        row = validate_and_process_row(row)
    return pd.Series(row)


def main():
    tz = pytz.timezone("Asia/Ho_Chi_Minh")
    current_time_now = datetime.now(tz)
    current_date = current_time_now.date()
    formatted_date = current_date.strftime("%Y-%m-%d")
    if current_time_now.hour < 12:
        no_run = 1
    else:
        no_run = 2
    processed_dir = f"{PROJECT_VARIABLES['repo_path']}/src/data/selenium/replicate_google_map_reviews/data/{formatted_date}/{no_run}/processed/"
    processed_pattern = os.path.join(processed_dir, "*.csv")
    processed_files = glob.glob(processed_pattern)
    llist_ = []
    current_datetime = datetime.now(tz)
    for t_p in processed_files:
        df = pd.read_csv(f"{t_p}")
        df = df[
            [
                "review_id",
                "contributor_id",
                "s_no",
                "name",
                "comment",
                "rating",
                "review_date",
                "is_reply",
                "tags",
                "review_link",
                "timestamp",
            ]
        ]

        # Assuming your timestamp column is named 'timestamp'
        # Convert from microseconds to seconds
        df["timestamp"] = df["timestamp"] / 1e6

        # Convert to datetime
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="s")

        # Adjust to GMT+7
        df["timestamp"] = df["timestamp"] + timedelta(hours=7)

        # Format the datetime as yyyy-mm-dd hh:mm:ss
        df["timestamp"] = df["timestamp"].dt.strftime("%Y-%m-%d %H:%M:%S")

        df["rating"] = df["rating"].str.split(" ").str[0]
        df["rating"] = df["rating"].astype(int)
        df["is_reply"] = df["is_reply"].astype(bool)
        df["tags"] = df["tags"].astype(str)
        tz = pytz.timezone("Asia/Ho_Chi_Minh")
        df["Created_Date"] = current_datetime
        df["Created_Date"] = pd.to_datetime(df["Created_Date"])
        df["review_date"] = df["timestamp"]
        df["contributor_id"] = df["contributor_id"].astype(str)
        df["review_id"] = df["review_id"].astype(str)
        df = df.drop(columns=["timestamp"])
        df["review_date"] = pd.to_datetime(df["review_date"])
        llist_.append(df)
    result = pd.concat(llist_)
    print(len(result))
    result.to_gbq("sql-server-replicate.Review.GoogleReviews", if_exists="append")

    # Upload to postgres
    # Store_ID,userName,reviewMessage,reviewTime,reviewDate,reviewRating
    # s_no, name, comment, review_date, rating
    # src\data\selenium\replicate_google_map_reviews
    google_sm: GCPSecretManager = SecretFactory.get_secret_provider('gcp')
    postgres_db_credentials: PostgresDBCredential = google_sm.get_secret_and_parse(
        secret_name="PHVN_Crendetials_POSTGRES",
        json_object_node="POSTGRES_DB",
        dataclass_type=PostgresDBCredential
    )
    user     = postgres_db_credentials.username
    password = postgres_db_credentials.password
    db_ip    = postgres_db_credentials.host
    db_port  = postgres_db_credentials.port
    db_name  = "llms"
    df = result
    df = rename_columns(df)
    df["review_content"] = df["review_content"].fillna("")
    df["review_labels"] = ""
    df["review_sentiment"] = ""
    df["review_link"] = df["review_link"].fillna("")
    df["review_id"] = df["review_id"].fillna("")
    df = astype_review(df)
    df["review_content"] = df["review_content"].replace("nan", None)

    df["review_link"] = df["review_link"].replace("nan", None)
    df["review_id"] = df["review_id"].replace("nan", None)
    df["created_date"] = df["Created_Date"]
    df = df.apply(process_row_step_1, axis=1)
    df = df.apply(process_row_step_2_3, axis=1)
    df = df[
        [
            "is_valid",
            "review_content",
            "review_date",
            "review_labels",
            "review_rating",
            "review_sentiment",
            "review_time",
            "review_user",
            "store_id",
            "review_id",
            "created_date",
            "review_link",
        ]
    ]

    encoded_password = quote_plus(password)
    engine = create_engine(
        f"postgresql+psycopg2://{user}:{encoded_password}@{db_ip}:"
        f"{db_port}/{db_name}"
    )
    df.to_sql("google", con=engine, if_exists="append", index=False)

    google_labels_df = df[["review_id", "review_labels", "review_sentiment"]]
    google_labels_df["review_id"] = google_labels_df["review_id"].astype(str)
    google_labels_df["review_labels"] = google_labels_df["review_labels"].astype(str)
    google_labels_df["review_labels"] = google_labels_df["review_labels"].replace(
        "", None
    )
    google_labels_df["review_sentiment"] = google_labels_df["review_sentiment"].astype(
        str
    )
    google_labels_df["review_sentiment"] = google_labels_df["review_sentiment"].replace(
        "", None
    )
    google_labels_df = google_labels_df.dropna(
        subset=["review_sentiment", "review_labels"], how="all"
    )
    google_labels_df.to_gbq(
        "sql-server-replicate.Review.GoogleLabel", if_exists="append"
    )
