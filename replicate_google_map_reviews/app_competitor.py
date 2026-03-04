"""Init google map reviews replication code"""

__author__ = "Thanh Do"
__version__ = "1.0.0"
__task__ = "DA-216"
__maintainer__ = "Thanh Do"
__email__ = "thanh.do@jrgvn.com"
__status__ = "Production"
import os
import time
import pytz
import json
import pandas as pd
from datetime import datetime
from selenium import webdriver
from urllib.parse import quote_plus
from sqlalchemy import create_engine, text
from src.data.selenium.replicate_google_map_reviews.config import (
    DriverLocation,
    is_get_all_review,
)
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from config.project_setup import PROJECT_VARIABLES
from models.postgres_credential import PostgresDBCredential
from src.utils.secret_factory.providers.gcp_secret_manager import GCPSecretManager
from src.utils.secret_factory.secret_factory import SecretFactory

def get_data(driver, store_id, competitor, limit=None):
    """
    this function get main text, score, name
    Args:
        driver: Selenium driver instance
        store_id: Store id in PHVN format
    """
    print("get data...")
    more_elemets = driver.find_elements_by_css_selector(".w8nwRe.kyuRq")
    for list_more_element in more_elemets:
        list_more_element.click()
    elements = driver.find_elements_by_class_name("jftiEf")
    lst_data = []
    no = 0
    for data in elements:
        print(f"loop #{no}")
        no += 1
        if limit is not None and no > limit:
            print("breaking")
            break
        inner_elements = data.find_elements_by_xpath(".//*")
        # Tag
        tag_elements = data.find_elements_by_class_name("PBK6be")
        name = ""
        for inner_element in inner_elements:
            d4r55_element = inner_element.find_element_by_class_name("d4r55")
            name = d4r55_element.text
            break
        text = ""
        try:
            text = data.find_element_by_xpath('.//div[@class="MyEned"]/span[1]').text
        except Exception as err:
            print("review w/o comment", err)
        score = data.find_element_by_xpath('.//span[@class="kvMYJc"]').get_attribute(
            "aria-label"
        )
        date_ranges = data.find_element_by_xpath('.//span[@class="rsqaWe"]').text
        tag_dictionaries = {}

        for tag_el in tag_elements:

            inner_tags = tag_el.find_elements_by_class_name("RfDO5c")

            if len(inner_tags) == 2:

                key, value = inner_tags[0].text, inner_tags[1].text

                tag_dictionaries[key] = value

            elif len(inner_tags) == 1:

                holder_text = inner_tags[0].text

                key_val_raws = holder_text.split(":")

                key, value = key_val_raws[0], key_val_raws[1].strip()

                tag_dictionaries[key] = value

        # print(84, tag_dictionaries)

        tag_dictionaries_in_string = json.dumps(tag_dictionaries)
        # Check if PHVN has replied the review
        is_reply = False
        is_reply_xpath_expression = './/span[@class="nM6d2c"]'
        try:
            driver.find_element_by_xpath(is_reply_xpath_expression)
            is_reply = True
        except NoSuchElementException:
            # Accept defaut is_reply (False)
            pass
        lst_data.append(
            [
                store_id,
                name,
                text,
                score,
                date_ranges,
                is_reply,
                competitor,
                tag_dictionaries_in_string,
            ]
        )

    return lst_data


def scroll_counter(driver):
    """Count the number of scrolls needed to get all google map reviews.
    Args:
        driver: Selenium driver instance
    Return:
        Number of scrolls in integer
    """
    result = (
        driver.find_element_by_class_name("jANrlb")
        .find_element_by_class_name("fontBodySmall")
        .text
    )
    result = result.replace(",", "")
    result = result.replace(".", "")
    result = result.split(" ")
    result = result[0].split("\n")
    total_reviews = int(result[0])
    return int(total_reviews / 10) + 1, total_reviews


def scrolling(counter, driver):
    """Start scrolling based on the number of counter
    Args:
        counter: Number of scrolls needed
        driver: Selenium driver instance
    """
    print("scrolling...")
    for _i in range(counter):
        print("Loop #", _i)
        try:
            scrollable_div = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "div.lXJj5c.Hk4XGb"))
            )
            driver.execute_script(
                'document.getElementsByClassName("dS8AEf")[0].scrollTop = document.getElementsByClassName("dS8AEf")[0].scrollHeight',
                scrollable_div,
            )
            time.sleep(3)
        except TimeoutException as time_out:
            # If we reach here then it's not scrollable => End of page => No need to scroll anymore
            print("Timeout occurred while waiting for the element.", time_out)
        except Exception as others:
            print("Other exceptions occurred!", others)
            raise others


def write_to_xlsx(data, file_name):
    """Write the result to csv file, these files will later be uploaded to bigquery
    Args:
        data: Array of strings that we will use to create a dataframe
        file_name: Name of the store that we are crawling
    """
    directory = os.path.dirname(file_name)

    # Create the directory (folder) if it doesn't exist
    if not os.path.exists(directory):
        os.makedirs(directory)
    print("write to excel...")
    cols = [
        "s_no",
        "name",
        "comment",
        "rating",
        "review_date",
        "is_reply",
        "competitor",
        "tags",
    ]
    df = pd.DataFrame(data, columns=cols)
    df.to_excel(file_name)


def main():
    """Daily replicate google map reviews by store"""
    file_path = f"{PROJECT_VARIABLES['repo_path']}/src/data/selenium/replicate_google_map_reviews/store_data_competitors.csv"
    tz = pytz.timezone("Asia/Ho_Chi_Minh")
    current_date = datetime.now(tz).date()
    formatted_date = current_date.strftime("%Y-%m-%d")
    df = pd.read_csv(file_path)
    # get previous run from Postgres
    google_sm: GCPSecretManager = SecretFactory.get_secret_provider('gcp')
    postgres_db_credentials: PostgresDBCredential = google_sm.get_secret_and_parse(
        secret_name="PHVN_Crendetials_POSTGRES",
        json_object_node="POSTGRES_DB",
        dataclass_type=PostgresDBCredential
    )    
    encoded_password = quote_plus(postgres_db_credentials.password)
    engine = create_engine(
        f"postgresql+psycopg2://{postgres_db_credentials.username}:{encoded_password}@{postgres_db_credentials.host}:{postgres_db_credentials.port}/airflow"
    )
    engine = engine.connect()
    query = f"""select * From public.google_review_metadata
                where "Brand_Name" not in ('PHVN', '4P')
    """
    print(query)
    previous_df = pd.read_sql_query(text(query), con=engine)

    previous_df["S_No"] = previous_df["S_No"].astype(str)
    print(previous_df)
    for _, row in df.iterrows():
        competitor = row["Competitor"]
        store_id = str(row["Store_ID"])
        if competitor == "4P":
            continue
        previous_store_metadata = previous_df[
            (previous_df["S_No"] == store_id)
            & (previous_df["Brand_Name"] == competitor)
        ]
        if previous_store_metadata.empty:
            continue
        previous_number_of_crawled_rows = int(
            previous_store_metadata["No_Rows"].iloc[0]
        )
        # print('here 153', previous_store_metadata)
        subprovince = row["subprovince"]
        title = row["Title"]
        url = row["URL"]
        if url == "" or url is None or str(url) == "nan":
            continue

        file_names_raw = None
        file_names = []
        try:
            file_names_raw = os.listdir(
                f"{PROJECT_VARIABLES['repo_path']}/src/data/selenium/replicate_google_map_reviews/data_competitor/{competitor}/{formatted_date}"
            )
        except Exception:
            pass
        if file_names_raw != None:
            for i in file_names_raw:
                temp = i.split(".")
                file_names.append(temp[0])
            print(file_names)
        if store_id in file_names:
            continue

        print(
            f"getting user reviews for store {store_id}, title: {title}, subprovince: {subprovince}"
        )
        print(url)
        options = webdriver.ChromeOptions()
        options.add_argument("--headless")  # show browser or not
        options.add_argument("--lang=vi")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-gpu")
        options.add_experimental_option("prefs", {"intl.accept_languages": "en,en_US"})
        DriverPath = DriverLocation
        driver = webdriver.Chrome(executable_path=DriverPath, options=options)
        driver.get(url)
        time.sleep(5)
        counter = 0
        try:
            counter, total_reviews = scroll_counter(driver)
        except Exception as err:
            print(err)
            print(
                f"store {store_id}, title: {title}, subprovince: {subprovince} dont have user reviews!"
            )
            pass
        if counter == 0:
            print(
                f"Skipping store: {store_id}, competitor: {competitor}, because this store has no review"
            )
            continue
        if not is_get_all_review and total_reviews == previous_number_of_crawled_rows:
            print(f"Skipping store: {store_id}, because we have all the review")
            continue
        number_of_reviews_to_get = None
        if not is_get_all_review:
            number_of_reviews_to_get = total_reviews - previous_number_of_crawled_rows
            print(f"Getting {number_of_reviews_to_get} remaining reviews!")
        data = None
        if is_get_all_review:
            scrolling(counter, driver)
        else:
            try:
                latest_el_dropdown = driver.find_element_by_xpath(
                    '//*[@id="QA0Szd"]/div/div/div[1]/div[2]/div/div[1]/div/div/div[2]/div[7]/div[2]/button/span'
                )
                latest_el_dropdown.click()
            except Exception as err:
                print(
                    f"Execution fail when finding latest_el_dropdown, with error: {err}"
                )
                latest_el_dropdown = driver.find_element_by_xpath(
                    '//*[@id="QA0Szd"]/div/div/div[1]/div[2]/div/div[1]/div/div/div[2]/div[8]/button/div[1]'
                )
                latest_el_dropdown.click()
            try:
                latest_el = driver.find_element_by_xpath(
                    '//*[@id="action-menu"]/div[2]'
                )
                latest_el.click()
            except Exception as err:
                print(f"Execution fail at previous latest_el, {err}, retry again ")
                latest_el = driver.find_element_by_xpath(
                    '//*[@id="QA0Szd"]/div/div/div[1]/div[2]/div/div[1]/div/div/div[2]/div[8]/button/div[1]'
                )
                latest_el.click()
            driver.implicitly_wait(10)
            remaining_counter = int(number_of_reviews_to_get / 10) + 1
            scrolling(remaining_counter, driver)
        data = get_data(driver, store_id, competitor, limit=number_of_reviews_to_get)
        # data['competitor'] = competitor
        # print(data)
        driver.close()
        write_to_xlsx(
            data,
            file_name=f"{PROJECT_VARIABLES['repo_path']}/src/data/selenium/replicate_google_map_reviews/data_competitor/{competitor}/{formatted_date}/{store_id}.xlsx",
        )
        print("Done!")


# if __name__ == "__main__":
#     main()
