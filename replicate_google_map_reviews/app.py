"""Init google map reviews replication code"""

__author__ = "Thanh Do"
__version__ = "1.0.0"
__task__ = "DA-216"
__maintainer__ = "Thanh Do"
__email__ = "thanh.do@jrgvn.com"
__status__ = "Production"

import os
import re
import time
import pytz
import json
import pandas as pd
from datetime import datetime
from selenium import webdriver
from google.oauth2 import service_account
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
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from config.project_setup import PROJECT_VARIABLES
from models.postgres_credential import PostgresDBCredential
from src.utils.secret_factory.providers.gcp_secret_manager import GCPSecretManager
from src.utils.secret_factory.secret_factory import SecretFactory


def get_all_data(driver, store_id, total_reviews, directory, limit=None):
    """
    this function get main text, score, name
    Args:
        driver: Selenium driver instance
        store_id: Store id in PHVN format
        total_reviews: Total number of reviews
        directory: Directory to store the logs
        limit: Limit the number of reviews that needed to get
    """
    print("get data...")
    time.sleep(5)
    more_elements = driver.find_elements_by_css_selector(".w8nwRe.kyuRq")
    driver.implicitly_wait(20)
    for list_more_element in more_elements:
        try:
            list_more_element.click()
            driver.implicitly_wait(20)
        except:
            print("Element not interactable => Skipping")
    # Getting translated review (by Google), TODO will use later on
    # translate_elements = driver.find_elements_by_xpath('//*[@id="QA0Szd"]/div/div/div[1]/div[2]/div/div[1]/div/div/div[2]/div[10]/div[4]/div/div/div[4]/div[3]/button/span')
    # for translate_element in translate_elements:
    #     text = translate_element.text
    #     if text == 'Xem bản dịch (Tiếng Việt)':
    #         translate_element.click()

    # elements = driver.find_elements_by_class_name(
    # 'jftiEf')

    elements = driver.find_elements(By.CSS_SELECTOR, "[data-review-id].jftiEf")
    review_ids = [element.get_attribute("data-review-id") for element in elements]
    if limit is not None:
        try:
            limit = review_ids.index(limit)
        except:
            print("Limit out => breaking")
            return
    lst_data = []
    no = 0
    logs_raw = driver.get_log("performance")
    logs = [json.loads(lr["message"])["message"] for lr in logs_raw]
    # def log_filter(log_):
    #     return (
    #         # is an actual response
    #         log_["method"] == "Network.responseReceived"
    #         # and json
    #         and "json" in log_["params"]["response"]["mimeType"]
    #     )
    print(f"Printing log to {directory}")
    for log in logs:
        try:
            request_id = log["params"]["requestId"]
            resp_url = log["params"]["response"]["url"]
            # print(f"Caught {resp_url}")
            # https://www.google.com/maps/rpc/listugcposts?authuser=0&hl=vi&pb=!1m8!1s0x31752efd601e21c9%3A0x11b9dcabe9ec271d!3s!6m4!4m1!1e1!4m1!1e3!9b0!2m2!1i10!2s!5m2!1sSKxSZqbZDvLQ2roPqrWKuAw!7e81!8m5!1b1!2b1!3b1!5b1!7b1!11m6!1e3!2e1!3svi!4sUS!6m1!1i2!13m1!1e2
            if (
                "https://www.google.com/maps/reviews" in resp_url
                or "https://www.google.com/maps/rpc/listugcposts" in resp_url
                or "https://www.google.com/maps/place" in resp_url
            ):
                try:
                    # Get the API responses
                    res = driver.execute_cdp_cmd(
                        "Network.getResponseBody", {"requestId": request_id}
                    )
                    with open(
                        f"{directory}/network_log/{store_id}_network_log_{request_id}.json",
                        "w",
                        encoding="utf-8",
                    ) as f:
                        dict_string = str(res["body"])
                        f.write(dict_string)
                except Exception as err:
                    # print(77, err)
                    continue
        except Exception as err2:
            # print(80, err2)
            continue

    for data in elements:
        print(f"loop #{no}")
        print(f"Store ID: {store_id}, {len(lst_data)} out of {total_reviews}")
        review_id = data.get_attribute("data-review-id")
        contributor_id = ""

        button_element = data.find_element(By.CSS_SELECTOR, "button[data-href]")

        # Extract the value of data-href attribute
        contributor_id_raw = button_element.get_attribute("data-href")
        match = re.search(r"(?<=/)\d+(?=/)", contributor_id_raw)

        # If match found, extract the numeric part
        if match:
            numeric_part = match.group(0)
            contributor_id = numeric_part
        else:
            print("No match found.")
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
        inner_text = ""
        try:
            inner_text = data.find_element_by_xpath(
                './/div[@class="MyEned"]/span[1]'
            ).text
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
            # Accept default is_reply (False)
            pass
        # DA-1007: Process getting review link
        share_link_value = None
        share_element = data.find_elements_by_class_name("GBkF3d")
        if len(share_element) != 0:
            print("share_ element: ", share_element)
            # There are two elements with this class name, first one is 'Like', second one is 'Share'
            share_element[1].click()
            driver.implicitly_wait(10)
            # Extract the share link out,
            input_element = driver.find_element_by_css_selector(
                "input.vrsrZe[type='text']"
            )
            share_link_value = input_element.get_attribute("value")
            # Close the share link modal
            driver.implicitly_wait(10)
            close_modal_el = driver.find_element_by_xpath(
                '//*[@id="modal-dialog"]/div/div[2]/div/button'
            )
            close_modal_el.click()

        lst_data.append(
            [
                review_id,
                contributor_id,
                store_id,
                name,
                inner_text,
                score,
                date_ranges,
                is_reply,
                tag_dictionaries_in_string,
                share_link_value,
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


def scrolling_until_element_found(
    driver, target_data_review_id, max_attempts=10, scroll_pause_time=3, is_first=False
):
    """Scroll until an element with a specific data-review-id is found or the maximum number of attempts is reached.

    Args:
        driver: Selenium driver instance
        target_data_review_id: The data-review-id of the target element
        max_attempts: Maximum number of scroll attempts before giving up
        scroll_pause_time: Time in seconds to wait between scrolls
        is_first: Use this as a flag to stop getting Google Reviews.
    """
    print("scrolling...")

    attempts = 0

    while attempts < max_attempts:
        try:
            # Check if the element with the specific data-review-id is present
            element = WebDriverWait(driver, 5).until(
                EC.presence_of_element_located(
                    (By.CSS_SELECTOR, f"div[data-review-id='{target_data_review_id}']")
                )
            )
            # If found, break the loop
            if element:
                print(f"Element with data-review-id {target_data_review_id} found.")
                return element  # Return the found element

        except TimeoutException:
            # If the target element is not found within the timeout, perform a scroll
            print(
                f"Scrolling... Attempt {attempts + 1} of {max_attempts}. Element not found yet."
            )
            try:
                scrollable_div = WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located(
                        (By.CSS_SELECTOR, "div.lXJj5c.Hk4XGb")
                    )
                )
            except TimeoutException as err:
                print(f"Error when finding element at {is_first} retry", err)
                if is_first:
                    return None
                raise err
            # driver.execute_script(
            #     'document.getElementsByClassName("dS8AEf")[0].scrollTop = document.getElementsByClassName("dS8AEf")[0].scrollHeight',
            #     scrollable_div
            # )
            actions = ActionChains(driver)
            actions.move_to_element(scrollable_div).perform()
            print("Moved to the scrollable div.")
            time.sleep(scroll_pause_time)
            attempts += 1

        except Exception as others:
            print("Other exceptions occurred!", others)
            if not is_first:
                raise others

    print(
        f"Element with data-review-id {target_data_review_id} not found after {max_attempts} attempts."
    )
    return None  # Return None if the element is not found after the maximum attempts


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
    ]
    df = pd.DataFrame(data, columns=cols)
    df.to_excel(file_name)


def main():
    """Daily replicate Google Map reviews by store"""
    file_path = f"{PROJECT_VARIABLES['repo_path']}/src/data/selenium/replicate_google_map_reviews/store_data.csv"
    tz = pytz.timezone("Asia/Ho_Chi_Minh")
    current_time_now = datetime.now(tz)
    current_date = current_time_now.date()
    formatted_date = current_date.strftime("%Y-%m-%d")
    df = pd.read_csv(file_path)
    if current_time_now.hour < 12:
        no_run = 1
    else:
        no_run = 2
    main_data_directory = f"{PROJECT_VARIABLES['repo_path']}/src/data/selenium/replicate_google_map_reviews/data/{formatted_date}/{no_run}"
    os.makedirs(f"{main_data_directory}/network_log", exist_ok=True)
    query = """
        WITH RankedReviews AS (
            SELECT 
                review_id, 
                s_no, 
                review_date,
                ROW_NUMBER() OVER (PARTITION BY s_no ORDER BY review_date DESC) AS rn
            FROM `sql-server-replicate.Review.GoogleReviews`
        )
        SELECT 
            s_no,
            review_id, 
            review_date
        FROM RankedReviews
        WHERE rn <= 10
        ORDER BY s_no, review_date DESC

    """
    fs = pd.read_gbq(query)
    result_dict = {}
    # Iterate over the DataFrame rows to build the dictionary
    for _, row in fs.iterrows():
        store_id = row["s_no"]
        review_id = row["review_id"]
        # If store_id is not already in the dictionary, create a new list for it
        if store_id not in result_dict:
            result_dict[store_id] = []

        # Append the review_id to the list of the corresponding store
        result_dict[store_id].append(review_id)

    # result_dict = fs.set_index('s_no')[['first_review_id', 'second_review_id']].apply(tuple, axis=1).to_dict()
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
    query = f"""
        SELECT * FROM public.google_review_metadata
        WHERE "Brand_Name" = 'PHVN'
    """
    previous_df = pd.read_sql_query(text(query), con=engine)
    previous_df["S_No"] = previous_df["S_No"].astype(str)

    for _, row in df.iterrows():
        # Start for loop for list store to be proccessed
        store_id = str(row["Store_ID"])
        previous_store_metadata = previous_df[previous_df["S_No"] == store_id]

        # Skip if there's no previous metadata (i.e., first time scraping)
        # Since we have no data to compare, we proceed to scrape all reviews
        if previous_store_metadata.empty:
            previous_number_of_crawled_rows = 0
        else:
            previous_number_of_crawled_rows = int(
                previous_store_metadata["No_Rows"].iloc[0]
            )
        # No need to init limit_review_id, because it will be init after this if/else
        # Check if the store_id exists in result_dict
        if store_id in result_dict:
            limit_review_id = result_dict[store_id][0]
        else:
            # If store_id is not in result_dict, set limit_review_id to None
            limit_review_id = None

        subprovince = row["subprovince"]
        title = row["Title"]
        url = row["URL"]
        if not url or str(url).lower() == "nan":
            continue

        file_names = []
        try:
            file_names_raw = os.listdir(main_data_directory)
            if file_names_raw:
                file_names = [os.path.splitext(i)[0] for i in file_names_raw]
        except FileNotFoundError:
            pass

        if store_id in file_names:
            continue

        print(
            f"Getting user reviews for store {store_id}, title: {title}, subprovince: {subprovince}"
        )
        print(url)
        desired_capabilities = DesiredCapabilities.CHROME
        desired_capabilities["goog:loggingPrefs"] = {"performance": "ALL"}
        options = webdriver.ChromeOptions()
        options.add_argument("--headless")  # show browser or not
        options.add_argument("--lang=vi")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-gpu")
        options.add_experimental_option("prefs", {"intl.accept_languages": "en,en_US"})
        DriverPath = DriverLocation
        driver = webdriver.Chrome(
            executable_path=DriverPath,
            options=options,
            desired_capabilities=desired_capabilities,
        )
        driver.get(url)
        # There are 2 modes (all/latest)
        time.sleep(5)
        counter = 0
        total_reviews = 0
        try:
            counter, total_reviews = scroll_counter(driver)
        except Exception:
            print(
                f"Store {store_id}, title: {title}, subprovince: {subprovince} does not have user reviews!"
            )
            continue  # Skip this store if no reviews are found

        if counter == 0:
            print(f"Skipping store: {store_id}, because this store has no reviews")
            continue

        if not is_get_all_review:
            number_of_reviews_to_get = total_reviews - previous_number_of_crawled_rows
        else:
            number_of_reviews_to_get = total_reviews

        if is_get_all_review or limit_review_id is None:
            # If we are getting all reviews or limit_review_id is None, scroll through all reviews
            scrolling(counter, driver)
        else:
            try:
                # Click on the dropdown to sort by latest
                latest_el_dropdown = driver.find_element_by_xpath(
                    '//*[@id="QA0Szd"]/div/div/div[1]/div[2]/div/div[1]/div/div/div[2]/div[7]/div[2]/button/span'
                )
                latest_el_dropdown.click()
            except Exception as err:
                print(f"Error finding latest_el_dropdown: {err}")
                # Try alternative XPaths if the first one fails
                alternative_xpaths = [
                    '//*[@id="QA0Szd"]/div/div/div[1]/div[2]/div/div[1]/div/div/div[2]/div[8]/button/div[1]',
                    '//*[@id="QA0Szd"]/div/div/div[1]/div[2]/div/div[1]/div/div/div[2]/div[6]/button/div[1]',
                    '//*[@id="QA0Szd"]/div/div/div[1]/div[2]/div/div[1]/div/div/div[2]/div[9]/button/div[1]',
                    '//*[@id="QA0Szd"]/div/div/div[1]/div[2]/div/div[1]/div/div/div[2]/div[7]/button/div[1]',
                ]
                for xpath in alternative_xpaths:
                    try:
                        latest_el_dropdown = driver.find_element_by_xpath(xpath)
                        latest_el_dropdown.click()
                        break  # Exit the loop if successful
                    except Exception as err2:
                        print(f"Alternative XPath failed: {xpath}, error: {err2}")
                        continue  # Try the next XPath
                else:
                    print(
                        "Failed to find and click on the latest_el_dropdown. Skipping this store."
                    )
                    continue  # Skip this store if none of the XPaths work

            try:
                # Click on the "Latest" option
                latest_el = driver.find_element_by_xpath(
                    '//*[@id="action-menu"]/div[2]'
                )
                latest_el.click()
            except Exception as err:
                print(f"Error clicking on the latest option: {err}")
                continue  # Skip this store if unable to sort by latest

            driver.implicitly_wait(10)
            remaining_counter = int(number_of_reviews_to_get / 10) + 1
            remaining_counter = max(remaining_counter, 20)  # Ensure at least 20 scrolls

            # Scroll until the limit_review_id is found
            list_review_ids = result_dict[store_id]
            for id in list_review_ids:
                # start for loop for list review_id that needed to be checked
                el_status = scrolling_until_element_found(driver, id, is_first=True)
                if el_status != None:
                    limit_review_id = id
                    break
            # end for loop for list review_id that needed to be checked
            driver.implicitly_wait(10)

        data = get_all_data(
            driver,
            store_id,
            total_reviews,
            directory=main_data_directory,
            limit=limit_review_id,
        )
        driver.delete_all_cookies()
        driver.refresh() 
        driver.close()
        write_to_xlsx(data, file_name=f"{main_data_directory}/{store_id}.xlsx")
        print("Done!")
    # End for loop for list store to be proccessed
