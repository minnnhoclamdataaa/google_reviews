import os
import glob
import pytz
import json
import pandas as pd
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from src.data.selenium.replicate_google_map_reviews.config import DriverLocation
from config.project_setup import PROJECT_VARIABLES


def main():
    # Define the path to the directory containing the .xlsx files
    tz = pytz.timezone("Asia/Ho_Chi_Minh")
    current_time_now = datetime.now(tz)
    current_date = current_time_now.date()
    formatted_date = current_date.strftime("%Y-%m-%d")
    folder_path = f"{PROJECT_VARIABLES['repo_path']}/src/data/selenium/replicate_google_map_reviews/data"
    if current_time_now.hour < 12:
        no_run = 1
    else:
        no_run = 2
    excel_dir = f"{folder_path}/{formatted_date}/{no_run}/"
    processed_dir = f"{folder_path}/{formatted_date}/{no_run}/processed/"
    os.makedirs(processed_dir, exist_ok=True)
    print(f"Directory created: {processed_dir}")
    # Define the pattern to match the .xlsx files
    excel_pattern = os.path.join(excel_dir, "*.xlsx")
    processed_pattern = os.path.join(processed_dir, "*.csv")
    # Get a list of all .xlsx files in the directory
    excel_files = glob.glob(excel_pattern)
    processed_files = glob.glob(processed_pattern)

    excel_filenames = {os.path.basename(f) for f in excel_files}
    processed_filenames = {os.path.basename(f).split(".")[0] for f in processed_files}
    unprocessed_files = [
        f
        for f in excel_files
        if os.path.basename(f).split(".")[0] not in processed_filenames
    ]

    # Configure Selenium to capture network logs
    desired_capabilities = DesiredCapabilities.CHROME
    desired_capabilities["goog:loggingPrefs"] = {"performance": "ALL"}
    options = webdriver.ChromeOptions()
    options.add_argument("--headless")  # show browser or not
    options.add_argument("--lang=vi")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_experimental_option("prefs", {"intl.accept_languages": "en,en_US"})
    driver = webdriver.Chrome(
        executable_path=DriverLocation,
        options=options,
        desired_capabilities=desired_capabilities,
    )
    for t_p in unprocessed_files:
        excel_file = f"{t_p}"
        df = pd.read_excel(excel_file)
        # excel_file = 'data/2024-05-26\\D812.xlsx'
        file_name = os.path.basename(excel_file)
        file_id = os.path.splitext(file_name)[0]
        # Print the current Excel file being processed
        print(f"Processing Excel file: {file_name}")

        # Define the pattern to match the corresponding .json files
        json_pattern = f"{folder_path}/{formatted_date}/{no_run}/network_log/{file_id}_network_log_*.json"

        # Get a list of all matching .json files
        json_files = glob.glob(json_pattern)
        import re

        result_dict = {}
        # Loop through each .json file
        for json_file in json_files:
            # Read the JSON file
            with open(json_file, "r", encoding="utf-8") as f:
                json_data = f.read()
            json_data = json_data.replace('\\"', '"')
            pattern = re.compile(
                r'"([A-Za-z0-9]+)",\["0x0:0x[a-fA-F0-9]+",null,(\d+),(\d+),'
            )
            # Find all matches of the pattern in the file contents
            matches = pattern.findall(json_data)
            # Output the matching strings
            if matches:
                print("Matching strings found in the file:")
                for match in matches:
                    if match:
                        # print(match)
                        dynamic_string = match[0]
                        value = int(
                            match[2]
                        )  # Considering you want to convert the value to integer
                        result_dict[dynamic_string] = value
                        # print("Resulting dictionary:", result_dict)
                    else:
                        # print(json_data)
                        print("No match found.")

            else:
                print("No matching strings found in the file.")

        filtered_df = df[~df["review_id"].isin(result_dict.keys())]
        if len(filtered_df) == 0:
            # Define the function to capture logs
            def capture_logs(store_id):
                logs_raw = driver.get_log("performance")
                logs = [json.loads(lr["message"])["message"] for lr in logs_raw]

                # file_paths = []
                def log_filter(log_):
                    return (
                        log_["method"] == "Network.responseReceived"
                        and "json" in log_["params"]["response"]["mimeType"]
                    )

                # print(36, logs)
                for log in logs:
                    try:
                        request_id = log["params"]["requestId"]
                        resp_url = log["params"]["response"]["url"]

                        # print(resp_url)
                        if (
                            "https://www.google.com/maps/reviews" in resp_url
                            or "https://www.google.com/maps/rpc/listugcposts"
                            in resp_url
                            or "https://www.google.com/maps/place" in resp_url
                        ):
                            # print(44, resp_url)
                            try:
                                res = driver.execute_cdp_cmd(
                                    "Network.getResponseBody", {"requestId": request_id}
                                )
                                with open(
                                    f"data/{store_id}_network_log_{request_id}.json",
                                    "w",
                                    encoding="utf-8",
                                ) as f:
                                    dict_string = str(res["body"])
                                    f.write(dict_string)
                                # file_paths.append(f"data/{store_id}_network_log_{request_id}.json")
                            except Exception as err1:
                                print(err1)
                    except Exception as err2:
                        print(err2)
                # return file_paths

            for index, row in filtered_df.iterrows():
                review_id = row["review_id"]
                review_link = row["review_link"]
                store_id = row["s_no"]
                # print(review_id, review_link)
                driver.get(review_link)
                # time.sleep(5)  # Wait for the page to load and logs to be captured
                capture_logs(store_id)

                # Print the current Excel file being processed
            print(f"Processing Excel file: {file_name}")

            # Define the pattern to match the corresponding .json files
            json_pattern = f"{folder_path}/{formatted_date}/{no_run}/network_log/{file_id}_network_log_*.json"

            # Get a list of all matching .json files
            json_files = glob.glob(json_pattern)
            import re

            result_dict = {}
            result_dict_review_link = {}
            # Loop through each .json file
            for json_file in json_files:
                # Read the JSON file
                with open(json_file, "r", encoding="utf-8") as f:
                    json_data = f.read()
                json_data = json_data.replace('\\"', '"')

                pattern = re.compile(
                    r'"([A-Za-z0-9]+)",\["0x0:0x[a-fA-F0-9]+",null,(\d+),(\d+),'
                )
                matches = pattern.findall(json_data)
                if matches:
                    print("Matching strings found in the file:")
                    for match in matches:
                        if match:
                            dynamic_string = match[0]
                            value = int(
                                match[2]
                            )  # Considering you want to convert the value to integer
                            result_dict[dynamic_string] = value
                        else:
                            print("No match found.")
                else:
                    print("No matching strings found in the file.")
                for review_id_key in result_dict.keys():
                    # Find all matches
                    pattern_review_link = rf"https://www\.google\.com/maps/reviews/data[^\"]*{review_id_key}[^\"]*"
                    matches_review_links = re.findall(pattern_review_link, json_data)
                    decoded_matches = [
                        match.encode().decode("unicode-escape")
                        for match in matches_review_links
                    ]
                    # Print the results
                    for match_review_link in decoded_matches:
                        result_dict_review_link[review_id_key] = match_review_link
                        break

            # Optionally, read and process the Excel file
            excel_data = pd.read_excel(excel_file)

            # print(excel_data.head())
            def get_timestamp(review_id):
                return result_dict.get(review_id, None)

            def extract_review_link(review_id):
                return result_dict_review_link.get(review_id, None)

            excel_data["timestamp"] = excel_data["review_id"].apply(get_timestamp)
            excel_data["review_link"] = excel_data["review_id"].apply(
                extract_review_link
            )
            excel_data.to_csv(f"{processed_dir}/{file_id}.csv", index=False)
    driver.quit()
