import time
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
SEARCH_KEYWORD = "starbucks quận 2"


def search_place(driver):

    time.sleep(5)

    # click vào thanh search
    search_box = driver.find_element(
        By.XPATH,
        "/html/body/div[1]/div[2]/div[9]/div[3]/div[1]/div[1]/div/div[1]/form/input"
    )

    search_box.click()
    search_box.send_keys(SEARCH_KEYWORD)
    search_box.send_keys(Keys.ENTER)

    time.sleep(5)

    # click kết quả đầu tiên
    first_result = driver.find_element(By.CSS_SELECTOR, "a.hfpxzc")
    first_result.click()

    time.sleep(5)

from selenium.webdriver.support import expected_conditions as EC


def open_reviews(driver):

    review_btn = WebDriverWait(driver, 20).until(
        EC.element_to_be_clickable((
            By.XPATH,
            "/html/body/div[1]/div[2]/div[9]/div[8]/div/div/div[1]/div[3]/div/div[1]/div/div/div[2]/div[3]/div/div[1]/button[3]/div[2]/div[2]"
        ))
    )

    review_btn.click()

    time.sleep(5)


def get_reviews(driver):

    reviews = []

    driver.execute_script("""
    var el = document.querySelector('div.m6QErb');
    if(el){
        el.scrollTop = el.scrollHeight;
    }
    """)

    review_elements = driver.find_elements(By.CSS_SELECTOR, "div.jftiEf")

    for r in review_elements:

        try:
            name = r.find_element(By.CLASS_NAME, "d4r55").text
        except:
            name = None

        try:
            rating = r.find_element(By.CLASS_NAME, "kvMYJc").get_attribute("aria-label")
        except:
            rating = None

        try:
            comment = r.find_element(By.CLASS_NAME, "wiI7pd").text
        except:
            comment = None

        try:
            date = r.find_element(By.CLASS_NAME, "rsqaWe").text
        except:
            date = None

        reviews.append({
            "name": name,
            "rating": rating,
            "date": date,
            "comment": comment
        })

    return reviews


def main():

    chrome_options = Options()

    driver = webdriver.Chrome(options=chrome_options)

    driver.get("https://www.google.com/maps")

    search_place(driver)

    open_reviews(driver)

    reviews = get_reviews(driver)

    driver.quit()

    df = pd.DataFrame(reviews)

    df.to_csv("google_reviews.csv", index=False)

    print("Done. Total reviews:", len(df))


if __name__ == "__main__":
    main()