import time
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

SEARCH_KEYWORD = "khách sạn vũng tàu"


def search_place(driver):

    time.sleep(5)

    search_box = driver.find_element(
        By.XPATH,
        "/html/body/div[1]/div[2]/div[9]/div[3]/div[1]/div[1]/div/div[1]/form/input"
    )

    search_box.send_keys(SEARCH_KEYWORD)
    search_box.send_keys(Keys.ENTER)

    time.sleep(5)

def open_reviews(driver):

    review_btn = WebDriverWait(driver, 20).until(
        EC.element_to_be_clickable((
            By.XPATH,
            "/html/body/div[1]/div[2]/div[9]/div[8]/div/div/div[1]/div[3]/div/div[1]/div/div/div[2]/div[3]/div/div[1]/button[3]/div[2]/div[2]"
        ))
    )

    review_btn.click()

    time.sleep(5)


def scroll_reviews(driver):

    for _ in range(1):

        driver.execute_script("""
        document.querySelectorAll('*').forEach(function(el){
            if(el.scrollHeight > el.clientHeight){
                el.scrollTop = el.scrollHeight;
            }
        });
        """)

        time.sleep(2)


def get_reviews(driver, hotel_name):

    reviews = []

    scroll_reviews(driver)

    review_elements = driver.find_elements(By.CSS_SELECTOR, "div.jftiEf")
    more_buttons = driver.find_elements(By.CSS_SELECTOR, "button.w8nwRe.kyuRq")

    for btn in more_buttons:
        try:
            driver.execute_script("arguments[0].click();", btn)
            time.sleep(0.3)
        except:
            pass
    for r in review_elements:

        try:
            name = r.find_element(By.CLASS_NAME, "d4r55").text
        except:
            name = None

        try:
            rating = r.find_element(By.CLASS_NAME, "DU9Pgb").text
        except:
            rating = None

        try:
            comment = r.find_element(By.CLASS_NAME, "wiI7pd").text
        except:
            comment = None

        try:
            date = r.find_element(By.CLASS_NAME, "xRkPPb").text
        except:
            date = None

        reviews.append({
            "hotel_name": hotel_name,
            "reviewer": name,
            "rating": rating,
            "date": date,
            "comment": comment
        })

    return reviews


def crawl_places(driver):

    all_reviews = []

    results = driver.find_elements(By.CSS_SELECTOR, "a.hfpxzc")

    print("Total results:", len(results))

    for i in range(len(results)):

        print("Processing result:", i)

        results = driver.find_elements(By.CSS_SELECTOR, "a.hfpxzc")

        results[i].click()

        time.sleep(5)

        hotel_name = WebDriverWait(driver,10).until(
            EC.presence_of_element_located((By.CLASS_NAME,"DUwDvf"))
        ).text

        print("Hotel:", hotel_name)

        try:
            open_reviews(driver)

            reviews = get_reviews(driver, hotel_name)

            all_reviews.extend(reviews)

        except:
            print("No reviews found")

        driver.back()
        time.sleep(5)

    return all_reviews


def main():

    chrome_options = Options()

    chrome_options.add_argument("--lang=vi")
    chrome_options.add_experimental_option(
        "prefs", {"intl.accept_languages": "vi,vi_VN"}
    )

    driver = webdriver.Chrome(options=chrome_options)

    driver.get("https://www.google.com/maps")

    search_place(driver)

    reviews = crawl_places(driver)

    driver.quit()

    df = pd.DataFrame(reviews)

    df.to_csv("google_reviews.csv", index=False)

    print("Done. Total reviews:", len(df))


if __name__ == "__main__":
    main()
    
    
    
    
    