from datetime import datetime
from bs4 import BeautifulSoup
import requests
import pandas as pd
import numpy as np
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator
from airflow.utils.timezone import make_aware
import pytz

# Function to extract Product Title
def get_title(soup):
    try:
        # Outer Tag Object
        title = soup.find("span", attrs={"id":'productTitle'})

        # Inner NavigatableString Object
        title_value = title.text

        # Title as a string value
        title_string = title_value.strip()

    except AttributeError:
        title_string = ""

    return title_string

# Function to extract Product Price
def get_price(soup):
    try:
        price = soup.find("span", attrs={'class':'a-price-whole'}).text.rstrip('.')

    except AttributeError:
        try:
            # If there is some deal price
            price = soup.find("span", attrs={'class':'a-price a-text-price'}).text.rstrip('.')

        except:
            price = ""

    return price


# Function to extract Product Rating
def get_rating(soup):
    try:
        rating = soup.find("i", attrs={'class':'a-icon a-icon-star a-star-4-5'}).string.strip()

    except AttributeError:
        try:
            rating = soup.find("span", attrs={'class':'a-icon-alt'}).string.strip()
        except:
            rating = ""

    return rating

# Function to extract Number of User Reviews
def get_review_count(soup):
    try:
        review_count = soup.find("span", attrs={'id':'acrCustomerReviewText'}).string.strip()

    except AttributeError:
        review_count = ""

    return review_count

# Function to extract Availability Status
def get_availability(soup):
    try:
        available = soup.find("div", attrs={'id':'availability'})
        available = available.find("span").string.strip()

    except AttributeError:
        available = "Not Available"

    return available

def scrape_amazon():
    HEADERS = ({'User-Agent':'', 'Accept-Language': 'en-US, en;q=0.5'})

    d = {"title": [], "price": [], "rating": [], "reviews": [], "availability": []}

    for i in range(1, 21):
        url = f'https://www.amazon.in/s?k=smartphone&i=electronics&rh=n%3A1389401031&page={i}'
        try:
            response = requests.get(url, headers=HEADERS)

            if response.status_code == 200:
                soup = BeautifulSoup(response.text, "html.parser")

                links = soup.find_all("a", class_='a-link-normal s-no-hover s-underline-text s-underline-link-text s-link-style a-text-normal')
                links_list = [link.get('href') for link in links]

                for link in links_list:
                    new_url = "https://www.amazon.in" + link
                    new_response = requests.get(new_url, headers=HEADERS)

                    if new_response.status_code == 200:
                        new_soup = BeautifulSoup(new_response.text, "html.parser")

                        d['title'].append(get_title(new_soup))
                        d['price'].append(get_price(new_soup))
                        d['rating'].append(get_rating(new_soup))
                        d['reviews'].append(get_review_count(new_soup))
                        d['availability'].append(get_availability(new_soup))
                    else:
                        print(f"Failed to retrieve data from {new_url}. Status code: {new_response.status_code}")

            else:
                print(f"Failed to retrieve data from {url}. Status code: {response.status_code}")

        except requests.exceptions.RequestException as e:
            print(f"Error making request to {url}: {e}")

    # Move DataFrame creation and CSV writing outside the loop
    amazon_df = pd.DataFrame.from_dict(d)
    amazon_df['title'].replace('', np.nan, inplace=True)
    amazon_df = amazon_df.dropna(subset=['title'])
    amazon_df.to_csv("amazon_data.csv", header=True, index=False)

def send_email_notification():
    # Your email notification code
    email_task = EmailOperator(
        task_id='send_email_notification',
        to='your@email.com',
        subject='Amazon Web Scraping Completed',
        html_content='The Amazon web scraping task has completed successfully.',
        dag=dag
    )
    email_task.execute(context=None)

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': make_aware(datetime(2024, 4, 1, 11, 0, 0), pytz.timezone('Asia/Kolkata')),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

# Define the DAG
dag = DAG(
    'amazon_web_scraping_dag',
    default_args=default_args,
    schedule_interval='0 11 * * *',  # Daily at 11:00 AM IST
    catchup=False
)

# PythonOperators
scrape_task = PythonOperator(
    task_id='scrape_amazon',
    python_callable=scrape_amazon,
    dag=dag
)

email_task = PythonOperator(
    task_id='send_email_notification',
    python_callable=send_email_notification,
    dag=dag
)

# Set task dependencies
email_task >> scrape_task
