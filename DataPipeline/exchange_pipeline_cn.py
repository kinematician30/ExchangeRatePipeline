#!/usr/bin/env python3

import logging
from datetime import datetime, date

import psycopg2
import requests
import yaml

start_date = date.today()  # date for all requests

# Configure logging
logging.basicConfig(filename='..\logs\.log',
                    level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


# load database connection variables and connect to database
def connectDB() -> tuple:
    logger.info("connecting to database...")
    with open('..\conn.yaml', 'r') as file:
        config = yaml.safe_load(file)

        host = config.get('host')
        user = config.get('user')
        db = config.get('database')
        password = config.get('password')
        port = config.get('port')

        conn = psycopg2.connect(
            dbname=db,
            user=user,
            password=password,
            host=host,
            port=port
        )
        cur = conn.cursor()
        logger.info('Connected Successfully!')
        print('Connected successfully!')
    return conn, cur


dt = datetime.strftime(date.today(), '%Y-%m-%d')


# refs
def get_data():
    logger.info('Making a request....')
    # urls
    url_1 = f"https://cdn.jsdelivr.net/npm/@fawazahmed0/currency-api@{dt}/v1/currencies/ngn.json"
    url_2 = f'https://{dt}.currency-api.pages.dev/v1/currencies/ngn.json'
    try:
        resp = requests.get(url_1)
        data = resp.json()['ngn']

        logger.info("Successful Connection and Retrieval")

        return data
    except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
        logger.info(f"{url_1} bad, switching to {url_2}")
        resp = requests.get(url_2)
        data = resp.json()['ngn']

        logger.info("Successful Connection and Retrieval")

        return data


def extract_store_rate(data, connection, cursor):
    logger.info("Extracting data from api response...")
    rates = data['ngn']
    for k, v in rates.items():
        query = """
        INSERT INTO exchange_rate_ngn(curr_code, rate, ex_date) 
        VALUES (%s, %s, %s)
        """

        cursor.execute(query, (k, v, dt))
        logger.info(f"Inserted {k} and {v} on {dt}")

        # save the data
        connection.commit()
        # close the connection
        cursor.close()
        connection.close()


def main():
    conn, curr = connectDB()
    rates_data = get_data()
    print(rates_data)
    extract_store_rate(data=rates_data, connection=conn, cursor=curr)


if __name__ == "__main__":
    main()