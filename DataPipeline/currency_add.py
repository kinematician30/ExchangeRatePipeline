import logging
import psycopg2
import requests
import yaml

# Configure logging
logging.basicConfig(filename='..\logs\student_data_generation.log',
                    level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


# load database connection variables and connect to database
def connectDB() -> tuple:
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


def add_currency(conn, curr, url):
    resp = requests.get(url)  # get requests

    data = resp.json()  # parse into json format
    for k, v in data.items():
        query = """
        INSERT INTO currency(curr_code, curr_denom) 
        VALUES (%s, %s)
        """
        curr.execute(query, (k, v))
        logger.info(f"Inserted {k} and {v}")

    logger.info('All currency Added!!')
    conn.commit()
    curr.close()
    conn.close()


def main():
    BASE_URL = "https://cdn.jsdelivr.net/npm/@fawazahmed0/currency-api@latest/v1/currencies.json"

    con, cur = connectDB()
    add_currency(conn=con, curr=cur, url=BASE_URL)


if __name__ == "__main__":
    main()