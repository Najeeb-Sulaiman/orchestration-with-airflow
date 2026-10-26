import pandas as pd
from sqlalchemy import create_engine
from datetime import date
from include import config
from airflow.providers.postgres.hooks.postgres import PostgresHook


def extract():
    print("Extracting data from source")
    df = pd.read_csv(config.DATA_SOURCE)
    df.to_csv(f"{config.DATA_PATH}/raw_sales.csv", index=False)
    print("Source data extracted successfully")


def load():
    df = pd.read_csv(f"{config.DATA_PATH}/raw_sales.csv")
    # engine = create_engine(config.DATABASE_URL) # noqa E501
    engine = PostgresHook(postgres_conn_id="supabse_conn").get_sqlalchemy_engine
    with engine.connect() as connection:
        print("Connection to database successful!")
        record_no = df.to_sql('raw_sales', connection, if_exists='replace', index=False) # noqa E501
    print(f"{record_no} rows of data loaded to raw table!")


def transform():
    engine = create_engine(config.DATABASE_URL) # noqa E501

    with engine.connect() as connection:
        print("Connection to database successful!")
        query = """
        SELECT *
        FROM raw_sales;
        """
        df = pd.read_sql(query, engine)

        print("Starting transformation")
        df = df.fillna("Unkmown")
        df = df.drop_duplicates()
        df['order_date'] = pd.to_datetime(df['order_date'])
        df['total_sale'] = df['quantity'] * df['price']
        record_no = df.to_sql('sales', connection, if_exists='replace', index=False) # noqa E501
    print(f"{record_no} rows of data loaded to transformed table!")


def notify():
    engine = create_engine(config.DATABASE_URL) # noqa E501

    with engine.connect():
        print("Connection to database successful!")
        query = """
        SELECT *
        FROM sales;
        """
        df = pd.read_sql(query, engine)

        print("Generate report")
        total_revenue = sum(df["quantity"] * df["price"])
        order_count = df.shape[0]
        total_quantity = sum(df["quantity"])
        top_customer = df.groupby("customer_name")["total_sale"].sum().idxmax()
        top_product = df.groupby("product")["total_sale"].sum().idxmax()

        message = (
                f"CDEBite daily report for {date.today()}\n"
                f"Order Count: {order_count} | Total Quantity: {total_quantity} |\n" # noqa E501
                f"Top Customer: {top_customer} | Top Product: {top_product} |\n" # noqa E501
                f"Top restaurants: ${total_revenue}"
            )
    with open(f"{config.DATA_PATH}/report.txt", "w") as f:
        f.write(message)
