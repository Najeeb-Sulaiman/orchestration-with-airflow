import pandas as pd
from sqlalchemy import create_engine
import config


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


if __name__ == "__main__":
    transform()
