import pandas as pd
from sqlalchemy import create_engine
import config


def load():
    df = pd.read_csv("data/raw_sales.csv")
    engine = create_engine(config.DATABASE_URL) # noqa E501
    with engine.connect() as connection:
        print("Connection to database successful!")
        record_no = df.to_sql('raw_sales', connection, if_exists='replace', index=False) # noqa E501
    print(f"{record_no} rows of data loaded to raw table!")


if __name__ == "__main__":
    load()
