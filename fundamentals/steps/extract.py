import pandas as pd
import config


def extract():
    print("Extracting data from source")
    df = pd.read_csv(config.DATA_SOURCE)
    df.to_csv("data/raw_sales.csv", index=False)
    print("Source data extracted successfully")


if __name__ == "__main__":
    extract()
