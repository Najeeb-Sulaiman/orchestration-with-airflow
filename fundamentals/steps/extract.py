import pandas as pd

data_source = "https://docs.google.com/spreadsheets/d/e/2PACX-1vTIBPRSnUp10oHj5fhnUTlUJn-W7DtjSRtXIhySqW_1nZak5u4b7bPc9gTcWPHM22JL0aZJJa5QakM1/pub?gid=0&single=true&output=csv" # noqa E501


def extract():
    print("Extracting data from source")
    df = pd.read_csv(data_source)
    df.to_csv("data/raw_sales.csv", index=False)
    print("Source data extracted successfully")


if __name__ == "__main__":
    extract()
