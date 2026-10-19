import pandas as pd
from sqlalchemy import create_engine
import config
from datetime import date


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
    with open("data/report.txt", "w") as f:
        f.write(message)


if __name__ == "__main__":
    notify()
