import pandas as pd
from datetime import date, timedelta
import yfinance as yf
import os.path
from constant import *


def get_list_tickers():
    payload = pd.read_html(SOURCE)
    first_table = payload[0]
    tickers = first_table[CHOOSE_COLUMNS]
    tickers.to_csv(TICKER_FILE)


def get_daily_stock_data(start_date, end_date, history_crawl=False):
    # check list tickers exist
    if os.path.isfile(TICKER_FILE) is False:
        get_list_tickers()

    # pull data via api
    tickers = pd.read_csv(TICKER_FILE)

    # dataframe for all the tickers
    stock_data = pd.DataFrame()
    for index, ticker in tickers.iterrows():
        # download data
        data = yf.download(tickers=ticker[STOCK_INDEX], start=start_date, end=end_date)
        # add a column for ticker
        data = data.assign(stock_index=ticker[STOCK_INDEX])
        data = data.assign(name=ticker[STOCK_NAME])
        data = data.assign(sector=ticker[STOCK_SECTOR])
        # add date column
        data = data.assign(date=data.index)
        # append this ticker data to larger dataframe
        stock_data = pd.concat([stock_data, data])

    if history_crawl:
        stock_data.to_json(rf"{HISTORY_FILE}", orient="records")
    else:
        # write the ticker data into the file
        stock_data.to_json(rf"{DAILY_FILE}", orient="records")


def main():
    yesterday = str(date.today() + timedelta(days=-1))
    today = str(date.today())
    get_daily_stock_data(yesterday, today, history_crawl=True)


if __name__ == "__main__":
    main()
