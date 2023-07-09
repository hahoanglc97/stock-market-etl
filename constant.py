import pandas as pd

PATH_FILE = "/Users/ha/Project/AWS/hahoang-local_accessKeys.csv"
SOURCE = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"

aws = pd.read_csv(PATH_FILE)
IP_ADDRESS = aws["EC2 Public IP"][0]
AWS_ACCESS_KEY = aws["Access key ID"][0]
AWS_SECRET_KEY = aws["Secret access key"][0]
PORT = 9092
CLIENT_ID = "client_1"
AWS_REGION = "ap-southeast-1"
S3_BUCKET_NAME = "hh-youtube-data-storage"
S3_OUTPUT_DIRECTORY = "stock-data-folder"

TOPIC_NAME = "stock_market_analysis"

HISTORY_FILE = "historical_stock_data.json"
DAILY_FILE = "stock_data.json"
TICKER_FILE = "list_tickers.csv"

CHOOSE_COLUMNS = ["Symbol", "Security", "GICS Sector"]
STOCK_INDEX = "Symbol"
STOCK_NAME = "Security"
STOCK_SECTOR = "GICS Sector"

HISTORY_DATE_START = "2023-4-1"
