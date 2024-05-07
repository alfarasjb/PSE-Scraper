
from bs4 import BeautifulSoup  
import requests 
import pandas as pd 
from datetime import datetime as dt 

import sys, os
import importlib.util 
sys.path.append(os.environ['TOOLS_PATH']) 
import dbloader as db 



class PSEUpdater:
    
    def __init__(self):
        
        self.path = "https://ph.investing.com/equities/"

        self.links = {'AC' : "ayala-corp",
                    'ACEN' : "transasia-oil", 
                    'AEV' : "aboitiz-equity",
                    'AGI' : "alliance-globa",
                    'ALI' : "ayala-land",
                    'BDO' : "bdo-unibank",
                    'BLOOM' : "bloomberry-res",
                    'BPI' : "bk-of-phi-isla",
                    'DMC' : "dmci-holdings",
                    'GLO' : "globe-telecom",
                    'ICT' : "intl-container",
                    'JFC' : "jollibee-foods", 
                    'JGS' : "jg-summit",
                    'LTG' : "lt-group",
                    'MBT' : "metropolitan-b",
                    'MER' : "manila-electri",
                    'NIKL' : "nickel-asia-co",
                    'PGOLD' : "puregold-price",
                    'SCC' : "semirara-minin",
                    'SM' : "sm-investment",
                    'SMC' : "san-miguel-cor",
                    'SMPH' : "sm-prime-hldgs",
                    'TEL' : "phi-long-dis-t",
                    'URC' : "universal-robi"}
        self. source_header = ['Date', 'Price', 'Open', 'High', 'Low', 'Vol.', 'Change %']
        
    @staticmethod
    def build_table(t):
        rows = []

        for i, row in enumerate(t.find_all('tr')):
            if i == 0:
                header = [e.text.strip() for e in row.find_all('th')]
            else:
                rows.append([e.text.strip() for e in row.find_all('td')])

        df = pd.DataFrame(rows)
        return df 
    
    def find_table_index(self, tables):
        for i, table in enumerate(tables):
            target_header = [e.text.strip() for e in table.find_all('th')]
            if 'Date' in target_header:
                return i 
        return None
    
    def get_scraped_data(self, ticker):

        stock_tag = self.links[ticker]
        base = f"https://ph.investing.com/equities/{stock_tag}-historical-data"
        print(f"Fetching: {ticker} URL: {base}")
        p = requests.get(base)
        soup = BeautifulSoup(p.content, "html.parser")
        head = soup.find("h1")

        scraped_ticker = head.text.split(' ')[-1][1:-1]
        if ticker != scraped_ticker: 
            raise ValueError(f"Invalid Data. Target Ticker: {ticker} Received Ticker: {scraped_ticker}")

        tables = soup.find_all("table")
        data = self.build_table(tables[self.find_table_index(tables)])
        data.columns = self.source_header
        data = data.set_index('Date', drop=True)
        data.index = pd.to_datetime(data.index)
        data = data[::-1]
        return data 

    def update_all_symbols(self):
        loader = db.DBLoader()
        updated_tickers= 0
        for ticker, tag in self.links.items():
            source = loader.load_data(ticker, loader.resolutions.RESOLUTION_D1)
            scraped = self.get_scraped_data(ticker)

            source_start_date, source_end_date = source[:1].index.item().date(), source[-1:].index.item().date()
            scrape_start_date, scrape_end_date = scraped[:1].index.item().date(), scraped[-1:].index.item().date()
            print(f"Source Start Date: {source_start_date} Source End Date: {source_end_date} Num Existing Rows: {len(source)}")
            print(f"Scrape Start Date: {scrape_start_date} Scrape End Date: {scrape_end_date} Received Rows: {len(scraped)}")

            rows_to_append = scraped.loc[scraped.index.date > source_end_date]
            rows_to_append.columns = source.columns 

            if len(rows_to_append) == 0:
                print(f"{ticker} is up to date.")
                continue 
            
            print(f"Rows to append: {len(rows_to_append)}")

            updated_dataframe = pd.concat([source, rows_to_append]).drop_duplicates(keep="first")
            
            csv_file = loader.file_path(ticker, loader.resolutions.RESOLUTION_D1) 
            updated_dataframe.to_csv(csv_file)
            updated_tickers+=1 

        return updated_tickers

    def check_missing_data(self):
        loader = db.DBLoader()
        to_update = []
        for ticker in loader.query('PH_EQUITIES'):
            source = loader.load_data(ticker, loader.resolutions.RESOLUTION_D1)

            source_start_date, source_end_date = source[:1].index.item().date(), source[-1:].index.item().date()
            if source_end_date < dt.now().date():
                #print(f"{ticker} is not up to date. Last Data Point: {source_end_date}")
                to_update.append(ticker)

        #print(f"Tickers to update: {to_update}")
        
        return to_update 

    def load_historical_data(self, ticker):
        loader = db.DBLoader()

        data = loader.load_data(ticker, loader.resolutions.RESOLUTION_D1)

        return data