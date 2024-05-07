import pse_scraper 
import pandas as pd 



def data_actions():

    ACTIONS = [""]

    action = input("Choose Action: ")    


def retrieve_latest(): 
    ticker = choose_ticker()
    if ticker is None:
        retrieve_latest()
    print()
    print(f"Selected Ticker: {ticker}")
    data = scraper.get_scraped_data(ticker)
    print(data)


def retrieve_stored(): 
    ticker = choose_ticker()
    if ticker is None:
        retrieve_stored()


    print()
    print(f"Selected Ticker: {ticker}")

    data = scraper.load_historical_data(ticker)
    print(data)

def choose_ticker():
    available_tickers = list(scraper.links.keys())

    choices = ""
    for i, t in enumerate(available_tickers):
        choices += f"{i+1}. {t}  "
    print(choices)

    option = input("Select Ticker: ")

    if option.isnumeric():
        ticker = available_tickers[int(option) - 1]
        return ticker 
    else:
        option = option.upper()
        if not option in available_tickers:
            print(f"Invalid Input: {option}")
            return None 
        else: 
            return option 
        

def update_all_symbols():
    num_updated = scraper.update_all_symbols()
    print()
    print(f"Updated {num_updated} Tickers.")


def check_missing_data():
    
    tickers_to_update = scraper.check_missing_data()
    print(f"Tickers to update: {tickers_to_update}")

if __name__ == "__main__":

    print()
    print("#################################")
    print("########## PSE SCRAPER ##########")
    print("#################################")
    print()
    print()

    scraper = pse_scraper.PSEUpdater()

    OPTIONS = {
        "Check Missing Data" : check_missing_data, 
        "Retrieve Latest Data" : retrieve_latest,
        "Retrieve Stored Data" : retrieve_stored,
        "Update All Symbols" : update_all_symbols
    }

    while True:
        print()
        print("######### SELECT OPTION ##########")
        print()
        print("0. Exit")

        for i, o in enumerate(OPTIONS):
            print(f"{i+1}. {o}")

        print()
        option = int(input("Select Option: "))

        if option == 0:
            break 

        keys = list(OPTIONS.keys())

        OPTIONS[keys[option-1]]()