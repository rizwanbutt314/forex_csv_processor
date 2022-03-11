import yfinance as yf
from utils import (
    get_table_data,
    save_to_db,
    get_today_date,
    delete_all_data_from_db,
    send_table_data_to_slack
)


def map_balance_data(db_data):
    ticker = db_data['ticker']
    name = db_data['name']
    no_of_shares = db_data['current_shares_held']
    total_gbp = db_data['total_gbp']
    currency = db_data['currency']
    current_price = None

    ticker_data = yf.Ticker(
        f"{ticker.lower()}.l" if currency == "GBX" else ticker)
    ticker_data = ticker_data.info
    try:
        current_price = ticker_data['regularMarketPrice']
    except AttributeError:
        raise Exception("Ticker data not found")
    except:
        print(f"Ticker: {ticker}")
        print(ticker_data)

    if current_price == 0:
        try:
            ticker_data = yf.Ticker(f"{ticker.lower()}.l")
            ticker_data = ticker_data.info
            current_price = ticker_data['regularMarketPrice']
        except:
            pass

    if current_price is None:
        try:
            ticker_data = yf.Ticker(f"{ticker.lower()}.l")
            ticker_data = ticker_data.info
            current_price = ticker_data['regularMarketPrice']
        except:
            pass

    if current_price is None:
        try:
            ticker_data = yf.Ticker(f"{ticker.lower()}.f")
            ticker_data = ticker_data.info
            current_price = ticker_data['regularMarketPrice']
        except:
            pass

    if current_price is None:
        print(f"Ticker: {ticker}")
        raise Exception("Ticker Current Price not found")

    # Currency based price conversion
    if currency == "GBX":
        current_price = current_price / 100
    elif currency in ["USD", "EUR"]:
        _ticker = f"{currency}GBP=X"
        _ticker_data = yf.Ticker(_ticker)
        regular_market_price = _ticker_data.info['regularMarketPrice']
        current_price = current_price * regular_market_price

    # data mapping for slack message:

    value_gbp = no_of_shares * current_price
    return {
        'name': name,
        'ticker': ticker,
        'current_price': round(current_price, 2),
        'no_of_shares': no_of_shares,
        'value_gbp': round(no_of_shares * current_price, 2),
        'average_price': round(total_gbp, 2),
        'return_value': round(value_gbp - total_gbp, 2),
        'return_percentage': round((value_gbp - total_gbp)/total_gbp * 100, 2),
        '_date': get_today_date(),
        'total_deposits': db_data['total_deposits'],
        'free_funds': db_data['free_funds'],
        'account_total_gbp': db_data['account_total_gbp'],
    }


def format_portfolio_account_data(data):

    deposits = data[0]['total_deposits']
    balance = data[0]['free_funds']
    total_gbp = data[0]['account_total_gbp']
    sum_of_return = sum(list(map(lambda x: x['return_value'], data)))
    equity = balance + total_gbp + sum_of_return
    profit = equity - deposits
    pnl_percentage = (equity - (balance + total_gbp))/equity * 100

    return {
        'account_name': '212',
        'deposits': data[0]['total_deposits'],
        'balance': balance,
        'profit': profit,
        'equity': equity,
        'pnl_percentage': pnl_percentage,
        '_date': get_today_date()
    }


def main():
    # get wallet_212 table data
    print("Getting data from wallet_212 table...")
    wallet_db_data = get_table_data(table_name="wallet_212")
    mapped_db_data = list(map(map_balance_data, wallet_db_data))

    # Save to balance_212 mapped data
    print("Saving processed data to balance_212 table...")
    delete_all_data_from_db(table_name="balance_212")
    save_to_db(mapped_db_data, table_name="balance_212")

    # Save portfolio_account data
    print("Saving formatted portfolio account data...")
    profile_account_data = format_portfolio_account_data(mapped_db_data)
    delete_all_data_from_db(table_name="portfolio_account")
    save_to_db([profile_account_data], table_name="portfolio_account")

    print("Sending data to slack...")
    send_table_data_to_slack("stock-data", mapped_db_data)


if __name__ == "__main__":
    main()
