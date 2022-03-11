import pandas as pd
from decimal import Decimal
from utils import (
    delete_all_data_from_db,
    save_to_db,
    is_float_digit,
    is_nan,
    get_latest_csv_file,
    get_today_date,
    save_txt,
    delete_all_data_from_db,
    account_summary_txt_filename,
    isin_summary_txt_filename
)


def aggregate_group_data_for_isin_summary(group):
    """
        Function to process and format the data for each ISIN summary
    """

    group_aggregated_data = {}

    # Total shares bought
    total_shares_bought = group.loc[(group['Action'] == "Market buy") | (
        group['Action'] == "Stop buy"), 'No. of shares'].tolist()

    total_shares_bought = sum(list(map(Decimal, total_shares_bought)))

    # Total shares sold
    total_shares_sold = group.loc[(group['Action'] == "Market sell") | (
        group['Action'] == "Stop sell"), 'No. of shares'].tolist()
    total_shares_sold = sum(list(map(Decimal, total_shares_sold)))

    # Current Shares held
    total_shares = total_shares_bought - total_shares_sold

    # Sum of Total GBP for buy
    gbp_buy_sum = group.loc[group['Action'].str.contains(
        "buy"), "Total (GBP)"].tolist()
    gbp_buy_sum = sum(list(map(Decimal, gbp_buy_sum)))

    # Sum of Total GBP for sell
    gbp_sell_sum = group.loc[group['Action'].str.contains(
        "sell"), "Total (GBP)"].tolist()
    gbp_sell_sum = sum(list(map(Decimal, gbp_sell_sum)))

    # Sum of Result GBP for sell
    result_gbp_sell_sum = group.loc[group['Action'].str.contains(
        "sell"), "Result (GBP)"].tolist()
    result_gbp_sell_sum = sum(list(map(Decimal, result_gbp_sell_sum)))

    # Conversion Fee Sum
    conversion_fee = group["Currency conversion fee (GBP)"].tolist()
    currency_conversion_fee = list(
        map(lambda x: x if not is_nan(x) else '0.0', conversion_fee))
    conversion_fee_sum = float(
        sum(list(map(Decimal, currency_conversion_fee))))

    # Total GBP
    total_gpb = Decimal(str(gbp_buy_sum)) - Decimal(str(gbp_sell_sum)
                                                    ) + Decimal(str(result_gbp_sell_sum))

    total_gpb = Decimal(str(total_gpb)) - Decimal(str(conversion_fee_sum))

    # Total dividends
    total_dividends = group.loc[group['Action'].str.contains(
        "Dividend"), "Total (GBP)"].tolist()
    total_dividends = float(sum(list(map(Decimal, total_dividends))))

    group_aggregated_data['_date'] = get_today_date()
    group_aggregated_data['isin'] = group.name
    group_aggregated_data['_name'] = group["Name"].iloc[0]
    group_aggregated_data['ticker'] = group["Ticker"].iloc[0]
    group_aggregated_data['currency'] = group["Currency (Price / share)"].iloc[0]
    group_aggregated_data['total_shares_bought'] = float(total_shares_bought)
    group_aggregated_data['total_shares_sold'] = float(total_shares_sold)
    group_aggregated_data['current_shares_held'] = float(total_shares)
    group_aggregated_data['total_gbp'] = float(total_gpb)
    group_aggregated_data['result'] = float(result_gbp_sell_sum)
    group_aggregated_data['total_dividends'] = float(total_dividends)
    return pd.Series(group_aggregated_data, index=['_date', 'isin', '_name', 'ticker', 'currency', 'total_shares_bought',
                                                   'total_shares_sold', 'current_shares_held', 'total_gbp', 'result', 'total_dividends'])


def generate_account_summary(df, total_gbp, total_result):
    """
        Function to generate account summary for whole csv file
    """

    # Total deposits
    total_deposits = df.loc[df['Action'] == "Deposit", "Total (GBP)"].tolist()
    total_deposits = float(sum(list(map(Decimal, total_deposits))))

    # Total withdrawals
    total_withdrawals = df.loc[df['Action'] ==
                               "Withdrawal", "Total (GBP)"].tolist()
    total_withdrawals = float(sum(list(map(Decimal, total_withdrawals))))

    # Total available
    total_available = total_deposits - total_withdrawals

    # Total dividends
    total_dividends = df.loc[df['Action'].str.contains(
        "Dividend"), "Total (GBP)"].tolist()
    total_dividends = float(sum(list(map(Decimal, total_dividends))))

    # Total currency conversion free
    currency_conversion_fee = df["Currency conversion fee (GBP)"].tolist()
    currency_conversion_fee = list(
        map(lambda x: x if not is_nan(x) else '0.0', currency_conversion_fee))
    currency_conversion_fee = float(
        sum(list(map(Decimal, currency_conversion_fee))))

    # Total stamp duty reserve tax
    # stamp_duty_reserve_tax = df["Stamp duty reserve tax (GBP)"].tolist()
    # stamp_duty_reserve_tax = list(map(lambda x: x if str(
    #     x) not in ['nan', ''] else '0.0', stamp_duty_reserve_tax))
    # stamp_duty_reserve_tax = float(
    #     sum(list(map(Decimal, stamp_duty_reserve_tax))))

    # Free
    free = float(Decimal(str(total_deposits)) -
                 Decimal(str(total_withdrawals)) - Decimal(str(total_gbp)))

    # Free funds
    free_funds = float(Decimal(
        str(free)) + Decimal(str(total_dividends)) + Decimal(str(total_result)))

    free_funds = float(Decimal(str(free_funds)) -
                       Decimal(str(currency_conversion_fee)))

    total_deposits = float(Decimal(str(free_funds)) + Decimal(str(total_gbp)))

    account_summary = {
        "_date": get_today_date(),
        "total_deposits": total_deposits,
        "total_withdrawals": total_withdrawals,
        "total_available": total_available,
        "free": free,
        "free_funds": free_funds,
        "total_dividends": total_dividends,
        "currency_conversion_fee": currency_conversion_fee,
        # "stamp_duty_reserve_tax": stamp_duty_reserve_tax,
        "total_gbp": total_gbp,
        "total_result": total_result
    }

    save_txt(account_summary_txt_filename, account_summary)
    save_to_db([account_summary], table_name="account_summary")

    return account_summary


def generate_each_isin_summary(df):
    """
        Function to generate summary against each ISIN
    """

    group_df = df.groupby(["ISIN"]).apply(
        aggregate_group_data_for_isin_summary)

    total_gbp = group_df["total_gbp"].tolist()
    total_gbp = float(sum(list(map(Decimal, total_gbp))))

    total_result = group_df["result"].tolist()
    total_result = float(sum(list(map(Decimal, total_result))))

    df_data = group_df.to_dict("records")

    save_txt(isin_summary_txt_filename, df_data)
    save_to_db(df_data, table_name="isin_summary")

    return total_gbp, total_result, df_data


def main():
    """
        Function to get latest csv file from given folder and generate account summary + 
        summary for each ISIN
    """

    # Get latest csv file from given folder
    latest_csv_file = get_latest_csv_file()
    print(f"Processing latest CSV file: {latest_csv_file}")

    df = pd.read_csv(latest_csv_file)

    # Clean and Fix data for processing
    df["Exchange rate"] = df["Exchange rate"].apply(
        lambda x: x if is_float_digit(str(x)) else str(0))

    df = df.astype({'Exchange rate': 'str', 'Total (GBP)': 'str', 'Result (GBP)': 'str', 'Price / share': 'str',
                   'No. of shares': 'str', 'Currency conversion fee (GBP)': 'str'})

    # Generate each ISIN summary
    total_gbp, total_result, df_data = generate_each_isin_summary(df)

    # Generate Account Summary
    account_summary = generate_account_summary(df, total_gbp, total_result)

    # Ingest data to wallet_212
    wallet_212_data = list(
        filter(lambda x: x["current_shares_held"] > 0, df_data))

    # delete existing data from wallet_212
    delete_all_data_from_db(table_name="wallet_212")

    sum_of_total_gbp = sum(
        list(map(lambda x: x['total_gbp'], wallet_212_data)))

    # Insert new data to wallet_212
    wallet_212_data_updated = list(map(lambda x: {
                                   **x, **{'name': x['_name'], 'total_deposits': account_summary['total_deposits'], 'free_funds': account_summary['free_funds'], '_date': get_today_date(), 'account_total_gbp': sum_of_total_gbp}}, wallet_212_data))
    save_to_db(wallet_212_data_updated, table_name="wallet_212")


if __name__ == "__main__":
    main()
