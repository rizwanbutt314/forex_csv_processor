import os
import math
import json
import pandas as pd
from decimal import Decimal

DATA_FOLDER = "data/"
basepath = os.path.dirname(os.path.abspath(__file__))
account_summary_filename = "account_summary.json"
isin_summary_filename = "isin_summary.json"


def is_nan(val):
    try:
        x = float(str(val).lower())
        return math.isnan(x)
    except:
        return False


def save_json(filename, data):
    filepath = os.path.join(basepath, filename)
    with open(filepath, "w") as f:
        json.dump(data, f)


def get_latest_csv_file():
    all_files = os.listdir(DATA_FOLDER)
    csv_files = list(filter(lambda f: f.endswith('.csv'), all_files))
    csv_files = list(
        map(lambda file: os.path.join(DATA_FOLDER, file), csv_files))
    latest_csv = max(csv_files, key=os.path.getctime)
    return latest_csv


def is_float_digit(n):
    try:
        float(n)
        return True
    except:
        return False


def change_to_float(val):
    return float(Decimal(str(val)))


def aggregate_group_data_for_isin_summary(group):
    """
    average_buy_price = (average of all Price/Share for Market/Stop buy)

    average_sell_price = (average of all Price/Share for Market/Stop sell)

    result = total_shares_sold * (average_sell_price - average_buy_price) 

    total_gpb = (total_shares * share_price) / exchange rate

    PnL% = if result is available then:
        (((total_shares_sold * average_sell_price) - (total_shares_bought * average_buy_price) ) / (total_shares_bought * average_buy_price)) * 100


    """

    group_aggregated_data = {}

    # average_buy_price = group.loc[(group['Action'] == "Market buy") | (
    #     group['Action'] == "Stop buy"), 'Price / share'].mean()
    # average_buy_price = change_to_float(average_buy_price)

    # if is_nan(average_buy_price):
    #     average_buy_price = 0

    # average_sell_price = group.loc[(group['Action'] == "Market sell") | (
    #     group['Action'] == "Stop sell"), 'Price / share'].mean()
    # average_sell_price = change_to_float(average_sell_price)

    # if is_nan(average_sell_price):
    #     average_sell_price = 0

    total_shares_bought = group.loc[(group['Action'] == "Market buy") | (
        group['Action'] == "Stop buy"), 'No. of shares'].tolist()

    total_shares_bought = sum(list(map(Decimal, total_shares_bought)))

    # total_shares_bought = change_to_float(total_shares_bought)

    total_shares_sold = group.loc[(group['Action'] == "Market sell") | (
        group['Action'] == "Stop sell"), 'No. of shares'].tolist()
    total_shares_sold = sum(list(map(Decimal, total_shares_sold)))
    # total_shares_sold = change_to_float(total_shares_sold)

    # average_exchange_rate = group["Exchange rate"].mean()
    # if is_nan(average_exchange_rate):
    #     average_exchange_rate = 0

    # total_market_buy = float(group.loc[(group['Action'] == "Market buy") | (
    #     group['Action'] == "Stop buy"), 'Total (GBP)'].sum())

    # total_market_sell = float(group.loc[(group['Action'] == "Market sell") | (
    #     group['Action'] == "Stop sell"), 'Total (GBP)'].sum())

    # total_currency_conversion_fees = group["Currency conversion fee (GBP)"].sum(
    # )

    total_shares = total_shares_bought - total_shares_sold
    # sum_gbp = group["Total (GBP)"].sum()

    gbp_buy_sum = group.loc[group['Action'].str.contains(
        "buy"), "Total (GBP)"].tolist()
    gbp_buy_sum = sum(list(map(Decimal, gbp_buy_sum)))
    # gbp_buy_sum = change_to_float(gbp_buy_sum)

    gbp_sell_sum = group.loc[group['Action'].str.contains(
        "sell"), "Total (GBP)"].tolist()
    gbp_sell_sum = sum(list(map(Decimal, gbp_sell_sum)))
    # gbp_sell_sum = change_to_float(gbp_sell_sum)

    result_gbp_sell_sum = group.loc[group['Action'].str.contains(
        "sell"), "Result (GBP)"].tolist()
    result_gbp_sell_sum = sum(list(map(Decimal, result_gbp_sell_sum)))
    # result_gbp_sell_sum = change_to_float(result_gbp_sell_sum1)

    total_gpb = Decimal(str(gbp_buy_sum)) - Decimal(str(gbp_sell_sum)
                                                    ) + Decimal(str(result_gbp_sell_sum))
    total_gpb = change_to_float(total_gpb)

    # share_price = sum_gbp / total_shares if total_shares > 0 else 0

    # result = total_shares_sold * (average_sell_price - average_buy_price)
    # result = (total_shares_sold * (average_sell_price - average_buy_price) ) / average_exchange_rate
    # result = group["Result (GBP)"].sum()

    # total_gpb = (total_shares * share_price) / average_exchange_rate if total_shares > 0 else 0

    # pnl_percentage = 0 if not result else (((total_shares_sold * average_sell_price) - (
    #     total_shares_bought * average_buy_price)) / (total_shares_bought * average_buy_price)) * 100

    # if group.name == "CA05156X8843":
    #     # print(group.dtypes)
    #     # print(group["Result (GBP)"])
    #     print("gbp_buy_sum: ", gbp_buy_sum)
    #     print("gbp_sell_sum: ", gbp_sell_sum)
    #     print("result_gbp_sell_sum: ", result_gbp_sell_sum)
    #     # print("result", type(result))

    #     print("Output: ", total_gpb)

    group_aggregated_data['isin'] = group.name
    group_aggregated_data['name'] = group["Name"].iloc[0]
    group_aggregated_data['ticker'] = group["Ticker"].iloc[0]
    # group_aggregated_data['total_shares'] = total_shares
    # group_aggregated_data['share_price'] = share_price
    # group_aggregated_data['exchange_rate'] = average_exchange_rate
    # group_aggregated_data['average_buy_price'] = average_buy_price
    # group_aggregated_data['average_sell_price'] = average_sell_price
    group_aggregated_data['total_shares_bought'] = float(total_shares_bought)
    group_aggregated_data['total_shares_sold'] = float(total_shares_sold)
    group_aggregated_data['current_shares_held'] = float(total_shares)
    group_aggregated_data['total_gbp'] = float(total_gpb)
    group_aggregated_data['result'] = float(result_gbp_sell_sum)
    # group_aggregated_data['pnl_percentage'] = pnl_percentage
    return pd.Series(group_aggregated_data, index=['isin', 'name', 'ticker', 'total_shares_bought', 'total_shares_sold', 'current_shares_held', 'total_gbp', 'result'])


def generate_account_summary(df, total_gbp, total_result):
    total_deposits = df.loc[df['Action'] == "Deposit", "Total (GBP)"].tolist()
    total_deposits = float(sum(list(map(Decimal, total_deposits))))

    total_withdrawals = df.loc[df['Action'] ==
                               "Withdrawal", "Total (GBP)"].tolist()
    total_withdrawals = float(sum(list(map(Decimal, total_withdrawals))))

    total_available = total_deposits - total_withdrawals

    total_dividends = df.loc[df['Action'].str.contains(
        "Dividend"), "Total (GBP)"].tolist()
    total_dividends = float(sum(list(map(Decimal, total_dividends))))

    currency_conversion_fee = df["Currency conversion fee (GBP)"].tolist()
    currency_conversion_fee = list(map(lambda x: x if str(x) not in ['nan', ''] else '0.0', currency_conversion_fee))
    currency_conversion_fee = float(
        sum(list(map(Decimal, currency_conversion_fee))))
    
    stamp_duty_reserve_tax = df["Stamp duty reserve tax (GBP)"].tolist()
    stamp_duty_reserve_tax = list(map(lambda x: x if str(x) not in ['nan', ''] else '0.0', stamp_duty_reserve_tax))
    stamp_duty_reserve_tax = float(
        sum(list(map(Decimal, stamp_duty_reserve_tax))))

    account_summary = {
        "total_deposits": total_deposits,
        "total_withdrawals": total_withdrawals,
        "total_available": total_available,
        "free": total_deposits - total_withdrawals - total_gbp,
        "free": float(Decimal(str(total_deposits)) - Decimal(str(total_withdrawals)) - Decimal(str(total_gbp))),
        "total_dividends": total_dividends,
        "currency_conversion_fee": currency_conversion_fee,
        "stamp_duty_reserve_tax": stamp_duty_reserve_tax,
        "total_gbp": total_gbp,
        "total_result": total_result
    }

    save_json(account_summary_filename, account_summary)


def generate_each_isin_summary(df):
    group_df = df.groupby(["ISIN"]).apply(
        aggregate_group_data_for_isin_summary)

    total_gbp = group_df["total_gbp"].tolist()
    total_gbp = float(sum(list(map(Decimal, total_gbp))))

    total_result = group_df["result"].tolist()
    total_result = float(sum(list(map(Decimal, total_result))))

    save_json(isin_summary_filename, group_df.to_dict("records"))
    return total_gbp, total_result


def main():
    latest_csv_file = get_latest_csv_file()
    print(f"Processing latest CSV file: {latest_csv_file}")

    df = pd.read_csv(latest_csv_file)

    # Clean and Fix Exchange rate data for processing
    df["Exchange rate"] = df["Exchange rate"].apply(
        lambda x: x if is_float_digit(str(x)) else str(0))
    # df["Exchange rate"] = pd.to_numeric(df["Exchange rate"], downcast="str")
    # df["Total (GBP)"] = pd.to_numeric(df["Total (GBP)"], downcast="str")
    # df["Result (GBP)"] = pd.to_numeric(df["Result (GBP)"], downcast="str")
    # df["Price / share"] = pd.to_numeric(df["Price / share"], downcast="str")
    # df["No. of shares"] = pd.to_numeric(df["No. of shares"], downcast="str")

    df = df.astype({'Exchange rate': 'str', 'Total (GBP)': 'str', 'Result (GBP)': 'str', 'Price / share': 'str',
                   'No. of shares': 'str', 'Currency conversion fee (GBP)': 'str', 'Stamp duty reserve tax (GBP)': 'str'})

    # Generate each ISIN summary
    total_gbp, total_result = generate_each_isin_summary(df)

    # Generate Account Summary
    generate_account_summary(df, total_gbp, total_result)


if __name__ == "__main__":
    main()
