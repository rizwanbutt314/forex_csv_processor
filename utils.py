import os
import math
import json
import mysql.connector
from datetime import date
from slack import WebClient

# Data files settings
DATA_FOLDER = "data/"
basepath = os.path.dirname(os.path.abspath(__file__))
account_summary_filename = "account_summary.json"
isin_summary_filename = "isin_summary.json"
account_summary_txt_filename = "account_summary.txt"
isin_summary_txt_filename = "isin_summary.txt"

# MYSQL Settings
DB_HOST = "localhost"
DB_USERNAME = "root"
DB_PASSWORD = "mysql123"
DB_DATABASE = "frx"

TABLES_MAPPING = {
    "account_summary": {
        "where_columns": ["_date"],
        "columns": [
            "total_deposits",
            "total_withdrawals",
            "total_available",
            "free",
            "free_funds",
            "total_dividends",
            "currency_conversion_fee",
            "stamp_duty_reserve_tax",
            "total_gbp",
            "total_result",
            "_date"
        ]
    },
    "isin_summary": {
        "where_columns": ["isin", "_date"],
        "columns": [
            "isin",
            "_name",
            "ticker",
            "total_shares_bought",
            "total_shares_sold",
            "current_shares_held",
            "total_gbp",
            "result",
            "total_dividends",
            "_date",
            "currency"
        ]
    },
    "wallet_212": {
        "where_columns": [],
        "columns": [
            "name",
            "ticker",
            "current_shares_held",
            "total_deposits",
            "free_funds",
            "total_gbp",
            "_date",
            "account_total_gbp",
            "currency"
        ]
    },
    "balance_212": {
        "where_columns": [],
        "columns": [
            "name",
            "ticker",
            "current_price",
            "no_of_shares",
            "value_gbp",
            "average_price",
            "return_value",
            "return_percentage",
            "_date"
        ]
    },
    "portfolio_account": {
        "where_columns": [],
        "columns": [
            "account_name",
            "deposits",
            "balance",
            "profit",
            "equity",
            "pnl_percentage",
            "_date"
        ]
    }
}


def get_today_date():
    today = date.today()
    return today.strftime("%Y-%m-%d")


def is_nan(val):
    """
        Function to check the given input is of nan type or not.
    """
    try:
        x = float(str(val).lower())
        return math.isnan(x)
    except:
        return False


def save_json(filename, data):
    """
        Function to save data to json file
    """

    filepath = os.path.join(basepath, filename)
    with open(filepath, "w") as f:
        json.dump(data, f)


def save_txt(filename, data):
    """
        Function to save data to txt file
    """

    filepath = os.path.join(basepath, filename)
    with open(filepath, "w") as f:
        f.write(json.dumps(data, indent=4))


def get_latest_csv_file():
    """
        Function to get the latest csv file from the given folder
    """
    all_files = os.listdir(DATA_FOLDER)
    csv_files = list(filter(lambda f: f.endswith('.csv'), all_files))
    csv_files = list(
        map(lambda file: os.path.join(DATA_FOLDER, file), csv_files))
    latest_csv = max(csv_files, key=os.path.getctime)
    return latest_csv


def is_float_digit(n):
    """
        Function to check given input is valid for float type
    """
    try:
        float(n)
        return True
    except:
        return False


def save_to_db(data, table_name=""):
    if not table_name:
        raise Exception("Table Name missing...")

    table_meta_data = TABLES_MAPPING[table_name]
    where_columns = table_meta_data["where_columns"]
    columns = table_meta_data["columns"]

    update_query_cols_str = ", ".join([f"{col} = %s" for col in columns])

    insert_query_cols_str = ", ".join([f"{col}" for col in columns])
    insert_query_vals_str = ", ".join([f"%s" for col in columns])

    where_columns_cols_str = " AND ".join(
        [f"{col} = %s" for col in where_columns])

    mydb = mysql.connector.connect(
        host=DB_HOST,
        user=DB_USERNAME,
        password=DB_PASSWORD,
        database=DB_DATABASE,
        auth_plugin='mysql_native_password'
    )

    mycursor = mydb.cursor(buffered=True)

    # Check for duplicates
    filtered_data = list()
    for record in data:

        where_columns_vals = (record.get(col, None) for col in where_columns)

        # Check existing entry
        myresult = None
        if where_columns:
            sql = f"""SELECT * FROM {table_name} WHERE {where_columns_cols_str}"""
            val = (*where_columns_vals, )

            mycursor.execute(sql, val)
            myresult = mycursor.fetchone()

        if not myresult:
            filtered_data.append(record)
        else:
            # Update the existing data
            update_query_cols = (record.get(col, None) for col in columns)
            where_columns_vals = (record.get(col, None)
                                  for col in where_columns)

            sql = f"""UPDATE {table_name} SET {update_query_cols_str} WHERE {where_columns_cols_str} """
            val = (*update_query_cols, *where_columns_vals)
            mycursor.execute(sql, val)
            mydb.commit()

    # INSERT data
    sql = f"INSERT INTO {table_name} ({insert_query_cols_str}) VALUES ({insert_query_vals_str})"

    data_to_db = [tuple((fd.get(col, None) for col in columns))
                  for fd in filtered_data]
    mycursor.executemany(sql, data_to_db)
    mydb.commit()

    mycursor.close()
    mydb.close()


def delete_all_data_from_db(table_name="", where_columns={}):
    """
    Function to delte data from db. if where_columns is provided then it will be used
    as where condition in the delete query. where_columns dictionary should be like:
    
    where_columns = {
        "column_name": "column_value"
    }
    """
    if not table_name:
        raise Exception("Table Name missing...")

    mydb = mysql.connector.connect(
        host=DB_HOST,
        user=DB_USERNAME,
        password=DB_PASSWORD,
        database=DB_DATABASE,
        auth_plugin='mysql_native_password'
    )
    mycursor = mydb.cursor(buffered=True)

    # If where columns data is provided
    if where_columns:
        cols = where_columns.keys()
        where_columns_cols_str = " AND ".join([f"{col} = %s" for col in cols])

        sql = f"""DELETE FROM {table_name} WHERE {where_columns_cols_str} """

        where_columns_vals = (where_columns.get(col, None) for col in cols)
        val = (*where_columns_vals, )
        mycursor.execute(sql, val)
    else:
        sql = f"""DELETE FROM {table_name} """
        mycursor.execute(sql, )
    mydb.commit()
    mycursor.close()
    mydb.close()


def get_table_data(table_name=""):
    if not table_name:
        raise Exception("Table Name missing...")

    mydb = mysql.connector.connect(
        host=DB_HOST,
        user=DB_USERNAME,
        password=DB_PASSWORD,
        database=DB_DATABASE,
        auth_plugin='mysql_native_password'
    )

    mycursor = mydb.cursor(buffered=True, dictionary=True)

    sql = f"""SELECT * FROM {table_name} """

    mycursor.execute(sql, )
    db_data = mycursor.fetchall()

    return db_data


# Handle slack table columns sizing
SLACK_TABLES_COLUMNS_SIZE_MAPPING = {
    "stock-data": {
        "Name": 35,
        "Ticker": 8,
        "Current Price": 18,
        "No of Shares": 14,
        "Value GBP": 12,
        "Average Price": 14,
        "Return": 12,
        "Return %": 12,
    },
    "portfolio-data": {
        "Account Name": 20,
        "Deposits": 12,
        "Balance": 12,
        "Profit": 14,
        "Equity": 12,
        "PNL %": 12,
    }
}

DB_TABLE_COLUMNS_MAPPING_FOR_SLACK = {
    "stock-data": {
        "Name": "name",
        "Ticker": "ticker",
        "Current Price": "current_price",
        "No of Shares": "no_of_shares",
        "Value GBP": "value_gbp",
        "Average Price": "average_price",
        "Return": "return_value",
        "Return %": "return_percentage",
    },
    "portfolio-data": {
        "Account Name": "account_name",
        "Deposits": "deposits",
        "Balance": "balance",
        "Profit": "profit",
        "Equity": "equity",
        "PNL %": "pnl_percentage",
    }
}

SLACK_USER_TOKEN = "xoxp-3225338370068-3246759464832-3220112306406-a0c203664e676b934240d6c0e762ee84"


def format_table_strucutre(table_type="stock-data", data=[]):
    table_sizing_meta = SLACK_TABLES_COLUMNS_SIZE_MAPPING[table_type]

    single_dashes_mapping = {key: "-"*val for key,
                             val in table_sizing_meta.items()}
    double_dashes_mapping = {key: "="*val for key,
                             val in table_sizing_meta.items()}
    columns_mappings = {
        key: f" {key}{(' '*val)}"[:val] for key, val in table_sizing_meta.items()}

    single_dashes_list = list()
    double_dashes_list = list()
    columns_list = list()
    table_sizing_keys = table_sizing_meta.keys()
    for key in table_sizing_keys:
        single_dashes_list.append(single_dashes_mapping[key])
        double_dashes_list.append(double_dashes_mapping[key])
        columns_list.append(columns_mappings[key])

    single_dashes_str = " ".join(single_dashes_list)
    double_dashes_str = " ".join(double_dashes_list)
    columns_str = "|".join(columns_list)
    header = f" {single_dashes_str} \n|{columns_str}|\n {double_dashes_str}"

    db_table_columns_meta = DB_TABLE_COLUMNS_MAPPING_FOR_SLACK[table_type]

    columns_data_list = list()
    for d in data:
        columns_data = list()
        for key in table_sizing_keys:
            sizing_val = table_sizing_meta[key]
            columns_data.append(
                f" {str(d[db_table_columns_meta[key]])}{(' '*sizing_val)}"[:sizing_val])

        columns_data_str = "|".join(columns_data)
        columns_data_list.append(f"|{columns_data_str}|")
        columns_data_list.append(f" {single_dashes_str}")

    columns_data_str = "\n".join(columns_data_list)

    full_table_str = f"{header}\n{columns_data_str}"
    return full_table_str


def send_table_data_to_slack(table_type="", stock_data=[]):
    formatted_table_str = format_table_strucutre(table_type, stock_data)
    client = WebClient(token=SLACK_USER_TOKEN)
    response = client.chat_postMessage(
        channel=f'#{table_type}',
        text=f"```{formatted_table_str}```")
