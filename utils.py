import os
import math
import json
import mysql.connector
from datetime import date

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
            "name",
            "ticker",
            "total_shares_bought",
            "total_shares_sold",
            "current_shares_held",
            "total_gbp",
            "result",
            "total_dividends",
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
