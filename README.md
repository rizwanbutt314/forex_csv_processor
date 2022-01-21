### Description:
The purpose of this script is to get the latest csv file from a given folder and then generate
account summary + summary for each ISIN.

### PreReqs:
* Python: 3.6+

### Setup:
* create a virtual environment: `virtualenv -p /usr/bin/python3 env` (Optional)
* activate the environemnt: `source ./env/bin/activate` (Optional when you don't need first step)
* install requirements: `pip install -r requirements.txt`
* Edit `main.py` file to update the Data folder path according to yours
* Following is the Data folder variable which needs to be updated
```
DATA_FOLDER = "data/"
```

### Run:
* Command to run scraper: `python main.py`

### Note:
*  `requirements.txt` file contains the list of packages that are required to install.
* After processing, two json files will be generated with the names `account_summary.json` and `isin_summary.json`


### Formulas for Account Summary:
```
total_deposits = sum of Total (GBP) column values where Action = Deposit

total_withdrawals = sum of Total (GBP) column values where Action = Withdrawal

total_available = total_deposits - total_withdrawals

free = total_deposits - total_withdrawals - total_gbp

free_funds = free + total_dividends + total_result

total_dividends = sum of Total (GBP) column values where Action contains Dividend

currency_conversion_fee = sum of Currency conversion fee (GBP) values

stamp_duty_reserve_tax = sum of Stamp duty reserve tax (GBP) values

total_gbp = Sum of total_gpb of each ISIN summary

total_result = Sum of total_result of each ISIN summary

```

### Formulas for each ISIN Summary:
```
isin = isin value

name = First value of Name column for each ISIN

ticker = First value of Ticker column for each ISIN

total_shares_bought = Sum of all No. of shares when Action contains buy 

total_shares_sold = Sum of all No. of shares when Action contains sell 

current_shares_held = total_shares_bought - total_shares_sold

total_gbp = Sum of Total GBP for buy - Sum of Total GBP for sell + Sum of Result GBP for sell

result = Sum of Result GBP for sell

total_dividends = sum of Total (GBP) column values where Action contains Dividend

```