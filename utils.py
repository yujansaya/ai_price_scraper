import pandas as pd
from environs import Env
import requests
import json
from dateutil.parser import parse


SIC_INFO_COLUMNS = [
    "specification", "price_update_url", 'sic',
    'last_index_price', 'index_price_end_date', 'index_calculation', 'index_provider_code', 'delivery_point'
]


env = Env()
env.read_env(".env")
def get_sic_info(sic_status):
    header = {"X-API-KEY": env("X-API-KEY")}
    # Get the latest frequency and date definition code in data dash api
    index_sics_url = f"https://api.datadash.stableprice.com/DDapi/v1/index_sics/"
    response = requests.get(index_sics_url, headers=header)
    if response.status_code != 200:
        warning_text = "Warning: Failed to load SICs information due to the failure of DD api."
        print(warning_text)
        return warning_text
    df = pd.DataFrame(response.json())
    df['source'] = df['index_provider_id'].str.get('name')

    sic_status["spider"] = sic_status["spider"].str.lower()
    df.rename(columns={'stable_index_code': 'sic'}, inplace=True)
    clean_df = df[SIC_INFO_COLUMNS].copy()
    clean_df['index_price_end_date'] = clean_df['index_price_end_date'].str.split("T").str[0]
    clean_df["specification"] = clean_df["specification"] + ", " + clean_df["delivery_point"]
    clean_df = pd.merge(clean_df, sic_status, on="sic", how="inner")

    # Get the latest scrape id of index in data dash api
    scrape_matchings_url = "https://api.datadash.stableprice.com/DDapi/v1/scrape_matchings/"
    response = requests.get(scrape_matchings_url, headers=header)
    if response.status_code != 200:
        warning_text = "Warning: Failed to load SIC Scrape IDs due to the failure of DD api."
        print(warning_text)

    # Extract the index scrape id and corresponding sic from the response
    index_df = pd.DataFrame(response.json())
    index_df["sic"] = index_df["index_details_id"].apply(
        lambda x: x["stable_index_code"]
    )
    index_df["scrape_id"] = index_df["scrape_details_id"].apply(
        lambda x: x["description"]
    )

    # Sort the dataframe by chain index and keep the latest scrape id for each sic
    index_df.sort_values(by="chain_index", inplace=True)
    index_df = index_df.groupby("sic").last()[["scrape_id"]].reset_index()
    # Remove everything before the first tab
    index_df["scrape_id"] = index_df["scrape_id"].str.split('\t', n=1).str[1].replace(r'\t', '-', regex=True)
    merged = pd.merge(index_df, clean_df, on="sic", how="right")
    return merged

def send_file_to_slack(filename, slack_user_email):
    env = Env()
    env.read_env(".env")
    """
    This function will directly send the file to the slack channel or user
    """
    try:
        if isinstance(slack_user_email, (tuple, list)):
            for email in slack_user_email:
                return send_file_to_slack(filename, email)

        with open(filename, "rb") as f:
            file_data = f.read()

        url = "https://dataintelligence.stableprice.com/slackbot/api/v1/send_file"
        request_headers = {
            "Authorization": f"Bearer {env.str('DI_SLACKBOT_AUTH_TOKEN')}",
        }
        payload = {
            "file": (filename, file_data, "application/octet-stream", {"Expires": "0"}),
            "message_title": filename,
        }

        if slack_user_email:
            payload["slack_user_email"] = slack_user_email
        else:
            print("Sending message to channel")
            payload["slack_channel_id"] = "C05303FP4NQ"

        response = requests.post(
            url=url,
            headers=request_headers,
            files=payload,
        )
        response = json.loads(response.content)
        print(response)
    except Exception as e:
        print("Something went wrong in sending the file:", e)


def safe_parse(date):
    try:
        return parse(str(date))
    except ValueError:
        return date  # Handle invalid dates


# Transform the column
def transform_prices(prices):
    # print(prices)
    if type(prices).__name__ == "list":
        if len(prices) == 1:  # If the list has only one element, return that element
            return prices[0]
        elif len(prices) == 2:  # If there are two elements, return their average
            return sum(prices) / 2
        else:
            return prices
    else:  # If more than two elements, leave the list as it is
        return prices


# Handles the logic of "status" column in the report
def determine_status(row):
    if "Bundle" in row["specification"]:
        return "Bundle"
    if row["most_recent_price"] == 0 or (
        row["end_date"] == row["price_date"] and row["most_recent_price"] == row["last_index_price"]
    ):
        return "All Good"
    if pd.isna(row["most_recent_price"]) and pd.isna(row["price_date"]):
        return "Nothing Extracted"
    if row["end_date"] != row["price_date"] and row["most_recent_price"] == row["last_index_price"]:
        return "Needs Attention: Same Price, Different Dates"
    if row["end_date"] == row["price_date"] and row["most_recent_price"] != row["last_index_price"]:
        return "Needs Attention: Different Price, Same Dates"
    return "Needs Attention: Different Price, Different Dates"


def merge_dataframes(sic_status, all_checked_sics, clean_df):
    """Merge and clean data from multiple sources."""
    rows = [{"sic": item.sic, "most_recent_price": item.most_recent_price, "price_date": item.price_date}
            for item in all_checked_sics]
    prices_df = pd.DataFrame(rows)
    prices_df["price_date"] = prices_df["price_date"].apply(safe_parse)
    sic_status["end_date"] = sic_status["end_date"].apply(safe_parse)
    temp = pd.merge(sic_status, prices_df, on="sic", how="outer")
    merged = pd.merge(temp, clean_df[["sic", "last_index_price", "specification"]], on="sic", how="outer")
    return merged


def create_status_column(merged_df):
    """Transform prices and determine the status column."""
    merged_df["most_recent_price"] = merged_df["most_recent_price"].apply(transform_prices)
    merged_df["status"] = merged_df.apply(determine_status, axis=1)
    return merged_df


def sort_and_reorder_columns(merged_df):
    """Sort the DataFrame and reorder columns."""
    status_priority = {"Needs Attention: Different Price, Different Dates": 5,
                       "Needs Attention: Different Price, Same Dates": 4,
                       "Needs Attention: Same Price, Different Dates": 3,
                       "Bundle": 2,
                       "All Good": 1,
                       "Nothing Extracted": 0}
    merged_df["status_priority"] = merged_df["status"].apply(lambda x: status_priority.get(x, 99))
    merged_df = merged_df.sort_values(by="status_priority", ascending=False)
    # merged_df = merged_df.sort_values(
    #     by="status", ascending=False, key=lambda x: x.str.contains("Needs Attention")
    # )
    new_order = [
        "sic", "spider", "end_date", "last_index_price",
        "price_date", "most_recent_price", "price_frequency",
        "status", "specification"
    ]
    return merged_df[new_order]


def safe_write_to_csv(df, output_file):
    """Safely write the DataFrame to a CSV file."""
    df = df.rename(
        columns={"price_date": "LLM Price Date", "most_recent_price": "LLM Price", "end_date": "Stable End Date",
                 "last_index_price": "Stable Last Price"})
    print(df.columns)
    df.to_csv(output_file, index=False)
    print(f"Data successfully written to {output_file}")

