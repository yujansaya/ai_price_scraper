from driver import WebDriverManager
from graph_builder import GraphBuilder
from graph_tools import *
from utils import *
# import logging
from datetime import datetime

# Get today's date for filename
today = datetime.today().strftime('%Y-%m-%d')

# logging.basicConfig(
#     level=logging.INFO,
#     format="%(asctime)s - %(levelname)s - %(message)s"
# )

download_dir = env("download_dir")
output_file = f"llm_sics_status_check_{today}.csv"


GRAPH_ROUTES = {
    USDA_MARS: [go_to_page, analyze_page_with_langchain, click_button, document_loader, get_llm_prices],
    USDA_SHIPPINGPOINT: [go_to_page, analyze_page_with_langchain, click_button, document_loader, get_llm_prices],
    USDA_CSV: [go_to_page, analyze_page_with_langchain, click_button, clean_body_content, get_llm_prices],
    USDA_DATAMART: [go_to_page, analyze_page_with_langchain, click_button,analyze_page_with_langchain, click_button, analyze_page_with_langchain, click_button, clean_body_content, document_loader, get_llm_prices],
    EMI: [go_to_page, analyze_page_with_langchain, login, go_to_page, clean_body_content, get_llm_prices],
    EIA: [go_to_page, clean_body_content, get_llm_prices],
    SOSLAND: [email_node, sosland_filename_matcher, document_loader, get_llm_prices],
    "email": [email_node, document_loader, get_llm_prices], #, "vesper", "cirad", "leftfield"
    JACOBSEN: [jacobsen_categorizer, go_to_page, analyze_page_with_langchain, login, go_to_page, clean_body_content, get_llm_prices ],
    UB: [go_to_page, analyze_page_with_langchain, login, analyze_page_with_langchain, click_button, analyze_page_with_langchain, click_button, email_node, analyze_page_with_langchain, login, go_to_page, clean_body_content, get_llm_prices], # high/low from table
    MINTEC: [go_to_page, analyze_page_with_langchain, login, go_to_page, analyze_page_with_langchain, click_button, clean_body_content, get_llm_prices], # high/low from table + click USD currency
    FASTMARKET: [go_to_page, analyze_page_with_langchain, login, clean_body_content, get_llm_prices],
    # "argus": [go_to_page, analyze_page_with_langchain, login,],
    # "rm_smp": [],
 }


def main(sic_status=[]):
    try:
        # for testing
        if not sic_status:
            sic_status = pd.read_csv("sics-alerts.csv")
            # Clean column names
            sic_status.columns = sic_status.columns.str.strip(" ,")
            # Clean string values
            sic_status = sic_status.map(lambda x: x.strip() if isinstance(x, str) else x)
            sic_status = sic_status.iloc[:, :4]

        # Getting SICs info from our DC
        clean_df = get_sic_info(sic_status)
    except Exception as e:
        logging.error(f"Error getting SIC info: {e}")
        return

    all_checked_sics = []

    # Initializing Selenium driver
    try:
        manager = WebDriverManager(download_dir)
        driver = manager.get_driver()
    except Exception as e:
        logging.error(f"Error initializing WebDriver: {e}")
        return

    # Running graphs per spider
    try:
        for spider in clean_df['spider'].unique():
            try:
                spider_data = clean_df[clean_df['spider'] == spider][
                    ['scrape_id', 'sic', "price_update_url", 'index_provider_code']
                ][~clean_df["specification"].str.contains("Bundle ", na=False)].copy()
                spider_data.rename(columns={'scrape_id': 'specification'}, inplace=True)
            except Exception as e:
                logging.error(f"Error filtering data for spider {spider}: {e}")
                continue

            route = []

            # Defining the routes for a graph depending on spider
            if spider in EMAIL_SPIDERS:
                route = GRAPH_ROUTES.get("email", [])
            else:
                route = next((value for key, value in GRAPH_ROUTES.items() if spider in key), [])
            if not route:
                logging.warning(f"No route found for spider {spider}")
                continue

            # Building and compiling the graph
            try:
                graph_builder = GraphBuilder(route, spider)
                graph = graph_builder.build()
            except Exception as e:
                logging.error(f"Error building graph for spider {spider}: {e}")
                continue

            # Defining the inputs of a graph
            inputs = {
                "specs": spider_data,
                "url": "",
                "spider": spider.lower(),
                "auth_code": None,
                "buttons_count": 0,
                "file_path": "",
                "logged_in": False,
                "driver": driver
            }
            # Defining the configurations of a graph, that's where we pass username/password is it requires login
            config = {
                "configurable": {
                    "username": env(f"{spider}_USERNAME"),
                    "password": env(f"{spider}_PSWRD")
                } if spider in login_spiders else {},
                "recursion_limit": 100
            }

            # Invoking the graph and saving the results
            try:
                result = graph.invoke(inputs, config, stream_mode="values")
                all_checked_sics.extend(result["result"])
            except Exception as e:
                logging.error(f"Error invoking graph for spider {spider}: {e}")
                continue

    finally:
        # Terminate the driver once all graphs run
        manager.quit_driver()

    if not all_checked_sics:
        logging.error("No results were processed.")
        return

    try:
        # Merge dataframes
        merged = merge_dataframes(sic_status, all_checked_sics, clean_df)

        # Create 'status' column
        merged = create_status_column(merged)

        # Sort and reorder columns
        merged = sort_and_reorder_columns(merged)

        # Write to CSV
        safe_write_to_csv(merged, output_file)

    except Exception as e:
        logging.error(f"Error during data processing: {e}", exc_info=True)


if __name__ == "__main__":
    main()




