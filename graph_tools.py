import html
import os
import tiktoken
import pandas as pd
from langchain_core.prompts import PromptTemplate, ChatPromptTemplate
from langchain_core.runnables import RunnableConfig
import time
from items import State, Items
from bs4 import BeautifulSoup
from rapidfuzz import process
from langchain_community.document_loaders import UnstructuredExcelLoader, TextLoader
from langchain_community.document_loaders.csv_loader import CSVLoader
from unstructured.partition.pdf import partition_pdf
from selenium.common.exceptions import NoSuchElementException, TimeoutException
from langchain_groq import ChatGroq
from langchain_openai import ChatOpenAI
import re
from environs import Env
import imaplib
import email
from email.policy import default
from io import BytesIO
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from lxml import etree
from constants import *
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

env = Env()
env.read_env(".env")
GMAIL_USER = env("GMAIL_USER")
GMAIL_PASS = env("GMAIL_PASS")
download_dir = env("download_dir")

buttons_logic = {
    UB: ["'try another method' button", "'Email' button", "' Daily ' radio button", "' Contiguous ' radio button"],
    USDA_DATAMART: ["'continue' button", "'generate report' button", "first found <a> element within the <table> structure"],
    USDA_CSV: ["' Latest Report ' <a> element"],
    USDA_MARS: ["'latest report' link for pdf file"],
    USDA_SHIPPINGPOINT: ["'latest report' link for pdf file"],
    JACOBSEN: ["login"],
    MINTEC: ["login", "'USD' button", "'USD' button"],
    EMI: ['login'],
    FASTMARKET: ["login"],
    EIA: ["go_to"]
}

login_spiders = [EMI, JACOBSEN, UB, MINTEC, FASTMARKET]

CATEGORIES_JACOBSEN = ["Animal-Fats-and-Oils",
"Animal-Proteins",
"Sausage-Casings",
"Biodiesel",
"Hide-and-Leather",
"Hemp",
"Grain-and-Feed-Ingredients",
"Organics-Non-GMO",
"Vegetable-Oils",
"Animal-Fats-and-Oils-International",
"Animal-Proteins-International",
"Hide-and-Leather-International",]

EMAIL_SPIDERS = [VESPER, LEFTFIELD, CIRAD] # except Sosland, it has a bit different logic since there are 3 files to choose from, LLM will assign the filename based on SICs' specs

gpt_mini = ChatOpenAI(model="gpt-4o-mini",
                 # max_tokens=4096
                 )
gpt = ChatOpenAI(model="gpt-4o",
                 # max_tokens=4096
                 )
# groq = ChatGroq(model_name="llama-3.2-90b-vision-preview", temperature=0)
groq = ChatGroq(model_name="llama-3.1-70b-versatile", temperature=0)


def go_to_page(state: State):
    data_df = state['specs']
    data_list = []
    if state["spider"] == JACOBSEN:
        category = data_df["category"].unique()[0]
        url = f"https://members.thejacobsen.com/Price-Guide-Commentary/{data_df["category"].unique()[0]}.aspx"
        if state["logged_in"]:
            data_list = data_df[data_df["category"] == category][["sic", "specification"]].to_dict(orient="records")
            data_df.drop(data_df[data_df["category"] == category].index, inplace=True)
    elif any(keyword in state["spider"] for keyword in ["usda", "eia"]):
        delete_files()
        url = data_df["price_update_url"].unique()[0]
        data_list = data_df[data_df["price_update_url"] == url][["sic", "specification"]].to_dict(orient="records")
        data_df.drop(data_df[data_df["price_update_url"] == url].index, inplace=True)
    elif state["spider"] == EMI:
        if state["logged_in"]:
            url = "https://clients.expressmarketsinc.com/emi/chicken/daily/php/daily.php"
            data_list = data_df[["sic", "specification"]].to_dict(orient="records")
            data_df = pd.DataFrame()
        else:
            url = "https://clients.expressmarketsinc.com/login.php"
    elif state["spider"] == FASTMARKET:
        url = "https://www.risiinfo.com/ic/dashboard/recovered-paper-graphic-paper-pulp-packaging-paper-and-board-north-america/315134"
        # if state["logged_in"]:
        data_list = data_df[["sic", "specification"]].to_dict(orient="records")
        data_df = pd.DataFrame()
    else:
        data_df["index_provider_code"] = data_df["index_provider_code"].str.replace("Comcode: ", "", regex=False)
        ipc = data_df["index_provider_code"].iloc[0]
        url = f"https://www.comtell.com/markets/items/{ipc}" if state["spider"] == UB else f"https://www.mintecanalytics.com/workspace/commodity/search/{ipc}"
        if state["logged_in"]:
            data_list = data_df[data_df["index_provider_code"] == ipc][["sic", "specification"]].to_dict(orient="records")
            data_df.drop(data_df[data_df["index_provider_code"] == ipc].index, inplace=True)
    driver = state["driver"]
    driver.get(url)
    logging.info("Navigating to "+ url)
    time.sleep(10)
    return {"specs": data_df, "current_specs": data_list, "url": url}


def attempt_login(driver, username, password, login_xpath, auth_code=None):
    """Helper function for login node"""
    if auth_code:
        driver.find_element("xpath", login_xpath['code_input']).send_keys(auth_code)
        driver.find_element("xpath", login_xpath['continue_button']).click()
    else:
        driver.find_element("xpath", login_xpath['username']).send_keys(username)
        driver.find_element("xpath", login_xpath['password']).send_keys(password)
        driver.find_element("xpath", login_xpath['login_button']).click()


def login(state: State, config: RunnableConfig):
    driver = state["driver"]
    username = config.get("configurable", {}).get("username")
    password = config.get("configurable", {}).get("password")
    logging.info("Logging in to " + state["spider"])
    try:
        attempt_login(driver, username, password, state["login_xpath"], state["auth_code"])
        # if state["auth_code"]:
        #     driver.find_element("xpath", state["login_xpath"]['code_input']).send_keys(state["auth_code"])
        #     driver.find_element("xpath", state["login_xpath"]['continue_button']).click()
        #     time.sleep(3)
        #     return {"logged_in": True}
        # else:
        #     driver.find_element("xpath", state["login_xpath"]["username"]).send_keys(username)
        #     driver.find_element("xpath", state["login_xpath"]['password']).send_keys(password)
        #     driver.find_element("xpath", state["login_xpath"]['login_button']).click()
    except (IndexError, NoSuchElementException, Exception) as e:
        logging.error(f"Error encountered: {str(e)}. Locating with GPT...")
        # Clearing the fields
        driver.find_element("xpath", state["login_xpath"]["username"]).clear()
        driver.find_element("xpath", state["login_xpath"]['password']).clear()
        # Locate dynamically with GPT and click
        try:
            element = analyze_page_with_langchain(state, True)
            logging.info("Trying to login again: " + str(element))
            attempt_login(driver, username, password, state["login_xpath"], state["auth_code"])
            # if state["auth_code"]:
            #     driver.find_element("xpath", element["login_xpath"]['code_input']).send_keys(state["auth_code"])
            #     driver.find_element("xpath", element["login_xpath"]['continue_button']).click()
            # else:
            #     driver.find_element("xpath", element["login_xpath"]["username"]).send_keys(username)
            #     driver.find_element("xpath", element["login_xpath"]['password']).send_keys(password)
            #     driver.find_element("xpath", element["login_xpath"]['login_button']).click()
        except Exception as final_error:
            logging.error(f"Final error while locating and clicking: {str(final_error)}")
    time.sleep(15 if state["spider"] == FASTMARKET else 8)
    return {"logged_in": False} if (state["spider"] == UB and not state["auth_code"]) else {"logged_in": True}


def get_llm_prices(state: State,  # data = None
                   ):
    prompt_template = """
You are provided with extracted text (from web, PDF, or Excel) containing tables and a user-supplied dataframe. Your task is to extract the most recent price and its corresponding date for each specification in the dataframe, following these instructions:

1. Match Specifications Accurately
Match product specifications in the dataframe with those in the text, accounting for minor differences (e.g., region, percentage, unit size, or other variations).
For example, treat "1/8 Trim Bnls Butt 1 Pc VAC" and "1/8 Trim Bnls Butt VAC" as distinct products, noting "1 Pc" as the difference. So it must match perfectly the item_description from specification. Always choose the closest possible match.
If identical product names appear across different tables (e.g., "Choice Cuts" vs. "Select Cuts"), treat them as separate items.
Consider table names, geography, and headers for contextual clues.
2. Extract Date and Price
For each specification:
Extract the most recent date and its corresponding price.
Return prices as a list of floats, prioritizing:
Top Third price/Bottom Third price, 
or mostly high/mostly low,
or HIGH/LOW prices.
If none of mentioned is available, use the weighted average price (wtd avg).
If there is just a 'Price Range', return Wtd Avg.
It is very much possible that there is no price for the particular product even if it is listed in the text, so just return "0.0".
Ensure you match the correct product and price with absolute certainty. You may extract ONLY ONE product per specification.
3. Pricing Context
Differentiate products with the same name based on pricing frequency (e.g., daily vs. weekly prices) using headers or contextual information.
Treat identical products across different tables as separate items (e.g., "Choice Cuts" and "Select Cuts").
Consider geography (e.g., table names or regions).
If a product appears as FROZEN (FZN), treat them as distinct product from the one that is the same description emitted FZN. 

Return results in the following structure for each dataframe item:
    "sic": The product's SIC index from the dataframe.
    "most_recent_price": A list of floats representing the most recent prices (Top Third and Bottom Third or mostly high and mostly low or High and Low prices, if available, if no, just one corresponding price.)
    "price_date": The corresponding date of the extracted price.
    
    Special Considerations:
If multiple entries exist for the same product across dates, prioritize the most recent date.
Pay extra attention to distinctions based on geography, table headers, and product details.
If there are table names, make sure to identify first which table to use, which name is closest to the specification.
    """
    prompt = ChatPromptTemplate.from_messages(
        [
            (
                "system",
                prompt_template,
            ),
            ("user", "{dataframe}, \n {web_text}"),
        ])
    # llm = gpt if state["spider"] in ["usda_datamart", "usda_shippingpoint", "sosland", "usda_mars"] else groq
    chain = prompt | gpt.with_structured_output(
        Items)  # gpt.with_structured_output(Items) if state["file_path"] else groq_70b_json
    logging.info("passed html to llm to get prices...")
    # Get predictions from the LLM
    result = chain.invoke({"dataframe": state["specs"][["sic", "specification"]].to_dict(orient="records") if state[
                                                                                        "spider"] in EMAIL_SPIDERS else
    state["current_specs"], "web_text": state["clean_content"]})
    try:
        os.remove(state['file_path'])
        logging.info(f"{state['file_path']} has been deleted.")
    except FileNotFoundError or KeyError:
        pass

    return {"result": dict(result)["items"], }


def click_button(state: State):
    driver = state["driver"]
    try:
        logging.info("ACTIVE BUTTON: " + state['active_button'])
        driver.find_element("xpath", state['active_button']).click()
        logging.info("clicked" + state['active_button'])
    except NoSuchElementException as e:
        logging.error(f"NoSuchElementException for ACTIVE BUTTON: {state['active_button']} - {str(e)}")
        # Try clicking the first button in found_buttons
        try:
            logging.info("Trying FOUND BUTTON: " + state['found_buttons'][0])
            driver.find_element("xpath", state['found_buttons'][0]).click()
            logging.info("Clicked: " + state['found_buttons'][0])
        except (IndexError, NoSuchElementException) as e:
            logging.error(f"Fallback Error: {str(e)}. Locating with GPT...")
            # Locate dynamically with GPT and click
            try:
                element = analyze_page_with_langchain(state, True)
                logging.info("Clicked dynamically located element: " + element)
                driver.find_element("xpath", element).click()
            except Exception as final_error:
                logging.error(f"Final error while locating and clicking: {str(final_error)}")
    except Exception as generic_error:
        logging.error(f"Unexpected error: {str(generic_error)}")
    finally:
        logging.info("Success!")
        time.sleep(5)


def usda_clean_body(tables, date):
    # Extract tables with more than 1 column
    table_data = []
    for table in tables[3:]:
        rows = table.find_all('tr')
        try:
            # Check column count (from the first row)
            if rows:
                table_name = rows[0].find_all('th')[0].get_text(strip=True)
                first_row = rows[1].find_all('th')
                # Extract headers
                headers = [header.get_text(strip=True) for header in first_row]
                if not headers:
                    headers = [f"Column_{i}" for i in range(len(first_row))]

                # Extract data rows
                table_rows = []
                for row in rows[2:]:
                    cells = [cell.get_text(strip=True) for cell in row.find_all(['td'])]
                    table_rows.append(cells)

                # Create DataFrame
                df = pd.DataFrame(table_rows, columns=headers)
                df.name = table_name
                table_data.append(df)

                logging.info(f"Extracted Table with {len(headers)} columns")
        except Exception as e:
            pass
    # Initialize a list to store the results with the table name added
    table_dicts_with_names = []

    # Iterate over all tables in the table_data list
    for df in table_data:
        # Retrieve the table's name
        table_name = df.name  # Get the table's name (set previously)

        # Convert the DataFrame to a list of dictionaries
        table_dict = df.to_dict(orient="records")

        # Add the table name to each record
        for record in table_dict:
            record["table_name"] = table_name
            record["date"] = date

        # Append the table's records with the table name to the result list
        table_dicts_with_names.append(table_dict)
    return table_dicts_with_names


def clean_body_content(state: State):
    driver = state["driver"]
    # Get all window handles (tabs)
    tabs = driver.window_handles
    if len(tabs) > 1:
        # Switch to the newly opened tab (last in the list)
        driver.switch_to.window(tabs[-1])
    try:
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.TAG_NAME, "table"))
        )
        logging.info("Table found!")
    except TimeoutException:
        logging.error("Table did not appear within the given time.")
    body_content = driver.page_source
    soup = BeautifulSoup(body_content, "html.parser")
    if state["spider"] == USDA_DATAMART:
        html_tree = etree.HTML(str(soup))
        # Find the date before formatting tables
        first_a_in_table = html_tree.xpath(state["active_button"])[0]
        return {"clean_content": usda_clean_body(soup.find_all('table'), first_a_in_table.text)}
    logging.info("Cleaning HTML...")
    if state["spider"] not in [JACOBSEN, EMI, FASTMARKET, EIA]:
        try:
            # Process each table in the document
            for table in soup.find_all('table'):
                rows = table.find_all('tr')

                # If table has more than 5 rows, keep only the first 5
                if len(rows) > 25:
                    logging.info("cutting off rows....")
                    # Keep only the first 5 rows
                    first_rows = rows[:25]
                    last_rows = rows[-5:]
                    combined_rows = first_rows + last_rows
                    # Clear the table content and reinsert only the first 5 rows
                    table.clear()
                    for row in combined_rows:
                        table.append(row)
        except Exception as e:
            pass
    # Find and delete the div with id "full-chart-content" for mintec
    target_div = soup.find("div", id="full-chart-content")
    if target_div:
        target_div.decompose()
    for tag in soup(
            ['style', 'script', 'meta', 'img', 'b', 'i', 'u', 'font', 'small', 'big', 'center', 'head', 'option',
             'p' if state["spider"] == JACOBSEN else ""]):
        tag.decompose()
    # Get text or further process the content
    if state["spider"] in [UB, FASTMARKET, EIA]:
        # Extract all tables
        tables = soup.find_all("table")
        table_text = tables[0].get_text(separator="\n", strip=True)
        return {"clean_content": table_text}
    cleaned_content = soup.get_text(separator="\n")
    cleaned_content = "\n".join(
        line.strip() for line in cleaned_content.splitlines() if line.strip()
    )
    if state['spider'] == USDA_CSV:
        # Split the content into lines
        lines = cleaned_content.strip().split('\n')

        # Extract the first `num_rows` and last `num_rows` rows
        first_rows = lines[:2]
        last_rows = lines[-1:]

        # Combine first and last rows
        combined_rows = first_rows + last_rows
        return {"clean_content": combined_rows}

    all_text = ' '.join(table.get_text(separator='\n', strip=True) for table in soup.find_all('table'))
    return {"clean_content": cleaned_content if state["spider"] == EMI else all_text}


def extract_page_content(state: State):
    driver = state["driver"]
    page_html = driver.page_source
    soup = BeautifulSoup(page_html, "html.parser")
    logging.info("Extracting HTML...")
    # Find and delete the div with id "full-chart-content" for mintec
    target_div = soup.find("div", id="full-chart-content")
    if target_div:
        target_div.decompose()
    # Remove unnecessary tags
    for tag in soup(
            ['style', 'script', 'meta', 'img', 'b', 'i', 'u', 'em', 'strong', 'font', 'small', 'big', 'center', 'head',
             'option']):
        tag.decompose()

    try:
        # Process each table in the document
        for table in soup.find_all('table'):
            rows = table.find_all('tr')

            # If table has more than 5 rows, keep only the first 5
            if len(rows) > 5:
                # Keep only the first 5 rows
                logging.info("cutting off rows....")
                first_five_rows = rows[:5]
                last_rows = rows[-5:]
                combined_rows = first_five_rows + last_rows
                # Clear the table content and reinsert only the first 5 rows
                table.clear()
                for row in combined_rows:
                    table.append(row)
    except Exception as e:
        pass

    # Convert to a string and remove extra whitespace and newlines
    # cleaned_html = str(soup).replace('\n', '').replace('\t', '').strip()
    decoded_html = html.unescape(str(soup))
    return decoded_html


def analyze_page_with_langchain(state: State, use_GPT=False):
    """To analyse html body to find XPaths of requested objects."""
    buttons = []
    spider = state["spider"]
    button_count = state["buttons_count"] if not use_GPT else state[
                                                                  "buttons_count"] - 1  # if we triggered use_GPT be True in click_button function, it means the XPath found by llama model is wrong, so we need to go back to previously analyzed button
    if button_count >= len(buttons_logic[spider]) and not use_GPT:
        button_count = 2 if spider in ["urner_barry_api",
                                       "mintec"] else 0  # reseting the number of clicked buttons after finishing one whole cycle of the graph
    if state["spider"] in login_spiders and not button_count:
        format = """    
            {{
                  username: XPath for username input,
                  password: XPath for password input,
                  login_button: XPath for login/next button
            }}
                """
    elif state["spider"] in login_spiders and state["auth_code"]:
        format = """    
            {{
                  code_input: XPath for 'Enter the code' input,
                  continue_button: XPath for 'Continue' button
            }}
                """
    else:
        format = f"""    
        {{
              button_xpath: XPath of {buttons_logic[state["spider"]][button_count - 1 if state["spider"] == UB else button_count]},
        }}
            """
    prompt_template = """
    Given the following HTML content:

    {html_content}

    Locate only the elements described in {format}, matching each element exactly as specified without guessing or assuming. Pay extra attention not only to element type (like button) but also particular text that describes it. If an element is missing from the content, respond with 'Element not found' for that specific entry. Focus solely on identifying the exact element requested.

    Return the XPath for each element as a dictionary in the JSON format below, with only ONE unique XPath per element, sufficient to locate it dynamically. Focus on returning xpath of a good quality format, to easily locate elements and to avoid causing "NoSuchElement" errors. Pay extra attention to row count if it's inside table for example, to locate properly the element. If you want to match an element containing a specific text (substring), use the contains() function.

    Your response should strictly follow the JSON structure and include nothing else:
    Format:
    {format}
    """

    prompt = PromptTemplate(template=prompt_template, input_variables=["html_content", "format"])
    chain = prompt | gpt_mini.with_structured_output(method="json_mode")
    # chain = prompt | gpt_mini.with_structured_output(method="json_mode") if state["spider"] in ["mintec","fastmarketrisi"] or use_GPT else prompt | groq.with_structured_output(method="json_mode")
    logging.info(f"passed html to llm to find {format}...")

    # def process_chunk(chunk):
    #     # chain = prompt | gpt_mini.with_structured_output(method="json_mode") if state["spider"] == "mintec" else groq.with_structured_output(method="json_mode")
    #     result = chain.invoke({"html_content": chunk, "format": format})
    #     if result['button_xpath'] != "Element not found":
    #         return result['button_xpath']
    #     return None

    html_content = extract_page_content(state)
    try:
        # Get predictions from the LLM
        result = chain.invoke(
            {"html_content": html_content.replace('\n', '').replace('\t', '').strip(), "format": format})
        logging.info(f"Found XPaths: {result}")
    except Exception as e:
        logging.error("Error: " + str(e))
        logging.info("Splitting into HTML into smaller chunks... ")
        chunks = tokenize_and_split_html(html_content)
        # with ThreadPoolExecutor() as executor:
        #     # Map chunks to the processing function
        #     results = executor.map(process_chunk, chunks)
        for chunk in chunks:
            result = chain.invoke({"html_content": chunk, "format": format})
            if result['button_xpath'] != "Element not found":
                buttons.append(result['button_xpath'])
        best_match, score, _ = process.extractOne(format.strip("}{\nbutton_xpath"), buttons)
        buttons.remove(best_match)
        logging.info(f"Found XPath: {best_match}")
        button_count += 1
        return {"button_xpath": [best_match], "active_button": best_match, "found_buttons": buttons,
                "buttons_count": button_count} if not use_GPT else best_match
    button_count += 1
    if len(result) == 1:
        return {"button_xpath": [result["button_xpath"]], "active_button": result["button_xpath"],
                "buttons_count": button_count} if not use_GPT else result["button_xpath"]
    else:
        return {"login_xpath": result, "buttons_count": button_count}


def document_loader(state: State):
    """This tool reads downloaded reports and returns the extracted prices and corresponding dates."""
    path = ""
    data = ""
    # time.sleep(8)
    if state["spider"] == SOSLAND:
        data_df = state['specs']
        # data_df = pd.DataFrame(state['specs'])
        filename = data_df["filename"].unique()[0]
        data_list = data_df[data_df["filename"] == filename].to_dict(orient="records")
        data_df.drop(data_df[data_df["filename"] == filename].index, inplace=True)
        path = download_dir + "/" + filename
        loader = UnstructuredExcelLoader(path, mode="elements")
        data = loader.load()
        # data_df.to_dict(orient="records")
        return {"clean_content": data, "file_path": path, "specs": data_df, "current_specs": data_list, }
    else:
        for filename in os.listdir(download_dir):
            logging.info("Loading document " + filename)
            path = download_dir + "/" + filename
            if filename.lower().endswith('.txt'):
                loader = TextLoader(path)
                data = loader.load()
            elif filename.lower().endswith('.csv'):
                loader = CSVLoader(path)
                data = loader.load()
            elif filename.lower().endswith('.xlsx') or filename.lower().endswith('.xls'):
                loader = UnstructuredExcelLoader(path, mode="elements")
                data = loader.load()
            elif filename.lower().endswith('.pdf'):
                # loader = PyPDFLoader(download_dir + "/" + filename, extract_images=False)
                try:
                    raw_pdf_elements = partition_pdf(
                        filename=path,
                        extract_images_in_pdf=False,
                        infer_table_structure=True,
                        chunking_strategy="by_title",
                        max_characters=4000,
                        new_after_n_chars=3800,
                        combine_text_under_n_chars=2000,
                        image_output_dir_path="",
                    )
                    for page in raw_pdf_elements:
                        data += str(page)
                except OSError as e:
                    logging.error(e)
            else:
                logging.info(f"The file extension is not supported for file {filename}.")
            return {"clean_content": data, "file_path": path}


def jacobsen_categorizer(state: State):
    format = """    
    [{{
          category: chosen category from the list of categories,
          sic: corresponding sic index from provided dataframe,       
    }},]
        """
    data = state["specs"][["specification", "sic"]].to_dict(orient="records")
    prompt_template = """
    Given the following list of categories:

    {categories}

    Analyze to which category belongs each product in the provided dictionary based on its specification: {product}. Pay very close attention to the specification. International categories are in the perspective of USA, so if there is any country/region mentioned in the specification that is outside of the USA, it should belong to one fo the international categories. But not all categories have international equivalent. Any organic/non-gmo product goes to organic/non-gmo category. Animal-fats-and-oils category is ONLY for fats and oils derived from animals (except used cooking oil and Distiller's Corn Oil that would belong to this category). Choose categories STRICTLY ONLY from the provided list of categories, do not assume categories! Make sure to categorize ALL items from the dictionary.

    Your response should strictly follow the JSON structure and include nothing else:

    Format:

    {format}
    """

    prompt = PromptTemplate(template=prompt_template, input_variables=["categories", "format", "product"])
    chain = prompt | groq.with_structured_output(method="json_mode")
    logging.info(f"Assigning categories for Jacobsen SICs...")
    # Get predictions from the LLM
    result = chain.invoke({"categories": CATEGORIES_JACOBSEN, "format": format, "product": data})
    result_list = next(iter(result.values()))
    logging.info(f"Check if LLM returned all the requested SICs by comparing lengths of 2 lists: {len(result_list)}, {len(data)}")
    logging.info(f'Result: {result_list}')
    df_merged = pd.merge(state["specs"], pd.DataFrame(result_list), on='sic', how='left')
    return {'specs': df_merged}


def sosland_filename_matcher(state: State):
    format = """    
    [{{
          filename: chosen filename from the list of filenames,
          sic: corresponding sic index from provided dataframe,       
    }},]
        """
    data = state["specs"][["sic", "specification"]].to_dict(orient='records')
    prompt_template = """
    Given the following list of filenames:
    {filenames}
    Analyze to which filename belongs each product in the provided dataframe based on its specification: {product}. Just to note, there are some specifications containing "HFCS" which stands for "High Fructose Corn Syrup". Make sure to return ALL products provided in the dataframe.
    Your response should strictly follow the JSON structure and include nothing else:
    Format:
    {format}
    """
    filenames = os.listdir(download_dir)
    prompt = PromptTemplate(template=prompt_template, input_variables=["filenames", "format", "product"])
    chain = prompt | groq.with_structured_output(method="json_mode")
    logging.info(f"Assigning best matching filename to Sosland SICs...")
    result = chain.invoke({"filenames": filenames, "format": format, "product": data})
    result_list = next(iter(result.values()))
    logging.info(f"Check if LLM returned all the requested SICs by comparing lengths of 2 lists: {len(result_list)}, {len(data)}")
    df_merged = pd.merge(state["specs"], pd.DataFrame(result_list), on='sic', how='left')
    logging.info(f'Result: {result_list}')
    return {'specs': df_merged}


# Function to split based on token count while preserving HTML structure
def tokenize_and_split_html(html, max_tokens=5800):
    tokenizer = tiktoken.get_encoding("cl100k_base")
    chunks = []
    current_chunk = []
    current_tokens = 0

    # for html in html_list:
    tokens = tokenizer.encode(html)
    token_count = len(tokens)

    # If a single HTML string is larger than max_tokens, split it further
    if token_count > max_tokens:
        logging.info(f"Splitting large HTML content of {token_count} tokens")

        # Split the large HTML string into lines or smaller pieces
        lines = html.split('\n')
        for line in lines:
            line_tokens = tokenizer.encode(line)
            line_token_count = len(line_tokens)

            # If adding this line exceeds the limit, finalize the current chunk
            if current_tokens + line_token_count > max_tokens:
                chunks.append("".join(current_chunk))
                current_chunk = []
                current_tokens = 0

            # Add the line to the current chunk
            current_chunk.append(line)
            current_tokens += line_token_count

    else:
        # Handle normally if the HTML string is within token limits
        logging.info("Retrying to locate elements XPaths...")
        if current_tokens + token_count > max_tokens:
            chunks.append("".join(current_chunk))
            current_chunk = []
            current_tokens = 0

        current_chunk.append(html)
        current_tokens += token_count

    # Add the last chunk
    if current_chunk:
        chunks.append("".join(current_chunk))

    return chunks


def delete_files():
    # Delete previous reports in the directory to avoid confusion
    for filename in os.listdir(download_dir):
        file_path = os.path.join(download_dir, filename)
        # Check if it is a file and delete
        os.remove(file_path)
        logging.info(f"Deleted: {file_path}")


def email_node(state: State):
    # time.sleep(5)
    delete_files()

    # Connect to the IMAP server
    mail = imaplib.IMAP4_SSL("imap.gmail.com")
    mail.login(GMAIL_USER, GMAIL_PASS)
    mail.select("inbox")

    # Search for the latest email from COMTELL or Expana with a verification code
    status, messages = mail.search(None, "X-GM-RAW",
                                   f'"{state["spider"].lower() if state["spider"].lower() != UB else "comtell"}"')
    email_ids = messages[0].split()
    if email_ids:
        # Fetch the latest email ID
        latest_email_id = email_ids[-1]
        status, msg_data = mail.fetch(latest_email_id, "(RFC822)")
        raw_email = msg_data[0][1]

        # Parse the email
        msg = email.message_from_bytes(raw_email, policy=default)
        if state["spider"] == UB:
            # Extract the email body text
            logging.info("Logging into the mail box...")
            email_body = ""
            if msg.is_multipart():
                for part in msg.iter_parts():
                    if part.get_content_type() == "text/plain":
                        email_body = part.get_payload(decode=True).decode()
                        break
            else:
                email_body = msg.get_payload(decode=True).decode()
            # Find the verification code using regex (assuming it's a 6-digit code)
            code_match = re.search(r'\b\d{6}\b', email_body)
            if code_match:
                verification_code = code_match.group(0)
                logging.info(f"Verification Code for Urner Barry: {verification_code}")
                return {"auth_code": verification_code}
            else:
                logging.info("No verification code for Urner Barry found in the latest email.")
                return {"auth_code": ""}
        else:
            # Find and download attachments
            for part in msg.iter_attachments():
                # Get the content type and filename of the attachment
                content_type = part.get_content_type()
                filename = part.get_filename()

                # Check if the attachment is one of the desired file types
                if content_type in ["application/pdf",
                                    "text/plain",
                                    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                    "text/csv"]:
                    if filename:  # Ensure the attachment has a filename
                        # Define the path to save the file
                        filepath = os.path.join(download_dir, filename)
                        # For Excel files, process rows in each sheet while keeping original formatting
                        if content_type == "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet":
                            # Load the attachment into a pandas dictionary with sheet names as keys
                            attachment_data = part.get_payload(decode=True)
                            excel_data = pd.read_excel(BytesIO(attachment_data), sheet_name=None, header=None)

                            # Create a dictionary to store the processed sheets
                            trimmed_sheets = {}
                            # Process each sheet individually
                            for sheet_name, sheet_data in excel_data.items():
                                # Check if the sheet has more than 15 rows
                                if state["spider"].lower() == CIRAD:
                                    trimmed_data = sheet_data.iloc[:, [0, -1]]
                                    trimmed_data.dropna(inplace=True)
                                elif len(sheet_data) > 15:
                                    sheet_data.dropna(thresh=2, inplace=True)
                                    # Select first 10 and last 5 rows without altering headers or structure
                                    trimmed_data = pd.concat([sheet_data.iloc[:6, :], sheet_data.iloc[-5:, :]],
                                                             ignore_index=True)
                                else:
                                    # If less than 15 rows, keep the entire sheet
                                    trimmed_data = sheet_data

                                # Add the processed sheet to the dictionary
                                trimmed_sheets[sheet_name] = trimmed_data

                            # Save the processed sheets to a new Excel file while preserving formatting
                            with pd.ExcelWriter(filepath, engine="openpyxl") as writer:
                                for sheet_name, sheet_data in trimmed_sheets.items():
                                    sheet_data.to_excel(writer, sheet_name=sheet_name, index=False, header=False)
                        else:
                            # For other file types, save the attachment as usual
                            with open(filepath, "wb") as f:
                                f.write(part.get_payload(decode=True))
    else:
        logging.info(f"No emails found from {state['spider']}.")

    mail.logout()