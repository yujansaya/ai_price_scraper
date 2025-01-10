# Repository Overview
This repository provides a pipeline for investigating SICs found in di_sic_status_check. It extracts, processes, and analyzes commodity prices from various data sources, including web scraping, document processing, and interaction with LLMs. The tool handles multiple spiders (data sources), processes extracted information, and evaluates the status of SICs based on price and date consistency.

## Features
* ##### Graph-Based Workflow Execution:

Uses a StateGraph to dynamically build workflows for data extraction and processing tailored to each spider.
Supports spiders like USDA, Urner Barry, Mintec, and more.
* ##### LLM Integration:

Leverages OpenAI and Groq LLMs for content extraction, classification, and decision-making tasks.
Generates structured outputs for price and date consistency checks.
* ##### Data Cleaning & Validation:

Ensures extracted data is clean and structured, including handling PDFs, Excel, CSV, and web content.
Filters irrelevant data and validates extracted prices against a user-supplied specification list.
* ##### Dynamic Spider Support:

Modular design allows easy addition of new spiders with custom workflows.
Handles different content types (tables, documents, emails) and login mechanisms.
* ##### Output:

Generates a consolidated CSV file with SICs, extracted prices, corresponding dates, and a status column indicating potential discrepancies.

# Installation
##### Prerequisites
* Python 3.8 or higher
* Required Python libraries: check requirements.txt file
##### Setup
Clone the repository:
```
git clone <repository_url>
cd <repository_directory>
```
Install dependencies:
```
pip install -r requirements.txt
```
Set up environment variables in a .env file:
```
GMAIL_USER=<your_gmail_address>
GMAIL_PASS=<your_gmail_password>
<spider>_USERNAME=<username>
<spider>_PSWRD=<password>
```
Define the download directory for processed files in main.py:
```
download_dir = "/path/to/download/directory"
```
# Usage
##### Running the Script
Execute the main pipeline:
```
python main.py
```

##### Input File
(only for testing, in production dataframe from `data-intelligence-cronjobs/sic_status_check.py` is passed.)
* Place the SICs CSV file (`sics-alerts-test.csv`) in the working directory. Ensure it contains:
  * `sic`, `spider`, `end_date`, and `price_frequency` columns.

##### Output
The output CSV file (`llm_sics_status_check.csv`) will include:
* **SIC**: According SIC from our database.
* **Stable** End Date: Latest known price date.
* **Stable Last Price**: Latest known price.
* **LLM Price Date**: Extracted date of the most recent price.
* **LLM Price**: Extracted most recent price.
* **Status**: Indicates consistency or issues (e.g., "Needs Attention: Same Price, Different Dates").
* **Specification**: According specification from our database.

# Key Components
##### Graph-Based Workflow
* `graph_builder.py`: Dynamically builds execution graphs for each spider, handling unique logic for various data sources.
* `graph_nodes.py`: Defines individual conditional nodes and their logic (e.g., deciding route for login spiders).
* `items.py`: Validates LLM outputs, manages workflow state:
  1. **LLM Output Validation**:
     * `PriceItem` and `Items` classes (via `pydantic`) ensure extracted prices and dates conform to the expected schema.
  2. **Graph Workflow Management**:
     * `State` class stores inputs (e.g., specifications, URLs), intermediate results, and outputs for the dynamic execution of workflows.
##### LLM Integration
* `graph_tools.py`: Describes all functions used by graphs such as HTML extraction, interaction with webpages, downloading reports etc. Integrates LLMs for tasks like price extraction, category matching (for Jacobsen), and HTML element XPath identification.
* Supports both OpenAI (`gpt-4o`) and Groq models (`llama 3.1 70b`) for different spiders. Uses dynamic prompts.
##### Main Execution
* `main.py`: Orchestrates the end-to-end pipeline:
  1. Reads and cleans input data.
  2. Dynamically assigns spiders and builds their workflows.
  3. Processes each SIC and consolidates results into a CSV file.
* `utils.py`: Provides tools for fetching and processing SIC information, integrating API data, generating price status reports, and handling Slack notifications for extracted data.
# Extending the Project
##### Adding New Spiders
1. Define the spider's workflow in `GRAPH_ROUTES` in `main.py`.
2. Implement spider-specific logic in `graph_nodes.py` or `graph_tools.py` if new functions need to be implemented.
##### Adding New LLM Tasks
1. Define a new prompt in `graph_tools.py`.
2. Use the `ChatPromptTemplate` to structure input and output.
3. Integrate the LLM task into an appropriate graph node.
# Troubleshooting
* **Environment Variables Missing**: Ensure all required environment variables are defined in the `.env` file.
* **Driver Errors**: Verify that Selenium WebDriver is installed and compatible with your browser version.
* **LLM Output Issues**: Adjust prompt templates for clarity and specificity if the LLM outputs incorrect data.
# Future Enhancements
* Add support for additional data formats and spiders.
* Optimize several graphs execution with parallel processing if needed (though it will cause much larger amounts of tokens per minute).

