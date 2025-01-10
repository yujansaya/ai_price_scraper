from typing import List
from pydantic import BaseModel, Field
from typing_extensions import TypedDict
from typing import Annotated
import operator
import pandas as pd
from selenium import webdriver


# The output of LLM for prices to make sure the format is correct
class PriceItem(BaseModel):
    """Information about specified item to tell user."""
    sic: str = Field(description="The sic index for each item from the user-provided dataframe")
    # specification: str = Field(description="The specification for each item from the user-provided dataframe")
    most_recent_price: List[float] = Field(
        description="The most recent price from the user-provided text for the corresponding specification")
    price_date: str = Field(description="The corresponding date of the most recent price from the user-provided text")


class Items(BaseModel):
    """Returning the list of items, final response of llm"""
    items: List[PriceItem]


# state of the graph class
class State(TypedDict):
    # The operator.add reducer fn makes this append-only
    result: Annotated[list, operator.add] #output
    spider: str #input
    url: str #input
    specs: pd.DataFrame #input
    current_specs: list
    login_xpath: dict[str, str] #internal
    button_xpath: Annotated[list, operator.add] #internal
    clean_content: str #internal
    active_button: str #internal
    file_path: str #internal
    auth_code: str #internal
    # ipc: list #input
    found_buttons: list #internal
    logged_in: bool #internal
    buttons_count: int #internal
    driver: webdriver