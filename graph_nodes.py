from typing import Sequence
from langgraph.graph import END
from items import State


def datamart(state: State) -> Sequence[str]:
    if state["buttons_count"] < 3:
        return ["analyze_page_with_langchain"]
    else:
        return ["clean_body_content"]


def usda(state: State) -> Sequence[str]:
    if ".pdf" in state["url"] or ".txt" in state["url"]:
        return ["document_loader"]
    else:
        return ["analyze_page_with_langchain"]


def jacobsen1(state: State) -> Sequence[str]:
    if state["logged_in"]:
        return ["clean_body_content"]
    else:
        return ["analyze_page_with_langchain"]


def jacobsen2(state: State) -> Sequence[str]:
    if state["specs"].empty:
        return [END]
    else:
        return ["go_to_page"]


def sosland(state: State) -> Sequence[str]:
    if state["specs"].empty:
        return [END]
    else:
        return ["document_loader"]


def urner_barry1(state: State) -> Sequence[str]:
    if state["auth_code"]:
        return ["clean_body_content"]
    else:
        return ["analyze_page_with_langchain"]


def urner_barry2(state: State) -> Sequence[str]:
    if state["buttons_count"] == 3:
        return ["email_node"]
    else:
        return ["analyze_page_with_langchain"]


def urner_barry3(state: State) -> Sequence[str]:
    if state["auth_code"]:
        return ["go_to_page"]
    else:
        return ["analyze_page_with_langchain"]


def urner_barry4(state: State) -> Sequence[str]:
    if state["auth_code"] or state["buttons_count"] == 1:
        return ["login"]
    else:
        return ["click_button"]


def mintec(state: State) -> Sequence[str]:
    if state["logged_in"]:
        return ["click_button"]
    else:
        return ["login"]