from langgraph.graph import START, StateGraph, END
from items import State
from graph_nodes import *
from graph_tools import *


class GraphBuilder:
    def __init__(self, route, spider):
        self.route = route
        self.spider = spider
        self.builder = StateGraph(State)

    def add_nodes(self):
        for node in set(self.route):
            self.builder.add_node(node.__name__, node)

    def add_edges(self):
        self.builder.add_edge(START, self.route[0].__name__)

        if self.spider in ["usda_csv", "usda_mars", "usda_shippingpoint", "eia", "fastmarketrisi"]:
            for i in range(1, len(self.route)):
                self.builder.add_edge(self.route[i - 1].__name__, self.route[i].__name__)
            self.builder.add_conditional_edges("get_llm_prices", jacobsen2, [END, "go_to_page"])
        elif self.spider == "sosland":
            for i in range(1, len(self.route)):
                self.builder.add_edge(self.route[i - 1].__name__, self.route[i].__name__)
            self.builder.add_conditional_edges("get_llm_prices", sosland, [END, "document_loader"])
        elif self.spider in ["vesper", "leftfield", "cirad"]:
            for i in range(1, len(self.route)):
                self.builder.add_edge(self.route[i - 1].__name__, self.route[i].__name__)
            self.builder.add_edge("get_llm_prices", END)
        elif self.spider == "urner_barry_api":
            self.builder.add_conditional_edges("go_to_page", urner_barry1,
                                          ["clean_body_content", "analyze_page_with_langchain"])
            self.builder.add_conditional_edges("click_button", urner_barry2, ["email_node", "analyze_page_with_langchain"])
            self.builder.add_conditional_edges("login", urner_barry3, ["go_to_page", "analyze_page_with_langchain"])
            self.builder.add_conditional_edges("analyze_page_with_langchain", urner_barry4, ["click_button", "login"])
            self.builder.add_edge("email_node", "analyze_page_with_langchain")
            self.builder.add_edge("clean_body_content", "get_llm_prices")
            self.builder.add_conditional_edges("get_llm_prices", jacobsen2, [END, "go_to_page"])
        elif self.spider == "mintec":
            self.builder.add_edge("go_to_page", "analyze_page_with_langchain")
            self.builder.add_conditional_edges("analyze_page_with_langchain", mintec, ["click_button", "login"])
            self.builder.add_edge("login", "go_to_page")
            self.builder.add_edge("click_button", "clean_body_content")
            self.builder.add_edge("clean_body_content", "get_llm_prices")
            self.builder.add_conditional_edges("get_llm_prices", jacobsen2, [END, "go_to_page"])
        elif self.spider == "usda_datamart":
            self.builder.add_conditional_edges("go_to_page", usda, ["document_loader", "analyze_page_with_langchain"])
            # self.builder.add_edge(self.route[0].__name__, self.route[1].__name__) # datamart
            self.builder.add_edge(self.route[1].__name__, self.route[2].__name__)  # datamart
            self.builder.add_conditional_edges("click_button", datamart,
                                          ["analyze_page_with_langchain", "clean_body_content"])
            self.builder.add_edge("clean_body_content", "get_llm_prices")
            self.builder.add_edge("document_loader", "get_llm_prices")
            self.builder.add_conditional_edges("get_llm_prices", jacobsen2, [END, "go_to_page"])
        elif self.spider == "the_jacobsen":
            self.builder.add_edge(self.route[0].__name__, self.route[1].__name__)  # datamart
            # self.builder.add_edge(self.route[1].__name__, self.route[2].__name__) #datamart
            self.builder.add_conditional_edges("go_to_page", jacobsen1,
                                          ["analyze_page_with_langchain", "clean_body_content"])
            self.builder.add_conditional_edges("get_llm_prices", jacobsen2, [END, "go_to_page"])
            # self.builder.add_edge("click_button", "clean_body_content")
            self.builder.add_edge("analyze_page_with_langchain", "login")
            self.builder.add_edge("clean_body_content", "get_llm_prices")
            self.builder.add_edge("login", "go_to_page")
        elif self.spider == "emi":
            self.builder.add_conditional_edges("go_to_page", jacobsen1,
                                          ["analyze_page_with_langchain", "clean_body_content"])
            self.builder.add_edge("analyze_page_with_langchain", "login")
            self.builder.add_edge("login", "go_to_page")
            self.builder.add_edge("clean_body_content", "get_llm_prices")
            self.builder.add_edge(self.route[-1].__name__, END)
        else:
            pass

    def build(self):
        self.add_nodes()
        self.add_edges()
        return self.builder.compile()
