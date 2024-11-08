from utils import load_configs

import os

configs = load_configs()


class SubgraphHelper:
    def __init__(self):
        self.baseurl = configs["thegraph"]["baseurl"]
        self.subraph_id = configs["thegraph"]["subgraph_id"]
        self.api_key = os.environ["THEGRAPH_API_KEY"]
        self.url = f"{self.baseurl}/{self.api_key}/subgraphs/id/{self.subraph_id}"
        self.headers = {"Content-Type": "application/json"}

    def format_query(self, query: str, *args) -> str:
        query = query.replace("{", "{{").replace("}", "}}")

        for arg in args:
            query = query.replace(f"${arg}", f"{{{arg}}}")

        return query
