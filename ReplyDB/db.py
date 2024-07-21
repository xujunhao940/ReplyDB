import json
import numpy as np
import pandas as pd
import uuid
import os

from whoosh.fields import Schema, TEXT, KEYWORD, ID, STORED
from whoosh.analysis import StemmingAnalyzer
from whoosh import index
from whoosh.qparser import QueryParser


class ReplyDB:
    def __init__(self, path: str) -> None:
        """
        :param path: path to json file
        """
        self.data = pd.DataFrame([])
        self.path = path

        self.load(path)

    def __getitem__(self, key: str) -> pd.Series:
        """
        :param key: key of the column
        :return: pd.Series
        """
        return self.data[key]

    def load(self, path: str) -> None:
        """
        :param path: path to json file
        :return: None
        """
        if not os.path.exists(path):
            with open(path, 'w', encoding='utf-8') as f:
                f.write("[]")

        with open(path, 'r', encoding='utf-8') as f:
            content = f.read()
            if len(content) != 0:
                self.data = pd.DataFrame(json.loads(content))

    def save(self) -> None:
        json.dump(self.data.to_dict('records'), open(self.path, 'w', encoding='utf-8'))

    def insert(self, data: dict or list) -> None:
        """
        :param data: dict or list e.g. [{"name": "John", "age": 18} or {"name": "Jane", "age": 20}]
        :return: None
        """
        if isinstance(data, dict):
            data = [data]
        for i in data:
            i["_id"] = uuid.uuid4().int
        self.data = self.data._append(data, ignore_index=True)
        self.save()

    def find(self, expression: pd.Series) -> pd.DataFrame:
        """
        :param expression: pd.Series e.g. db["age"] >= 18
        :return: pd.DataFrame
        """
        return self.data[expression]

    def create_search_index(self, field: str) -> None:
        """
        :param field: str
        :return: None
        """
        schema = Schema(id=ID(stored=True),
                        body=TEXT(analyzer=StemmingAnalyzer()))

        if not os.path.exists("indexdir"):
            os.mkdir("indexdir")

        ix = index.create_in("indexdir", schema, indexname=field)
        writer = ix.writer()

        for i in self.data.to_dict("records"):
            writer.add_document(id=str(i["_id"]), body=i[field])

        writer.commit()

    @staticmethod
    def search(field: str, query: str) -> pd.Series:
        """
        :param field: str
        :param query: str
        :return: pd.Series: _id
        """
        ix = index.open_dir("indexdir", indexname=field)
        qp = QueryParser("body", schema=ix.schema)
        with ix.searcher() as searcher:
            results = searcher.search(qp.parse(query))
            return pd.Series([i["id"] for i in results])
