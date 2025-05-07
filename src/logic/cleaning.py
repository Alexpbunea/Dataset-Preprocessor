# -*- coding: utf-8 -*-

from pyspark.sql import DataFrame

class cleaning:
    def __init__(self, dataset: DataFrame):
        self.dataset = dataset
        self.columns_to_drop = []
        self.rows_to_drop = []

    