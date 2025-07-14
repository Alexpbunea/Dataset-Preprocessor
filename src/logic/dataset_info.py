import sys
import pandas as pd
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, count, when, isnan, sum as spark_sum, lit
from pyspark.sql.functions import col, when, isnan, sum as spark_sum, expr
from functools import reduce
from pyspark.sql import functions as F
import copy

"""
GENERAL INFO:
    num_rows: number of rows in the dataset
    num_columns: number of columns in the dataset
    column_names: names of the columns in the dataset
    data_types: data types of the columns in the dataset
    column_null_counts: number of nulls in each column
    row_null_counts: number of nulls in each row
    null_percentages: percentage of nulls in each column
"""


class DatasetInfo:
    def __init__(self, spark: SparkSession, dataframe: DataFrame):
        self.spark = spark
        
        self.dataframe = dataframe
        self.dataframe_original = None
        
        self.general_info = {}
        self.general_info_original = {}

        self.dataframe_sorted_columns = None
        self.dataframe_sorted_rows = None

        self.train_df = None
        self.val_df = None
        self.test_df = None
        
    
    def set_dataframe(self, dataframe):
        self.dataframe = dataframe

    def get_dataframe(self):
        return self.dataframe
    

    def set_dataframe_original(self, dataframe):
        self.dataframe_original = dataframe

    def get_dataframe_original(self):
        return self.dataframe_original

    
    
    
    def set_dataframe_sorted_columns(self, dataframe):
        self.dataframe_sorted_columns = dataframe
    
    def get_dataframe_sorted_columns(self):
        return self.dataframe_sorted_columns
    
    
    def set_dataframe_sorted_rows(self, dataframe):
        self.dataframe_sorted_rows = dataframe
    
    def get_dataframe_sorted_rows(self):
        return self.dataframe_sorted_rows
    

    def set_train_df(self, dataframe):
        self.train_df = dataframe
    
    def get_train_df(self):
        return self.train_df
    

    def set_val_df(self, dataframe):
        self.val_df = dataframe

    def get_val_df(self):
        return self.val_df
    
    
    def set_test_df(self, dataframe):
        self.test_df = dataframe
    
    def get_test_df(self):
        return self.test_df
        
        
    
    
    def set_general_info(self, info = None):
        if info is None:
            self.general_info["num_rows"] = self.dataframe.count()
            #self.total_rows = self.general_info["num_rows"]
            self.general_info["num_columns"] = len(self.dataframe.columns)
            self.general_info["column_names"] = self.dataframe.columns
            self.general_info["data_types"] = {col: self.dataframe.schema[col].dataType for col in self.dataframe.columns}

            self.calculate_nulls()
            self.set_null_percentages()
        else:
            self.general_info = info.copy()
    
    def set_general_info_original(self, info = None):
        if info is None:
            self.general_info_original = self.general_info.copy()
        else:
            self.general_info_original = info.copy()
    
    
    def get_general_info_original(self):
        return self.general_info_original

    
    def get_general_info(self):
        return self.general_info
    
    def get_general_info_original(self):
        return self.general_info_original
    
    
    def calculate_nulls(self):
        """
        Calculate the number of nulls for each column and row in the dataset.
        
        Returns:
            tuple: (column_null_counts, row_null_counts)
                - column_null_counts: DataFrame with column_name and null_count columns
                - row_null_counts: DataFrame with row indices and their null counts
        """
        column_names = self.general_info["column_names"]
        num_columns = self.general_info["num_columns"]

        # 1. Column null counts (más eficiente y directo)
        null_exprs = [spark_sum(when(col(c).isNull() | isnan(col(c)), 1).otherwise(0)).alias(c) for c in column_names]
        col_nulls_df = self.dataframe.select(null_exprs)

        # Stack para convertir columnas en filas (column_name, null_count)
        stack_expr = ", ".join([f"'{c}', `{c}`" for c in column_names])
        column_null_counts = col_nulls_df.select(expr(f"stack({num_columns}, {stack_expr})").alias("column_name", "null_count"))

        # 2. Row null counts (más vectorizado y escalable)
        #row_nulls_expr = sum(when(col(c).isNull() | isnan(col(c)), 1).otherwise(0) for c in column_names).alias("null_count")
        row_nulls_expr = reduce(
            lambda a, b: a + b,
            [when(col(c).isNull() | isnan(col(c)), 1).otherwise(0) for c in column_names]
        )#.alias("null_count")
        row_null_counts = self.dataframe.withColumn("null_count", row_nulls_expr).select("null_count")
        
        

        self.general_info["column_null_counts"] = column_null_counts
        self.general_info["row_null_counts"] = row_null_counts

        return column_null_counts, row_null_counts
    

    def set_null_percentages(self):
        """
        This is not needed, but I wanted to calculate them to show in the table of the cleaning phase
        """
        col_nulls = self.general_info["column_null_counts"].collect()
        num_rows = self.general_info["num_rows"]
        self.general_info["null_percentages"] = {
            row["column_name"]: (row["null_count"] / num_rows * 100) if num_rows else 0.0
            for row in col_nulls
        }
    
    def get_null_percentages(self):
        return self.general_info.get("null_percentages")

    
