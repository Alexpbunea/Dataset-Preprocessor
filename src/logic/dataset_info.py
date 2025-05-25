import sys
import pandas as pd
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, count, when, isnan, sum as spark_sum, lit
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
        # Calculate nulls for each column (as before)
        column_null_counts_raw = self.dataframe.select([
            spark_sum(when(col(c).isNull() | isnan(col(c)), 1).otherwise(0)).alias(c)
            for c in self.general_info["column_names"]
        ])
        
        # Restructure column nulls into column_name and null_count format for easier sorting
        stack_expr = ", ".join([f"'{col}', `{col}`" for col in self.general_info["column_names"]])
        column_null_counts = column_null_counts_raw.select(
            F.expr(f"stack({self.general_info["num_columns"]}, {stack_expr}) as (column_name, null_count)")
        )
        
        # Row nulls calculation remains the same
        null_conditions = [when(col(c).isNull() | isnan(col(c)), 1).otherwise(0).alias(f"null_{c}") 
                        for c in self.general_info["column_names"]]
        
        df_with_null_flags = self.dataframe.select("*", *null_conditions)
        
        sum_expr = null_conditions[0].alias("null_count")
        for i in range(1, len(null_conditions)):
            sum_expr = (sum_expr + null_conditions[i]).alias("null_count")
        
        if len(null_conditions) == 1:
            row_null_counts = df_with_null_flags.select(null_conditions[0].alias("null_count"))
        else:
            row_null_counts = df_with_null_flags.select(sum_expr)
        
        self.general_info["column_null_counts"] = column_null_counts
        self.general_info["row_null_counts"] = row_null_counts

        return column_null_counts, row_null_counts
    

    def set_null_percentages(self):
        """
        This is not needed, but I wanted to calculate them to show in the table of the cleaning phase
        """
        column_null_counts, _ = self.general_info["column_null_counts"], self.general_info["row_null_counts"]
        column_data = column_null_counts.collect()
        column_nulls_dict = {row["column_name"]: row["null_count"] for row in column_data}
        self.general_info["null_percentages"] = {
                        col: (count /  self.general_info["num_rows"] * 100) if  self.general_info["num_rows"] > 0 else 0.0
                        for col, count in column_nulls_dict.items()
        }
    
    def get_null_percentages(self):
        return self.general_info.get("null_percentages")

    
