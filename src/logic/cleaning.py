# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, count, when, isnan, sum as spark_sum, lit
from functools import reduce
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number


class cleaning:
    #null_percentages = None
    def __init__(self, dataset_info):
        #self.dataset = dataset
        self.dataset_info = dataset_info

        #self.dataframe = self.dataset_info.get_dataframe()
        self.dataframe = self.dataset_info.get_dataframe_original()
        self.dataframe_info = self.dataset_info.get_general_info_original()
        
        self.delete_button = None
        
        self.first_option = []
        self.second_option = []
        
        self.columns_to_drop = []
        self.rows_to_drop = []
    
    def set_columns_to_drop(self, columns_to_drop):
        self.columns_to_drop = columns_to_drop
        
    def set_rows_to_drop(self, rows_to_drop):
        self.rows_to_drop = rows_to_drop
    
    def set_delete_button(self, delete_button):
        self.delete_button = delete_button
        
        
    def drop_data(self, dataframe=None):
        """
        Drop columns and rows specified in the columns_to_drop and rows_to_drop lists.
        
        Returns:
            DataFrame: The dataset after dropping specified columns and rows
        """
        if dataframe is None:
            result_df = self.dataframe
        else:
            result_df = dataframe
        
        # Drop columns
        if self.columns_to_drop:
            result_df = result_df.drop(*self.columns_to_drop)
        
        # Drop rows based on row indices
        if self.rows_to_drop:
            w = Window.partitionBy(lit(1)).orderBy(lit(1))
            result_df = result_df.withColumn("_row_idx", row_number().over(w))
            result_df = result_df.filter(~result_df._row_idx.isin(self.rows_to_drop)).drop("_row_idx")
        
        #self.dataframe = result_df
        return result_df

    

    