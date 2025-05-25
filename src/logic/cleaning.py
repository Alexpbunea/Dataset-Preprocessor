# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, count, when, isnan, sum as spark_sum, lit
from functools import reduce
from pyspark.sql import functions as F


class cleaning:
    #null_percentages = None
    def __init__(self, dataset_info):
        #self.dataset = dataset
        self.dataset_info = dataset_info

        self.dataset = self.dataset_info.get_dataframe()
        self.columns_to_drop = []
        self.rows_to_drop = []
        self.total_rows = self.dataset.count()
        self.null_percentages = None
        
    
    def drop_data(self):
        """
        Drop columns and rows specified in the columns_to_drop and rows_to_drop lists.
        
        Returns:
            DataFrame: The dataset after dropping specified columns and rows
        """
        result_df = self.dataset
        
        # Drop columns
        if self.columns_to_drop:
            result_df = result_df.drop(*self.columns_to_drop)
        
        # Drop rows based on row indices
        if self.rows_to_drop:
            # Create a row number column to identify rows
            from pyspark.sql.window import Window
            from pyspark.sql.functions import row_number
            
            window = Window.orderBy("dummy")
            result_df = result_df.withColumn("dummy", lit(1))
            result_df = result_df.withColumn("row_id", row_number().over(window))
            
            # Filter out the rows to drop
            result_df = result_df.filter(~col("row_id").isin(self.rows_to_drop))
            
            # Drop the temporary columns
            result_df = result_df.drop("dummy", "row_id")
        
        self.dataset = result_df
        return result_df

    

    