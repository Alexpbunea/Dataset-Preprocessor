# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, count, when, isnan, sum as spark_sum, lit

class cleaning:
    def __init__(self, dataset: DataFrame):
        self.dataset = dataset
        self.columns_to_drop = []
        self.rows_to_drop = []
    
    def calculate_nulls(self):
        """
        Calculate the number of nulls for each column and row in the dataset.
        
        Returns:
            tuple: (column_null_counts, row_null_counts)
                - column_null_counts: DataFrame with columns and their null counts
                - row_null_counts: DataFrame with row indices and their null counts
        """
        # Calculate nulls for each column
        column_null_counts = self.dataset.select([
            spark_sum(when(col(c).isNull() | isnan(col(c)), 1).otherwise(0)).alias(c)
            for c in self.dataset.columns
        ])
        
        # Calculate nulls for each row
        null_conditions = [when(col(c).isNull() | isnan(col(c)), 1).otherwise(0) for c in self.dataset.columns]
        row_null_counts = self.dataset.withColumn(
            "null_count", spark_sum(*null_conditions)
        ).select("null_count")
        
        return column_null_counts, row_null_counts
    
    def drop_data(self, rows_to_drop, columns_to_drop):
        """
        Drop columns and rows specified in the columns_to_drop and rows_to_drop lists.
        
        Returns:
            DataFrame: The dataset after dropping specified columns and rows
        """
        result_df = self.dataset
        
        # Drop columns
        if columns_to_drop:
            result_df = result_df.drop(*columns_to_drop)
        
        # Drop rows based on row indices
        if rows_to_drop:
            # Create a row number column to identify rows
            from pyspark.sql.window import Window
            from pyspark.sql.functions import row_number
            
            window = Window.orderBy("dummy")
            result_df = result_df.withColumn("dummy", lit(1))
            result_df = result_df.withColumn("row_id", row_number().over(window))
            
            # Filter out the rows to drop
            result_df = result_df.filter(~col("row_id").isin(rows_to_drop))
            
            # Drop the temporary columns
            result_df = result_df.drop("dummy", "row_id")
        
        self.dataset = result_df
        return result_df

    

    