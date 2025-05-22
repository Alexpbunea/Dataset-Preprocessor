# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, count, when, isnan, sum as spark_sum, lit
from functools import reduce
from pyspark.sql import functions as F

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
        # Create null condition columns for each column
        null_conditions = [when(col(c).isNull() | isnan(col(c)), 1).otherwise(0).alias(f"null_{c}") 
                         for c in self.dataset.columns]
        
        # First add these as columns
        df_with_null_flags = self.dataset.select("*", *null_conditions)
        
        # Then use a sum expression to add them together
        sum_expr = null_conditions[0].alias("null_count")
        for i in range(1, len(null_conditions)):
            sum_expr = (sum_expr + null_conditions[i]).alias("null_count")
        
        # If there's only one column, just use that directly
        if len(null_conditions) == 1:
            row_null_counts = df_with_null_flags.select(null_conditions[0].alias("null_count"))
        else:
            # Otherwise calculate the sum using the expression
            row_null_counts = df_with_null_flags.select(sum_expr)
        
        return column_null_counts, row_null_counts
    
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

    

    