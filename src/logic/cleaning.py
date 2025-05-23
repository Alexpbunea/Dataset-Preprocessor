# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, count, when, isnan, sum as spark_sum, lit
from functools import reduce
from pyspark.sql import functions as F

class cleaning:
    #null_percentages = None
    def __init__(self, dataset: DataFrame):
        self.dataset = dataset
        self.columns_to_drop = []
        self.rows_to_drop = []
        self.total_rows = self.dataset.count()
        self.null_percentages = None
        

    def get_null_percentages(self):
        return self.null_percentages

    
    def calculate_nulls(self):
        """
        Calculate the number of nulls for each column and row in the dataset.
        
        Returns:
            tuple: (column_null_counts, row_null_counts)
                - column_null_counts: DataFrame with column_name and null_count columns
                - row_null_counts: DataFrame with row indices and their null counts
        """
        # Calculate nulls for each column (as before)
        column_null_counts_raw = self.dataset.select([
            spark_sum(when(col(c).isNull() | isnan(col(c)), 1).otherwise(0)).alias(c)
            for c in self.dataset.columns
        ])
        
        # Restructure column nulls into column_name and null_count format for easier sorting
        stack_expr = ", ".join([f"'{col}', `{col}`" for col in self.dataset.columns])
        column_null_counts = column_null_counts_raw.select(
            F.expr(f"stack({len(self.dataset.columns)}, {stack_expr}) as (column_name, null_count)")
        )
        
        # Row nulls calculation remains the same
        null_conditions = [when(col(c).isNull() | isnan(col(c)), 1).otherwise(0).alias(f"null_{c}") 
                        for c in self.dataset.columns]
        
        df_with_null_flags = self.dataset.select("*", *null_conditions)
        
        sum_expr = null_conditions[0].alias("null_count")
        for i in range(1, len(null_conditions)):
            sum_expr = (sum_expr + null_conditions[i]).alias("null_count")
        
        if len(null_conditions) == 1:
            row_null_counts = df_with_null_flags.select(null_conditions[0].alias("null_count"))
        else:
            row_null_counts = df_with_null_flags.select(sum_expr)
        
        """
        This is not needed, but I wanted to calculate them to show in the table of the cleaning phase
        """
        column_data = column_null_counts.collect()
        column_nulls_dict = {row["column_name"]: row["null_count"] for row in column_data}
        self.null_percentages = {
                        col: (count / self.total_rows * 100) if self.total_rows > 0 else 0.0
                        for col, count in column_nulls_dict.items()
        }
        
        
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

    

    