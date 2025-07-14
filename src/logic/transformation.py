# -*- coding: utf-8 -*-

from pyspark.sql.functions import col, countDistinct
from pyspark.sql import functions as F
from pyspark.sql.functions import when, col
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler, StandardScaler, MinMaxScaler, RobustScaler
from pyspark.ml import Pipeline

class Transformation:
    def __init__(self, dataset_info):
        self.dataset_info = dataset_info
        self.dataframe = self.dataset_info.get_dataframe()
        self.dataframe_info = self.dataset_info.get_general_info()
        self.columns_to_transform = []

        self.binary_columns = []
        self.categorical_columns = []
        self.numerical_columns = []

    def set_columns_to_transform(self, columns_to_transform):
        self.columns_to_transform = columns_to_transform

    def get_binary_columns(self):
        return self.binary_columns

    def get_categorical_columns(self):
        return self.categorical_columns

    def get_numerical_columns(self):
        return self.numerical_columns

    def get_column_categories(self, column):
        """Get unique categories for a specific column"""
        try:
            distinct_df = self.dataframe.select(column).distinct().limit(16)
            unique_values = [row[0] for row in distinct_df.collect() if row[0] is not None]
            return unique_values
        except Exception as e:
            print(f"[WARNING] -> Failed to get categories for column {column}: {e}")
            return []

    def categorize_columns(self):
        """Categorize columns into binary, categorical, and numerical"""
        columns = self.dataframe_info['column_names']
        dtypes = dict(self.dataframe.dtypes)

        # Get text/string columns for further analysis
        text_cols = [
            col for col in columns
            if 'string' in dtypes.get(col, '').lower() or 'text' in dtypes.get(col, '').lower()
        ]

        # Calculate distinct counts for text columns
        distinct_counts = {}
        if text_cols:
            try:
                count_exprs = [F.countDistinct(F.col(col)).alias(col) for col in text_cols]
                result = self.dataframe.agg(*count_exprs).collect()[0]
                # Convert result to dictionary with proper column mapping
                distinct_counts = {col: result[col] for col in text_cols}
            except Exception as e:
                print(f"[WARNING] -> Failed to calculate distinct counts: {e}")
                # Fallback: calculate individually
                for col in text_cols:
                    try:
                        count = self.dataframe.select(col).distinct().count()
                        distinct_counts[col] = count
                    except Exception as col_e:
                        print(f"[WARNING] -> Failed to count distinct values for {col}: {col_e}")
                        distinct_counts[col] = 0

        for column in columns:
            dtype = dtypes.get(column, 'unknown')

            if any(t in dtype.lower() for t in ['int', 'float', 'double', 'decimal']):
                # Numerical columns
                self.numerical_columns.append(column)
            elif 'string' in dtype.lower() or 'text' in dtype.lower():
                distinct_count = distinct_counts.get(column, 0)

                if distinct_count == 2:
                    # Binary columns (only 2 distinct values)
                    self.binary_columns.append(column)
                elif 2 < distinct_count <= 15:
                    # Categorical columns (3-15 distinct values)
                    self.categorical_columns.append(column)
                # Columns with >15 distinct values are excluded

        print(f"[INFO] -> Binary columns: {self.binary_columns}")
        print(f"[INFO] -> Categorical columns: {self.categorical_columns}")
        print(f"[INFO] -> Numerical columns: {self.numerical_columns}")

    def apply_binary_transformation(self, columns_config):
        """Apply binary string to numerical conversion using DataFrame operations"""
        try:
            transformed_columns = []

            for col_name, should_convert in columns_config.items():
                if not should_convert:
                    continue

                try:
                    # Get unique non-null values using DataFrame operations instead of RDD
                    unique_df = (self.dataframe
                                 .select(col_name)
                                 .filter(F.col(col_name).isNotNull())
                                 .distinct()
                                 .limit(5))  # Limit to avoid memory issues

                    # Collect unique values safely
                    unique_rows = unique_df.collect()
                    unique_values = [row[0] for row in unique_rows]

                    # Validate it's truly binary
                    if len(unique_values) != 2:
                        print(f"[WARNING] -> Column {col_name} has {len(unique_values)} unique values, skipping")
                        continue

                    # Sort values for consistent mapping
                    val1, val2 = sorted(unique_values)

                    # Apply transformation preserving nulls
                    self.dataframe = self.dataframe.withColumn(
                        col_name,
                        F.when(F.col(col_name).isNull(), None)
                        .when(F.col(col_name) == val1, 0)
                        .when(F.col(col_name) == val2, 1)
                        .otherwise(None)  # Safety fallback
                    )

                    transformed_columns.append(col_name)
                    print(f"[INFO] -> Converted {col_name}: '{val1}' -> 0, '{val2}' -> 1")

                except Exception as col_e:
                    print(f"[ERROR] -> Failed to transform column {col_name}: {col_e}")
                    continue

            if transformed_columns:
                print(f"[INFO] -> Successfully transformed {len(transformed_columns)} columns: {transformed_columns}")
            else:
                print("[WARNING] -> No columns were transformed")

            return self.dataframe

        except Exception as e:
            print(f"[ERROR] -> Binary transformation failed: {e}")
            raise e

    def apply_categorical_transformation(self, encoding_config):
        """Apply categorical encoding (One-Hot or Label Encoding) without UDFs"""
        try:
            from pyspark.sql.functions import col, size, expr

            for col_name, encoding_method in encoding_config.items():
                if encoding_method == "Label Encoding":
                    # String Indexer for Label Encoding
                    indexer = StringIndexer(inputCol=col_name, outputCol=f"{col_name}_indexed", handleInvalid="keep")
                    indexer_model = indexer.fit(self.dataframe)
                    self.dataframe = indexer_model.transform(self.dataframe)

                    # Replace original column with indexed version
                    self.dataframe = self.dataframe.withColumn(
                        col_name,
                        F.when(F.col(f"{col_name}_indexed").isNull(), None)
                        .when(F.isnan(F.col(f"{col_name}_indexed")), None)  # Convert NaN to null
                        .otherwise(F.col(f"{col_name}_indexed"))
                    ).drop(f"{col_name}_indexed")

                    print(f"[INFO] -> Applied Label Encoding to column: {col_name}")

                elif encoding_method == "One-Hot Encoding":
                    # String Indexer + One-Hot Encoder
                    indexer = StringIndexer(inputCol=col_name, outputCol=f"{col_name}_indexed", handleInvalid="keep")
                    encoder = OneHotEncoder(inputCol=f"{col_name}_indexed", outputCol=f"{col_name}_encoded",
                                            dropLast=False)

                    # Create and fit pipeline
                    pipeline = Pipeline(stages=[indexer, encoder])
                    model = pipeline.fit(self.dataframe)
                    self.dataframe = model.transform(self.dataframe)

                    # Get the vector size to create individual columns
                    sample_row = self.dataframe.select(f"{col_name}_encoded").first()
                    if sample_row and sample_row[0] is not None:
                        vector_size = sample_row[0].size

                        # Use built-in PySpark vector functions to extract elements
                        from pyspark.ml.functions import vector_to_array

                        # Convert vector to array
                        self.dataframe = self.dataframe.withColumn(
                            f"{col_name}_array",
                            vector_to_array(col(f"{col_name}_encoded"))
                        )

                        # Create individual binary columns from array elements
                        for i in range(vector_size):
                            self.dataframe = self.dataframe.withColumn(
                                f"{col_name}_{i}",
                                F.when(F.col(f"{col_name}_array").isNull(), None)
                                .otherwise(col(f"{col_name}_array").getItem(i).cast("int"))
                            )

                        # Drop temporary columns
                        self.dataframe = self.dataframe.drop(
                            col_name,
                            f"{col_name}_indexed",
                            f"{col_name}_encoded",
                            f"{col_name}_array"
                        )

                        print(
                            f"[INFO] -> Applied One-Hot Encoding to column: {col_name} (created {vector_size} binary columns)")
                    else:
                        print(f"[WARNING] -> Failed to process One-Hot Encoding for column: {col_name}")

            print(f"[INFO] -> Successfully applied categorical encoding: {encoding_config}")
            return self.dataframe

        except Exception as e:
            print(f"[ERROR] -> Categorical transformation failed: {e}")
            raise e

    def apply_numerical_transformation(self, scaling_config):
        """Apply feature scaling to numerical columns with proper null/NaN handling"""
        try:
            transformed_columns = []

            for col_name, scaling_method in scaling_config.items():
                if scaling_method == "None":
                    continue

                try:
                    # Preserve original null information
                    null_condition = F.col(col_name).isNull()
                    
                    #self.dataframe = self.dataframe.filter(col(col_name).isNotNull())  # Ensure column exists
                    # Ensure the column is numeric
                    self.dataframe = self.dataframe.withColumn(col_name, F.col(col_name).cast("double"))
                    print(f"[DEBUG] -> Original column values for {col_name}:")
                    self.dataframe.select(col_name).show(10)

                    # Create vector assembler for single column
                    assembler = VectorAssembler(
                        inputCols=[col_name],
                        outputCol=f"{col_name}_vector",
                        handleInvalid="keep"  # Keep nulls as nulls in the vector
                    )
                    vector_df = assembler.transform(self.dataframe)
                    print(f"[DEBUG] -> Vector column:")
                    vector_df.select(f"{col_name}_vector").show(10)
                    vector_df.filter(F.col(f"{col_name}_vector").isNull()).select(f"{col_name}_vector").show(10)

                    # Apply scaling based on method
                    if scaling_method == "StandardScaler":
                        scaler = StandardScaler(
                            inputCol=f"{col_name}_vector",
                            outputCol=f"{col_name}_scaled",
                            withMean=False,
                            withStd=True
                        )
                    elif scaling_method == "MinMaxScaler":
                        scaler = MinMaxScaler(
                            inputCol=f"{col_name}_vector",
                            outputCol=f"{col_name}_scaled"
                        )
                    elif scaling_method == "RobustScaler":
                        scaler = RobustScaler(
                            inputCol=f"{col_name}_vector",
                            outputCol=f"{col_name}_scaled"
                        )
                    else:
                        print(f"[WARNING] -> Unknown scaling method: {scaling_method}")
                        continue

                    # Fit and transform
                    fit_df = vector_df.filter(F.col(f"{col_name}_vector").isNotNull())
                    scaler_model = scaler.fit(fit_df)
                    scaled_df = scaler_model.transform(vector_df)

                    print(f"[DEBUG] -> Scaled vector column:")
                    scaled_df.select(f"{col_name}_scaled").show(10)

                    # Extract scaled values
                    from pyspark.ml.functions import vector_to_array

                    # Step 1: Convert vector to array
                    scaled_df = scaled_df.withColumn(
                        f"{col_name}_array",
                        vector_to_array(F.col(f"{col_name}_scaled"))
                    )
                    print(f"[DEBUG] -> Array column:")
                    scaled_df.select(f"{col_name}_array").show(10)

                    # Step 2: Extract the first element of the array
                    scaled_df = scaled_df.withColumn(
                        f"{col_name}_scaled_value",
                        F.col(f"{col_name}_array").getItem(0)
                    )

                    # Step 3: Handle both original nulls and generated NaNs with debugging
                    print(f"[DEBUG] -> Checking scaled values for column: {col_name}")
                    scaled_df.select(f"{col_name}_scaled_value").show(30)  # Show first 5 values
                    scaled_df = scaled_df.withColumn(
                        col_name,
                        F.when(null_condition, None)  # Preserve original nulls
                        .when(F.isnan(F.col(f"{col_name}_scaled_value")), None)  # Convert NaNs to null
                        .otherwise(F.col(f"{col_name}_scaled_value"))
                    )

                    # Step 4: Cleanup
                    self.dataframe = scaled_df.drop(
                        f"{col_name}_vector",
                        f"{col_name}_scaled",
                        f"{col_name}_array",
                        f"{col_name}_scaled_value"
                    )

                    transformed_columns.append(col_name)
                    print(f"[INFO] -> Applied {scaling_method} to column: {col_name}")

                except Exception as col_e:
                    print(f"[ERROR] -> Failed to scale column {col_name}: {col_e}")
                    continue

            if transformed_columns:
                print(f"[INFO] -> Successfully scaled {len(transformed_columns)} columns: {transformed_columns}")
            else:
                print("[WARNING] -> No columns were scaled")

            return self.dataframe

        except Exception as e:
            print(f"[ERROR] -> Numerical transformation failed: {e}")
            raise e