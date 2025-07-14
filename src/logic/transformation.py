# -*- coding: utf-8 -*-

from pyspark.sql.functions import col, countDistinct
from pyspark.sql import functions as F
from pyspark.sql.functions import when, col
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler, StandardScaler, MinMaxScaler, RobustScaler
from pyspark.ml.functions import vector_to_array
from pyspark.ml import Pipeline


class Transformation:
    """
    Handles data transformation operations including:
    - Binary column transformation (string to 0/1)
    - Categorical encoding (Label Encoding, One-Hot Encoding)
    - Numerical scaling (StandardScaler, MinMaxScaler, RobustScaler)
    """
    
    def __init__(self, dataset_info):
        """Initialize transformation with dataset information"""
        self.dataset_info = dataset_info
        self.dataframe = self.dataset_info.get_dataframe()
        self.dataframe_info = self.dataset_info.get_general_info()
        self.columns_to_transform = []

        # Column categorization
        self.binary_columns = []
        self.categorical_columns = []
        self.numerical_columns = []

    # === GETTER/SETTER METHODS ===
    
    def set_columns_to_transform(self, columns_to_transform):
        """Set columns that will be transformed"""
        self.columns_to_transform = columns_to_transform

    def get_binary_columns(self):
        """Get list of binary columns"""
        return self.binary_columns

    def get_categorical_columns(self):
        """Get list of categorical columns"""
        return self.categorical_columns

    def get_numerical_columns(self):
        """Get list of numerical columns"""
        return self.numerical_columns

    # === COLUMN ANALYSIS METHODS ===
    
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
        """
        Categorize columns into binary, categorical, and numerical based on:
        - Data type (string vs numeric)
        - Number of distinct values (2 = binary, 3-15 = categorical)
        """
        columns = self.dataframe_info['column_names']
        dtypes = dict(self.dataframe.dtypes)

        # Get text/string columns for further analysis
        text_cols = [
            col for col in columns
            if 'string' in dtypes.get(col, '').lower() or 'text' in dtypes.get(col, '').lower()
        ]

        # Calculate distinct counts for text columns
        distinct_counts = self._calculate_distinct_counts(text_cols)

        # Categorize each column
        for column in columns:
            dtype = dtypes.get(column, 'unknown')

            if any(t in dtype.lower() for t in ['int', 'float', 'double', 'decimal']):
                self.numerical_columns.append(column)
            elif 'string' in dtype.lower() or 'text' in dtype.lower():
                distinct_count = distinct_counts.get(column, 0)
                
                if distinct_count == 2:
                    self.binary_columns.append(column)
                elif 2 < distinct_count <= 15:
                    self.categorical_columns.append(column)
                # Skip columns with >15 distinct values

        print(f"[INFO] -> Binary columns: {self.binary_columns}")
        print(f"[INFO] -> Categorical columns: {self.categorical_columns}")
        print(f"[INFO] -> Numerical columns: {self.numerical_columns}")

    def _calculate_distinct_counts(self, text_cols):
        """Calculate distinct value counts for text columns"""
        distinct_counts = {}
        
        if not text_cols:
            return distinct_counts
            
        try:
            # Try batch calculation first
            count_exprs = [F.countDistinct(F.col(col)).alias(col) for col in text_cols]
            result = self.dataframe.agg(*count_exprs).collect()[0]
            distinct_counts = {col: result[col] for col in text_cols}
        except Exception as e:
            print(f"[WARNING] -> Batch distinct count failed: {e}")
            # Fallback: calculate individually
            for col in text_cols:
                try:
                    count = self.dataframe.select(col).distinct().count()
                    distinct_counts[col] = count
                except Exception as col_e:
                    print(f"[WARNING] -> Failed to count distinct values for {col}: {col_e}")
                    distinct_counts[col] = 0
        
        return distinct_counts

    # === TRANSFORMATION METHODS ===

    def apply_binary_transformation(self, columns_config):
        """
        Transform binary string columns to numerical (0/1) format
        
        Args:
            columns_config: Dict mapping column names to boolean (should transform)
            
        Returns:
            Updated dataframe with binary columns transformed
        """
        try:
            transformed_columns = []

            for col_name, should_convert in columns_config.items():
                if not should_convert:
                    continue

                try:
                    # Get unique values for validation
                    unique_values = self._get_unique_values(col_name)
                    
                    if len(unique_values) != 2:
                        print(f"[WARNING] -> Column {col_name} has {len(unique_values)} unique values, skipping")
                        continue

                    # Apply binary transformation
                    val1, val2 = sorted(unique_values)
                    self.dataframe = self.dataframe.withColumn(
                        col_name,
                        F.when(F.col(col_name).isNull(), None)
                        .when(F.col(col_name) == val1, 0)
                        .when(F.col(col_name) == val2, 1)
                        .otherwise(None)
                    )

                    transformed_columns.append(col_name)
                    print(f"[INFO] -> Converted {col_name}: '{val1}' -> 0, '{val2}' -> 1")

                except Exception as col_e:
                    print(f"[ERROR] -> Failed to transform column {col_name}: {col_e}")
                    continue

            self._log_transformation_result(transformed_columns, "binary transformation")
            return self.dataframe

        except Exception as e:
            print(f"[ERROR] -> Binary transformation failed: {e}")
            raise e

    def _get_unique_values(self, col_name):
        """Get unique non-null values for a column"""
        unique_df = (self.dataframe
                     .select(col_name)
                     .filter(F.col(col_name).isNotNull())
                     .distinct()
                     .limit(5))
        unique_rows = unique_df.collect()
        return [row[0] for row in unique_rows]

    def apply_categorical_transformation(self, encoding_config):
        """
        Apply categorical encoding transformations
        
        Args:
            encoding_config: Dict mapping column names to encoding methods
                           ("Label Encoding" or "One-Hot Encoding")
        
        Returns:
            Updated dataframe with categorical columns encoded
        """
        try:
            for col_name, encoding_method in encoding_config.items():
                if encoding_method == "Label Encoding":
                    self._apply_label_encoding(col_name)
                elif encoding_method == "One-Hot Encoding":
                    self._apply_onehot_encoding(col_name)

            print(f"[INFO] -> Successfully applied categorical encoding: {encoding_config}")
            return self.dataframe

        except Exception as e:
            print(f"[ERROR] -> Categorical transformation failed: {e}")
            raise e

    def _apply_label_encoding(self, col_name):
        """Apply Label Encoding to a categorical column"""
        indexer = StringIndexer(inputCol=col_name, outputCol=f"{col_name}_indexed", handleInvalid="keep")
        indexer_model = indexer.fit(self.dataframe)
        self.dataframe = indexer_model.transform(self.dataframe)

        # Replace original column with indexed version, handling nulls and NaNs
        self.dataframe = self.dataframe.withColumn(
            col_name,
            F.when(F.col(f"{col_name}_indexed").isNull(), None)
            .when(F.isnan(F.col(f"{col_name}_indexed")), None)
            .otherwise(F.col(f"{col_name}_indexed"))
        ).drop(f"{col_name}_indexed")

        print(f"[INFO] -> Applied Label Encoding to column: {col_name}")

    def _apply_onehot_encoding(self, col_name):
        """Apply One-Hot Encoding to a categorical column"""
        # Create pipeline: String Indexer -> One-Hot Encoder
        indexer = StringIndexer(inputCol=col_name, outputCol=f"{col_name}_indexed", handleInvalid="keep")
        encoder = OneHotEncoder(inputCol=f"{col_name}_indexed", outputCol=f"{col_name}_encoded", dropLast=False)
        
        pipeline = Pipeline(stages=[indexer, encoder])
        model = pipeline.fit(self.dataframe)
        self.dataframe = model.transform(self.dataframe)

        # Convert vector to individual binary columns
        sample_row = self.dataframe.select(f"{col_name}_encoded").first()
        if sample_row and sample_row[0] is not None:
            vector_size = sample_row[0].size
            self._convert_vector_to_columns(col_name, vector_size)
            print(f"[INFO] -> Applied One-Hot Encoding to column: {col_name} (created {vector_size} binary columns)")
        else:
            print(f"[WARNING] -> Failed to process One-Hot Encoding for column: {col_name}")

    def _convert_vector_to_columns(self, col_name, vector_size):
        """Convert one-hot encoded vector to individual binary columns"""
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

        # Clean up temporary columns
        self.dataframe = self.dataframe.drop(
            col_name,
            f"{col_name}_indexed",
            f"{col_name}_encoded",
            f"{col_name}_array"
        )

    def apply_numerical_transformation(self, scaling_config):
        """
        Apply feature scaling to numerical columns
        
        Args:
            scaling_config: Dict mapping column names to scaling methods
                          ("StandardScaler", "MinMaxScaler", "RobustScaler", "None")
        
        Returns:
            Updated dataframe with numerical columns scaled
        """
        try:
            transformed_columns = []

            for col_name, scaling_method in scaling_config.items():
                if scaling_method == "None":
                    continue

                try:
                    # Process single column
                    scaled_df = self._scale_single_column(col_name, scaling_method)
                    if scaled_df is not None:
                        self.dataframe = scaled_df
                        transformed_columns.append(col_name)
                        print(f"[INFO] -> Applied {scaling_method} to column: {col_name}")

                except Exception as col_e:
                    print(f"[ERROR] -> Failed to scale column {col_name}: {col_e}")
                    continue

            self._log_transformation_result(transformed_columns, "numerical scaling")
            return self.dataframe

        except Exception as e:
            print(f"[ERROR] -> Numerical transformation failed: {e}")
            raise e

    def _scale_single_column(self, col_name, scaling_method):
        """Scale a single numerical column"""
        # Preserve original null information
        null_condition = F.col(col_name).isNull()
        
        # Ensure the column is numeric
        df = self.dataframe.withColumn(col_name, F.col(col_name).cast("double"))
        
        # Create vector for scaling
        vector_df = self._create_vector_column(df, col_name)
        
        # Apply scaling
        scaler = self._get_scaler(scaling_method, col_name)
        if scaler is None:
            return None
            
        scaled_df = self._apply_scaler(vector_df, scaler, col_name)
        
        # Extract scaled values and handle nulls
        final_df = self._extract_scaled_values(scaled_df, col_name, null_condition)
        
        return final_df

    def _create_vector_column(self, df, col_name):
        """Create vector column for scaling"""
        assembler = VectorAssembler(
            inputCols=[col_name],
            outputCol=f"{col_name}_vector",
            handleInvalid="keep"
        )
        return assembler.transform(df)

    def _get_scaler(self, scaling_method, col_name):
        """Get the appropriate scaler based on method"""
        if scaling_method == "StandardScaler":
            return StandardScaler(
                inputCol=f"{col_name}_vector",
                outputCol=f"{col_name}_scaled",
                withMean=False,  # Avoid NaN issues
                withStd=True
            )
        elif scaling_method == "MinMaxScaler":
            return MinMaxScaler(
                inputCol=f"{col_name}_vector",
                outputCol=f"{col_name}_scaled"
            )
        elif scaling_method == "RobustScaler":
            return RobustScaler(
                inputCol=f"{col_name}_vector",
                outputCol=f"{col_name}_scaled"
            )
        else:
            print(f"[WARNING] -> Unknown scaling method: {scaling_method}")
            return None

    def _apply_scaler(self, vector_df, scaler, col_name):
        """Apply scaler to vector column"""
        # Fit scaler only on non-null data to avoid NaN issues
        fit_df = vector_df.filter(F.col(f"{col_name}_vector").isNotNull())
        scaler_model = scaler.fit(fit_df)
        return scaler_model.transform(vector_df)

    def _extract_scaled_values(self, scaled_df, col_name, null_condition):
        """Extract scaled values from vector and handle nulls"""
        # Convert vector to array
        scaled_df = scaled_df.withColumn(
            f"{col_name}_array",
            vector_to_array(F.col(f"{col_name}_scaled"))
        )

        # Extract first element of array
        scaled_df = scaled_df.withColumn(
            f"{col_name}_scaled_value",
            F.col(f"{col_name}_array").getItem(0)
        )

        # Handle nulls and NaNs
        scaled_df = scaled_df.withColumn(
            col_name,
            F.when(null_condition, None)
            .when(F.isnan(F.col(f"{col_name}_scaled_value")), None)
            .otherwise(F.col(f"{col_name}_scaled_value"))
        )

        # Clean up temporary columns
        return scaled_df.drop(
            f"{col_name}_vector",
            f"{col_name}_scaled",
            f"{col_name}_array",
            f"{col_name}_scaled_value"
        )

    # === UTILITY METHODS ===
    
    def _log_transformation_result(self, transformed_columns, operation):
        """Log transformation results"""
        if transformed_columns:
            print(f"[INFO] -> Successfully applied {operation} to {len(transformed_columns)} columns: {transformed_columns}")
        else:
            print(f"[WARNING] -> No columns were processed for {operation}")