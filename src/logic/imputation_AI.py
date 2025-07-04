import os
import pickle
from typing import List, Dict, Any
import numpy as np
import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, isnan, when, count
from pyspark.ml.feature import VectorAssembler, StringIndexer, OneHotEncoder
from pyspark.ml.regression import LinearRegression, RandomForestRegressor, DecisionTreeRegressor
from pyspark.ml.classification import RandomForestClassifier, DecisionTreeClassifier
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator


class ImputationAI:
    """
    This class handles the AI-based imputation phase of the dataset preprocessing.
    It provides methods to train ML models and predict missing values using Spark ML.
    """

    def __init__(self, dataset_info):
        self.dataset_info = dataset_info
        self.model = None
        self.pipeline = None
        self.feature_columns = None
        self.target_column = None
        self.model_type = None
        self.models_dir = "saved_models"

        # Create models directory if it doesn't exist
        os.makedirs(self.models_dir, exist_ok=True)

    def train_model(self, feature_columns: List[str], target_column: str, model_type: str) -> bool:
        """
        Train the selected model using feature and target columns.

        Args:
            feature_columns: List of column names to use as features
            target_column: Column name to use as target
            model_type: Type of model ('Linear Regression', 'Random Forest', 'Decision Tree')

        Returns:
            bool: True if training successful, False otherwise
        """
        try:
            df = self.dataset_info.get_dataframe()

            # Store configuration
            self.feature_columns = feature_columns
            self.target_column = target_column
            self.model_type = model_type

            # Filter out rows where target column is null for training
            training_df = df.filter(col(target_column).isNotNull())

            # Check if we have enough data for training
            if training_df.count() == 0:
                print(f"[ERROR] -> No complete rows found for training")
                return False

            # Prepare features
            assembler = VectorAssembler(
                inputCols=feature_columns,
                outputCol="features",
                handleInvalid="skip"  # Skip rows with invalid values
            )

            # Select appropriate model based on target column type
            target_dtype = dict(df.dtypes)[target_column]
            is_numeric = any(dtype in target_dtype.lower() for dtype in ['int', 'float', 'double'])

            if is_numeric:
                model = self._get_regression_model(model_type)
            else:
                model = self._get_classification_model(model_type)

            if model is None:
                print(f"[ERROR] -> Unsupported model type: {model_type}")
                return False

            # Create pipeline
            self.pipeline = Pipeline(stages=[assembler, model])

            # Train the model
            print(f"[INFO] -> Training {model_type} model...")
            self.model = self.pipeline.fit(training_df)

            # Save the model
            self._save_model()

            print(f"[SUCCESS] -> Model trained successfully")
            return True

        except Exception as e:
            print(f"[ERROR] -> Training failed: {e}")
            return False

    def _get_regression_model(self, model_type: str):
        """Get regression model based on type"""
        if model_type == "Linear Regression":
            return LinearRegression(featuresCol="features", labelCol=self.target_column)
        elif model_type == "Random Forest":
            return RandomForestRegressor(featuresCol="features", labelCol=self.target_column, numTrees=10)
        elif model_type == "Decision Tree":
            return DecisionTreeRegressor(featuresCol="features", labelCol=self.target_column)
        return None

    def _get_classification_model(self, model_type: str):
        """Get classification model based on type"""
        if model_type == "Random Forest":
            return RandomForestClassifier(featuresCol="features", labelCol=self.target_column, numTrees=10)
        elif model_type == "Decision Tree":
            return DecisionTreeClassifier(featuresCol="features", labelCol=self.target_column)
        # Linear Regression not applicable for classification
        return None

    def impute_missing_values(self) -> bool:
        """
        Use the trained model to predict and fill missing values in the target column.

        Returns:
            bool: True if imputation successful, False otherwise
        """
        try:
            if self.model is None:
                print(f"[ERROR] -> No trained model available")
                return False

            df = self.dataset_info.get_dataframe()

            # Get rows with missing target values
            missing_df = df.filter(col(self.target_column).isNull())
            complete_df = df.filter(col(self.target_column).isNotNull())

            if missing_df.count() == 0:
                print(f"[INFO] -> No missing values found in target column")
                return True

            print(f"[INFO] -> Imputing {missing_df.count()} missing values...")

            # Make predictions for missing values
            predictions = self.model.transform(missing_df)

            # Extract predictions and update the missing values
            prediction_col = "prediction"
            filled_df = predictions.withColumn(
                self.target_column,
                col(prediction_col)
            ).drop(prediction_col, "features")

            # Combine complete and filled data
            result_df = complete_df.union(filled_df)
            print(result_df.head(10))  # Show first 5 rows for debugging

            # Update the dataset
            self.dataset_info.set_dataframe(result_df)

            print(f"[SUCCESS] -> Missing values imputed successfully")
            return True

        except Exception as e:
            print(f"[ERROR] -> Imputation failed: {e}")
            return False

    def _save_model(self):
        """Save the trained model and metadata"""
        try:
            model_name = f"{self.model_type.lower().replace(' ', '_')}_{self.target_column}"
            model_path = os.path.join(self.models_dir, model_name)

            # Save Spark ML model
            self.model.write().overwrite().save(model_path)

            # Save metadata
            metadata = {
                'feature_columns': self.feature_columns,
                'target_column': self.target_column,
                'model_type': self.model_type,
                'model_path': model_path
            }

            metadata_path = os.path.join(self.models_dir, f"{model_name}_metadata.pkl")
            with open(metadata_path, 'wb') as f:
                pickle.dump(metadata, f)

            print(f"[INFO] -> Model saved to {model_path}")

        except Exception as e:
            print(f"[ERROR] -> Failed to save model: {e}")

    def load_model(self, model_name: str) -> bool:
        """
        Load a previously saved model.

        Args:
            model_name: Name of the model to load

        Returns:
            bool: True if loading successful, False otherwise
        """
        try:
            from pyspark.ml import PipelineModel

            model_path = os.path.join(self.models_dir, model_name)
            metadata_path = os.path.join(self.models_dir, f"{model_name}_metadata.pkl")

            # Load metadata
            with open(metadata_path, 'rb') as f:
                metadata = pickle.load(f)

            # Load model
            self.model = PipelineModel.load(model_path)
            self.feature_columns = metadata['feature_columns']
            self.target_column = metadata['target_column']
            self.model_type = metadata['model_type']

            print(f"[SUCCESS] -> Model loaded from {model_path}")
            return True

        except Exception as e:
            print(f"[ERROR] -> Failed to load model: {e}")
            return False

    def get_available_models(self) -> List[str]:
        """Get list of available saved models"""
        try:
            models = []
            for file in os.listdir(self.models_dir):
                if file.endswith('_metadata.pkl'):
                    model_name = file.replace('_metadata.pkl', '')
                    models.append(model_name)
            return models
        except Exception:
            return []