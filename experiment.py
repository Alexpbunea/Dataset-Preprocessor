import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor
from sklearn.linear_model import LogisticRegression, LinearRegression
from sklearn.svm import SVC, SVR
from sklearn.metrics import (
    accuracy_score, precision_score, recall_score, f1_score, roc_auc_score,
    mean_squared_error, mean_absolute_error, r2_score
)
from sklearn.preprocessing import StandardScaler, LabelEncoder, MinMaxScaler
import time
import matplotlib.pyplot as plt
import seaborn as sns
import warnings
warnings.filterwarnings('ignore')

class DatasetExperiment:
    def __init__(self, original_dataset_path, preprocessed_train_path, 
                 preprocessed_val_path, preprocessed_test_path, target_column):
        """
        Initialize the experiment with dataset paths and target column.
        
        Args:
            original_dataset_path: Path to the original, non-preprocessed dataset
            preprocessed_train_path: Path to preprocessed training set
            preprocessed_val_path: Path to preprocessed validation set  
            preprocessed_test_path: Path to preprocessed test set
            target_column: Name of the target column for prediction
        """
        self.original_dataset_path = original_dataset_path
        self.preprocessed_train_path = preprocessed_train_path
        self.preprocessed_val_path = preprocessed_val_path
        self.preprocessed_test_path = preprocessed_test_path
        self.target_column = target_column
        
        self.original_data = None
        self.preprocessed_train = None
        self.preprocessed_val = None
        self.preprocessed_test = None
        
        self.results = {
            'original': {},
            'preprocessed': {}
        }
        
    def load_datasets(self):
        """Load all datasets for comparison."""
        print("Loading datasets...")
        
        # Load original dataset
        self.original_data = pd.read_csv(self.original_dataset_path)
        print(f"Original dataset shape: {self.original_data.shape}")
        
        # Load preprocessed datasets
        self.preprocessed_train = pd.read_csv(self.preprocessed_train_path)
        self.preprocessed_val = pd.read_csv(self.preprocessed_val_path)
        self.preprocessed_test = pd.read_csv(self.preprocessed_test_path)
        
        print(f"Preprocessed train shape: {self.preprocessed_train.shape}")
        print(f"Preprocessed val shape: {self.preprocessed_val.shape}")
        print(f"Preprocessed test shape: {self.preprocessed_test.shape}")
        
    def prepare_original_data(self):
        """Prepare original data with minimal preprocessing - only numerical columns."""
        print("Preparing original dataset...")
        
        df = self.original_data.copy()
        
        # First, handle missing values in the target column
        if df[self.target_column].isnull().any():
            print(f"Found {df[self.target_column].isnull().sum()} missing values in target column '{self.target_column}'")
            # Drop rows with missing target values
            df = df.dropna(subset=[self.target_column])
            print(f"Dataset shape after removing missing targets: {df.shape}")
        
        # Keep only numerical columns (including target)
        numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
        print(f"Numerical columns found: {numeric_cols}")
        
        # Ensure target column is in the list
        if self.target_column not in numeric_cols:
            print(f"Warning: Target column '{self.target_column}' is not numerical!")
            return None, None, None, None, None, None
        
        # Keep only numerical columns
        df_numeric = df[numeric_cols].copy()
        print(f"Shape after keeping only numerical columns: {df_numeric.shape}")
        
        # Fill missing values with median for all numerical columns except target
        for col in numeric_cols:
            if col != self.target_column:
                median_val = df_numeric[col].median()
                if pd.isna(median_val):  # If median is NaN (all values are NaN), fill with 0
                    median_val = 0
                df_numeric[col].fillna(median_val, inplace=True)
        
        # Separate features and target
        X = df_numeric.drop(columns=[self.target_column])
        y = df_numeric[self.target_column]
        
        # **APPLY SAME SCALING AS YOUR APP**
        # MinMax scale the target to match preprocessed data
        target_scaler = MinMaxScaler()
        y_scaled = target_scaler.fit_transform(y.values.reshape(-1, 1)).flatten()
        
        # Store scaler for potential inverse transform
        self.target_scaler = target_scaler
        
        # Additional check for any remaining NaN values in features
        if X.isnull().any().any():
            print("Warning: Features still contain NaN values after preprocessing")
            # Fill any remaining NaNs with 0
            X = X.fillna(0)
            print("Filled remaining NaN values with 0")
        
        # Additional check for any remaining NaN values in target
        if pd.isna(y_scaled).any():
            print("Warning: Target still contains NaN values after preprocessing")
            mask = ~pd.isna(y_scaled)
            X = X[mask]
            y_scaled = y_scaled[mask]
        
        print(f"Final feature matrix shape: {X.shape}")
        print(f"Final target vector shape: {y_scaled.shape}")
        print(f"Target value range after MinMax scaling: {y_scaled.min():.4f} to {y_scaled.max():.4f}")
        
        # Split the data with same proportions as your app (70/15/15)
        X_train, X_temp, y_train, y_temp = train_test_split(
            X, y_scaled, test_size=0.3, random_state=42
        )
        X_val, X_test, y_val, y_test = train_test_split(
            X_temp, y_temp, test_size=0.5, random_state=42  # 0.5 of 30% = 15%
        )
        
        return X_train, X_val, X_test, y_train, y_val, y_test
    
    def analyze_preprocessing_impact(self):
        """Analyze the impact of your preprocessing pipeline."""
        print("\n" + "=" * 60)
        print("PREPROCESSING ANALYSIS")
        print("=" * 60)
        
        # Original dataset info
        orig_shape = self.original_data.shape
        orig_nulls = self.original_data.isnull().sum().sum()
        orig_null_cols = (self.original_data.isnull().sum() / len(self.original_data) > 0.5).sum()
        
        # Preprocessed dataset info
        prep_shape = (len(self.preprocessed_train) + len(self.preprocessed_val) + len(self.preprocessed_test), 
                      self.preprocessed_train.shape[1])
        prep_nulls = (self.preprocessed_train.isnull().sum().sum() + 
                      self.preprocessed_val.isnull().sum().sum() + 
                      self.preprocessed_test.isnull().sum().sum())
        
        print(f"Dataset Shape:")
        print(f"  Original: {orig_shape}")
        print(f"  Preprocessed: {prep_shape}")
        print(f"  Columns removed: {orig_shape[1] - prep_shape[1]}")
        
        print(f"\nNull Values:")
        print(f"  Original total nulls: {orig_nulls:,}")
        print(f"  Preprocessed total nulls: {prep_nulls:,}")
        print(f"  Null reduction: {((orig_nulls - prep_nulls) / orig_nulls * 100):.1f}%")
        
        print(f"\nColumns with >50% nulls removed: {orig_null_cols}")
        
        print(f"\nData Value (Target) Analysis:")
        orig_target_nulls = self.original_data[self.target_column].isnull().sum()
        print(f"  Original nulls in target: {orig_target_nulls:,}")
        print(f"  Target nulls handled by: AI Random Forest imputation")
        
        print(f"\nTransformations Applied:")
        print(f"  • Deleted 3 columns with >50% null values")
        print(f"  • Unit column: String → Binary encoding")
        print(f"  • 3 columns: Label encoding applied")
        print(f"  • Data_value: MinMax scaling applied")
        print(f"  • Null values: AI Random Forest imputation")
        print(f"  • Data split: 70% train, 15% val, 15% test")
    
    def prepare_preprocessed_data(self):
        """Prepare preprocessed data from your app's output - only numerical columns."""
        print("Preparing preprocessed datasets...")
        
        # Get numerical columns from preprocessed data (should already be mostly numerical)
        numeric_cols = self.preprocessed_train.select_dtypes(include=[np.number]).columns.tolist()
        print(f"Numerical columns in preprocessed data: {numeric_cols}")
        
        # Ensure target column is numerical
        if self.target_column not in numeric_cols:
            print(f"Warning: Target column '{self.target_column}' is not numerical in preprocessed data!")
            return None, None, None, None, None, None
        
        # Keep only numerical columns for all datasets
        X_train = self.preprocessed_train[numeric_cols].drop(columns=[self.target_column])
        y_train = self.preprocessed_train[self.target_column]
        
        X_val = self.preprocessed_val[numeric_cols].drop(columns=[self.target_column])
        y_val = self.preprocessed_val[self.target_column]
        
        X_test = self.preprocessed_test[numeric_cols].drop(columns=[self.target_column])
        y_test = self.preprocessed_test[self.target_column]
        
        # Check for any remaining NaN values in preprocessed features
        for dataset_name, X_dataset in [("train", X_train), ("val", X_val), ("test", X_test)]:
            if X_dataset.isnull().any().any():
                print(f"Warning: Preprocessed {dataset_name} features contain NaN values")
                # Fill any remaining NaNs with 0
                if dataset_name == "train":
                    X_train = X_train.fillna(0)
                elif dataset_name == "val":
                    X_val = X_val.fillna(0)
                else:
                    X_test = X_test.fillna(0)
        
        print(f"Preprocessed feature matrix shape: {X_train.shape}")
        print(f"Preprocessed target vector shape: {y_train.shape}")
        
        return X_train, X_val, X_test, y_train, y_val, y_test
    
    def is_classification_task(self, y):
        """Determine if this is a classification or regression task."""
        unique_values = len(np.unique(y))
        return unique_values < 20 or y.dtype == 'object'
    
    def get_models(self, is_classification=True):
        """Get appropriate models based on task type."""
        if is_classification:
            return {
                'RandomForest': RandomForestClassifier(n_estimators=100, random_state=42),
                'LogisticRegression': LogisticRegression(random_state=42, max_iter=1000),
                'SVM': SVC(random_state=42, probability=True)
            }
        else:
            return {
                'RandomForest': RandomForestRegressor(n_estimators=100, random_state=42),
                'LinearRegression': LinearRegression(),
                'SVR': SVR()
            }
    
    def evaluate_classification(self, model, X_train, X_val, X_test, y_train, y_val, y_test):
        """Evaluate classification model performance."""
        start_time = time.time()
        
        # Train model
        model.fit(X_train, y_train)
        training_time = time.time() - start_time
        
        # Predictions
        y_val_pred = model.predict(X_val)
        y_test_pred = model.predict(X_test)
        
        # Metrics
        metrics = {
            'training_time': training_time,
            'val_accuracy': accuracy_score(y_val, y_val_pred),
            'test_accuracy': accuracy_score(y_test, y_test_pred),
            'val_precision': precision_score(y_val, y_val_pred, average='weighted', zero_division=0),
            'test_precision': precision_score(y_test, y_test_pred, average='weighted', zero_division=0),
            'val_recall': recall_score(y_val, y_val_pred, average='weighted', zero_division=0),
            'test_recall': recall_score(y_test, y_test_pred, average='weighted', zero_division=0),
            'val_f1': f1_score(y_val, y_val_pred, average='weighted', zero_division=0),
            'test_f1': f1_score(y_test, y_test_pred, average='weighted', zero_division=0)
        }
        
        # ROC AUC for binary classification
        if len(np.unique(y_test)) == 2:
            try:
                y_val_proba = model.predict_proba(X_val)[:, 1]
                y_test_proba = model.predict_proba(X_test)[:, 1]
                metrics['val_roc_auc'] = roc_auc_score(y_val, y_val_proba)
                metrics['test_roc_auc'] = roc_auc_score(y_test, y_test_proba)
            except:
                pass
        
        return metrics
    
    def evaluate_regression(self, model, X_train, X_val, X_test, y_train, y_val, y_test):
        """Evaluate regression model performance."""
        start_time = time.time()
        
        # Train model
        model.fit(X_train, y_train)
        training_time = time.time() - start_time
        
        # Predictions
        y_val_pred = model.predict(X_val)
        y_test_pred = model.predict(X_test)
        
        # Metrics
        metrics = {
            'training_time': training_time,
            'val_mse': mean_squared_error(y_val, y_val_pred),
            'test_mse': mean_squared_error(y_test, y_test_pred),
            'val_mae': mean_absolute_error(y_val, y_val_pred),
            'test_mae': mean_absolute_error(y_test, y_test_pred),
            'val_r2': r2_score(y_val, y_val_pred),
            'test_r2': r2_score(y_test, y_test_pred)
        }
        
        return metrics
    
    def run_experiment(self):
        """Run the complete experiment comparing original vs preprocessed data."""
        print("Starting Dataset Preprocessing Experiment")
        print("=" * 50)
        
        # Load datasets
        self.load_datasets()
        
        # Analyze preprocessing impact
        self.analyze_preprocessing_impact()
        
        # Prepare data
        X_train_orig, X_val_orig, X_test_orig, y_train_orig, y_val_orig, y_test_orig = self.prepare_original_data()
        X_train_prep, X_val_prep, X_test_prep, y_train_prep, y_val_prep, y_test_prep = self.prepare_preprocessed_data()
        
        # Determine task type
        is_classification = self.is_classification_task(y_train_orig)
        task_type = "Classification" if is_classification else "Regression"
        print(f"\nTask Type: {task_type}")
        print()
        
        # Get models
        models = self.get_models(is_classification)
        
        # Run experiments
        for model_name, model in models.items():
            print(f"Training {model_name}...")
            
            # Original data
            print(f"  Original data...")
            if is_classification:
                orig_metrics = self.evaluate_classification(
                    model, X_train_orig, X_val_orig, X_test_orig,
                    y_train_orig, y_val_orig, y_test_orig
                )
            else:
                orig_metrics = self.evaluate_regression(
                    model, X_train_orig, X_val_orig, X_test_orig,
                    y_train_orig, y_val_orig, y_test_orig
                )
            
            # Preprocessed data
            print(f"  Preprocessed data...")
            if is_classification:
                prep_metrics = self.evaluate_classification(
                    model, X_train_prep, X_val_prep, X_test_prep,
                    y_train_prep, y_val_prep, y_test_prep
                )
            else:
                prep_metrics = self.evaluate_regression(
                    model, X_train_prep, X_val_prep, X_test_prep,
                    y_train_prep, y_val_prep, y_test_prep
                )
            
            # Store results
            self.results['original'][model_name] = orig_metrics
            self.results['preprocessed'][model_name] = prep_metrics
        
        # Generate report
        self.generate_report(is_classification)
        self.plot_comparison(is_classification)
        
        # Generate summary
        self.generate_summary()
    
    def generate_report(self, is_classification=True):
        """Generate a comprehensive comparison report."""
        print("\n" + "=" * 60)
        print("EXPERIMENT RESULTS")
        print("=" * 60)
        
        for model_name in self.results['original'].keys():
            print(f"\n{model_name}:")
            print("-" * 40)
            
            orig = self.results['original'][model_name]
            prep = self.results['preprocessed'][model_name]
            
            if is_classification:
                metrics = ['test_accuracy', 'test_precision', 'test_recall', 'test_f1', 'training_time']
                if 'test_roc_auc' in orig:
                    metrics.append('test_roc_auc')
            else:
                metrics = ['test_mse', 'test_mae', 'test_r2', 'training_time']
            
            for metric in metrics:
                if metric in orig and metric in prep:
                    orig_val = orig[metric]
                    prep_val = prep[metric]
                    
                    if metric == 'training_time':
                        improvement = ((orig_val - prep_val) / orig_val) * 100
                        print(f"  {metric.replace('_', ' ').title()}: {orig_val:.4f}s → {prep_val:.4f}s ({improvement:+.1f}%)")
                    elif metric in ['test_mse', 'test_mae']:  # Lower is better
                        improvement = ((orig_val - prep_val) / orig_val) * 100
                        print(f"  {metric.replace('_', ' ').title()}: {orig_val:.4f} → {prep_val:.4f} ({improvement:+.1f}%)")
                    else:  # Higher is better
                        improvement = ((prep_val - orig_val) / orig_val) * 100
                        print(f"  {metric.replace('_', ' ').title()}: {orig_val:.4f} → {prep_val:.4f} ({improvement:+.1f}%)")
    
    def plot_comparison(self, is_classification=True):
        """Create visualization comparing the results."""
        models = list(self.results['original'].keys())
        
        if is_classification:
            metric = 'test_accuracy'
            title = 'Test Accuracy Comparison'
            ylabel = 'Accuracy'
        else:
            metric = 'test_r2'
            title = 'Test R² Score Comparison'
            ylabel = 'R² Score'
        
        # Extract values
        orig_values = [self.results['original'][model][metric] for model in models]
        prep_values = [self.results['preprocessed'][model][metric] for model in models]
        
        # Create plot
        x = np.arange(len(models))
        width = 0.35
        
        fig, ax = plt.subplots(figsize=(10, 6))
        bars1 = ax.bar(x - width/2, orig_values, width, label='Original Data', alpha=0.8)
        bars2 = ax.bar(x + width/2, prep_values, width, label='Preprocessed Data', alpha=0.8)
        
        ax.set_xlabel('Models')
        ax.set_ylabel(ylabel)
        ax.set_title(title)
        ax.set_xticks(x)
        ax.set_xticklabels(models)
        ax.legend()
        
        # Add value labels on bars
        for bars in [bars1, bars2]:
            for bar in bars:
                height = bar.get_height()
                ax.annotate(f'{height:.3f}',
                           xy=(bar.get_x() + bar.get_width() / 2, height),
                           xytext=(0, 3),
                           textcoords="offset points",
                           ha='center', va='bottom')
        
        plt.tight_layout()
        plt.savefig('experiment_results.png', dpi=300, bbox_inches='tight')
        plt.show()
    
    def generate_summary(self):
        """Generate a summary of preprocessing benefits."""
        print("\n" + "=" * 60)
        print("PREPROCESSING IMPACT SUMMARY")
        print("=" * 60)
        
        improvements = {'mse': [], 'mae': [], 'r2': [], 'time': []}
        
        for model_name in self.results['original'].keys():
            orig = self.results['original'][model_name]
            prep = self.results['preprocessed'][model_name]
            
            # Calculate improvements
            mse_imp = ((orig['test_mse'] - prep['test_mse']) / orig['test_mse']) * 100
            mae_imp = ((orig['test_mae'] - prep['test_mae']) / orig['test_mae']) * 100
            r2_imp = ((prep['test_r2'] - orig['test_r2']) / abs(orig['test_r2'])) * 100
            time_imp = ((orig['training_time'] - prep['training_time']) / orig['training_time']) * 100
            
            improvements['mse'].append(mse_imp)
            improvements['mae'].append(mae_imp)
            improvements['r2'].append(r2_imp)
            improvements['time'].append(time_imp)
        
        print(f"Average Improvements:")
        print(f"  MSE (Mean Squared Error): {np.mean(improvements['mse']):+.1f}%")
        print(f"  MAE (Mean Absolute Error): {np.mean(improvements['mae']):+.1f}%")
        print(f"  R² Score: {np.mean(improvements['r2']):+.1f}%")
        print(f"  Training Time: {np.mean(improvements['time']):+.1f}%")
        
        print(f"\nKey Preprocessing Benefits:")
        print(f"  ✓ Removed low-quality features (>50% null)")
        print(f"  ✓ Proper encoding of categorical variables")
        print(f"  ✓ Target variable scaling for better convergence")
        print(f"  ✓ AI-based intelligent null imputation")
        print(f"  ✓ Optimal train/validation/test split")


# Example usage
if __name__ == "__main__":
    # Configure your experiment
    experiment = DatasetExperiment(
        original_dataset_path="./machine-readable-business-employment-data-dec-2024-quarter.csv",
        preprocessed_train_path="./logs/dataset_splitted/train/train.csv",  # Adjust paths based on your app's output
        preprocessed_val_path="./logs/dataset_splitted/validation/val.csv",
        preprocessed_test_path="./logs/dataset_splitted/test/test.csv",
        target_column="Data_value"  # Replace with your actual target column
    )
    
    # Run the experiment
    experiment.run_experiment()