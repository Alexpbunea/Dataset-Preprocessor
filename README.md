# Dataset-Preprocessor

A comprehensive, user-friendly application for preprocessing datasets for machine learning tasks. This tool provides a complete pipeline for data cleaning, transformation, imputation, and analysis using both traditional statistical methods and advanced AI techniques.

> ⚠️ **Note:** This is a student project developed for academic and personal use only.  
> It is not intended for commercial or enterprise environments.

## Overview

The Dataset Preprocessor is a PySide6-based desktop application that streamlines the data preprocessing workflow. It combines the power of Apache Spark for distributed processing with machine learning capabilities to handle various data preprocessing tasks efficiently.

## Supported Data Formats

Currently supported:
- **CSV**: Comma-separated values

Planned for future versions:
- **Excel**: .xlsx and .xls files
- **JSON**: JavaScript Object Notation
- **Parquet**: Columnar storage format
- **Custom formats**

## Key Features

### 🔧 **Data Cleaning & Transformation**
- **Data Type Detection**: Automatic detection and handling of numerical, categorical, and binary data
- **Operations**: Delete and modify columns and rows with ease
- **Data Validation**: Built-in validation to ensure data integrity throughout the process

### 🧹 **Advanced Data Cleaning**
- **Missing Value Detection**: Comprehensive analysis of null values and missing data patterns
- **Outlier Detection**: Statistical methods to identify and handle outliers
- **Duplicate Removal**: Efficient duplicate detection and removal algorithms
- **Data Standardization**: Normalize and standardize data formats

### 🤖 **Intelligent Data Imputation**
- **Statistical Methods**: Mean, median, mode imputation using Apache Spark
- **AI-Powered Imputation**: Machine learning models for intelligent missing value prediction
  - Linear Regression
  - Random Forest
  - Decision Tree
- **Feature Engineering**: Smart feature selection for AI imputation models

### 📊 **Data Transformation**
- **Encoding Techniques**:
  - Label Encoding for categorical variables
  - One-Hot Encoding for multi-category variables
  - Binary Encoding for binary data
- **Scaling Methods**:
  - StandardScaler for normal distribution
  - MinMaxScaler for bounded scaling
  - RobustScaler for outlier-resistant scaling
- **Custom Transformations**: Apply custom transformation logic

### 📈 **Data Analysis & Visualization**
- **Statistical Comparison**: Before/after analysis of data transformations
- **Visual Comparison**: Interactive charts and graphs
  - Histograms for distribution analysis
  - Box plots for outlier visualization
  - Scatter plots for correlation analysis
- **Data Profiling**: Comprehensive dataset statistics and metrics

### 🔄 **Data Splitting**
- **Train-Test Split**: Configurable ratios for machine learning workflows
- **Stratified Sampling**: Maintain data distribution across splits
- **Validation Sets**: Create validation datasets for model evaluation

## Technical Architecture

### **Core Technologies**
- **Frontend**: PySide6 (Qt for Python) for modern, responsive UI
- **Backend**: Apache Spark for distributed data processing
- **Machine Learning**: Scikit-learn integration for AI imputation
- **Visualization**: Matplotlib and Seaborn for data visualization

### **Modular Design**
- **Phase-Based Workflow**: Intuitive step-by-step processing pipeline
- **Extensible Architecture**: Easy to add new preprocessing methods
- **Robust Error Handling**: Comprehensive error management and user feedback
- **Responsive UI**: Adaptive interface that scales with window size

## Workflow Phases

### 1. **Initial Data Loading**
- Select and load datasets from various file formats
- Initial data preview and basic statistics
- Data type detection and validation

### 2. **Data Cleaning**
- Remove duplicates and handle missing values
- Column management and data type corrections
- Initial data quality assessment

### 3. **Data Transformation**
- Apply encoding and scaling transformations
- Handle categorical and numerical data separately

### 4. **Data Imputation**
- Choose between statistical and AI-powered imputation
- Configure imputation parameters
- Apply imputation results

### 5. **Data Splitting**
- Configure train/test/validation splits
- Maintain data distribution across splits
- Export split datasets

### 6. **Comparison & Analysis**
- Compare original vs. processed data
- Statistical analysis of changes
- Visual comparison tools

### 7. **Final Export**
- Export processed datasets

## Installation

### Prerequisites
- Python 3.12.9
- Hadoop
- Apache Spark
- Required Python packages (see `requirements.txt`)

### Setup
```bash
# Clone the repository
git clone https://github.com/Alexpbunea/Dataset-Preprocessor.git
cd Dataset-Preprocessor

# Install dependencies
pip install -r requirements.txt

# Run the application
python main.py
```

## Usage

1. **Launch the application**: Run `python main.py`
2. **Load your dataset**: Select your data file through the initial interface
3. **Follow the workflow**: Navigate through each preprocessing phase
4. **Configure settings**: Adjust parameters for each preprocessing step
5. **Apply transformations**: Execute the preprocessing operations
6. **Review results**: Use comparison tools to validate changes
7. **Export data**: Save your processed dataset

## Data Format Support

- **CSV**: Comma-separated values
- **Excel**: .xlsx and .xls files
- **JSON**: JavaScript Object Notation
- **Parquet**: Columnar storage format
- **Custom formats**: Extensible for additional formats

## Benefits

### **For Students and Data Practitioners**
- Simplifies repetitive preprocessing tasks
- Ensures consistent data quality
- Helps prototype machine learning pipelines faster
- Encourages reproducible workflows

### **For Personal Projects**
- No coding required for basic preprocessing
- Visual and intuitive interface
- Supports exploratory data analysis
- Can be used to experiment with different cleaning techniques

### ⚠️ **Note**
This project is not suitable for commercial or enterprise-level deployment.

## Contributing

At this time, external contributions are not being accepted. This project is maintained solely as an academic showcase.

## Support

This project is not actively maintained. For any issues or questions, feel free to open an issue on GitHub.

## License

This project is licensed under the MIT License - see the LICENSE file for details.


---

## About the Project

This project was developed solely for educational purposes during a university course in Data Engineering and Artificial Intelligence.  
It is intended for **personal and non-commercial use only**.

The software is provided “as-is” without any warranties or guarantees.  
The authors are not responsible for any outcomes resulting from the use of this application in production environments or business-critical contexts.


***© 2025 Alexander Pérez & Saioa Prieto. All rights reserved.***