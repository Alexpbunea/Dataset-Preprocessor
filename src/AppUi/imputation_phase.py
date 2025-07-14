# -*- coding: utf-8 -*-

from PySide6.QtCore import QCoreApplication, Qt, QTimer
from PySide6.QtGui import QIcon, QPixmap
from PySide6.QtWidgets import (
    QMainWindow, QLabel, QPushButton, QSizePolicy, QVBoxLayout, 
    QWidget, QHBoxLayout, QFileDialog, QTableWidget, QTableWidgetItem, 
    QHeaderView, QScrollArea, QAbstractItemView, QLineEdit, QComboBox, 
    QRadioButton, QGroupBox, QFormLayout, QMessageBox
)
from src.utils import *
from src.logic.imputation_AI import ImputationAI
from pyspark.sql.functions import monotonically_increasing_id, col as spark_col
from pyspark.sql.window import Window
from pyspark.sql.functions import col, sum, when, isnan
from functools import reduce


class Ui_imputation_phase(object):
    """
    Imputation phase UI class for the Dataset Preprocessor application.
    
    This class provides the interface for handling missing values in datasets using
    both traditional Spark statistical methods and AI-powered imputation techniques.
    """
    
    def __init__(self):
        """Initialize the UI components and variables"""
        # Core components
        self.MainWindow = None
        self.cleaning_logic = None
        self.dataset_info = None
        self.centralwidget = None
        self.verticalLayout = None
        self.utils = None
        
        # UI elements
        self.title_label = None
        self.subtitle_label = None
        self.status_label = None
        
        # Spark method components
        self.spark_calculations = None
        self.spark_methods_container = None
        self.spark_methods_layout = None
        self.columns_label = None
        self.methods_table = None
        
        # AI method components
        self.machine_learning = None
        self.ai_table_container = None
        self.ai_table_layout = None
        self.model_selection_container = None
        self.model_selection_layout = None
        self.model_label = None
        self.model_combo = None
        self.ai_table = None
        
        # Action buttons
        self.pushButton = None
        self.pushButton2 = None
        self.pushButton3 = None

    def setupUi(self, MainWindow):
        """Initialize and configure the imputation phase UI"""
        self.MainWindow = MainWindow
        
        # Configure main window
        self._setup_main_window()
        
        # Initialize data handling
        self._initialize_data_handlers()
        
        # Create central widget and layout
        self._create_central_widget()
        
        # Create UI components
        self._create_widgets()
        
        # Configure widget properties and layout
        self._setup_widget_properties()
        self._setup_layout()
        
        # Initialize utilities and styling
        self._initialize_utilities()
        self._setup_event_handlers()
        
        # Apply styling and responsive design
        self.setup_styles()
        self._install_resize_event()
        
        # Set central widget
        MainWindow.setCentralWidget(self.centralwidget)

    # === SETUP METHODS ===

    def _setup_main_window(self):
        """Configure main window properties"""
        if not self.MainWindow.objectName():
            self.MainWindow.setObjectName(u"MainWindow")
        self.MainWindow.resize(900, 600)
        self.MainWindow.setMinimumSize(600, 400)

    def _initialize_data_handlers(self):
        """Initialize data handling components"""
        self.cleaning_logic = None
        self.dataset_info = None

    def _create_central_widget(self):
        """Create and configure the central widget"""
        self.centralwidget = QWidget(self.MainWindow)
        self.centralwidget.setObjectName(u"centralwidget")
        
        # Main vertical layout
        self.verticalLayout = QVBoxLayout(self.centralwidget)
        self.verticalLayout.setContentsMargins(20, 20, 20, 20)
        self.verticalLayout.setSpacing(20)

    def _create_widgets(self):
        """Create all UI widgets"""
        # Header components
        self.title_label = QLabel("Imputation", self.centralwidget)
        self.subtitle_label = QLabel("Substituting missing values in your dataset with reasonable estimations", self.centralwidget)
        
        # Create imputation method containers
        self._create_spark_method_components()
        self._create_ai_method_components()
        
        # Create action buttons
        self._create_action_buttons()

    def _create_spark_method_components(self):
        """Create Spark statistical method components"""
        # Radio button for Spark methods
        self.spark_calculations = QRadioButton("Spark methods:", self.centralwidget)
        
        # Container for Spark methods
        self.spark_methods_container = QWidget(self.centralwidget)
        self.spark_methods_layout = QVBoxLayout(self.spark_methods_container)
        
        # Components within Spark container
        self.columns_label = QLabel("Select columns and imputation methods:", self.spark_methods_container)
        
        # Table for method selection
        self.methods_table = QTableWidget(0, 3)
        self.methods_table.setHorizontalHeaderLabels(["Column", "Data Type", "Imputation Method"])
        self.methods_table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        self.methods_table.verticalHeader().setVisible(False)
        self.methods_table.setSelectionBehavior(QAbstractItemView.SelectRows)
        
        # Add components to Spark layout
        self.spark_methods_layout.addWidget(self.columns_label)
        self.spark_methods_layout.addWidget(self.methods_table)

    def _create_ai_method_components(self):
        """Create AI imputation method components"""
        # Radio button for AI methods
        self.machine_learning = QRadioButton("Artificial intelligence:", self.centralwidget)
        
        # Container for AI methods
        self.ai_table_container = QWidget(self.centralwidget)
        self.ai_table_layout = QVBoxLayout(self.ai_table_container)
        
        # Model selection container
        self._create_model_selection_container()
        
        # AI table for feature/target selection
        self.ai_table = QTableWidget(0, 3)
        self.ai_table.setHorizontalHeaderLabels(["Column", "Use as feature (X)", "Use as target (Y)"])
        self.ai_table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        self.ai_table.verticalHeader().setVisible(False)
        self.ai_table.setSelectionBehavior(QAbstractItemView.SelectRows)
        
        # Add components to AI layout
        self.ai_table_layout.addWidget(self.model_selection_container)
        self.ai_table_layout.addWidget(self.ai_table)

    def _create_model_selection_container(self):
        """Create model selection container"""
        self.model_selection_container = QWidget(self.ai_table_container)
        self.model_selection_layout = QHBoxLayout(self.model_selection_container)
        self.model_selection_layout.setContentsMargins(0, 0, 0, 10)
        
        # Model selection components
        self.model_label = QLabel("Select AI model:", self.model_selection_container)
        self.model_combo = QComboBox(self.model_selection_container)
        self.model_combo.addItems(["Linear Regression", "Random Forest", "Decision Tree"])
        
        # Add to layout
        self.model_selection_layout.addWidget(self.model_label)
        self.model_selection_layout.addWidget(self.model_combo)
        self.model_selection_layout.addStretch()

    def _create_action_buttons(self):
        """Create action buttons"""
        self.pushButton = QPushButton("Continue", self.centralwidget)
        self.pushButton2 = QPushButton("Back", self.centralwidget)
        self.pushButton3 = QPushButton("Apply", self.centralwidget)

    def _setup_widget_properties(self):
        """Configure widget properties and initial states"""
        # Header alignment
        self.title_label.setAlignment(Qt.AlignCenter)
        self.subtitle_label.setAlignment(Qt.AlignCenter)
        
        # Radio button initial states
        self.spark_calculations.setChecked(False)
        self.machine_learning.setChecked(False)
        
        # Button size policies
        for button in [self.pushButton, self.pushButton2, self.pushButton3]:
            button.setSizePolicy(QSizePolicy.Fixed, QSizePolicy.Fixed)

    def _setup_layout(self):
        """Arrange widgets in the main layout"""
        # Add header components
        self.verticalLayout.addWidget(self.title_label)
        self.verticalLayout.addWidget(self.subtitle_label)
        self.verticalLayout.addStretch(1)
        
        # Add radio buttons
        self.verticalLayout.addWidget(self.spark_calculations)
        self.verticalLayout.addWidget(self.machine_learning)
        
        # Add method containers
        self.verticalLayout.addWidget(self.spark_methods_container)
        self.verticalLayout.addWidget(self.ai_table_container)
        self.verticalLayout.addStretch(1)
        
        # Create button layout
        self._create_button_layout()

    def _create_button_layout(self):
        """Create the bottom button layout with status label"""
        button_container = QHBoxLayout()
        button_container.setContentsMargins(0, 0, 0, 0)
        button_container.setSpacing(10)
        
        # Status label
        self.status_label = QLabel("")
        self.status_label.setMinimumWidth(200)
        self.status_label.setAlignment(Qt.AlignVCenter | Qt.AlignLeft)
        button_container.addWidget(self.status_label)
        
        # Add stretch and buttons
        button_container.addStretch()
        button_container.addWidget(self.pushButton3)
        button_container.addWidget(self.pushButton2)
        button_container.addWidget(self.pushButton)
        
        # Add to main layout
        self.verticalLayout.addLayout(button_container)

    def _initialize_utilities(self):
        """Initialize utility components"""
        self.utils = Utils(None, self.centralwidget, self.title_label, self.subtitle_label, 
                          self.pushButton, self.pushButton2, self.pushButton3, None, None)

    def _setup_event_handlers(self):
        """Setup event handlers for UI components"""
        # Radio button toggle events
        self.spark_calculations.toggled.connect(self.toggle_imputation_options)
        self.machine_learning.toggled.connect(self.toggle_imputation_options)
        
        # Button click events
        self.pushButton3.clicked.connect(self.apply_imputation_methods)
        
        # Initial toggle to set correct visibility
        self.toggle_imputation_options()

    # === TOGGLE AND POPULATION METHODS ===

    def toggle_imputation_options(self):
        """Toggle container visibility based on selected radio button"""
        show_spark = self.spark_calculations.isChecked()
        self.spark_methods_container.setVisible(show_spark)

        show_ai = self.machine_learning.isChecked()
        self.ai_table_container.setVisible(show_ai)

        # Populate tables when containers become visible
        if show_spark and self.dataset_info and self.methods_table.rowCount() == 0:
            self.populate_methods_table()
        
        if show_ai and self.dataset_info and self.ai_table.rowCount() == 0:
            self.populate_ai_table()

    def populate_methods_table(self):
        """Populate the Spark methods table with dataset columns"""
        self.methods_table.setRowCount(0)
        
        if not self.dataset_info:
            return
        
        # Get dataset information
        columns = self.dataset_info.get_general_info()['column_names']
        dtypes_list = self.dataset_info.get_dataframe().dtypes
        dtypes = dict(dtypes_list)
        
        # Configure table properties
        self._configure_methods_table()
        
        # Add rows for each column
        for col in columns:
            self._add_methods_table_row(col, dtypes)

    def _configure_methods_table(self):
        """Configure methods table properties"""
        # Set uniform row heights
        self.methods_table.verticalHeader().setDefaultSectionSize(40)
        
        # Set header resize modes
        self.methods_table.horizontalHeader().setSectionResizeMode(0, QHeaderView.Stretch)
        self.methods_table.horizontalHeader().setSectionResizeMode(1, QHeaderView.ResizeToContents)
        self.methods_table.horizontalHeader().setSectionResizeMode(2, QHeaderView.Stretch)

    def _add_methods_table_row(self, column, dtypes):
        """Add a row to the methods table"""
        row = self.methods_table.rowCount()
        self.methods_table.insertRow(row)
        
        # Column name (non-editable)
        col_item = QTableWidgetItem(column)
        col_item.setFlags(col_item.flags() & ~Qt.ItemIsEditable)
        self.methods_table.setItem(row, 0, col_item)
        
        # Data type (non-editable)
        dtype = dtypes.get(column, 'unknown')
        dtype_item = QTableWidgetItem(dtype)
        dtype_item.setFlags(dtype_item.flags() & ~Qt.ItemIsEditable)
        self.methods_table.setItem(row, 1, dtype_item)
        
        # Imputation method combo box
        method_combo = self._create_method_combo(dtype)
        self.methods_table.setCellWidget(row, 2, method_combo)

    def _create_method_combo(self, dtype):
        """Create imputation method combo box based on data type"""
        method_combo = QComboBox()
        method_combo.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        method_combo.setStyleSheet(self._combo_style())
        method_combo.setMinimumHeight(40)
        
        # Add appropriate methods based on data type
        if 'int' in dtype or 'float' in dtype or 'double' in dtype:
            method_combo.addItems(["None", "Mean", "Median", "Mode"])
        else:
            method_combo.addItems(["None", "Mode"])
            
        return method_combo

    def populate_ai_table(self):
        """Populate the AI table with dataset columns and options"""
        self.ai_table.setRowCount(0)
        
        if not self.dataset_info:
            return
        
        columns = self.dataset_info.get_general_info()['column_names']
        
        # Set uniform row heights
        self.ai_table.verticalHeader().setDefaultSectionSize(40)
        
        # Add rows for each column
        for col in columns:
            self._add_ai_table_row(col)

    def _add_ai_table_row(self, column):
        """Add a row to the AI table"""
        row = self.ai_table.rowCount()
        self.ai_table.insertRow(row)
        
        # Column name (non-editable)
        col_item = QTableWidgetItem(column)
        col_item.setFlags(col_item.flags() & ~Qt.ItemIsEditable)
        self.ai_table.setItem(row, 0, col_item)
        
        # Feature selection combo box
        feature_combo = QComboBox()
        feature_combo.addItems(["No", "Yes"])
        feature_combo.setStyleSheet(self._combo_style())
        self.ai_table.setCellWidget(row, 1, feature_combo)
        
        # Target selection combo box
        target_combo = QComboBox()
        target_combo.addItems(["No", "Yes"])
        target_combo.setStyleSheet(self._combo_style())
        self.ai_table.setCellWidget(row, 2, target_combo)

    # === DATA MANAGEMENT METHODS ===

    def set_dataset_info(self, dataset_info):
        """Set the dataset info and trigger table population if needed"""
        self.dataset_info = dataset_info

    def get_selected_methods(self):
        """Get selected imputation methods from the methods table"""
        methods = {}
        for row in range(self.methods_table.rowCount()):
            col = self.methods_table.item(row, 0).text()
            method_combo = self.methods_table.cellWidget(row, 2)
            methods[col] = method_combo.currentText().lower()
        return methods

    def show_status(self, message, color):
        """Show status message with color"""
        self.status_label.setText(message)
        self.status_label.setStyleSheet(f"color: {color}; font-weight: bold;")
        QTimer.singleShot(5000, lambda: self.status_label.setText(""))

    def apply_imputation_methods(self):
        """Apply the selected imputation methods to the dataset"""
        if self.spark_calculations.isChecked():
            self._apply_spark_imputation()
        elif self.machine_learning.isChecked():
            self._apply_ai_imputation()
        else:
            self.show_status("Please select an imputation method", "orange")

    def _apply_spark_imputation(self):
        """Apply Spark statistical imputation methods"""
        try:
            print("[INFO] -> Trying to substitute null values with Spark methods")
            
            # Get selected methods
            selected_methods = self.get_selected_methods()
            
            # Validate selection
            if all(method == "none" for method in selected_methods.values()):
                self.show_status("No Spark imputation method selected: all options are 'None'", "orange")
                print("[ERROR] -> No Spark imputation method selected")
                return
            
            # Get current DataFrame
            df = self.dataset_info.get_dataframe()
            
            # Apply imputation methods
            df = self._apply_spark_methods_to_dataframe(df, selected_methods)
            
            # Update dataset
            self.dataset_info.set_dataframe(df)
            
            print("[SUCCESS] -> Substituted null values with Spark methods")
            self.show_status("Imputation applied successfully!", "green")
            
        except Exception as e:
            print(f"[ERROR] -> Spark imputation failed: {e}")
            self.show_status("Imputation failed!", "red")

    def _apply_spark_methods_to_dataframe(self, df, selected_methods):
        """Apply Spark imputation methods to dataframe"""
        # Import necessary Spark functions
        from pyspark.sql import functions as F
        
        for column, method in selected_methods.items():
            if method == "mean":
                mean_val = df.select(F.mean(F.col(column))).collect()[0][0]
                df = df.na.fill({column: mean_val})
                
            elif method == "median":
                quantiles = df.approxQuantile(column, [0.5], 0.0)
                median_val = quantiles[0] if quantiles else None
                if median_val is not None:
                    df = df.na.fill({column: median_val})
                    
            elif method == "mode":
                mode_df = (df.groupBy(column)
                          .count()
                          .orderBy(F.desc("count"))
                          .limit(1))
                mode_row = mode_df.collect()
                mode_val = mode_row[0][0] if mode_row else None
                if mode_val is not None:
                    df = df.na.fill({column: mode_val})
        
        return df

    def _apply_ai_imputation(self):
        """Apply AI imputation methods"""
        try:
            print("[INFO] -> Substituting null values with Artificial Intelligence")
            
            # Get selected model and features
            selected_model = self.model_combo.currentText()
            feature_cols, target_cols = self._get_ai_selections()
            
            # Validate selections
            if not self._validate_ai_selections(feature_cols, target_cols):
                return
            
            print(f"[INFO] -> Selected AI model: {selected_model}")
            print(f"[INFO] -> Feature columns: {feature_cols}")
            print(f"[INFO] -> Target column: {target_cols[0]}")
            
            # Apply AI imputation
            ai_imputer = ImputationAI(self.dataset_info)
            
            if ai_imputer.train_model(feature_cols, target_cols[0], selected_model):
                if ai_imputer.impute_missing_values():
                    self.show_status("AI imputation applied successfully!", "green")
                    print(f"[SUCCESS] -> AI imputation applied with {selected_model}")
                else:
                    self.show_status("AI imputation failed during prediction!", "red")
            else:
                self.show_status("AI model training failed!", "red")
                
        except Exception as e:
            print(f"[ERROR] -> AI imputation failed: {e}")
            self.show_status("AI imputation failed!", "red")

    def _get_ai_selections(self):
        """Get feature and target column selections from AI table"""
        feature_cols = []
        target_cols = []
        
        for row in range(self.ai_table.rowCount()):
            col_name = self.ai_table.item(row, 0).text()
            feature_combo = self.ai_table.cellWidget(row, 1)
            target_combo = self.ai_table.cellWidget(row, 2)
            
            if feature_combo.currentText() == "Yes":
                feature_cols.append(col_name)
            if target_combo.currentText() == "Yes":
                target_cols.append(col_name)
        
        return feature_cols, target_cols

    def _validate_ai_selections(self, feature_cols, target_cols):
        """Validate AI feature and target selections"""
        if len(feature_cols) == 0 or len(target_cols) == 0:
            self.show_status("Please select at least one feature and one target column", "orange")
            print("[ERROR] -> Please select at least one feature and one target column")
            return False
        
        if len(target_cols) > 1:
            self.show_status("Only one target column can be selected", "red")
            print("[ERROR] -> Only one target column can be selected")
            return False
        
        return True

    # === UTILITY METHODS ===

    @staticmethod
    def _combo_style():
        """Style for combo boxes in tables"""
        return """
            QComboBox {
                color: black;
                border: none;
                background-color: transparent;
                padding: 0;
                margin: 0;
            }
            QComboBox::drop-down {
                subcontrol-origin: padding;
                subcontrol-position: center right;
                width: 20px;
                border: none;
            }
            QComboBox QAbstractItemView {
                border: 1px solid #cccccc;
                color: black;
            }
        """

    # === STYLING METHODS ===
    def setup_styles(self, title_size=36, subtitle_size=18, button_size=4):
        """Setup styles for all UI components"""
        # Get base styles from utils
        self.centralwidget, title_style, subtitle_style, button_style, button_style_back, table_style, _, controls_style_str = self.utils.setup_styles(
            title_size, subtitle_size, button_size)

        # Apply control styles to central widget
        current_centralwidget_style = self.centralwidget.styleSheet()
        self.centralwidget.setStyleSheet(current_centralwidget_style + "\n" + controls_style_str)

        # Apply component styles
        self.title_label.setStyleSheet(title_style)
        self.subtitle_label.setStyleSheet(subtitle_style)
        self.pushButton.setStyleSheet(button_style)
        self.pushButton2.setStyleSheet(button_style_back)
        self.pushButton3.setStyleSheet(button_style)
        self.methods_table.setStyleSheet(table_style)
        self.ai_table.setStyleSheet(table_style)

        # Apply additional component styles
        self._apply_additional_styles(button_size)

    def _apply_additional_styles(self, button_size):
        """Apply additional component-specific styles"""
        # Label style
        label_style = f"""
            QLabel {{
                color: #333333;
                font-size: {max(12, int(button_size * 0.9))}px;
            }}
        """
        
        # Model combo style
        model_combo_style = f"""
            QComboBox {{
                color: black;
                background-color: white;
                border: 1px solid #cccccc;
                border-radius: 3px;
                padding: 5px;
                font-size: {button_size}px;
            }}
            QComboBox::drop-down {{
                subcontrol-origin: padding;
                subcontrol-position: top right;
                width: 20px;
                border-left: 1px solid #cccccc;
            }}
            QComboBox QAbstractItemView {{
                background-color: white;
                color: black;
                selection-background-color: #e0e0e0;
                selection-color: black;
            }}
        """
        
        # Apply styles
        self.columns_label.setStyleSheet(label_style)
        self.model_combo.setStyleSheet(model_combo_style)

    def _install_resize_event(self):
        """Install responsive resize event handler"""
        original_resize_event = self.MainWindow.resizeEvent

        def new_resize_event(event):
            # Calculate responsive sizes
            new_title_size = int(self.MainWindow.height() * 0.06)
            new_subtitle_size = int(self.MainWindow.height() * 0.03)
            new_button_size = int(self.MainWindow.height() * 0.021)

            # Apply responsive styling
            self.setup_styles(
                title_size=new_title_size,
                subtitle_size=new_subtitle_size,
                button_size=new_button_size,
            )
            
            # Call original resize event
            original_resize_event(event)

        self.MainWindow.resizeEvent = new_resize_event

    def retranslateUi(self, MainWindow):
        """Set UI text translations"""
        MainWindow.setWindowTitle(QCoreApplication.translate("MainWindow", u"Dataset Preprocessor", None))