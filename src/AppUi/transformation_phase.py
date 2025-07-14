# -*- coding: utf-8 -*-

import os
import sys
from PySide6.QtCore import QCoreApplication, Qt, QTimer
from PySide6.QtGui import QIcon, QPixmap
from PySide6.QtWidgets import (
    QMainWindow, QLabel, QPushButton, QSizePolicy, QVBoxLayout,
    QWidget, QHBoxLayout, QFileDialog, QTableWidget, QTableWidgetItem,
    QHeaderView, QAbstractItemView, QComboBox, QRadioButton
)
from src.utils import *
from src.logic.transformation import Transformation


class Ui_transformation_phase(object):
    """
    UI for data transformation phase including:
    - Binary string to numerical conversion
    - Categorical encoding (One-Hot, Label Encoding)
    - Feature scaling (StandardScaler, MinMaxScaler, RobustScaler)
    """
    
    def __init__(self):
        """Initialize all UI components"""
        self.MainWindow = None
        self.cleaning_logic = None
        self.dataset_info = None
        self.transformer = None
        
        # UI Components
        self.centralwidget = None
        self.verticalLayout = None
        self.title_label = None
        self.subtitle_label = None
        self.status_label = None
        self.utils = None
        
        # Radio buttons
        self.binary_conversion = None
        self.categorical_encoding = None
        self.feature_scaling = None
        
        # Containers and tables
        self.binary_container = None
        self.categorical_container = None
        self.scaling_container = None
        self.binary_table = None
        self.categorical_table = None
        self.scaling_table = None
        
        # Buttons
        self.pushButton = None
        self.pushButton2 = None
        self.pushButton3 = None

    # === SETUP METHODS ===
    
    def setupUi(self, MainWindow):
        """Initialize the main UI components and layout"""
        self.MainWindow = MainWindow
        self._configure_main_window()
        self._create_main_layout()
        self._create_transformation_options()
        self._create_containers()
        self._create_buttons()
        self._finalize_setup()

    def _configure_main_window(self):
        """Configure main window properties"""
        if not self.MainWindow.objectName():
            self.MainWindow.setObjectName(u"MainWindow")
        self.MainWindow.resize(900, 600)
        self.MainWindow.setMinimumSize(600, 400)
        
        self.cleaning_logic = None
        self.dataset_info = None

    def _create_main_layout(self):
        """Create the main central widget and layout"""
        self.centralwidget = QWidget(self.MainWindow)
        self.centralwidget.setObjectName(u"centralwidget")

        self.verticalLayout = QVBoxLayout(self.centralwidget)
        self.verticalLayout.setContentsMargins(20, 20, 20, 20)
        self.verticalLayout.setSpacing(20)

        # Title and subtitle
        self.title_label = QLabel("Transformation", self.centralwidget)
        self.subtitle_label = QLabel(
            "Converting all your data into a uniform format that can be efficiently processed by machine learning",
            self.centralwidget)

    def _create_transformation_options(self):
        """Create radio buttons for transformation options"""
        self.binary_conversion = QRadioButton("Convert from binary string to numerical", self.centralwidget)
        self.categorical_encoding = QRadioButton("Convert from categorical to One-Hot or Label-Encoding", self.centralwidget)
        self.feature_scaling = QRadioButton("Characteristics scaling", self.centralwidget)

        # Connect radio buttons to toggle function
        self.binary_conversion.toggled.connect(self.toggle_transformation_options)
        self.categorical_encoding.toggled.connect(self.toggle_transformation_options)
        self.feature_scaling.toggled.connect(self.toggle_transformation_options)

    def _create_containers(self):
        """Create containers for each transformation type"""
        self._create_binary_container()
        self._create_categorical_container()
        self._create_scaling_container()

    def _create_buttons(self):
        """Create action buttons"""
        self.pushButton = QPushButton("Continue", self.centralwidget)
        self.pushButton2 = QPushButton("Back", self.centralwidget)
        self.pushButton3 = QPushButton("Apply", self.centralwidget)

    def _finalize_setup(self):
        """Finalize UI setup with styling and event handling"""
        self._setup_widget_properties()
        self._setup_layout()

        self.utils = Utils(None, self.centralwidget, self.title_label, self.subtitle_label, self.pushButton,
                           self.pushButton2, self.pushButton3, None, None)
        self.setup_styles()

        self.pushButton3.clicked.connect(self.apply_transformation)
        self.MainWindow.setCentralWidget(self.centralwidget)
        self._install_resize_event()

    # === CONTAINER CREATION METHODS ===

    def _create_binary_container(self):
        """Create container for binary conversion options"""
        self.binary_container = QWidget(self.centralwidget)
        binary_layout = QVBoxLayout(self.binary_container)

        binary_label = QLabel("Select columns to convert from binary string to numerical:", self.binary_container)
        self.binary_table = self._create_table(["Column", "Convert"])

        binary_layout.addWidget(binary_label)
        binary_layout.addWidget(self.binary_table)

    def _create_categorical_container(self):
        """Create container for categorical encoding options"""
        self.categorical_container = QWidget(self.centralwidget)
        categorical_layout = QVBoxLayout(self.categorical_container)

        categorical_label = QLabel("Select categorical columns and encoding method:", self.categorical_container)
        self.categorical_table = self._create_table(["Column", "Categories", "Encoding Method"])

        categorical_layout.addWidget(categorical_label)
        categorical_layout.addWidget(self.categorical_table)

    def _create_scaling_container(self):
        """Create container for feature scaling options"""
        self.scaling_container = QWidget(self.centralwidget)
        scaling_layout = QVBoxLayout(self.scaling_container)

        scaling_label = QLabel("Select numerical columns and scaling method:", self.scaling_container)
        self.scaling_table = self._create_table(["Column", "Data Type", "Scaling Method"])

        scaling_layout.addWidget(scaling_label)
        scaling_layout.addWidget(self.scaling_table)

    def _create_table(self, headers):
        """Create a standardized table widget"""
        table = QTableWidget(0, len(headers))
        table.setHorizontalHeaderLabels(headers)
        table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        table.verticalHeader().setVisible(False)
        table.setSelectionBehavior(QAbstractItemView.SelectRows)
        return table

    def _setup_widget_properties(self):
        """Configure widget properties and initial states"""
        self.title_label.setAlignment(Qt.AlignCenter)
        self.subtitle_label.setAlignment(Qt.AlignCenter)

        # Set initial radio button states
        self.binary_conversion.setChecked(False)
        self.categorical_encoding.setChecked(False)
        self.feature_scaling.setChecked(False)
        
        # Set initial container visibility
        self.binary_container.setVisible(False)
        self.categorical_container.setVisible(False)
        self.scaling_container.setVisible(False)

        # Configure button size policies
        for button in [self.pushButton, self.pushButton2, self.pushButton3]:
            button.setSizePolicy(QSizePolicy.Fixed, QSizePolicy.Fixed)

    def _setup_layout(self):
        """Arrange widgets in the main layout"""
        # Add title and subtitle
        self.verticalLayout.addWidget(self.title_label)
        self.verticalLayout.addWidget(self.subtitle_label)
        self.verticalLayout.addStretch(1)

        # Add radio buttons
        self.verticalLayout.addWidget(self.binary_conversion)
        self.verticalLayout.addWidget(self.categorical_encoding)
        self.verticalLayout.addWidget(self.feature_scaling)

        # Add containers
        self.verticalLayout.addWidget(self.binary_container)
        self.verticalLayout.addWidget(self.categorical_container)
        self.verticalLayout.addWidget(self.scaling_container)
        self.verticalLayout.addStretch(1)

        # Create button layout
        self._create_button_layout()

    def _create_button_layout(self):
        """Create the bottom button layout with status label"""
        button_container = QHBoxLayout()
        button_container.setContentsMargins(0, 0, 0, 0)
        button_container.setSpacing(10)

        self.status_label = QLabel("")
        self.status_label.setMinimumWidth(200)
        self.status_label.setAlignment(Qt.AlignVCenter | Qt.AlignLeft)
        
        button_container.addWidget(self.status_label)
        button_container.addStretch()
        button_container.addWidget(self.pushButton3)
        button_container.addWidget(self.pushButton2)
        button_container.addWidget(self.pushButton)

        self.verticalLayout.addLayout(button_container)

    # === UI INTERACTION METHODS ===

    def toggle_transformation_options(self):
        """Toggle container visibility based on selected radio button"""
        self.binary_container.setVisible(self.binary_conversion.isChecked())
        self.categorical_container.setVisible(self.categorical_encoding.isChecked())
        self.scaling_container.setVisible(self.feature_scaling.isChecked())

        # Populate tables when containers become visible
        self._populate_active_table()

    def _populate_active_table(self):
        """Populate the currently active table based on selected option"""
        if self.binary_conversion.isChecked() and self.dataset_info and self.binary_table.rowCount() == 0:
            self.populate_binary_table()
        elif self.categorical_encoding.isChecked() and self.dataset_info and self.categorical_table.rowCount() == 0:
            self.populate_categorical_table()
        elif self.feature_scaling.isChecked() and self.dataset_info and self.scaling_table.rowCount() == 0:
            self.populate_scaling_table()

    # === TABLE POPULATION METHODS ===
    
    def populate_binary_table(self):
        """Populate table for binary string conversion"""
        self.binary_table.setRowCount(0)

        if not self._validate_transformer():
            return

        binary_columns = self.transformer.get_binary_columns()
        for col in binary_columns:
            self._add_binary_row(col)

    def _add_binary_row(self, col):
        """Add a row to the binary conversion table"""
        row = self.binary_table.rowCount()
        self.binary_table.insertRow(row)

        # Column name (non-editable)
        col_item = QTableWidgetItem(col)
        col_item.setFlags(col_item.flags() & ~Qt.ItemIsEditable)
        self.binary_table.setItem(row, 0, col_item)

        # Convert dropdown
        convert_combo = QComboBox()
        convert_combo.addItems(["No", "Yes"])
        convert_combo.setStyleSheet(self._combo_style())
        self.binary_table.setCellWidget(row, 1, convert_combo)

    def populate_categorical_table(self):
        """Populate table for categorical encoding"""
        self.categorical_table.setRowCount(0)

        if not self._validate_transformer():
            return

        categorical_columns = self.transformer.get_categorical_columns()
        for col in categorical_columns:
            self._add_categorical_row(col)

    def _add_categorical_row(self, col):
        """Add a row to the categorical encoding table"""
        row = self.categorical_table.rowCount()
        self.categorical_table.insertRow(row)

        # Column name (non-editable)
        col_item = QTableWidgetItem(col)
        col_item.setFlags(col_item.flags() & ~Qt.ItemIsEditable)
        self.categorical_table.setItem(row, 0, col_item)

        # Categories display
        categories_text = self._get_categories_text(col)
        categories_item = QTableWidgetItem(categories_text)
        categories_item.setFlags(categories_item.flags() & ~Qt.ItemIsEditable)
        self.categorical_table.setItem(row, 1, categories_item)

        # Encoding method dropdown
        encoding_combo = QComboBox()
        encoding_combo.addItems(["None", "One-Hot Encoding", "Label Encoding"])
        encoding_combo.setStyleSheet(self._combo_style())
        self.categorical_table.setCellWidget(row, 2, encoding_combo)

    def _get_categories_text(self, col):
        """Get formatted categories text for display"""
        try:
            categories = self.transformer.get_column_categories(col)
            categories_text = ", ".join([str(cat) for cat in categories[:5]])
            if len(categories) > 5:
                categories_text += f" ... ({len(categories)} total)"
            return categories_text
        except Exception as e:
            print(f"[WARNING] -> Failed to get categories for column {col}: {e}")
            return "Unable to load categories"

    def populate_scaling_table(self):
        """Populate table for feature scaling"""
        self.scaling_table.setRowCount(0)

        if not self._validate_transformer():
            return

        numerical_columns = self.transformer.get_numerical_columns()
        dtypes = dict(self.dataset_info.get_dataframe().dtypes)

        for col in numerical_columns:
            self._add_scaling_row(col, dtypes)

    def _add_scaling_row(self, col, dtypes):
        """Add a row to the scaling table"""
        row = self.scaling_table.rowCount()
        self.scaling_table.insertRow(row)

        # Column name (non-editable)
        col_item = QTableWidgetItem(col)
        col_item.setFlags(col_item.flags() & ~Qt.ItemIsEditable)
        self.scaling_table.setItem(row, 0, col_item)

        # Data type (non-editable)
        dtype_item = QTableWidgetItem(dtypes.get(col, 'unknown'))
        dtype_item.setFlags(dtype_item.flags() & ~Qt.ItemIsEditable)
        self.scaling_table.setItem(row, 1, dtype_item)

        # Scaling method dropdown
        scaling_combo = QComboBox()
        scaling_combo.addItems(["None", "StandardScaler", "MinMaxScaler", "RobustScaler"])
        scaling_combo.setStyleSheet(self._combo_style())
        self.scaling_table.setCellWidget(row, 2, scaling_combo)

    def _validate_transformer(self):
        """Validate that transformer is initialized"""
        if not self.transformer:
            print("[ERROR] -> Transformer not initialized")
            return False
        return True

    # === TRANSFORMATION APPLICATION METHODS ===

    def apply_transformation(self):
        """Apply the selected transformation method"""
        if self.binary_conversion.isChecked():
            self.apply_binary_conversion()
        elif self.categorical_encoding.isChecked():
            self.apply_categorical_encoding()
        elif self.feature_scaling.isChecked():
            self.apply_feature_scaling()
        else:
            self.show_status("Please select a transformation method", "orange")

    def apply_binary_conversion(self):
        """Apply binary string to numerical conversion"""
        try:
            print("[INFO] -> Applying binary string to numerical conversion")
            
            columns_config = self._get_binary_configuration()
            if not any(columns_config.values()):
                self.show_status("No columns selected for conversion", "orange")
                return

            # Apply transformation and update dataset
            self.transformer.apply_binary_transformation(columns_config)
            self._update_dataset_info()
            
            selected_columns = [k for k, v in columns_config.items() if v]
            self.show_status("Binary conversion applied successfully!", "green")
            print(f"[INFO] -> Binary conversion completed for: {selected_columns}")

        except Exception as e:
            print(f"[ERROR] -> Binary conversion failed: {e}")
            self.show_status("Binary conversion failed!", "red")

    def apply_categorical_encoding(self):
        """Apply categorical encoding"""
        try:
            print("[INFO] -> Applying categorical encoding")
            
            encoding_config = self._get_categorical_configuration()
            if not encoding_config:
                self.show_status("No encoding methods selected", "orange")
                return

            # Apply transformation and update dataset
            self.transformer.apply_categorical_transformation(encoding_config)
            self._update_dataset_info()
            
            self.show_status("Categorical encoding applied successfully!", "green")
            print(f"[INFO] -> Categorical encoding completed: {encoding_config}")

        except Exception as e:
            print(f"[ERROR] -> Categorical encoding failed: {e}")
            self.show_status("Categorical encoding failed!", "red")

    def apply_feature_scaling(self):
        """Apply feature scaling"""
        try:
            print("[INFO] -> Applying feature scaling")
            
            scaling_config = self._get_scaling_configuration()
            if not scaling_config:
                self.show_status("No scaling methods selected", "orange")
                return

            # Apply transformation and update dataset
            self.transformer.apply_numerical_transformation(scaling_config)
            self._update_dataset_info()
            
            self.show_status("Feature scaling applied successfully!", "green")
            print(f"[INFO] -> Feature scaling completed: {scaling_config}")

        except Exception as e:
            print(f"[ERROR] -> Feature scaling failed: {e}")
            self.show_status("Feature scaling failed!", "red")

    def _get_binary_configuration(self):
        """Extract binary conversion configuration from table"""
        columns_config = {}
        for row in range(self.binary_table.rowCount()):
            col_name = self.binary_table.item(row, 0).text()
            convert_combo = self.binary_table.cellWidget(row, 1)
            should_convert = convert_combo.currentText() == "Yes"
            columns_config[col_name] = should_convert
        return columns_config

    def _get_categorical_configuration(self):
        """Extract categorical encoding configuration from table"""
        encoding_config = {}
        for row in range(self.categorical_table.rowCount()):
            col_name = self.categorical_table.item(row, 0).text()
            encoding_combo = self.categorical_table.cellWidget(row, 2)
            encoding_method = encoding_combo.currentText()
            if encoding_method != "None":
                encoding_config[col_name] = encoding_method
        return encoding_config

    def _get_scaling_configuration(self):
        """Extract scaling configuration from table"""
        scaling_config = {}
        for row in range(self.scaling_table.rowCount()):
            col_name = self.scaling_table.item(row, 0).text()
            scaling_combo = self.scaling_table.cellWidget(row, 2)
            scaling_method = scaling_combo.currentText()
            if scaling_method != "None":
                scaling_config[col_name] = scaling_method
        return scaling_config

    def _update_dataset_info(self):
        """Update dataset info with transformed dataframe"""
        self.dataset_info.set_dataframe(self.transformer.dataframe)

    # === UTILITY METHODS ===

    def show_status(self, message, color):
        """Display status message with specified color"""
        self.status_label.setText(message)
        self.status_label.setStyleSheet(f"color: {color}; font-weight: bold;")
        QTimer.singleShot(5000, lambda: self.status_label.setText(""))

    def set_dataset_info(self, dataset_info):
        """Initialize dataset info and transformer"""
        self.dataset_info = dataset_info
        try:
            self.transformer = Transformation(self.dataset_info)
            self.transformer.categorize_columns()
            print("[INFO] -> Column categorization completed successfully")
        except Exception as e:
            print(f"[ERROR] -> Failed to categorize columns in the transformation phase: {e}")
            self.transformer = None

    @staticmethod
    def _combo_style():
        """Return CSS style for combo boxes in tables"""
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
        """Apply styling to UI components"""
        self.centralwidget, title_style, subtitle_style, button_style, button_style_back, table_style, _, controls_style_str = self.utils.setup_styles(
            title_size, subtitle_size, button_size)

        current_centralwidget_style = self.centralwidget.styleSheet()
        self.centralwidget.setStyleSheet(current_centralwidget_style + "\n" + controls_style_str)

        # Apply styles to components
        self.title_label.setStyleSheet(title_style)
        self.subtitle_label.setStyleSheet(subtitle_style)
        self.pushButton.setStyleSheet(button_style)
        self.pushButton2.setStyleSheet(button_style_back)
        self.pushButton3.setStyleSheet(button_style)

        # Apply table styles
        for table in [self.binary_table, self.categorical_table, self.scaling_table]:
            table.setStyleSheet(table_style)

    def _install_resize_event(self):
        """Install responsive resize event handler"""
        original_resize_event = self.MainWindow.resizeEvent

        def new_resize_event(event):
            new_title_size = int(self.MainWindow.height() * 0.06)
            new_subtitle_size = int(self.MainWindow.height() * 0.03)
            new_button_size = int(self.MainWindow.height() * 0.021)

            self.setup_styles(
                title_size=new_title_size,
                subtitle_size=new_subtitle_size,
                button_size=new_button_size,
            )

            original_resize_event(event)

        self.MainWindow.resizeEvent = new_resize_event

    def retranslateUi(self, MainWindow):
        """Set window title"""
        MainWindow.setWindowTitle(QCoreApplication.translate("MainWindow", u"Dataset Preprocessor", None))