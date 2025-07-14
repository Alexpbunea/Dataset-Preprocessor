# -*- coding: utf-8 -*-

import os
import sys
from PySide6.QtCore import QCoreApplication, Qt, QTimer
from PySide6.QtGui import QIcon, QPixmap
from PySide6.QtWidgets import (
    QMainWindow, QLabel, QPushButton, QSizePolicy, QVBoxLayout,
    QWidget, QHBoxLayout, QFileDialog, QTableWidget, QTableWidgetItem,
    QHeaderView, QAbstractItemView, QSpinBox, QDoubleSpinBox, QCheckBox
)
from src.utils import *


def resource_path(relative_path):
    """ Get the absolute path to a resource, works for dev and packaged apps. """
    if getattr(sys, 'frozen', False):  # If running as a PyInstaller bundle
        base_path = sys._MEIPASS
    else:  # If running in a normal Python environment
        base_path = os.path.abspath(".")
    return os.path.join(base_path, relative_path)


class Ui_data_phase(object):
    """
    UI for data splitting phase including:
    - Train/Validation/Test split configuration
    - Random seed and shuffle options
    - Split size validation and application
    """
    
    def __init__(self):
        """Initialize all UI components"""
        self.MainWindow = None
        self.dataset_info = None
        
        # UI Components
        self.centralwidget = None
        self.verticalLayout = None
        self.title_label = None
        self.subtitle_label = None
        self.status_label = None
        self.utils = None
        
        # Split configuration
        self.split_container = None
        self.split_table = None
        
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
        self._create_split_container()
        self._create_buttons()
        self._finalize_setup()

    def _configure_main_window(self):
        """Configure main window properties"""
        if not self.MainWindow.objectName():
            self.MainWindow.setObjectName(u"MainWindow")
        self.MainWindow.resize(900, 600)
        self.MainWindow.setMinimumSize(600, 400)
        self.dataset_info = None

    def _create_main_layout(self):
        """Create the main central widget and layout"""
        self.centralwidget = QWidget(self.MainWindow)
        self.centralwidget.setObjectName(u"centralwidget")

        self.verticalLayout = QVBoxLayout(self.centralwidget)
        self.verticalLayout.setContentsMargins(20, 20, 20, 20)
        self.verticalLayout.setSpacing(20)

        # Title and subtitle
        self.title_label = QLabel("Data splitting", self.centralwidget)
        self.subtitle_label = QLabel("Adjust split percentages and options", self.centralwidget)

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

        self.pushButton3.clicked.connect(self.apply_data_splitting)
        self.MainWindow.setCentralWidget(self.centralwidget)
        self._install_resize_event()
        self.populate_split_table()

    # === CONTAINER CREATION METHODS ===

    def _create_split_container(self):
        """Create container for split configuration options"""
        self.split_container = QWidget(self.centralwidget)
        split_layout = QVBoxLayout(self.split_container)

        split_label = QLabel("Configure data splitting parameters:", self.split_container)
        self.split_table = self._create_split_table()

        split_layout.addWidget(split_label)
        split_layout.addWidget(self.split_table)

    def _create_split_table(self):
        """Create the split configuration table"""
        table = QTableWidget(0, 3)
        table.setHorizontalHeaderLabels(["Parameter", "Value", "Description"])
        table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        table.verticalHeader().setVisible(False)
        table.setSelectionBehavior(QAbstractItemView.SelectRows)
        return table

    def _setup_widget_properties(self):
        """Configure widget properties and initial states"""
        self.title_label.setAlignment(Qt.AlignCenter)
        self.subtitle_label.setAlignment(Qt.AlignCenter)
        self.split_container.setVisible(True)
        
        # Configure button size policies
        for button in [self.pushButton, self.pushButton2, self.pushButton3]:
            button.setSizePolicy(QSizePolicy.Fixed, QSizePolicy.Fixed)

    def _setup_layout(self):
        """Arrange widgets in the main layout"""
        # Add title and subtitle
        self.verticalLayout.addWidget(self.title_label)
        self.verticalLayout.addWidget(self.subtitle_label)
        self.verticalLayout.addStretch(1)
        
        # Add split container
        self.verticalLayout.addWidget(self.split_container)
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

    # === TABLE POPULATION METHODS ===

    def populate_split_table(self):
        """Populate table with split configuration options"""
        self.split_table.setRowCount(0)
        
        parameters = self._get_split_parameters()
        for param_name, default_value, description in parameters:
            self._add_parameter_row(param_name, default_value, description)

    def _get_split_parameters(self):
        """Get the default split parameters configuration"""
        return [
            ("Train Size", 0.7, "Proportion of dataset for training (0.1 - 0.8)"),
            ("Validation Size", 0.2, "Proportion of dataset for validation (0.0 - 0.5)"),
            ("Test Size", 0.1, "Proportion of dataset for testing (0.1 - 0.5)"),
            ("Random Seed", 42, "Seed for reproducible splits"),
            ("Shuffle", True, "Shuffle data before splitting"),
            ("Stratify", True, "Maintain class distribution in splits")
        ]

    def _add_parameter_row(self, param_name, default_value, description):
        """Add a parameter row to the split table"""
        row = self.split_table.rowCount()
        self.split_table.insertRow(row)
        
        # Parameter name (non-editable)
        param_item = QTableWidgetItem(param_name)
        param_item.setFlags(param_item.flags() & ~Qt.ItemIsEditable)
        self.split_table.setItem(row, 0, param_item)
        
        # Description (non-editable)
        desc_item = QTableWidgetItem(description)
        desc_item.setFlags(desc_item.flags() & ~Qt.ItemIsEditable)
        self.split_table.setItem(row, 2, desc_item)
        
        # Value widget (editable)
        widget = self._create_parameter_widget(param_name, default_value)
        widget.setStyleSheet(self._widget_style())
        self.split_table.setCellWidget(row, 1, widget)

    def _create_parameter_widget(self, param_name, default_value):
        """Create appropriate widget based on parameter type"""
        if "Size" in param_name:
            widget = QDoubleSpinBox()
            widget.setRange(0.0, 0.8)
            widget.setSingleStep(0.05)
            widget.setValue(default_value)
            widget.setDecimals(2)
            return widget
        elif param_name == "Random Seed":
            widget = QSpinBox()
            widget.setRange(1, 9999)
            widget.setValue(default_value)
            return widget
        else:  # Boolean parameters
            widget = QCheckBox()
            widget.setChecked(default_value)
            return widget

    def get_split_config(self):
        """Extract configuration from the split table"""
        config = {}
        for row in range(self.split_table.rowCount()):
            param_name = self.split_table.item(row, 0).text()
            widget = self.split_table.cellWidget(row, 1)
            
            if widget:
                if isinstance(widget, QDoubleSpinBox):
                    config[param_name] = widget.value()
                elif isinstance(widget, QSpinBox):
                    config[param_name] = widget.value()
                elif isinstance(widget, QCheckBox):
                    config[param_name] = widget.isChecked()
        return config

    # === DATA SPLITTING METHODS ===

    def apply_data_splitting(self):
        """Apply the data splitting configuration"""
        try:
            # Validate dataset availability
            if not self._validate_dataset():
                return
            
            # Get and validate configuration
            config = self.get_split_config()
            if not self._validate_split_config(config):
                return
            
            # Apply splitting
            splits = self._perform_data_split(config)
            if splits:
                self._save_splits(splits)
                self._log_split_results(splits)
                self.show_status("Data splitting applied successfully!", "green")

        except Exception as e:
            print(f"[ERROR] -> Data splitting failed: {e}")
            self.show_status("Data splitting failed!", "red")

    def _validate_dataset(self):
        """Validate that dataset is available"""
        if self.dataset_info is None or not hasattr(self.dataset_info, "dataframe"):
            print("[ERROR] -> No dataset loaded!")
            self.show_status("No dataset loaded!", "red")
            return False
        return True

    def _validate_split_config(self, config):
        """Validate split configuration"""
        train_size = config.get("Train Size", 0)
        val_size = config.get("Validation Size", 0)
        test_size = config.get("Test Size", 0)
        
        total_size = train_size + val_size + test_size
        if abs(total_size - 1.0) > 0.01:
            self.show_status(f"Split sizes must sum to 1.0 (current: {total_size:.2f})", "red")
            return False
        return True

    def _perform_data_split(self, config):
        """Perform the actual data splitting"""
        df = self.dataset_info.dataframe
        
        train_size = config.get("Train Size", 0)
        val_size = config.get("Validation Size", 0)
        test_size = config.get("Test Size", 0)
        seed = config.get("Random Seed", 42)
        shuffle = config.get("Shuffle", True)

        if shuffle:
            splits = df.randomSplit([train_size, val_size, test_size], seed=seed)
        else:
            splits = self._manual_split(df, train_size, val_size, test_size)
        
        return splits

    def _manual_split(self, df, train_size, val_size, test_size):
        """Perform manual splitting without shuffle"""
        count = df.count()
        train_end = int(train_size * count)
        val_end = train_end + int(val_size * count)
        
        train_df = df.limit(train_end)
        val_df = df.subtract(train_df).limit(val_end - train_end)
        test_df = df.subtract(train_df).subtract(val_df)
        
        return [train_df, val_df, test_df]

    def _save_splits(self, splits):
        """Save splits to dataset info"""
        train_df, val_df, test_df = splits
        
        # Store splits in dataset info
        self.dataset_info.train_df = train_df
        self.dataset_info.val_df = val_df
        self.dataset_info.test_df = test_df

    def _log_split_results(self, splits):
        """Log the results of the splitting operation"""
        train_df, val_df, test_df = splits
        train_count = train_df.count()
        val_count = val_df.count()
        test_count = test_df.count()
        
        print(f"[INFO] -> Train rows: {train_count}, Validation rows: {val_count}, Test rows: {test_count}")

    # === UTILITY METHODS ===

    def show_status(self, message, color):
        """Display status message with specified color"""
        self.status_label.setText(message)
        self.status_label.setStyleSheet(f"color: {color}; font-weight: bold;")
        QTimer.singleShot(5000, lambda: self.status_label.setText(""))

    def set_dataset_info(self, dataset_info):
        """Initialize dataset info and populate table"""
        self.dataset_info = dataset_info
        self.populate_split_table()

    @staticmethod
    def _widget_style():
        """Return CSS style for widgets in tables"""
        return """
            QDoubleSpinBox, QSpinBox, QCheckBox {
                color: black;
                border: none;
                background-color: transparent;
                padding: 2px;
                margin: 0;
            }
            QCheckBox::indicator {
                width: 18px;
                height: 18px;
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
        self.split_table.setStyleSheet(table_style)

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