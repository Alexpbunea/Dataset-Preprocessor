# -*- coding: utf-8 -*-

import os
import sys
from PySide6.QtCore import QCoreApplication, Qt
from PySide6.QtGui import QIcon, QPixmap
from PySide6.QtWidgets import (
    QMainWindow, QLabel, QPushButton, QSizePolicy, 
    QVBoxLayout, QWidget, QHBoxLayout, QTableWidget,
    QTableWidgetItem, QScrollArea, QSpinBox, QComboBox,
    QHeaderView, QAbstractItemView, QGroupBox, QFormLayout
)
from src.utils import *


def resource_path(relative_path):
    """ Get the absolute path to a resource, works for dev and packaged apps. """
    if getattr(sys, 'frozen', False):  # If running as a PyInstaller bundle
        base_path = sys._MEIPASS
    else:  # If running in a normal Python environment
        base_path = os.path.abspath(".")
    
    return os.path.join(base_path, relative_path)


class Ui_cleaning_phase(object):
    def setupUi(self, MainWindow):
        self.MainWindow = MainWindow
        if not MainWindow.objectName():
            MainWindow.setObjectName(u"MainWindow")
        MainWindow.resize(900, 600)
        MainWindow.setMinimumSize(600, 400)
        
        self.centralwidget = QWidget(MainWindow)
        self.centralwidget.setObjectName(u"centralwidget")
        
        self.verticalLayout = QVBoxLayout(self.centralwidget)
        self.verticalLayout.setContentsMargins(20, 20, 20, 20)
        self.verticalLayout.setSpacing(20)

        # Header section
        self.title_label = QLabel("Cleaning", self.centralwidget)
        self.subtitle_label = QLabel("Drop columns or rows with several anomalies or nulls", self.centralwidget)
        
        # Table section
        self.scroll_area = QScrollArea()
        self.scroll_area.setWidgetResizable(True)
        self.table = QTableWidget()
        self.table.setObjectName(u"dataTable")
        
        # Controls section
        self.controls_group = QGroupBox("Null Value Filtering")
        self.controls_layout = QFormLayout(self.controls_group)
        self.controls_layout.setAlignment(Qt.AlignCenter)
        
        # Filter type selection
        self.filter_type = QComboBox()
        self.filter_type.addItems(["Columns", "Rows"])
        self.controls_layout.addRow("Filter:", self.filter_type)
        
        # Threshold settings
        self.threshold_spin = QSpinBox()
        self.threshold_spin.setRange(0, 100)
        self.threshold_spin.setValue(50)
        self.threshold_spin.setSuffix("%")
        self.controls_layout.addRow("Null Threshold:", self.threshold_spin)
        
        # Apply filter button in a centered container
        button_container = QHBoxLayout()
        button_container.addStretch()
        self.apply_filter = QPushButton("Apply Filter")
        self.apply_filter.setMinimumWidth(200)  # Make button wider
        button_container.addWidget(self.apply_filter)
        button_container.addStretch()
        
        # Add empty label and button container to form
        self.controls_layout.addRow("", button_container)
        
        # Navigation buttons
        self.pushButton = QPushButton("Continue", self.centralwidget)
        self.pushButton2 = QPushButton("Back", self.centralwidget)

        self._setup_widget_properties()
        self._setup_layout()
        
        self.utils = Utils(None, self.centralwidget, self.title_label, self.subtitle_label, self.pushButton, self.pushButton2, self.table, self.scroll_area)
        self.setup_styles()
        
        MainWindow.setCentralWidget(self.centralwidget)
        self._install_resize_event()

    def _setup_widget_properties(self):
        # Header properties
        self.title_label.setAlignment(Qt.AlignCenter)
        self.subtitle_label.setAlignment(Qt.AlignCenter)
        
        # Table properties
        self.table.setAlternatingRowColors(True)
        self.table.setEditTriggers(QTableWidget.NoEditTriggers)
        self.table.setSelectionBehavior(QTableWidget.SelectRows)
        self.table.setSelectionMode(QTableWidget.MultiSelection)
        self.table.verticalHeader().setVisible(True)
        self.table.horizontalHeader().setStretchLastSection(False)
        self.table.horizontalHeader().setSectionResizeMode(QHeaderView.Interactive)
        self.table.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOn)
        self.table.setVerticalScrollBarPolicy(Qt.ScrollBarAsNeeded)
        
        # Control properties
        self.controls_group.setMaximumWidth(300)
        self.pushButton.setSizePolicy(QSizePolicy.Fixed, QSizePolicy.Fixed)
        self.pushButton2.setSizePolicy(QSizePolicy.Fixed, QSizePolicy.Fixed)

    def _setup_layout(self):
        # Add header
        self.verticalLayout.addWidget(self.title_label)
        self.verticalLayout.addWidget(self.subtitle_label)
        
        # Add table
        self.scroll_area.setWidget(self.table)
        self.verticalLayout.addWidget(self.scroll_area)
        
        # Add controls
        self.verticalLayout.addWidget(self.controls_group)
        
        # Add navigation buttons
        button_container = QHBoxLayout()
        button_container.setContentsMargins(0, 0, 0, 0)
        button_container.setSpacing(10)
        button_container.addStretch()
        button_container.addWidget(self.pushButton2)
        button_container.addWidget(self.pushButton)
        self.verticalLayout.addLayout(button_container)

    

    def _install_resize_event(self):
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
        MainWindow.setWindowTitle(QCoreApplication.translate("MainWindow", u"Dataset Preprocessor", None))

    
        



    def calculate_null_percentages(self, dataframe):
        """Calculate the percentage of null values in each column and row."""
        if dataframe is None:
            return None, None

        # Calculate column null percentages
        col_nulls = {}
        total_rows = dataframe.count()
        for col in dataframe.columns:
            null_count = dataframe.filter(dataframe[col].isNull()).count()
            col_nulls[col] = (null_count / total_rows) * 100

        # Calculate row null percentages (for first 500 rows)
        row_nulls = []
        rows = dataframe.limit(500).collect()
        total_cols = len(dataframe.columns)
        
        for row in rows:
            null_count = sum(1 for value in row.values() if value is None)
            row_nulls.append((null_count / total_cols) * 100)

        return col_nulls, row_nulls

    def highlight_items_above_threshold(self, threshold):
        """Highlight items that exceed the null threshold."""
        if self.filter_type.currentText() == "Columns":
            # Highlight column headers
            for col in range(self.table.columnCount()):
                header = self.table.horizontalHeaderItem(col)
                if header and hasattr(header, 'null_percentage') and header.null_percentage >= threshold:
                    header.setBackground(Qt.red)
                    header.setForeground(Qt.white)
                else:
                    header.setBackground(Qt.white)
                    header.setForeground(Qt.black)
        else:
            # Highlight row headers
            for row in range(self.table.rowCount()):
                header = self.table.verticalHeaderItem(row)
                if header and hasattr(header, 'null_percentage') and header.null_percentage >= threshold:
                    header.setBackground(Qt.red)
                    header.setForeground(Qt.white)
                else:
                    header.setBackground(Qt.white)
                    header.setForeground(Qt.black)

    def setup_connections(self, dataframe):
        """Set up signal connections for interactive filtering."""
        def on_filter_changed():
            self.apply_null_filter(dataframe)

        self.threshold_spin.valueChanged.connect(on_filter_changed)
        self.filter_type.currentTextChanged.connect(on_filter_changed)
        self.apply_filter.clicked.connect(lambda: self.apply_null_filter(dataframe))

    def apply_null_filter(self, dataframe):
        """Apply the null filtering based on current settings."""
        if dataframe is None:
            return

        threshold = self.threshold_spin.value()
        filter_type = self.filter_type.currentText()
        
        # Get null percentages
        col_nulls, row_nulls = self.calculate_null_percentages(dataframe)
        
        if filter_type == "Columns":
            # Store null percentages in column headers
            for idx, (col, percentage) in enumerate(col_nulls.items()):
                header = QTableWidgetItem(col)
                header.null_percentage = percentage
                self.table.setHorizontalHeaderItem(idx, header)
        else:
            # Store null percentages in row headers
            for row, percentage in enumerate(row_nulls):
                header = QTableWidgetItem(str(row + 1))
                header.null_percentage = percentage
                self.table.setVerticalHeaderItem(row, header)
        
        # Highlight items above threshold
        self.highlight_items_above_threshold(threshold)









    """
    
    FUNCTIONS FOUND IN THE UTILS.PY FILE

    """

    def setup_styles(self, title_size=36, subtitle_size=18, button_size=14):
        self.centralwidget, title_style, subtitle_style, button_style, button_style_back, table_style, self.scroll_area, controls_style = self.utils.setup_styles(title_size, subtitle_size, button_size)

        self.title_label.setStyleSheet(title_style)
        self.subtitle_label.setStyleSheet(subtitle_style)
        self.pushButton.setStyleSheet(button_style)
        self.pushButton2.setStyleSheet(button_style_back)
        self.table.setStyleSheet(table_style)
        self.controls_group.setStyleSheet(controls_style)
        self.apply_filter.setStyleSheet(button_style)


    def populate_table(self, dataframe):
        """Fill the table with data from a Spark DataFrame and highlight null values."""
        self.utils.dataframe = dataframe
        self.utils.populate_table()

