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
    if getattr(sys, 'frozen', False):  
        base_path = sys._MEIPASS
    else: 
        base_path = os.path.abspath(".")
    
    return os.path.join(base_path, relative_path)


class Ui_cleaning_phase(object):
    def setupUi(self, MainWindow):
        self.MainWindow = MainWindow
        if not MainWindow.objectName():
            MainWindow.setObjectName(u"MainWindow")
        MainWindow.resize(900, 600)
        MainWindow.setMinimumSize(600, 400)
        
        # Initialize cleaning_logic reference
        self.cleaning_logic = None
        
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
        
        # Sort by label and combo box
        self.sort_label = QLabel("Sort by:", self.centralwidget)
        self.sort_combo = QComboBox(self.centralwidget)
        self.sort_combo.addItems(["Columns", "Rows"])
        
        # Navigation buttons
        self.pushButton = QPushButton("Continue", self.centralwidget)
        self.pushButton2 = QPushButton("Back", self.centralwidget)
        self.pushButton3 = QPushButton("Delete", self.centralwidget)

        self._setup_widget_properties()
        self._setup_layout()
        
        self.utils = Utils(None, self.centralwidget, self.title_label, self.subtitle_label, self.pushButton, self.pushButton2, self.pushButton3, self.table, self.scroll_area)
        self.setup_styles()
        
        # Connect sort_combo change event to sort_table_by_nulls function
        self.sort_combo.currentIndexChanged.connect(self.sort_table_by_nulls)
        
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
        
        # Sort by properties
        self.sort_label.setSizePolicy(QSizePolicy.Fixed, QSizePolicy.Fixed)
        self.sort_combo.setSizePolicy(QSizePolicy.Fixed, QSizePolicy.Fixed)
        self.sort_combo.setMinimumWidth(120)
        
        # Button properties
        self.pushButton.setSizePolicy(QSizePolicy.Fixed, QSizePolicy.Fixed)
        self.pushButton2.setSizePolicy(QSizePolicy.Fixed, QSizePolicy.Fixed)
        self.pushButton3.setSizePolicy(QSizePolicy.Fixed, QSizePolicy.Fixed)

    def _setup_layout(self):
        # Add header
        self.verticalLayout.addWidget(self.title_label)
        self.verticalLayout.addWidget(self.subtitle_label)
        
        # Add table
        self.scroll_area.setWidget(self.table)
        self.verticalLayout.addWidget(self.scroll_area)
        
        # Add bottom controls and navigation in one row
        button_container = QHBoxLayout()
        button_container.setContentsMargins(0, 0, 0, 0)
        button_container.setSpacing(10)
        
        # Add sort controls on the left
        button_container.addWidget(self.sort_label)
        button_container.addWidget(self.sort_combo)
        
        # Add stretch to push buttons to the right
        button_container.addStretch()
        
        # Add navigation buttons on the right
        button_container.addWidget(self.pushButton3)
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


    def sort_table_by_nulls(self):
        """
        Sort the table based on the number of nulls and the selected sort option.
        If 'Columns' is selected, sort from left to right (reorder columns).
        If 'Rows' is selected, sort from top to bottom (reorder rows).
        """
        if not hasattr(self.utils, 'dataframe') or not self.cleaning_logic:
            print("[INFO] No data available to sort")
            return
            
        # Get the sort option
        sort_option = self.sort_combo.currentText()
        
        try:
            # Calculate null counts using the cleaning class
            column_null_counts, row_null_counts = self.cleaning_logic.calculate_nulls()
            
            # Convert to Python format for easier sorting
            if sort_option == "Columns":
                # Sort columns by null counts (left to right = highest to lowest nulls)
                column_nulls_dict = {}
                column_data = column_null_counts.collect()[0]
                
                for col in self.utils.dataframe.columns:
                    column_nulls_dict[col] = column_data[col]
                
                # Sort columns by number of nulls (descending)
                sorted_columns = sorted(column_nulls_dict.items(), key=lambda x: x[1], reverse=True)
                
                # Get the sorted column names
                sorted_column_names = [col[0] for col in sorted_columns]
                
                # Create a new dataframe with reordered columns - only using select to avoid memory issues
                sorted_df = self.utils.dataframe.select(sorted_column_names)
                
                # Update the dataframe reference first, then populate
                #self.utils.dataframe = sorted_df
                self.utils.populate_table(50, 200, sorted_df)
                
            else:  # "Rows" option
                # Get row indices sorted by null counts (most nulls first)
                try:
                    # More memory-efficient approach: Add row numbers to the dataframe with null counts
                    from pyspark.sql.window import Window
                    from pyspark.sql.functions import row_number, col as spark_col

                    # Join original dataframe with null counts
                    # First create a monotonically increasing ID in both dataframes
                    from pyspark.sql.functions import monotonically_increasing_id
                    df_with_id = self.utils.dataframe.withColumn("_row_id", monotonically_increasing_id())
                    null_counts_with_id = row_null_counts.withColumn("_row_id", monotonically_increasing_id())
                    
                    # Join the dataframes
                    combined_df = df_with_id.join(
                        null_counts_with_id, 
                        on="_row_id"
                    )
                    
                    # Sort by null counts
                    sorted_df = combined_df.orderBy(spark_col("null_count").desc())
                    
                    # Drop the temporary columns
                    sorted_df = sorted_df.drop("_row_id", "null_count")
                    
                    # Update reference and populate table
                    #self.utils.dataframe = sorted_df
                    self.utils.populate_table(50, 200, sorted_df)
                    
                except Exception as e:
                    print(f"[ERROR] Row sorting failed with error: {e}")
                    import traceback
                    traceback.print_exc()
                    
                    # Fallback method if the above fails
                    print("[INFO] Using fallback method to sort rows")
                    self.utils.populate_table(50, 100)
                
            print(f"[INFO] Table sorted by {sort_option} based on null counts")
                
        except Exception as e:
            print(f"[ERROR] Error sorting table: {e}")
            import traceback
            traceback.print_exc()


    """
    
    FUNCTIONS FOUND IN THE UTILS.PY FILE

    """

    def setup_styles(self, title_size=36, subtitle_size=18, button_size=14):
        self.centralwidget, title_style, subtitle_style, button_style, button_style_back, table_style, self.scroll_area, controls_style = self.utils.setup_styles(title_size, subtitle_size, button_size)

        self.title_label.setStyleSheet(title_style)
        self.subtitle_label.setStyleSheet(subtitle_style)
        self.pushButton.setStyleSheet(button_style)
        self.pushButton2.setStyleSheet(button_style_back)
        self.pushButton3.setStyleSheet(button_style)
        self.table.setStyleSheet(table_style)
        
        # Style for sort combo and label
        self.sort_label.setStyleSheet(f"""
            QLabel {{
                color: #333333;
                font-size: {button_size}px;
                font-weight: bold;
            }}
        """)
        
        self.sort_combo.setStyleSheet(f"""
            QComboBox {{
                color: black;
                background-color: white;
                border: 1px solid #cccccc;
                border-radius: 3px;
                padding: 5px;
                font-size: {button_size}px;
            }}
            QComboBox::item {{
                color: black;
                background-color: white;
            }}
            QComboBox::item:selected {{
                color: black;
                background-color: #e0e0e0;
            }}
        """)


    def populate_table(self, dataframe):
        """Fill the table with data from a Spark DataFrame and highlight null values."""
        self.utils.dataframe = dataframe
        self.utils.populate_table(50, 200)

