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
from pyspark.sql.functions import monotonically_increasing_id
from src.utils import *


def resource_path(relative_path):
    """ Get the absolute path to a resource, works for dev and packaged apps. """
    if getattr(sys, 'frozen', False):  
        base_path = sys._MEIPASS
    else: 
        base_path = os.path.abspath(".")
    
    return os.path.join(base_path, relative_path)


class Ui_cleaning_phase(object):
    
    #null_percentages = None


    def setupUi(self, MainWindow):
        self.MainWindow = MainWindow
        if not MainWindow.objectName():
            MainWindow.setObjectName(u"MainWindow")
        MainWindow.resize(900, 600)
        MainWindow.setMinimumSize(600, 400)
        
        # Initialize cleaning_logic reference
        self.dataset_info = None
        
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
        self.sort_combo.addItems(["Original", "Rows", "Columns"])
        
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
        if not self.dataset_info:
            print("[INFO] No data available to sort")
            return
        sort_option = self.sort_combo.currentText()

        try:
            column_null_counts, row_null_counts = self.dataset_info.get_general_info().get("column_null_counts"), self.dataset_info.get_general_info().get("row_null_counts")
            
            if sort_option == "Columns":
                if self.dataset_info.get_dataframe_sorted_columns() is None:
                    sorted_columns = [row["column_name"] for row in 
                                    column_null_counts.orderBy("null_count", ascending=False).collect()]
                    
                    sorted_df = self.dataset_info.get_dataframe_original().select(sorted_columns)
                    self.dataset_info.set_dataframe_sorted_columns(sorted_df)
                    self.dataset_info.set_dataframe(self.dataset_info.get_dataframe_sorted_columns()) 
                    self.dataset_info.set_general_info() # Update general_info for the new active (column-sorted) dataframe
                    
                    self.utils.populate_table(50, 200, dataset_info=self.dataset_info, where_to_show="cleaning")
                
                else:
                    self.dataset_info.set_dataframe(self.dataset_info.get_dataframe_sorted_columns())
                    self.dataset_info.set_general_info() # Update general_info for the restored (column-sorted) dataframe
                    self.utils.populate_table(50, 200, dataset_info=self.dataset_info, where_to_show="cleaning")
                
            elif sort_option == "Rows":  # "Rows" option
                if self.dataset_info.get_dataframe_sorted_rows() is None:
                    try:
                        from pyspark.sql.window import Window
                        from pyspark.sql.functions import row_number, col as spark_col

                        
                        current_df_for_row_sorting = self.dataset_info.get_dataframe_original()
                        df_with_id = current_df_for_row_sorting.withColumn("_row_id", monotonically_increasing_id())
                        
                        original_active_df = self.dataset_info.get_dataframe()
                        self.dataset_info.set_dataframe(current_df_for_row_sorting)
                        self.dataset_info.set_general_info() # This will update general_info, including row_null_counts based on original
                        _, row_null_counts = self.dataset_info.get_general_info()["column_null_counts"], self.dataset_info.get_general_info()["row_null_counts"]

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
                        
                        self.dataset_info.set_dataframe_sorted_rows(sorted_df)
                        self.dataset_info.set_dataframe(self.dataset_info.get_dataframe_sorted_rows()) 
                        self.dataset_info.set_general_info() # Update general_info for the new active (row-sorted) dataframe

                        self.utils.populate_table(50, 200, dataset_info=self.dataset_info, where_to_show="cleaning")
                        
                    except Exception as e:
                        print(f"[ERROR] Row sorting failed with error: {e}")
                        import traceback
                        traceback.print_exc()
                        
                        # Fallback method if the above fails
                        print("[INFO] Using fallback method to sort rows")
                        if self.dataset_info.get_dataframe() is not None: # Ensure there's a dataframe
                           self.dataset_info.set_general_info() # Try to update general_info
                        self.utils.populate_table(50, 200, dataset_info=self.dataset_info, where_to_show="cleaning")
                else:
                    self.dataset_info.set_dataframe(self.dataset_info.get_dataframe_sorted_rows())
                    self.dataset_info.set_general_info() # Update general_info for the restored (row-sorted) dataframe
                    self.utils.populate_table(50, 200, dataset_info=self.dataset_info, where_to_show="cleaning")

            else: #Original dataframe
                self.dataset_info.set_dataframe(self.dataset_info.get_dataframe_original())
                self.dataset_info.set_general_info(self.dataset_info.get_general_info_original())
                self.utils.populate_table(50, 200, dataset_info=self.dataset_info, where_to_show="cleaning")

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
            QComboBox::drop-down {{
                subcontrol-origin: padding;
                subcontrol-position: top right;
                width: 20px;
                border-left: 1px solid #cccccc;
            }}
            QComboBox::down-arrow {{
                image: url(down_arrow.png);  /* opcional, si tienes un ícono */
            }}
            QComboBox QAbstractItemView {{
                background-color: white;
                color: black;
                selection-background-color: #e0e0e0;
                selection-color: black;
            }}
        """)


    def populate_table(self, dataframe):
        """Fill the table with data from a Spark DataFrame and highlight null values."""
        # This method in Ui_cleaning_phase seems to be a wrapper.
        # It should ideally pass dataset_info to the utils.populate_table
        # For now, assuming 'dataframe' is actually dataset_info based on main.py context
        if isinstance(dataframe, DatasetInfo): # Check if it's a DatasetInfo object
            self.utils.populate_table(50, 200, dataset_info=dataframe, where_to_show="cleaning")
        else:
            # Fallback or error handling if it's not a DatasetInfo object
            # This branch might indicate an issue in how populate_table is called from main.py for cleaning_ui
            print("[WARNING] populate_table in Ui_cleaning_phase received a DataFrame directly. Expected DatasetInfo.")
            # Attempt to use the existing self.dataset_info if available
            if self.dataset_info:
                 self.utils.populate_table(50, 200, dataset_info=self.dataset_info, where_to_show="cleaning")
            else:
                print("[ERROR] No DatasetInfo available in Ui_cleaning_phase to populate table.")

