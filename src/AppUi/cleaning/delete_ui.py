# -*- coding: utf-8 -*-

import os
import sys
from PySide6.QtCore import QCoreApplication, Qt
from PySide6.QtGui import QIcon, QPixmap # QIcon, QPixmap not used in this specific UI class directly
from PySide6.QtWidgets import (
    QMainWindow, QLabel, QPushButton, QSizePolicy, QVBoxLayout, 
    QWidget, QHBoxLayout, QFileDialog, QTableWidget, QTableWidgetItem, # Added QTableWidgetItem
    QHeaderView, QScrollArea, QAbstractItemView, QLineEdit, QComboBox, QRadioButton, QGroupBox, QFormLayout # Added new widgets
)
from src.utils import *
from pyspark.sql.functions import col, when, sum as spark_sum




def resource_path(relative_path):
    """ Get the absolute path to a resource, works for dev and packaged apps. """
    if getattr(sys, 'frozen', False): 
        base_path = sys._MEIPASS
    else:  
        base_path = os.path.abspath(".")
    
    return os.path.join(base_path, relative_path)


class Ui_delete(object):
    def __init__(self):
        self.UI_cleaning_phase = None
        self.utils_cleaning_phase = None

    def set_utils_cleaning_phase(self, utils_cleaning_phase):
        self.utils_cleaning_phase = utils_cleaning_phase
    
    def get_utils_cleaning_phase(self):
        return self.utils_cleaning_phase
    
    def set_UI_cleaning_phase(self, UI_cleaning_phase):
        print("Holaaa")
        self.UI_cleaning_phase = UI_cleaning_phase
    
    def get_UI_cleaning_phase(self):
        return self.UI_cleaning_phase

    
    
    def setupUi(self, MainWindow):
        self.MainWindow = MainWindow
        if not MainWindow.objectName():
            MainWindow.setObjectName(u"MainWindow")
        MainWindow.resize(350, 500)
        MainWindow.setMinimumSize(350, 500)
        MainWindow.setMaximumSize(350, 500)
        

        self.cleaning_logic = None
        self.dataset_info = None

        self.centralwidget = QWidget(MainWindow)
        self.centralwidget.setObjectName(u"centralwidget")
        
        self.verticalLayout = QVBoxLayout(self.centralwidget)
        self.verticalLayout.setContentsMargins(20, 20, 20, 20)
        self.verticalLayout.setSpacing(20)

        # Header section
        self.title_label = QLabel("Delete", self.centralwidget)
        self.subtitle_label = QLabel("Select one of the following options", self.centralwidget)
        
        # --- Deletion Options --- 
        self.delete_by_name_radio = QRadioButton("Delete by Name (comma-separated if multiple):", self.centralwidget)
        self.delete_name_input = QLineEdit(self.centralwidget)
        self.delete_name_input.setPlaceholderText("e.g., column_name1,row_index1,column_name2")

        self.delete_by_null_radio = QRadioButton("Delete by Null Percentage:", self.centralwidget)
        self.null_options_group = QGroupBox(self.centralwidget) # No title for the groupbox itself
        self.null_options_layout = QHBoxLayout(self.null_options_group)
        
        self.delete_type_combo = QComboBox(self.null_options_group)
        self.delete_type_combo.addItems(["Columns", "Rows"])
        
        self.null_percentage_label_prefix = QLabel("with more than", self.null_options_group)
        self.null_percentage_input = QLineEdit(self.null_options_group)
        self.null_percentage_input.setPlaceholderText("e.g., 50")
        self.null_percentage_label_suffix = QLabel("% of nulls", self.null_options_group)

        # Add widgets to the null_options_layout
        self.null_options_layout.addWidget(self.delete_type_combo)
        self.null_options_layout.addWidget(self.null_percentage_label_prefix)
        self.null_options_layout.addWidget(self.null_percentage_input)
        self.null_options_layout.addWidget(self.null_percentage_label_suffix)
        self.null_options_layout.addStretch(1)
        
        # Navigation buttons
        self.pushButton = QPushButton("Delete", self.centralwidget)
        self.pushButton2 = QPushButton("Back", self.centralwidget)

        self._setup_widget_properties()
        self._setup_layout()
        
        self.utils = Utils(None, self.centralwidget, self.title_label, self.subtitle_label, self.pushButton, self.pushButton2, None, None, None)
        self.setup_styles()

        self.pushButton.clicked.connect(self.deleting)
        
        MainWindow.setCentralWidget(self.centralwidget)


    def _setup_widget_properties(self):
        # Header properties
        self.title_label.setAlignment(Qt.AlignCenter)
        self.subtitle_label.setAlignment(Qt.AlignCenter)
        
        # Deletion options properties
        self.delete_by_name_radio.setChecked(True) # Default selection
        self.delete_name_input.setEnabled(True)
        self.null_options_group.setEnabled(False) # Disabled by default

        # Connect radio buttons to toggle enabled state of inputs
        self.delete_by_name_radio.toggled.connect(self.toggle_delete_options)
        self.delete_by_null_radio.toggled.connect(self.toggle_delete_options)
        
        # Button properties
        self.pushButton.setSizePolicy(QSizePolicy.Fixed, QSizePolicy.Fixed)
        self.pushButton2.setSizePolicy(QSizePolicy.Fixed, QSizePolicy.Fixed)

    def _setup_layout(self):
        # Add header
        self.verticalLayout.addWidget(self.title_label)
        self.verticalLayout.addWidget(self.subtitle_label)
        self.verticalLayout.addSpacing(15) # Add some space

        # Add Deletion by Name option
        self.verticalLayout.addWidget(self.delete_by_name_radio)
        self.verticalLayout.addWidget(self.delete_name_input)
        self.verticalLayout.addSpacing(10)

        # Add Deletion by Null Percentage option
        self.verticalLayout.addWidget(self.delete_by_null_radio)
        self.verticalLayout.addWidget(self.null_options_group)

        self.verticalLayout.addStretch(1)
        
        # Add navigation buttons
        button_container = QHBoxLayout()
        button_container.setContentsMargins(0, 0, 0, 0)
        button_container.setSpacing(10)
        
        button_container.addStretch()
        button_container.addWidget(self.pushButton2)
        button_container.addWidget(self.pushButton)
        
        self.verticalLayout.addLayout(button_container)
    


    
    def retranslateUi(self, MainWindow):
        MainWindow.setWindowTitle(QCoreApplication.translate("MainWindow", u"Dataset Preprocessor - Preview Phase", None))
        self.title_label.setText(QCoreApplication.translate("MainWindow", u"Preview", None))
        self.subtitle_label.setText(QCoreApplication.translate("MainWindow", u"Preview the original dataset", None))
        self.pushButton.setText(QCoreApplication.translate("MainWindow", u"Continue", None))
        self.pushButton2.setText(QCoreApplication.translate("MainWindow", u"Back", None))


    def toggle_delete_options(self):
        is_name_delete = self.delete_by_name_radio.isChecked()
        self.delete_name_input.setEnabled(is_name_delete)
        self.null_options_group.setEnabled(not is_name_delete)




    def deleting(self):
        if self.delete_name_input.text() != "" and self.delete_by_name_radio.isChecked():
            try:
                text = self.delete_name_input.text()
                text = text.replace(" ", "").split(",")
                print(f"[INFO] Deleting columns and/or rows by name or index: {text}")

                column_names = self.dataset_info.get_general_info()["column_names"]
                
                column_names_to_delete = []
                index_rows_to_delete = []
                for i in text:
                    if i in column_names:
                        column_names_to_delete.append(i)
                    else:
                        index_rows_to_delete.append(i)

                self.cleaning_logic.set_columns_to_drop(column_names_to_delete)
                self.cleaning_logic.set_rows_to_drop(index_rows_to_delete)
                
                dataframe = self.dataset_info.get_dataframe()
                if dataframe is not None:
                    new_dataframe = self.cleaning_logic.drop_data(dataframe)
                self.dataset_info.set_dataframe(new_dataframe)
                self.dataset_info.set_general_info()

                
                #deleting the memorized sorted dataframes in the dataset_info file because they are not valid anymore. They will be recalculated in the cleaning phase.
                self.dataset_info.set_dataframe_sorted_columns(self.dataset_info.get_dataframe())
                self.dataset_info.set_dataframe_sorted_rows(None)
                    
                
                if self.get_utils_cleaning_phase():
                    self.utils_cleaning_phase.populate_table(50, 200, dataset_info=self.dataset_info, where_to_show="cleaning")
                print(f"[SUCCESS] Deleted columns and/or rows by name or index: {text}")

            except Exception as e:
                print(f"[ERROR] -> [When trying to delete columns and/or rows by name or index] {e}")
        
        
        elif self.null_percentage_input.text() != "" and self.delete_by_null_radio.isChecked():
            
            if self.delete_type_combo.currentText() == "Rows":    
                try:
                    print(f"[INFO] Deleting rows with null % > {threshold}")
                    threshold_str = self.null_percentage_input.text().replace(",", ".")
                    threshold = float(threshold_str)
                    if not (0 <= threshold <= 100):
                        raise ValueError("Please enter a percentage between 0 and 100.")

                    dataframe = self.dataset_info.get_dataframe()
                    columns = dataframe.columns
                    n_cols = len(columns)

                    null_exprs = [when(col(c).isNull(), 1).otherwise(0) for c in columns]
                    df_with_nulls = dataframe.withColumn("_null_count", sum(expr for expr in null_exprs))
                    df_with_nulls = df_with_nulls.withColumn("_null_pct", (col("_null_count") / n_cols) * 100)

                    new_dataframe = df_with_nulls.filter(col("_null_pct") <= threshold).drop("_null_count", "_null_pct")

                    self.dataset_info.set_dataframe(new_dataframe)
                    self.dataset_info.set_general_info()

                    self.dataset_info.set_dataframe_sorted_columns(self.dataset_info.get_dataframe())
                    self.dataset_info.set_dataframe_sorted_rows(None)
                    
                    if self.get_utils_cleaning_phase():
                        self.utils_cleaning_phase.populate_table(50, 200, dataset_info=self.dataset_info, where_to_show="cleaning")

                    print(f"[SUCCESS] Deleted rows with null % > {threshold}")

                except Exception as e:
                    print(f"[ERROR] -> [When trying to delete Rows by null percentage] {e}")
            
            elif self.delete_type_combo.currentText() == "Columns":
                try:
                    threshold_str = self.null_percentage_input.text().replace(",", ".")
                    threshold = float(threshold_str)
                    if not (0 <= threshold <= 100):
                        raise ValueError("Please enter a percentage between 0 and 100.")
                        
                    null_percentages = self.dataset_info.get_null_percentages()
                    columns_to_delete = [col for col, perc in null_percentages.items() if perc > threshold]

                    if not columns_to_delete:
                        raise ValueError("No columns found with null percentage greater than the specified threshold.")
                    print(f"[INFO] Deleting columns with null % > {threshold}: {columns_to_delete}")

                    # Drop columns using your cleaning logic
                    self.cleaning_logic.set_columns_to_drop(columns_to_delete)
                    
                    dataframe = self.dataset_info.get_dataframe()
                    if dataframe is not None:
                        new_dataframe = self.cleaning_logic.drop_data(dataframe)
                    self.dataset_info.set_dataframe(new_dataframe)
                    self.dataset_info.set_general_info()

                    self.dataset_info.set_dataframe_sorted_columns(self.dataset_info.get_dataframe())
                    self.dataset_info.set_dataframe_sorted_rows(None)
                    
                    if self.get_utils_cleaning_phase():
                        self.utils_cleaning_phase.populate_table(50, 200, dataset_info=self.dataset_info, where_to_show="cleaning")
                    print(f"[SUCCESS] Deleted columns with null % > {threshold}: {columns_to_delete}")


                except Exception as e:
                    print(f"[ERROR] -> [When trying to delete Columns by null percentage] {e}")


        else:
            print("[ERROR] No valid input provided for deletion. Please enter valid column names or row indices.")







    """

    FUNCTIONS FOUND IN THE UTILS.PY FILE

    """

    def setup_styles(self, title_size=36, subtitle_size=18, button_size=10, base_font_size=14): 
        # Fetch all style strings from the utility function
        # self.utils.setup_styles modifies self.utils.centralwidget directly for its base style.
        # It also might try to style self.utils.scroll_area which is None for Ui_delete, but that's handled by a check in Utils.
        _, title_style, subtitle_style, button_style, button_style_back, _, _, controls_style_str = self.utils.setup_styles(
            title_size=title_size, 
            subtitle_size=subtitle_size, 
            button_size=button_size,
            base_font_size=12 # Provide a default base_font_size, adjust if needed for delete_ui
        )
        
        # Get the base style already applied to centralwidget by self.utils.setup_styles
        current_centralwidget_style = self.centralwidget.styleSheet()
        
        # Append the controls_style to the centralwidget's stylesheet
        # This allows the specific control styles to apply to children
        self.centralwidget.setStyleSheet(current_centralwidget_style + "\n" + controls_style_str)

        # Apply specific styles to title, subtitle, and buttons
        self.title_label.setStyleSheet(title_style)
        self.subtitle_label.setStyleSheet(subtitle_style)
        self.pushButton.setStyleSheet(button_style)
        self.pushButton2.setStyleSheet(button_style_back)

        self.delete_type_combo.setStyleSheet(f"""
            QComboBox {{
                color: black;
                background-color: white;
                border: 1px solid #cccccc;
                border-radius: 3px;
                padding: 5px;
                
            }}
            QComboBox::drop-down {{
                subcontrol-origin: padding;
                subcontrol-position: top right;
            
                border-left: 1px solid #cccccc;
            }}
            
            QComboBox QAbstractItemView {{
                background-color: white;
                color: black;
                selection-background-color: #e0e0e0;
                selection-color: black;
            }}
        """)
        

