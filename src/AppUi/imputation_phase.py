# -*- coding: utf-8 -*-

import os
import sys
from PySide6.QtCore import QCoreApplication, Qt
from PySide6.QtGui import QIcon, QPixmap # QIcon, QPixmap not used in this specific UI class directly
from PySide6.QtWidgets import (
    QMainWindow, QLabel, QPushButton, QSizePolicy, QVBoxLayout, 
    QWidget, QHBoxLayout, QFileDialog, QTableWidget, QTableWidgetItem, # Added QTableWidgetItem
    QHeaderView, QScrollArea, QAbstractItemView, QLineEdit, QComboBox, QRadioButton, QGroupBox, QFormLayout, QMessageBox # Added new widgets
)
from src.utils import *
from pyspark.sql.functions import monotonically_increasing_id, col as spark_col
from pyspark.sql.window import Window
from pyspark.sql.functions import col, sum, when, isnan
from functools import reduce

def resource_path(relative_path):
    """ Get the absolute path to a resource, works for dev and packaged apps. """
    if getattr(sys, 'frozen', False):  # If running as a PyInstaller bundle
        base_path = sys._MEIPASS
    else:  # If running in a normal Python environment
        base_path = os.path.abspath(".")
    
    return os.path.join(base_path, relative_path)


class Ui_imputation_phase(object):
    def setupUi(self, MainWindow):
        self.MainWindow = MainWindow
        if not MainWindow.objectName():
            MainWindow.setObjectName(u"MainWindow")
        MainWindow.resize(900, 600)
        MainWindow.setMinimumSize(600, 400)


        self.cleaning_logic = None
        self.dataset_info = None
        
        self.centralwidget = QWidget(MainWindow)
        self.centralwidget.setObjectName(u"centralwidget")
        
        self.verticalLayout = QVBoxLayout(self.centralwidget)
        self.verticalLayout.setContentsMargins(20, 20, 20, 20)
        self.verticalLayout.setSpacing(20)

        self.title_label = QLabel("Imputation", self.centralwidget)
        self.subtitle_label = QLabel("Substituting missing values in your dataset with reasonable estimations", self.centralwidget)
        


        self.spark_calculations = QRadioButton("Spark methods:", self.centralwidget)


        self.machine_learning = QRadioButton("Artificial intelligence:", self.centralwidget)


        self.spark_calculations.toggled.connect(self.toggle_imputation_options)
        self.machine_learning.toggled.connect(self.toggle_imputation_options)

        self.spark_methods_container = QWidget(self.centralwidget)
        self.spark_methods_layout = QVBoxLayout(self.spark_methods_container)

        self.columns_label = QLabel("Select columns and imputation methods:", self.spark_methods_container)

        self.methods_table = QTableWidget(0, 3)
        self.methods_table.setHorizontalHeaderLabels(["Column", "Data Type", "Imputation Method"])
        self.methods_table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        self.methods_table.verticalHeader().setVisible(False)
        self.methods_table.setSelectionBehavior(QAbstractItemView.SelectRows)

        self.spark_methods_layout.addWidget(self.columns_label)
        self.spark_methods_layout.addWidget(self.methods_table)

        self.verticalLayout.insertWidget(4, self.spark_methods_container)  # Justo después de los radio buttons

        self.toggle_imputation_options()

        
        self.pushButton = QPushButton("Continue", self.centralwidget)
        self.pushButton2 = QPushButton("Back", self.centralwidget)
        self.pushButton3 = QPushButton("Apply", self.centralwidget)

        self._setup_widget_properties()
        self._setup_layout()

        self.utils = Utils(None, self.centralwidget, self.title_label, self.subtitle_label, self.pushButton, self.pushButton2, self.pushButton3, None, None)
        self.setup_styles()

        self.pushButton3.clicked.connect(self.apply_imputation_methods)
        
        MainWindow.setCentralWidget(self.centralwidget)
        self._install_resize_event()

    def _setup_widget_properties(self):
        # Header properties
        self.title_label.setAlignment(Qt.AlignCenter)
        self.subtitle_label.setAlignment(Qt.AlignCenter)
        

        self.spark_calculations.setChecked(True)  # Default option
        self.machine_learning.setChecked(False)
        
        
        # Button properties
        self.pushButton.setSizePolicy(QSizePolicy.Fixed, QSizePolicy.Fixed)
        self.pushButton2.setSizePolicy(QSizePolicy.Fixed, QSizePolicy.Fixed)
        self.pushButton3.setSizePolicy(QSizePolicy.Fixed, QSizePolicy.Fixed)

    def _setup_layout(self):
        self.verticalLayout.addWidget(self.title_label)
        self.verticalLayout.addWidget(self.subtitle_label)
        
        self.verticalLayout.addStretch(1)  # Este stretch empujará todo hacia arriba
        #self.verticalLayout.addSpacing(1)

        self.verticalLayout.addWidget(self.spark_calculations)
        self.verticalLayout.addWidget(self.machine_learning)
        self.verticalLayout.addWidget(self.spark_methods_container)

        self.verticalLayout.addStretch(1)
        
        
        # Crear un layout horizontal para los botones
        button_container = QHBoxLayout()
        button_container.setContentsMargins(0, 0, 0, 0)
        button_container.setSpacing(10)  # Espaciado pequeño entre botones
        
        # Añadir stretch para empujar los botones a la derecha
        button_container.addStretch()
        
        # Añadir botones en orden inverso (Back primero, luego Continue)
        button_container.addWidget(self.pushButton3)
        button_container.addWidget(self.pushButton2)
        button_container.addWidget(self.pushButton)
        
        # Añadir el contenedor de botones al layout principal
        self.verticalLayout.addLayout(button_container)
    



    def toggle_imputation_options(self):
        show_spark = self.spark_calculations.isChecked()
        self.spark_methods_container.setVisible(show_spark)

        if show_spark and self.dataset_info and self.methods_table.rowCount() == 0:
            self.populate_methods_table()
        
    
    def populate_methods_table(self):
        self.methods_table.setRowCount(0)

        columns = self.dataset_info.get_general_info()['column_names']
        dtypes_list = self.dataset_info.get_dataframe().dtypes
        dtypes = dict(dtypes_list)

        # Set uniform row heights
        self.methods_table.verticalHeader().setDefaultSectionSize(40)
        
        # Set header resize modes
        self.methods_table.horizontalHeader().setSectionResizeMode(0, QHeaderView.Stretch)
        self.methods_table.horizontalHeader().setSectionResizeMode(1, QHeaderView.ResizeToContents)
        self.methods_table.horizontalHeader().setSectionResizeMode(2, QHeaderView.Stretch)

        for col in columns:
            row = self.methods_table.rowCount()
            self.methods_table.insertRow(row)

            col_item = QTableWidgetItem(col)
            col_item.setFlags(col_item.flags() & ~Qt.ItemIsEditable)
            self.methods_table.setItem(row, 0, col_item)

            dtype = dtypes.get(col, 'unknown')
            dtype_item = QTableWidgetItem(dtype)
            dtype_item.setFlags(dtype_item.flags() & ~Qt.ItemIsEditable)
            self.methods_table.setItem(row, 1, dtype_item)

            method_combo = QComboBox()
            
            # Set combo box to fill the cell
            method_combo.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
            
            # Simplified styling that matches table cells
            method_combo.setStyleSheet(f"""
                QComboBox {{
                    color: black;
                    border: none;
                    background-color: transparent;
                    padding: 0;
                    margin: 0;
                }}
                QComboBox::drop-down {{
                    subcontrol-origin: padding;
                    subcontrol-position: center right;
                    width: 20px;
                    border: none;
                }}
                QComboBox QAbstractItemView {{
                    border: 1px solid #cccccc;
                    color: black;
                }}
            """)

            if 'int' in dtype or 'float' in dtype or 'double' in dtype:
                method_combo.addItems(["None", "Mean", "Median", "Mode"])
            else:
                method_combo.addItems(["None", "Mode"])
                
            # Set combo box in cell
            #self.methods_table.setCellWidget(row, 2, method_combo)
            
            # Ensure combo box fills the cell
            self.methods_table.setCellWidget(row, 2, method_combo)
            method_combo.setMinimumHeight(40)
    
    def set_dataset_info(self, dataset_info):
        self.dataset_info = dataset_info

        if self.spark_calculations.isChecked():
            self.populate_methods_table()

    def get_selected_methods(self):
        methods = {}
        for row in range(self.methods_table.rowCount()):
            col = self.methods_table.item(row, 0).text()
            method_combo = self.methods_table.cellWidget(row, 2)
            methods[col] = method_combo.currentText().lower()
        return methods


    
    
    def apply_imputation_methods(self):
        """Apply the selected imputation methods to the dataset"""
        try:
            
            print(f"[INFO] -> [Trying to substitute null values with the Spark methods]")
            # Get the selected methods
            selected_methods = self.get_selected_methods()
            
            # Get the current DataFrame
            df = self.dataset_info.get_dataframe()
            
            # Apply each method to its column
            for column, method in selected_methods.items():
                if method == "mean":
                    # Calculate mean
                    mean_val = df.select(F.mean(F.col(column))).collect()[0][0]
                    df = df.na.fill({column: mean_val})
                    
                elif method == "median":
                    # Calculate median
                    quantiles = df.approxQuantile(column, [0.5], 0.0)
                    median_val = quantiles[0] if quantiles else None
                    if median_val is not None:
                        df = df.na.fill({column: median_val})
                    
                elif method == "mode":
                    # Calculate mode
                    mode_df = (df.groupBy(column)
                               .count()
                               .orderBy(F.desc("count"))
                               .limit(1))
                    mode_row = mode_df.collect()
                    mode_val = mode_row[0][0] if mode_row else None
                    if mode_val is not None:
                        df = df.na.fill({column: mode_val})
            
            
            df.write.mode("overwrite").option("header", True).csv("./imputed_dataset.csv")  # Save to CSV for debugging
            self.dataset_info.set_dataframe(df)
            
            print(f"[SUCCESS] -> [Substituted null values with the Spark methods]")
            
            # Refresh the table to show updated data
            if self.spark_calculations.isChecked():
                self.populate_methods_table()
                
        except Exception as e:
            # Show error message
            print(f"[ERROR] -> [When trying to substitute null values with the Spark methods] {e}")










    def setup_styles(self, title_size=36, subtitle_size=18, button_size=4):
        self.centralwidget, title_style, subtitle_style, button_style, button_style_back, table_style, _, controls_style_str = self.utils.setup_styles(title_size, subtitle_size, button_size)

        current_centralwidget_style = self.centralwidget.styleSheet()
        
        # Append the controls_style to the centralwidget's stylesheet
        # This allows the specific control styles to apply to children
        self.centralwidget.setStyleSheet(current_centralwidget_style + "\n" + controls_style_str)

        self.title_label.setStyleSheet(title_style)
        self.subtitle_label.setStyleSheet(subtitle_style)
        self.pushButton.setStyleSheet(button_style)
        self.pushButton2.setStyleSheet(button_style_back)
        self.pushButton3.setStyleSheet(button_style)
        self.methods_table.setStyleSheet(table_style)
        # table_style = f"""
        #     QTableWidget {{
        #         font-size: {max(10, int(button_size * 0.8))}px;
        #         gridline-color: #d0d0d0;
        #         background-color: white;
        #     }}
        #     QHeaderView::section {{
        #         background-color: #0078d7;
        #         color: white;
        #         padding: 4px;
        #         font-weight: bold;
        #     }}
        # """
        label_style = f"""
            QLabel {{
                color: #333333;
                font-size: {max(12, int(button_size * 0.9))}px;
            }}
        """

        self.columns_label.setStyleSheet(label_style)
        #self.methods_table.setStyleSheet(table_style)



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