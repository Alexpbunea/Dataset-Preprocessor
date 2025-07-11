# -*- coding: utf-8 -*-

#import os
#import sys
from PySide6.QtCore import QCoreApplication, Qt, QTimer
from PySide6.QtGui import QIcon, QPixmap
from PySide6.QtWidgets import (
    QMainWindow, QLabel, QPushButton, QSizePolicy, QVBoxLayout, 
    QWidget, QHBoxLayout, QFileDialog, QTableWidget, QTableWidgetItem, 
    QHeaderView, QScrollArea, QAbstractItemView, QLineEdit, QComboBox, QRadioButton, QGroupBox, QFormLayout, QMessageBox # Added new widgets
)
from src.utils import *
from src.logic.imputation_AI import ImputationAI
from pyspark.sql.functions import monotonically_increasing_id, col as spark_col
from pyspark.sql.window import Window
from pyspark.sql.functions import col, sum, when, isnan
from functools import reduce


class Ui_imputation_phase(object):
    def __init__(self):
        self.MainWindow = None
        self.cleaning_logic = None
        self.dataset_info = None
        self.centralwidget = None
        self.verticalLayout = None
        self.title_label = None
        self.subtitle_label = None
        self.spark_calculations = None
        self.spark_methods_container = None
        self.spark_methods_layout = None
        self.columns_label = None
        self.methods_table = None
        self.machine_learning = None
        self.ai_table_container = None
        self.ai_table_layout = None
        self.model_selection_container = None
        self.model_selection_layout = None
        self.model_label = None
        self.model_combo = None
        self.ai_table = None
        self.pushButton = None
        self.pushButton2 = None
        self.pushButton3 = None
        self.status_label = None
        self.utils = None




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

        #FOR THE SPARK OPTIONS
        self.spark_calculations = QRadioButton("Spark methods:", self.centralwidget)
        self.spark_calculations.toggled.connect(self.toggle_imputation_options)
        

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

        #FOR THE MACHINE LEARNING OPTIONS
        self.machine_learning = QRadioButton("Artificial intelligence:", self.centralwidget)
        self.machine_learning.toggled.connect(self.toggle_imputation_options)

        self.ai_table_container = QWidget(self.centralwidget)
        self.ai_table_layout = QVBoxLayout(self.ai_table_container)
        
        # Add model selection above the table
        self.model_selection_container = QWidget(self.ai_table_container)
        self.model_selection_layout = QHBoxLayout(self.model_selection_container)
        self.model_selection_layout.setContentsMargins(0, 0, 0, 10)  # Add some bottom margin
        
        self.model_label = QLabel("Select AI model:", self.model_selection_container)
        self.model_combo = QComboBox(self.model_selection_container)
        self.model_combo.addItems(["Linear Regression", "Random Forest", "Decision Tree"])
        
        self.model_selection_layout.addWidget(self.model_label)
        self.model_selection_layout.addWidget(self.model_combo)
        self.model_selection_layout.addStretch()  # Push to left
        
        self.ai_table_layout.addWidget(self.model_selection_container)
        
        # Create AI table with specified columns (removed Model column)
        self.ai_table = QTableWidget(0, 3)
        self.ai_table.setHorizontalHeaderLabels(["Column", "Use as feature (X)", "Use as target (Y)"])
        self.ai_table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        self.ai_table.verticalHeader().setVisible(False)
        self.ai_table.setSelectionBehavior(QAbstractItemView.SelectRows)
        
        self.ai_table_layout.addWidget(self.ai_table)
        
        #self.ai_table_layout.addWidget(self.ai_columns_label)
        #self.ai_table_layout.addWidget(self.ai_table)
        
        
        # Initially hide both tables
        #self.spark_methods_container.setVisible(False)
        #self.ai_table_container.setVisible(False)
        

        
        self.verticalLayout.insertWidget(0, self.spark_methods_container)  # Justo después de los radio buttons
        self.verticalLayout.insertWidget(1, self.ai_table_container)

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
        

        self.spark_calculations.setChecked(False)  # Default option
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
        self.verticalLayout.addWidget(self.ai_table_container)

        self.verticalLayout.addStretch(1)
        
        
        # Crear un layout horizontal para los botones
        button_container = QHBoxLayout()
        button_container.setContentsMargins(0, 0, 0, 0)
        button_container.setSpacing(10)  # Espaciado pequeño entre botones

        #layout
        self.status_label = QLabel("")
        self.status_label.setMinimumWidth(200)
        self.status_label.setAlignment(Qt.AlignVCenter | Qt.AlignLeft)
        button_container.addWidget(self.status_label)
        
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

        show_ai = self.machine_learning.isChecked()
        self.ai_table_container.setVisible(show_ai)

        if show_spark and self.dataset_info and self.methods_table.rowCount() == 0:
            self.populate_methods_table()
        
        if show_ai and self.dataset_info and self.ai_table.rowCount() == 0:
            self.populate_ai_table()
    
    def show_status(self, message, color):
        self.status_label.setText(message)
        self.status_label.setStyleSheet(f"color: {color}; font-weight: bold;")
        QTimer.singleShot(5000, lambda: self.status_label.setText(""))
        
    

#--------------------------------------------------------------------------------------------------------------------------------
    """
    FOR SPARK CALCULATIONS
    """
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
            method_combo.setStyleSheet(self._combo_style())

            if 'int' in dtype or 'float' in dtype or 'double' in dtype:
                method_combo.addItems(["None", "Mean", "Median", "Mode"])
            else:
                method_combo.addItems(["None", "Mode"])
                
            # Set combo box in cell
            #self.methods_table.setCellWidget(row, 2, method_combo)
            
            # Ensure combo box fills the cell
            self.methods_table.setCellWidget(row, 2, method_combo)
            method_combo.setMinimumHeight(40)

#---------------------------------------------------------------------------------------------------------------------------------
    """
    FOR AI CALCULATIONS
    """
    
    def populate_ai_table(self):
        """Populate the AI table with dataset columns and options"""
        self.ai_table.setRowCount(0)
        
        columns = self.dataset_info.get_general_info()['column_names']
        #dtypes_list = self.dataset_info.get_dataframe().dtypes
        #dtypes = dict(dtypes_list)

        
        # Set uniform row heights
        self.ai_table.verticalHeader().setDefaultSectionSize(40)
        
        for col in columns:
            row = self.ai_table.rowCount()
            self.ai_table.insertRow(row)
            
            # Column name (non-editable)
            col_item = QTableWidgetItem(col)
            col_item.setFlags(col_item.flags() & ~Qt.ItemIsEditable)
            self.ai_table.setItem(row, 0, col_item)
            
            # Use as feature (X) - Yes/No
            feature_combo = QComboBox()
            feature_combo.addItems(["No", "Yes"])
            feature_combo.setStyleSheet(self._combo_style())
            self.ai_table.setCellWidget(row, 1, feature_combo)
            
            # Use as target (Y) - Yes/No
            target_combo = QComboBox()
            target_combo.addItems(["No", "Yes"])
            target_combo.setStyleSheet(self._combo_style())
            self.ai_table.setCellWidget(row, 2, target_combo)

#--------------------------------------------------------------------------------------------------------------------------------

    def set_dataset_info(self, dataset_info):
        self.dataset_info = dataset_info

        #if self.spark_calculations.isChecked():
        #    self.populate_methods_table()

    def get_selected_methods(self):
        methods = {}
        for row in range(self.methods_table.rowCount()):
            col = self.methods_table.item(row, 0).text()
            method_combo = self.methods_table.cellWidget(row, 2)
            methods[col] = method_combo.currentText().lower()
        return methods

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


    
#--------------------------------------------------------------------------------------------------------------------------------
    """
    FOR APPLYING THE IMPUTATION METHODS
    """
    def apply_imputation_methods(self):
        """Apply the selected imputation methods to the dataset"""
        if self.spark_calculations.isChecked():
            try:
                
                print(f"[INFO] -> [Trying to substitute null values with the Spark methods]")
                # Get the selected methods
                selected_methods = self.get_selected_methods()
                
                # Get the current DataFrame
                df = self.dataset_info.get_dataframe()

                if all(method == "none" for method in selected_methods.values()):
                    self.show_status("No Spark imputation method selected: all options are 'None'.", "orange")
                    print("[ERROR] -> [No Spark imputation method selected: all options are 'None'.]")
                    return
                    
                
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
                
                
                #HADOOP !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
                #df.write.mode("overwrite").option("header", True).csv("./imputed_dataset.csv")  # Save to CSV for debugging
                print(df.head(10))
                self.dataset_info.set_dataframe(df)
                
                print(f"[SUCCESS] -> [Substituted null values with the Spark methods]")
                self.show_status("Imputation applied successfully!", "green")
                
                # Refresh the table to show updated data
                #if self.spark_calculations.isChecked():
                #    self.populate_methods_table()
                    
            except Exception as e:
                # Show error message
                print(f"[ERROR] -> [When trying to substitute null values with the Spark methods] {e}")
                self.show_status("Imputation failed!", "red")
        
        elif self.machine_learning.isChecked():
            try:
                print("[INFO] -> [Substituting null values with Artificial Intelligence]")
                
                # Get selected model
                selected_model = self.model_combo.currentText()
                print(f"[INFO] -> [Selected AI model] {selected_model}")
                
                # Get feature and target selections
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
                
                # Validate selections
                if len(feature_cols) == 0 or len(target_cols) == 0:
                    self.show_status("Please select at least one feature and one target column", "orange")
                    print("[ERROR] -> [Please select at least one feature and one target column]")
                    return
                
                if len(target_cols) > 1:
                    self.show_status("Only one target column can be selected", "red")
                    print("[ERROR] -> [Only one target column can be selected]")
                    return
                
                print(f"[INFO] -> [Feature columns] {feature_cols}")
                print(f"[INFO] -> [Target column] {target_cols[0]}")

                ai_imputer = ImputationAI(self.dataset_info)

                # Train the model
                if ai_imputer.train_model(feature_cols, target_cols[0], selected_model):
                    # Apply imputation
                    if ai_imputer.impute_missing_values():
                        self.show_status("AI imputation applied successfully!", "green")
                        print(f"[SUCCESS] -> [AI imputation applied with {selected_model}]")
                    else:
                        self.show_status("AI imputation failed during prediction!", "red")
                else:
                    self.show_status("AI model training failed!", "red")
                
            except Exception as e:
                print(f"[ERROR] -> [AI imputation failed] {e}")
                self.show_status("AI imputation failed!", "red")






    """

    FUNCTIONS FOUND IN THE UTILS.PY FILE

    """
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
        self.ai_table.setStyleSheet(table_style)

        label_style = f"""
            QLabel {{
                color: #333333;
                font-size: {max(12, int(button_size * 0.9))}px;
            }}
        """
        self.model_combo.setStyleSheet(f"""
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

        self.columns_label.setStyleSheet(label_style)
        #self.ai_columns_label.setStyleSheet(label_style)
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