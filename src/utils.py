# -*- coding: utf-8 -*-

import os
import sys
from PySide6.QtCore import QCoreApplication, Qt
from pyspark.sql import functions as F
from PySide6.QtGui import QColor
from PySide6.QtGui import QIcon, QPixmap # QIcon, QPixmap not used in this specific UI class directly
from PySide6.QtWidgets import (
    QMainWindow, QLabel, QPushButton, QSizePolicy, QVBoxLayout, 
    QWidget, QHBoxLayout, QFileDialog, QTableWidget, QTableWidgetItem, # Added QTableWidgetItem
    QHeaderView, QScrollArea, QAbstractItemView   # Added for table header styling/behavior
)
from src.logic.dataset_info import DatasetInfo

#global dataframe_original

class Utils:

    #spark = None
    #dataframe_original = None
    #dataframe_sorted_columns = None
    #dataframe_sorted_rows = None


    
    def __init__(self, dataframe = None, centralwidget = None, title = None, subtitle = None, pushButton = None, pushButton2 = None, pushButton3 = None, table = None, scroll_area = None, dataset_info=None):
        self.dataframe = dataframe
        self.dataset_info = dataset_info
        self.general_info = None
        self.centralwidget = centralwidget
        self.title_label = title
        self.subtitle_label = subtitle
        self.pushButton = pushButton
        self.pushButton2 = pushButton2
        self.pushButton3 = pushButton3
        self.table = table
        self.scroll_area = scroll_area
        self.max_cell_text_length = 0


    def populate_table(self, lenght=50, rows=500, dataset_info=None, where_to_show=None):
        null_percentages = 0

        if dataset_info is not None:
            self.dataset_info = dataset_info
            info = self.dataset_info.get_general_info()
            self.dataframe = self.dataset_info.get_dataframe()

            if self.dataframe is None or info.get("num_columns") == 0:
                self.table.setRowCount(0)
                self.table.setColumnCount(0)
                self.table.setHorizontalHeaderLabels(["No data to display"])
                return

        self.max_cell_text_length = lenght
        columns = None

        if self.dataset_info is not None:
            columns = info.get("column_names")

            if where_to_show == "preview":
                pass
            elif where_to_show == "cleaning":
                null_percentages = self.dataset_info.get_null_percentages()
                #from pyspark.sql.functions import monotonically_increasing_id, row_number, lit
                #from pyspark.sql.window import Window

                #df = self.dataframe.withColumn("tmp_id", monotonically_increasing_id())
                #window = Window.orderBy("tmp_id")
                #df = df.withColumn("row_index", row_number().over(window)).drop("tmp_id")
                #self.dataframe = df
            else:
                pass

        spark_rows = self.dataframe.take(rows)

        self.table.setUpdatesEnabled(False)
        self.table.horizontalHeader().setSectionResizeMode(QHeaderView.Interactive)

        try:
            display_rows = len(spark_rows) + (1 if null_percentages else 0)
            self.table.setRowCount(display_rows)
            self.table.setColumnCount(len(columns))
            self.table.setHorizontalHeaderLabels(columns)

            if null_percentages:
                for j, col in enumerate(columns):
                    percentage = null_percentages.get(col, 0.0)
                    percentage_item = QTableWidgetItem(f"{percentage:.1f}%")

                    if percentage > 50:
                        percentage_item.setBackground(QColor(255, 150, 150))
                    elif percentage > 20:
                        percentage_item.setBackground(QColor(255, 220, 150))

                    percentage_item.setToolTip(f"Null percentage: {percentage:.2f}%")
                    self.table.setItem(0, j, percentage_item)

                self.table.setVerticalHeaderItem(0, QTableWidgetItem("% Nulls"))

            offset = 1 if null_percentages else 0

            for i, row in enumerate(spark_rows):
                for j, col in enumerate(columns):
                    value = row[col]
                    str_value = str(value) if value is not None else "Null"

                    display_text = str_value
                    if len(str_value) > self.max_cell_text_length:
                        display_text = str_value[:self.max_cell_text_length] + "..."

                    item = QTableWidgetItem(display_text)

                    if len(str_value) > self.max_cell_text_length:
                        item.setToolTip(str_value)

                    self.table.setItem(i + offset, j, item)

            # Añadir encabezados verticales para filas de datos
            for i in range(len(spark_rows)):
                row_number = str(i + 1)
                row_position = i + offset
                self.table.setVerticalHeaderItem(row_position, QTableWidgetItem(row_number))

            total_width = sum(self.table.columnWidth(i) for i in range(self.table.columnCount()))
            self.table.setMinimumSize(total_width + 50, self.table.height())
            self.table.resizeColumnsToContents()

        finally:
            self.table.setUpdatesEnabled(True)
            self.table.horizontalHeader().setSectionResizeMode(QHeaderView.Interactive)

        self.table.setMinimumWidth(
            sum([self.table.columnWidth(i) for i in range(self.table.columnCount())]) + 50
        )

        return self.table, self.scroll_area
    
    

    def setup_styles(self, title_size=36, subtitle_size=18, button_size=14, base_font_size=12): 
        colors = {
            'background': '#f5f5f5',
            'title': '#333333',
            'subtitle': '#555555',
            'button': '#0078d7',
            'button_back': '#F7F7F7',
            'button_hover': '#005a9e',
            'text_white': 'white',
            "text_black": 'black',
            'border_gray': '#808080',
            'hover_gray': '#e0e0e0',
            'table_bg': 'white',
            'table_grid': '#d0d0d0',
            'table_header_bg': '#e0e0e0',
            'table_selection_bg': '#0078d7', 
            'table_selection_text': 'white',
            'text_primary': '#333333', 
            'table_grid': '#cccccc',
            'group_background': '#ffffff',
        }

        self.centralwidget.setStyleSheet(f"""
            QWidget {{
                background-color: {colors['background']};
            }}
        """)

        title_style = f"""
            QLabel {{
                color: {colors['title']};
                font-size: {title_size}px;
                font-weight: bold;
            }}
        """

        subtitle_style = f"""
            QLabel {{
                color: {colors['subtitle']};
                font-size: {subtitle_size}px;
            }}
        """

        # Adjusted button padding calculation
        button_padding_vertical = max(4, int(button_size * 0.5))
        button_padding_horizontal = max(10, int(button_size * 1.2))

        button_style = f"""
            QPushButton {{
                background-color: {colors['button']};
                color: {colors['text_white']};
                font-size: {button_size}px;
                font-weight: bold;
                border: none;
                border-radius: 5px;
                padding: {button_padding_vertical}px {button_padding_horizontal}px;
                min-width: 70px; 
            }}
            QPushButton:hover {{
                background-color: {colors['button_hover']};
            }}
        """

        button_style_back = f"""
            QPushButton {{
                background-color: {colors['button_back']};
                color: {colors['text_black']};
                font-size: {button_size}px;
                font-weight: bold;
                border: 1px solid {colors['border_gray']};
                border-radius: 5px;
                padding: {button_padding_vertical}px {button_padding_horizontal}px;
                min-width: 70px;
            }}
            QPushButton:hover {{
                background-color: {colors['hover_gray']};
                border: 1px solid {colors['border_gray']};
            }}
        """

        table_header_font_size = max(10, int(base_font_size * 0.95)) # Dynamic header font
        table_style = f"""
            QTableWidget {{
                background-color: {colors['table_bg']};
                gridline-color: {colors['table_grid']};
                border: 1px solid {colors['border_gray']};
                border-radius: 5px;
                font-size: {base_font_size}px;
                color: {colors['text_primary']};  /* Color principal del texto */
                qproperty-showGrid: true;         /* Muestra siempre las líneas */
            }}
            QTableWidget::item {{
                padding: 5px;
                border-right: 1px solid {colors['table_grid']};
                border-bottom: 1px solid {colors['table_grid']};
                color: {colors['text_primary']};  /* Color del texto en celdas */
            }}
            QTableWidget::item:selected {{
                background-color: {colors['table_selection_bg']};
                color: {colors['table_selection_text']};
            }}
            QHeaderView::section {{
                background-color: {colors['table_header_bg']};
                color: {colors['text_primary']};  /* Color texto cabecera */
                padding: 6px;
                border: 1px solid {colors['table_grid']};
                font-size: {table_header_font_size}px;
                font-weight: bold;
            }}
            QHeaderView::section:checked {{
                background-color: #d0d0d0;  /* Color cuando hay selección */
            }}
            /* Estilo para el área del scroll */
            QScrollArea {{
                border: none;
                background-color: {colors['table_bg']};
            }}
            QScrollBar:vertical, QScrollBar:horizontal {{
                background: {colors['background']};
                width: 12px;
                height: 12px;
            }}
            QScrollBar::handle:vertical, QScrollBar::handle:horizontal {{
                background: {colors['border_gray']};
                min-height: 30px;
                border-radius: 6px;
            }}
        """

        # Aplica también al scroll area
        if self.scroll_area is not None:
            self.scroll_area.setStyleSheet(f"""
                background-color: {colors['table_bg']};
                border: none;
            """)

        controls_style = f"""
            QGroupBox {{
                background-color: {colors['group_background']};
                border: 1px solid {colors['border_gray']};
                border-radius: 5px;
                margin-top: 1ex;
                font-size: {int(base_font_size * 0.9)}px;
                color: black;
            }}
            QGroupBox::title {{
                subcontrol-origin: margin;
                subcontrol-position: top center;
                padding: 0 3px;
                color: {colors['title']};
                font-weight: bold;
                color: black;
            }}
            QComboBox, QSpinBox {{
                padding: 5px;
                border: 1px solid {colors['border_gray']};
                border-radius: 3px;
                background: white;
                min-height: 25px;
                font-size: {int(base_font_size * 0.9)}px;
                color: black;
            }}
            QComboBox:hover, QSpinBox:hover {{
                border-color: {colors['button']};
                color: black;
            }}
            QLineEdit {{
                padding: 5px;
                border: 1px solid {colors['border_gray']};
                border-radius: 3px;
                background: white;
                font-size: {int(base_font_size)}px;
                color: black;
            }}
            QLineEdit:hover {{
                border-color: {colors['button']};
            }}
            QRadioButton {{
                font-size: {int(base_font_size)}px;
                color: black;
            }}
            QRadioButton::indicator {{
                width: 13px;
                height: 13px;
                border-radius: 7px;
                border: 1px solid black;     /* Black outline for unchecked state */
                background-color: white;     /* White fill for unchecked state */
            }}
            
            QRadioButton::indicator:checked {{
                background-color: {colors['button']};  /* Fill with theme button color */
                border: 1px solid {colors['button']};   /* Border also theme button color, making it look solid */
            }}

            QRadioButton::indicator:unchecked {{ /* This selector is a bit redundant if the base ::indicator is styled for unchecked */
                background-color: white;   /* Fondo blanco cuando no está seleccionado */
            }}
            QLabel {{
                font-size: {int(base_font_size)}px;
                color: {colors['text_black']};
                
            }}
        """

        return self.centralwidget, title_style, subtitle_style, button_style, button_style_back, table_style, self.scroll_area, controls_style
       # self.pushButton2.setStyleSheet(button_style_back)
        #self.table.setStyleSheet(table_style)

    @staticmethod
    def save_dataframe_as_csv(dataframe, output_path="logs/dataset_cleaned", use_spark_first=True):
        """
        Save a PySpark DataFrame as CSV with fallback to Pandas if Hadoop/HDFS is not available.
        
        Args:
            dataframe: PySpark DataFrame to save
            output_path: Path where to save the CSV (without .csv extension for Spark, with for Pandas)
            use_spark_first: Whether to try PySpark first (default: True)
        
        Returns:
            bool: True if successful, False otherwise
        """
        import os
        
        # Ensure logs directory exists
        os.makedirs("logs", exist_ok=True)
        
        if use_spark_first:
            try:
                # Try PySpark native CSV export first
                print("[INFO] Attempting to save using PySpark...")
                dataframe.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_path)
                print(f"[SUCCESS] Dataset saved using PySpark to: {output_path}")
                return True
                
            except Exception as spark_error:
                print(f"[WARNING] PySpark CSV export failed: {spark_error}")
                print("[INFO] Falling back to Pandas CSV export...")
                
                try:
                    # Fallback to Pandas CSV export
                    pandas_df = dataframe.toPandas()
                    csv_file_path = f"{output_path}.csv"
                    pandas_df.to_csv(csv_file_path, index=False)
                    print(f"[SUCCESS] Dataset saved using Pandas to: {csv_file_path}")
                    return True
                    
                except Exception as pandas_error:
                    print(f"[ERROR] Both PySpark and Pandas CSV export failed!")
                    print(f"PySpark error: {spark_error}")
                    print(f"Pandas error: {pandas_error}")
                    return False
        else:
            try:
                # Use Pandas directly
                print("[INFO] Saving using Pandas...")
                pandas_df = dataframe.toPandas()
                csv_file_path = f"{output_path}.csv"
                pandas_df.to_csv(csv_file_path, index=False)
                print(f"[SUCCESS] Dataset saved using Pandas to: {csv_file_path}")
                return True
                
            except Exception as pandas_error:
                print(f"[ERROR] Pandas CSV export failed: {pandas_error}")
                return False
