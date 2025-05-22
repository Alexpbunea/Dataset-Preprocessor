# -*- coding: utf-8 -*-

import os
import sys
from PySide6.QtCore import QCoreApplication, Qt
from PySide6.QtGui import QIcon, QPixmap # QIcon, QPixmap not used in this specific UI class directly
from PySide6.QtWidgets import (
    QMainWindow, QLabel, QPushButton, QSizePolicy, QVBoxLayout, 
    QWidget, QHBoxLayout, QFileDialog, QTableWidget, QTableWidgetItem, # Added QTableWidgetItem
    QHeaderView, QScrollArea, QAbstractItemView   # Added for table header styling/behavior
)

class Utils:
    def __init__(self, dataframe = None, centralwidget = None, title = None, subtitle = None, pushButton = None, pushButton2 = None, pushButton3 = None, table = None, scroll_area = None):
        self.dataframe = dataframe
        self.centralwidget = centralwidget


        self.title_label = title
        self.subtitle_label = subtitle
        self.pushButton = pushButton
        self.pushButton2 = pushButton2
        self.pushButton3 = pushButton3
        self.table = table
        self.scroll_area = scroll_area
        
        # Maximum character length for cell text
        self.max_cell_text_length = 0

    def populate_table(self, lenght = 50):
        self.max_cell_text_length = lenght
        """Fill the table with data from a Spark DataFrame."""
        if self.dataframe is None or len(self.dataframe.columns) == 0:
            self.table.setRowCount(0)
            self.table.setColumnCount(0)
            self.table.setHorizontalHeaderLabels(["No data to display"])
            return
        
        columns = self.dataframe.columns
        rows = self.dataframe.limit(500).collect()

        # Improves performance by disabling updates while populating the table
        self.table.setUpdatesEnabled(False)
        self.table.horizontalHeader().setSectionResizeMode(QHeaderView.Interactive)  # Mejor rendimiento

        try:
            self.table.setRowCount(len(rows))
            self.table.setColumnCount(len(columns))
            self.table.setHorizontalHeaderLabels(columns)

            max_col_width = self.scroll_area.width() // 2  
            
            for i, row in enumerate(rows):
                for j, col in enumerate(columns):
                    value = row[col]  # Accede al valor por nombre de columna
                    str_value = str(value) if value is not None else "Null"
                    
                    # Truncate long text and add ellipsis
                    display_text = str_value
                    if len(str_value) > self.max_cell_text_length:
                        display_text = str_value[:self.max_cell_text_length] + "..."
                    
                    item = QTableWidgetItem(display_text)
                    
                    # Set tooltip with full text for truncated cells
                    if len(str_value) > self.max_cell_text_length:
                        item.setToolTip(str_value)
                    
                    self.table.setItem(i, j, item)
                    self.table.setColumnWidth(j, min(self.table.columnWidth(j), max_col_width))
            

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
                font-size: {int(button_size * 0.9)}px;
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
                font-size: {int(button_size * 0.8)}px;
                color: black;
            }}
            QComboBox:hover, QSpinBox:hover {{
                border-color: {colors['button']};
                color: black;
            }}
            QLabel {{
                font-size: {int(button_size * 0.8)}px;
                color: {colors['text_black']};
                
            }}
        """

        return self.centralwidget, title_style, subtitle_style, button_style, button_style_back, table_style, self.scroll_area, controls_style
       # self.pushButton2.setStyleSheet(button_style_back)
        #self.table.setStyleSheet(table_style)
