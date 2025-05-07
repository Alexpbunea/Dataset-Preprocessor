# -*- coding: utf-8 -*-

import os
import sys
import pandas as pd 
from PySide6.QtCore import QCoreApplication, Qt
from PySide6.QtGui import QIcon, QPixmap # QIcon, QPixmap not used in this specific UI class directly
from PySide6.QtWidgets import (
    QMainWindow, QLabel, QPushButton, QSizePolicy, QVBoxLayout, 
    QWidget, QHBoxLayout, QFileDialog, QTableWidget, QTableWidgetItem, # Added QTableWidgetItem
    QHeaderView, QScrollArea, QAbstractItemView   # Added for table header styling/behavior
)


def resource_path(relative_path):
    """ Get the absolute path to a resource, works for dev and packaged apps. """
    if getattr(sys, 'frozen', False): 
        base_path = sys._MEIPASS
    else:  
        base_path = os.path.abspath(".")
    
    return os.path.join(base_path, relative_path)


class Ui_preview_phase(object):
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
        self.verticalLayout.setSpacing(15)

        self.title_label = QLabel("Preview", self.centralwidget)
        self.subtitle_label = QLabel("Preview the original dataset", self.centralwidget)
        
        self.scroll_area = QScrollArea()
        self.scroll_area.setWidgetResizable(False) 
        self.table = QTableWidget()
        self.table.setObjectName(u"dataTable")
        
        self.pushButton = QPushButton("Continue", self.centralwidget)
        self.pushButton2 = QPushButton("Back", self.centralwidget)

        self._setup_widget_properties()
        self._setup_layout()
        self.setup_styles()
        
        MainWindow.setCentralWidget(self.centralwidget)
        self._install_resize_event()


    def _setup_widget_properties(self):
        self.title_label.setAlignment(Qt.AlignCenter)
        self.subtitle_label.setAlignment(Qt.AlignCenter)
        self.pushButton.setSizePolicy(QSizePolicy.Fixed, QSizePolicy.Fixed)
        self.pushButton2.setSizePolicy(QSizePolicy.Fixed, QSizePolicy.Fixed)

        # Properties of the tabla
        self.table.setAlternatingRowColors(True)
        self.table.setEditTriggers(QTableWidget.NoEditTriggers)
        self.table.setSelectionBehavior(QTableWidget.SelectRows)
        self.table.setSelectionMode(QTableWidget.SingleSelection)
        self.table.verticalHeader().setVisible(False)

        # Headers of the table
        self.table.horizontalHeader().setStretchLastSection(False)
        self.table.horizontalHeader().setSectionResizeMode(QHeaderView.Fixed) 
        self.table.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOn) 
        self.table.setVerticalScrollBarPolicy(Qt.ScrollBarAsNeeded)  

        # Better visualization
        self.table.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        self.scroll_area.setWidgetResizable(True)
        self.table.setVerticalScrollMode(QAbstractItemView.ScrollPerPixel)
        self.table.setHorizontalScrollMode(QAbstractItemView.ScrollPerPixel)
        self.table.setWordWrap(False)  
        self.table.setTextElideMode(Qt.ElideRight)  
        

    def _setup_layout(self):
        self.verticalLayout.addWidget(self.title_label)
        self.verticalLayout.addWidget(self.subtitle_label)
        
        self.scroll_area.setWidget(self.table)
        self.verticalLayout.addWidget(self.scroll_area, 1)
        
        button_container = QHBoxLayout()
        button_container.setContentsMargins(0, 10, 0, 0)
        button_container.setSpacing(10)
        
        button_container.addStretch()
        button_container.addWidget(self.pushButton2)
        button_container.addWidget(self.pushButton)
        
        self.verticalLayout.addLayout(button_container)

    def setup_styles(self, title_size=36, subtitle_size=18, button_size=14, base_font_size=12): # Adjusted button_size, added base_font_size
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

        self.title_label.setStyleSheet(title_style)
        self.subtitle_label.setStyleSheet(subtitle_style)
        self.pushButton.setStyleSheet(button_style)
        self.pushButton2.setStyleSheet(button_style_back)
        self.table.setStyleSheet(table_style) 

    def _install_resize_event(self):
        original_resize_event = self.MainWindow.resizeEvent

        def new_resize_event(event):
            height = self.MainWindow.height()
            new_title_size = max(18, int(height * 0.05)) 
            new_subtitle_size = max(12, int(height * 0.025)) 
            new_button_size = max(10, int(height * 0.02)) 
            new_base_font_size = max(10, int(height * 0.018)) 

            self.setup_styles(
                title_size=new_title_size,
                subtitle_size=new_subtitle_size,
                button_size=new_button_size,
                base_font_size=new_base_font_size
            )
            
            if original_resize_event: # Check if it was actually there
                original_resize_event(event)
            else: 
                super(self.MainWindow.__class__, self.MainWindow).resizeEvent(event)


        self.MainWindow.resizeEvent = new_resize_event

    def populate_table(self, dataframe):
        """Fill the table with data from a Spark DataFrame."""
        if dataframe is None or len(dataframe.columns) == 0:
            self.table.setRowCount(0)
            self.table.setColumnCount(0)
            self.table.setHorizontalHeaderLabels(["No data to display"])
            return

        columns = dataframe.columns
        rows = dataframe.limit(500).collect()

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
                    self.table.setItem(i, j, QTableWidgetItem(str(value) if value is not None else "Null"))
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
    
    def retranslateUi(self, MainWindow):
        MainWindow.setWindowTitle(QCoreApplication.translate("MainWindow", u"Dataset Preprocessor - Cleaning Phase", None))
        self.title_label.setText(QCoreApplication.translate("MainWindow", u"Data Cleaning", None))
        self.subtitle_label.setText(QCoreApplication.translate("MainWindow", u"Inspect your data. You can select columns/rows to remove or modify.", None))
        self.pushButton.setText(QCoreApplication.translate("MainWindow", u"Continue", None))
        self.pushButton2.setText(QCoreApplication.translate("MainWindow", u"Back", None))
        

