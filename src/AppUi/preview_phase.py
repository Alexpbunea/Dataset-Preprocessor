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
from src.utils import *

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
        
        self.utils = Utils(None, self.centralwidget, self.title_label, self.subtitle_label, self.pushButton, self.pushButton2, self.table, self.scroll_area)
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

    
    
    def retranslateUi(self, MainWindow):
        MainWindow.setWindowTitle(QCoreApplication.translate("MainWindow", u"Dataset Preprocessor - Cleaning Phase", None))
        self.title_label.setText(QCoreApplication.translate("MainWindow", u"Data Cleaning", None))
        self.subtitle_label.setText(QCoreApplication.translate("MainWindow", u"Inspect your data. You can select columns/rows to remove or modify.", None))
        self.pushButton.setText(QCoreApplication.translate("MainWindow", u"Continue", None))
        self.pushButton2.setText(QCoreApplication.translate("MainWindow", u"Back", None))













    """

    FUNCTIONS FOUND IN THE UTILS.PY FILE

    """

    def setup_styles(self, title_size=36, subtitle_size=18, button_size=14, base_font_size=12): 
        self.centralwidget, title_style, subtitle_style, button_style, button_style_back, table_style, _, _ = self.utils.setup_styles(title_size, subtitle_size, button_size, base_font_size)
        self.title_label.setStyleSheet(title_style)
        self.subtitle_label.setStyleSheet(subtitle_style)
        self.pushButton.setStyleSheet(button_style)
        self.pushButton2.setStyleSheet(button_style_back)
        self.table.setStyleSheet(table_style) 


    def populate_table(self, dataframe):
            """Fill the table with data from a Spark DataFrame."""
            self.utils.dataframe = dataframe
            self.utils.populate_table()
        

