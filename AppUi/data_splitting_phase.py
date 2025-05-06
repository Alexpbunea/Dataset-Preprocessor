# -*- coding: utf-8 -*-

import os
import sys
from PySide6.QtCore import QCoreApplication, Qt
from PySide6.QtGui import QIcon, QPixmap
from PySide6.QtWidgets import QMainWindow, QLabel, QPushButton, QSizePolicy, QVBoxLayout, QWidget, QHBoxLayout, QFileDialog


def resource_path(relative_path):
    """ Get the absolute path to a resource, works for dev and packaged apps. """
    if getattr(sys, 'frozen', False):  # If running as a PyInstaller bundle
        base_path = sys._MEIPASS
    else:  # If running in a normal Python environment
        base_path = os.path.abspath(".")
    
    return os.path.join(base_path, relative_path)


class Ui_data_phase(object):
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

        self.title_label = QLabel("Data splitting", self.centralwidget)
        self.subtitle_label = QLabel("Save splits separately with consistent random seed", self.centralwidget)
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

    def _setup_layout(self):
        self.verticalLayout.addWidget(self.title_label)
        self.verticalLayout.addWidget(self.subtitle_label)
        
        self.verticalLayout.addStretch(1)  # Este stretch empujará todo hacia arriba
        
        # Crear un layout horizontal para los botones
        button_container = QHBoxLayout()
        button_container.setContentsMargins(0, 0, 0, 0)
        button_container.setSpacing(10)  # Espaciado pequeño entre botones
        
        # Añadir stretch para empujar los botones a la derecha
        button_container.addStretch()
        
        # Añadir botones en orden inverso (Back primero, luego Continue)
        button_container.addWidget(self.pushButton2)
        button_container.addWidget(self.pushButton)
        
        # Añadir el contenedor de botones al layout principal
        self.verticalLayout.addLayout(button_container)

    def setup_styles(self, title_size=36, subtitle_size=18, button_size=4):
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
            'hover_gray': '#e0e0e0'
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

        button_style = f"""
            QPushButton {{
                background-color: {colors['button']};
                color: {colors['text_white']};
                font-size: {button_size}px;
                font-weight: bold;
                border: none;
                border-radius: 5px;
                padding: {max(2, int(button_size * 0.5))}px {max(5, int(button_size * 1.5))}px;
                min-width: 50px;
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
                padding: {max(2, int(button_size * 0.5))}px {max(5, int(button_size * 1.5))}px;
                min-width: 50px;
            }}
            QPushButton:hover {{
                background-color: {colors['hover_gray']};
                border: 1px solid {colors['border_gray']};
            }}
        """

        self.title_label.setStyleSheet(title_style)
        self.subtitle_label.setStyleSheet(subtitle_style)
        self.pushButton.setStyleSheet(button_style)
        self.pushButton2.setStyleSheet(button_style_back)

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