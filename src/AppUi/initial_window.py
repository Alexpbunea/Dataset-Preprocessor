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


class Ui_initial_phase(object):
    def __init__(self):
        self.file_path = None
        self.file_name = None
        

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

        self.title_label = QLabel("Dataset Preprocessor", self.centralwidget)
        self.subtitle_label = QLabel("Efficiently prepare and manage your datasets", self.centralwidget)
        self.pushButton = QPushButton("Start Processing", self.centralwidget)

        self._setup_widget_properties()
        self._setup_layout()
        self.setup_styles()
        
        MainWindow.setCentralWidget(self.centralwidget)
        self._install_resize_event()

    def _setup_widget_properties(self):
        self.title_label.setAlignment(Qt.AlignCenter)
        self.subtitle_label.setAlignment(Qt.AlignCenter)
        self.pushButton.setSizePolicy(QSizePolicy.Fixed, QSizePolicy.Fixed)

    def _setup_layout(self):
        self.verticalLayout.addWidget(self.title_label)
        self.verticalLayout.addWidget(self.subtitle_label)
        
        self.verticalLayout.addStretch(1)
        self._setup_file_drop_area()
        self.verticalLayout.addStretch(1)
        
        self.file_status = QLabel("")
        self.file_status.setAlignment(Qt.AlignCenter)
        self.verticalLayout.addWidget(self.file_status)
        
        self.verticalLayout.addWidget(self.pushButton, 0, Qt.AlignCenter)
        self.verticalLayout.addSpacing(5)

    def _setup_file_drop_area(self):
        self.drop_container = QWidget()
        self.drop_container.setFixedSize(300, 150)
        self.drop_container.setStyleSheet("""
            QWidget {
                background-color: #ffffff;
                border: 2px dashed #0078d7;
                border-radius: 8px;
            }
            QWidget:hover {
                background-color: #f0f6ff;
                border: 2px dashed #005a9e;
            }
            QWidget[valid_file="true"] {
                border: 2px solid #2ecc71;
                background-color: #f0fff4;
            }
        """)

        drop_layout = QVBoxLayout(self.drop_container)
        drop_layout.setContentsMargins(15, 15, 15, 15)
        drop_layout.setSpacing(10)
        
        self.drop_icon = QLabel()
        #self.drop_icon.setPixmap(QIcon(":/icons/upload.svg").pixmap(48, 48))
        #self.drop_icon.setPixmap(QIcon.fromTheme("document-open").pixmap(48, 48))
        self.drop_icon.setAlignment(Qt.AlignCenter)
        
        self.drop_text = QLabel("Throw your dataset in here\n")
        self.drop_text.setAlignment(Qt.AlignCenter)
        self.drop_text.setStyleSheet("""
            QLabel {
                color: #555555;
                font-size: 14px;
            }
        """)
        
        self.browse_btn = QPushButton("Select dataset")
        self.browse_btn.setStyleSheet("""
            QPushButton {
                background-color: #e6f1ff;
                color: #0078d7;
                border: 1px solid #0078d7;
                border-radius: 4px;
                padding: 6px 16px;
                font-size: 13px;
            }
            QPushButton:hover {
                background-color: #d0e3ff;
            }
        """)
        self.browse_btn.clicked.connect(self._open_file_dialog)
        
        drop_layout.addStretch()
        drop_layout.addWidget(self.drop_icon)
        drop_layout.addWidget(self.drop_text)
        drop_layout.addWidget(self.browse_btn, 0, Qt.AlignCenter)
        drop_layout.addStretch()
        
        self.verticalLayout.addWidget(self.drop_container, 0, Qt.AlignCenter)

    def _open_file_dialog(self):
        file_path, _ = QFileDialog.getOpenFileName(
            self.MainWindow,
            "Select file",
            "",
            "Data files (*.csv *.xlsx *.json *.xls *jsonl)"
        )
        if file_path:
            self.file_path = file_path
            self._handle_file(file_path)

    def _handle_file(self, file_path):
        self.drop_container.setProperty("valid_file", "true")
        self.drop_container.style().polish(self.drop_container)
        
        self.file_name = file_path.split('/')[-1]
        self.file_status.setText(f"Dataset loaded: {self.file_name}")
        self.file_status.setStyleSheet("""
            QLabel {
                color: #2ecc71;
                font-size: 14px;
                font-weight: bold;
            }
        """)

        file_extension = file_path.split('.')[-1].lower()
        if file_extension == "csv":
            icon_path = resource_path("./src/AppUi/icons/csv_icon.png")
        elif file_extension in ["xlsx", "xls"]:
            icon_path = resource_path("./src/AppUi/icons/excel_icon.png")
        elif file_extension == "json":
            icon_path = resource_path("./src/AppUi/icons/json_icon.png")
        else:
            icon_path = resource_path("./src/AppUi/icons/default_file_icon.png")

        pixmap = QPixmap(icon_path).scaled(48, 48, Qt.KeepAspectRatio, Qt.SmoothTransformation)
        self.drop_icon.setPixmap(pixmap)
        self.drop_text.setText("File ready for processing")

        print("Dataset detected:", file_path)

    def setup_styles(self, title_size=36, subtitle_size=18, button_size=16):
        colors = {
            'background': '#f5f5f5',
            'title': '#333333',
            'subtitle': '#555555',
            'button': '#0078d7',
            'button_hover': '#005a9e',
            'text_white': 'white'
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
                padding: {max(8, int(button_size * 0.5))}px {max(20, int(button_size * 1.5))}px;
                min-width: 200px;
            }}
            QPushButton:hover {{
                background-color: {colors['button_hover']};
            }}
        """

        self.title_label.setStyleSheet(title_style)
        self.subtitle_label.setStyleSheet(subtitle_style)
        self.pushButton.setStyleSheet(button_style)

    def _install_resize_event(self):
        original_resize_event = self.MainWindow.resizeEvent

        def new_resize_event(event):
            new_title_size = int(self.MainWindow.height() * 0.06)
            new_subtitle_size = int(self.MainWindow.height() * 0.03)
            new_button_size = int(self.MainWindow.height() * 0.025)

            self.setup_styles(
                title_size=new_title_size,
                subtitle_size=new_subtitle_size,
                button_size=new_button_size
            )
            
            original_resize_event(event)

        self.MainWindow.resizeEvent = new_resize_event

    def retranslateUi(self, MainWindow):
        MainWindow.setWindowTitle(QCoreApplication.translate("MainWindow", u"Dataset Preprocessor", None))
