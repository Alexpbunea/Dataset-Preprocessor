# -*- coding: utf-8 -*-

import os
import sys
from PySide6.QtCore import QCoreApplication, Qt, QTimer
from PySide6.QtGui import QIcon, QPixmap
from PySide6.QtWidgets import (
    QMainWindow, QLabel, QPushButton, QSizePolicy, QVBoxLayout,
    QWidget, QHBoxLayout, QFileDialog, QTableWidget, QTableWidgetItem,
    QHeaderView, QAbstractItemView, QSpinBox, QDoubleSpinBox, QCheckBox
)
from src.utils import *


def resource_path(relative_path):
    """ Get the absolute path to a resource, works for dev and packaged apps. """
    if getattr(sys, 'frozen', False):  # If running as a PyInstaller bundle
        base_path = sys._MEIPASS
    else:  # If running in a normal Python environment
        base_path = os.path.abspath(".")
    return os.path.join(base_path, relative_path)


class Ui_data_phase(object):
    def __init__(self):
        self.MainWindow = None
        self.dataset_info = None
        self.centralwidget = None
        self.verticalLayout = None
        self.title_label = None
        self.subtitle_label = None
        self.split_container = None
        self.split_table = None
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

        self.dataset_info = None

        self.centralwidget = QWidget(MainWindow)
        self.centralwidget.setObjectName(u"centralwidget")

        self.verticalLayout = QVBoxLayout(self.centralwidget)
        self.verticalLayout.setContentsMargins(20, 20, 20, 20)
        self.verticalLayout.setSpacing(20)

        self.title_label = QLabel("Data splitting", self.centralwidget)
        self.subtitle_label = QLabel("Adjust split percentages and options", self.centralwidget)

        self._create_split_container()

        self.pushButton = QPushButton("Continue", self.centralwidget)
        self.pushButton2 = QPushButton("Back", self.centralwidget)
        self.pushButton3 = QPushButton("Apply", self.centralwidget)

        self._setup_widget_properties()
        self._setup_layout()

        self.utils = Utils(None, self.centralwidget, self.title_label, self.subtitle_label, self.pushButton,
                           self.pushButton2, self.pushButton3, None, None)
        self.setup_styles()

        self.pushButton3.clicked.connect(self.apply_data_splitting)

        MainWindow.setCentralWidget(self.centralwidget)
        self._install_resize_event()
        self.populate_split_table()  # <-- Importante: inicializa la tabla

    def _create_split_container(self):
        """Create container for split configuration options"""
        self.split_container = QWidget(self.centralwidget)
        split_layout = QVBoxLayout(self.split_container)

        split_label = QLabel("Configure data splitting parameters:", self.split_container)

        self.split_table = QTableWidget(0, 3)
        self.split_table.setHorizontalHeaderLabels(["Parameter", "Value", "Description"])
        self.split_table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        self.split_table.verticalHeader().setVisible(False)
        self.split_table.setSelectionBehavior(QAbstractItemView.SelectRows)

        split_layout.addWidget(split_label)
        split_layout.addWidget(self.split_table)

    def _setup_widget_properties(self):
        self.title_label.setAlignment(Qt.AlignCenter)
        self.subtitle_label.setAlignment(Qt.AlignCenter)
        self.split_container.setVisible(True)
        self.pushButton.setSizePolicy(QSizePolicy.Fixed, QSizePolicy.Fixed)
        self.pushButton2.setSizePolicy(QSizePolicy.Fixed, QSizePolicy.Fixed)
        self.pushButton3.setSizePolicy(QSizePolicy.Fixed, QSizePolicy.Fixed)

    def _setup_layout(self):
        self.verticalLayout.addWidget(self.title_label)
        self.verticalLayout.addWidget(self.subtitle_label)
        self.verticalLayout.addStretch(1)
        self.verticalLayout.addWidget(self.split_container)
        self.verticalLayout.addStretch(1)

        button_container = QHBoxLayout()
        button_container.setContentsMargins(0, 0, 0, 0)
        button_container.setSpacing(10)

        self.status_label = QLabel("")
        self.status_label.setMinimumWidth(200)
        self.status_label.setAlignment(Qt.AlignVCenter | Qt.AlignLeft)
        button_container.addWidget(self.status_label)

        button_container.addStretch()
        button_container.addWidget(self.pushButton3)
        button_container.addWidget(self.pushButton2)
        button_container.addWidget(self.pushButton)

        self.verticalLayout.addLayout(button_container)

    def populate_split_table(self):
        """Populate table with split configuration options"""
        self.split_table.setRowCount(0)
        parameters = [
            ("Train Size", 0.7, "Proportion of dataset for training (0.1 - 0.8)"),
            ("Validation Size", 0.2, "Proportion of dataset for validation (0.0 - 0.5)"),
            ("Test Size", 0.1, "Proportion of dataset for testing (0.1 - 0.5)"),
            ("Random Seed", 42, "Seed for reproducible splits"),
            ("Shuffle", True, "Shuffle data before splitting"),
            ("Stratify", True, "Maintain class distribution in splits")
        ]
        for param_name, default_value, description in parameters:
            row = self.split_table.rowCount()
            self.split_table.insertRow(row)
            param_item = QTableWidgetItem(param_name)
            param_item.setFlags(param_item.flags() & ~Qt.ItemIsEditable)
            self.split_table.setItem(row, 0, param_item)
            desc_item = QTableWidgetItem(description)
            desc_item.setFlags(desc_item.flags() & ~Qt.ItemIsEditable)
            self.split_table.setItem(row, 2, desc_item)
            # Create appropriate widget based on parameter type
            if "Size" in param_name:
                widget = QDoubleSpinBox()
                widget.setRange(0.0, 0.8)
                widget.setSingleStep(0.05)
                widget.setValue(default_value)
                widget.setDecimals(2)
            elif param_name == "Random Seed":
                widget = QSpinBox()
                widget.setRange(1, 9999)
                widget.setValue(default_value)
            else:  # Boolean parameters
                widget = QCheckBox()
                widget.setChecked(default_value)
            widget.setStyleSheet(self._widget_style())
            self.split_table.setCellWidget(row, 1, widget)

    def get_split_config(self):
        """Extract configuration from the split table"""
        config = {}
        for row in range(self.split_table.rowCount()):
            param_name = self.split_table.item(row, 0).text()
            widget = self.split_table.cellWidget(row, 1)
            if widget:
                if isinstance(widget, QDoubleSpinBox):
                    config[param_name] = widget.value()
                elif isinstance(widget, QSpinBox):
                    config[param_name] = widget.value()
                elif isinstance(widget, QCheckBox):
                    config[param_name] = widget.isChecked()
        return config

    def apply_data_splitting(self):
        """Apply the data splitting configuration"""
        try:
            if self.dataset_info is None or not hasattr(self.dataset_info, "dataframe"):
                print("[ERROR] -> No dataset loaded!")
                self.show_status("No dataset loaded!", "red")
                return
            config = self.get_split_config()
            df = self.dataset_info.dataframe  # Tu DataFrame principal

            train_size = config.get("Train Size", 0)
            val_size = config.get("Validation Size", 0)
            test_size = config.get("Test Size", 0)
            seed = config.get("Random Seed", 42)
            shuffle = config.get("Shuffle", True)

            total_size = train_size + val_size + test_size
            if abs(total_size - 1.0) > 0.01:
                self.show_status(f"Split sizes must sum to 1.0 (current: {total_size:.2f})", "red")
                return

            # Realiza el split
            if shuffle:
                splits = df.randomSplit([train_size, val_size, test_size], seed=seed)
            else:
                # Si no quieres shuffle, simplemente toma los cortes por índices
                count = df.count()
                train_end = int(train_size * count)
                val_end = train_end + int(val_size * count)
                train_df = df.limit(train_end)
                val_df = df.subtract(train_df).limit(val_end - train_end)
                test_df = df.subtract(train_df).subtract(val_df)
                splits = [train_df, val_df, test_df]

            train_df, val_df, test_df = splits

            print(f"[INFO] -> Train rows: {train_df.count()}, Validation rows: {val_df.count()}, Test rows: {test_df.count()}")
            self.show_status("Data splitting applied successfully!", "green")

            # Puedes guardar los splits en el objeto si lo necesitas:
            self.dataset_info.set_train_df(train_df)
            self.dataset_info.set_val_df(val_df)
            self.dataset_info.set_test_df(test_df)

            

        except Exception as e:
            print(f"[ERROR] -> Data splitting failed: {e}")
            self.show_status("Data splitting failed!", "red")

    def show_status(self, message, color):
        """Show status message with color"""
        self.status_label.setText(message)
        self.status_label.setStyleSheet(f"color: {color}; font-weight: bold;")
        QTimer.singleShot(5000, lambda: self.status_label.setText(""))

    def set_dataset_info(self, dataset_info):
        """Set the dataset info"""
        self.dataset_info = dataset_info
        self.populate_split_table()

    @staticmethod
    def _widget_style():
        return """
            QDoubleSpinBox, QSpinBox, QCheckBox {
                color: black;
                border: none;
                background-color: transparent;
                padding: 2px;
                margin: 0;
            }
            QCheckBox::indicator {
                width: 18px;
                height: 18px;
            }
        """

    def setup_styles(self, title_size=36, subtitle_size=18, button_size=4):
        self.centralwidget, title_style, subtitle_style, button_style, button_style_back, table_style, _, controls_style_str = self.utils.setup_styles(
            title_size, subtitle_size, button_size)
        current_centralwidget_style = self.centralwidget.styleSheet()
        self.centralwidget.setStyleSheet(current_centralwidget_style + "\n" + controls_style_str)
        self.title_label.setStyleSheet(title_style)
        self.subtitle_label.setStyleSheet(subtitle_style)
        self.pushButton.setStyleSheet(button_style)
        self.pushButton2.setStyleSheet(button_style_back)
        self.pushButton3.setStyleSheet(button_style)
        self.split_table.setStyleSheet(table_style)

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