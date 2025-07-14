# -*- coding: utf-8 -*-

import os
import sys
from PySide6.QtCore import QCoreApplication, Qt, QTimer
from PySide6.QtGui import QIcon, QPixmap
from PySide6.QtWidgets import (
    QMainWindow, QLabel, QPushButton, QSizePolicy, QVBoxLayout,
    QWidget, QHBoxLayout, QFileDialog, QTableWidget, QTableWidgetItem,
    QHeaderView, QAbstractItemView, QComboBox, QRadioButton, QTextEdit,
    QSplitter, QScrollArea
)
from src.utils import *
import matplotlib.pyplot as plt
import seaborn as sns
from matplotlib.backends.backend_qt5agg import FigureCanvasQTAgg as FigureCanvas
from matplotlib.figure import Figure
import numpy as np


def resource_path(relative_path):
    """ Get the absolute path to a resource, works for dev and packaged apps. """
    if getattr(sys, 'frozen', False):  # If running as a PyInstaller bundle
        base_path = sys._MEIPASS
    else:  # If running in a normal Python environment
        base_path = os.path.abspath(".")
    return os.path.join(base_path, relative_path)


class Ui_comparison_phase(object):
    def __init__(self):
        self.MainWindow = None
        self.dataset_info = None
        self.centralwidget = None
        self.verticalLayout = None
        self.title_label = None
        self.subtitle_label = None
        self.data_overview = None
        self.statistical_comparison = None
        self.visual_comparison = None
        self.overview_container = None
        self.statistical_container = None
        self.visual_container = None
        self.overview_table = None
        self.statistical_table = None
        self.visual_canvas = None
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

        self.title_label = QLabel("Comparison and Visualization", self.centralwidget)
        self.subtitle_label = QLabel("Compare original and processed datasets", self.centralwidget)

        # Radio buttons for comparison options
        self.data_overview = QRadioButton("Data Overview Comparison", self.centralwidget)
        self.statistical_comparison = QRadioButton("Statistical Comparison", self.centralwidget)
        self.visual_comparison = QRadioButton("Visual Comparison", self.centralwidget)

        # Connect radio buttons to toggle function
        self.data_overview.toggled.connect(self.toggle_comparison_options)
        self.statistical_comparison.toggled.connect(self.toggle_comparison_options)
        self.visual_comparison.toggled.connect(self.toggle_comparison_options)

        # Create containers for each comparison type
        self._create_overview_container()
        self._create_statistical_container()
        self._create_visual_container()

        self.pushButton = QPushButton("Continue", self.centralwidget)
        self.pushButton2 = QPushButton("Back", self.centralwidget)
        self.pushButton3 = QPushButton("Generate", self.centralwidget)

        self._setup_widget_properties()
        self._setup_layout()

        self.utils = Utils(None, self.centralwidget, self.title_label, self.subtitle_label, self.pushButton,
                           self.pushButton2, self.pushButton3, None, None)
        self.setup_styles()

        self.pushButton3.clicked.connect(self.generate_comparison)

        MainWindow.setCentralWidget(self.centralwidget)
        self._install_resize_event()

    def _create_overview_container(self):
        """Create container for data overview comparison"""
        self.overview_container = QWidget(self.centralwidget)
        overview_layout = QVBoxLayout(self.overview_container)

        overview_label = QLabel("Dataset Overview Comparison:", self.overview_container)

        self.overview_table = QTableWidget(0, 3)
        self.overview_table.setHorizontalHeaderLabels(["Metric", "Original", "Processed"])
        self.overview_table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        self.overview_table.verticalHeader().setVisible(False)
        self.overview_table.setSelectionBehavior(QAbstractItemView.SelectRows)

        overview_layout.addWidget(overview_label)
        overview_layout.addWidget(self.overview_table)

    def _create_statistical_container(self):
        """Create container for statistical comparison"""
        self.statistical_container = QWidget(self.centralwidget)
        statistical_layout = QVBoxLayout(self.statistical_container)

        statistical_label = QLabel("Statistical Comparison by Column:", self.statistical_container)

        self.statistical_table = QTableWidget(0, 4)
        self.statistical_table.setHorizontalHeaderLabels(["Column", "Original Stats", "Processed Stats", "Change"])
        self.statistical_table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        self.statistical_table.verticalHeader().setVisible(False)
        self.statistical_table.setSelectionBehavior(QAbstractItemView.SelectRows)

        statistical_layout.addWidget(statistical_label)
        statistical_layout.addWidget(self.statistical_table)

    def _create_visual_container(self):
        """Create container for visual comparison"""
        self.visual_container = QWidget(self.centralwidget)
        visual_layout = QVBoxLayout(self.visual_container)

        visual_label = QLabel("Visual Comparison - Select columns to visualize:", self.visual_container)

        # Create a splitter for column selection and visualization
        splitter = QSplitter(Qt.Horizontal)
        
        # Left side: Column selection
        column_widget = QWidget()
        column_layout = QVBoxLayout(column_widget)
        
        self.column_table = QTableWidget(0, 2)
        self.column_table.setHorizontalHeaderLabels(["Column", "Visualize"])
        self.column_table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        self.column_table.verticalHeader().setVisible(False)
        self.column_table.setMaximumWidth(300)
        
        column_layout.addWidget(self.column_table)
        
        # Right side: Visualization canvas
        self.visual_canvas = FigureCanvas(Figure(figsize=(8, 6)))
        
        splitter.addWidget(column_widget)
        splitter.addWidget(self.visual_canvas)
        splitter.setSizes([300, 500])

        visual_layout.addWidget(visual_label)
        visual_layout.addWidget(splitter)

    def _setup_widget_properties(self):
        self.title_label.setAlignment(Qt.AlignCenter)
        self.subtitle_label.setAlignment(Qt.AlignCenter)

        self.data_overview.setChecked(True)  # Default selection
        self.statistical_comparison.setChecked(False)
        self.visual_comparison.setChecked(False)
        self.overview_container.setVisible(True)
        self.statistical_container.setVisible(False)
        self.visual_container.setVisible(False)

        self.pushButton.setSizePolicy(QSizePolicy.Fixed, QSizePolicy.Fixed)
        self.pushButton2.setSizePolicy(QSizePolicy.Fixed, QSizePolicy.Fixed)
        self.pushButton3.setSizePolicy(QSizePolicy.Fixed, QSizePolicy.Fixed)

    def _setup_layout(self):
        self.verticalLayout.addWidget(self.title_label)
        self.verticalLayout.addWidget(self.subtitle_label)

        self.verticalLayout.addStretch(1)

        # Add radio buttons
        self.verticalLayout.addWidget(self.data_overview)
        self.verticalLayout.addWidget(self.statistical_comparison)
        self.verticalLayout.addWidget(self.visual_comparison)

        # Add containers
        self.verticalLayout.addWidget(self.overview_container)
        self.verticalLayout.addWidget(self.statistical_container)
        self.verticalLayout.addWidget(self.visual_container)

        self.verticalLayout.addStretch(1)

        # Button container
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

    def toggle_comparison_options(self):
        """Show/hide comparison containers based on selected radio button"""
        self.overview_container.setVisible(self.data_overview.isChecked())
        self.statistical_container.setVisible(self.statistical_comparison.isChecked())
        self.visual_container.setVisible(self.visual_comparison.isChecked())

        # Populate tables/canvas when containers become visible
        if self.data_overview.isChecked() and self.dataset_info:
            self.populate_overview_table()
        elif self.statistical_comparison.isChecked() and self.dataset_info:
            self.populate_statistical_table()
        elif self.visual_comparison.isChecked() and self.dataset_info:
            self.populate_visual_options()

    def populate_overview_table(self):
        """Populate overview comparison table"""
        self.overview_table.setRowCount(0)

        if not self.dataset_info:
            return

        try:
            original_info = self.dataset_info.get_general_info_original()
            current_info = self.dataset_info.get_general_info()

            metrics = [
                ("Number of Rows", original_info.get("num_rows", 0), current_info.get("num_rows", 0)),
                ("Number of Columns", original_info.get("num_columns", 0), current_info.get("num_columns", 0)),
                ("Total Null Values", self._count_total_nulls(original_info), self._count_total_nulls(current_info)),
                ("Memory Usage", "N/A", "N/A"),  # Could be implemented if needed
            ]

            for metric, original, processed in metrics:
                row = self.overview_table.rowCount()
                self.overview_table.insertRow(row)

                self.overview_table.setItem(row, 0, QTableWidgetItem(metric))
                self.overview_table.setItem(row, 1, QTableWidgetItem(str(original)))
                self.overview_table.setItem(row, 2, QTableWidgetItem(str(processed)))

        except Exception as e:
            print(f"[ERROR] -> Failed to populate overview table: {e}")

    def populate_statistical_table(self):
        """Populate statistical comparison table"""
        self.statistical_table.setRowCount(0)

        if not self.dataset_info:
            return

        try:
            original_df = self.dataset_info.get_dataframe_original()
            current_df = self.dataset_info.get_dataframe()

            if original_df is None or current_df is None:
                self.show_status("Original or current dataframe not available", "orange")
                return

            # Get numerical columns that exist in both dataframes
            original_cols = set(original_df.columns)
            current_cols = set(current_df.columns)
            common_cols = original_cols.intersection(current_cols)

            for col in common_cols:
                if self._is_numerical_column(original_df, col):
                    row = self.statistical_table.rowCount()
                    self.statistical_table.insertRow(row)

                    # Get statistics for both dataframes
                    original_stats = self._get_column_stats(original_df, col)
                    current_stats = self._get_column_stats(current_df, col)
                    change = self._calculate_change(original_stats, current_stats)

                    self.statistical_table.setItem(row, 0, QTableWidgetItem(col))
                    self.statistical_table.setItem(row, 1, QTableWidgetItem(original_stats))
                    self.statistical_table.setItem(row, 2, QTableWidgetItem(current_stats))
                    self.statistical_table.setItem(row, 3, QTableWidgetItem(change))

        except Exception as e:
            print(f"[ERROR] -> Failed to populate statistical table: {e}")

    def populate_visual_options(self):
        """Populate visual comparison options"""
        if hasattr(self, 'column_table'):
            self.column_table.setRowCount(0)

            if not self.dataset_info:
                return

            try:
                current_df = self.dataset_info.get_dataframe()
                if current_df is None:
                    return

                # Get numerical columns for visualization
                numerical_cols = self._get_numerical_columns(current_df)

                for col in numerical_cols:
                    row = self.column_table.rowCount()
                    self.column_table.insertRow(row)

                    col_item = QTableWidgetItem(col)
                    col_item.setFlags(col_item.flags() & ~Qt.ItemIsEditable)
                    self.column_table.setItem(row, 0, col_item)

                    visualize_combo = QComboBox()
                    visualize_combo.addItems(["No", "Histogram", "Box Plot", "Scatter Plot"])
                    visualize_combo.setStyleSheet(self._combo_style())
                    self.column_table.setCellWidget(row, 1, visualize_combo)

            except Exception as e:
                print(f"[ERROR] -> Failed to populate visual options: {e}")

    def generate_comparison(self):
        """Generate the selected comparison"""
        try:
            if self.data_overview.isChecked():
                self.generate_overview_comparison()
            elif self.statistical_comparison.isChecked():
                self.generate_statistical_comparison()
            elif self.visual_comparison.isChecked():
                self.generate_visual_comparison()
            else:
                self.show_status("Please select a comparison method", "orange")
        except Exception as e:
            print(f"[ERROR] -> Comparison generation failed: {e}")
            self.show_status("Comparison generation failed!", "red")

    def generate_overview_comparison(self):
        """Generate overview comparison"""
        print("[INFO] -> Generating overview comparison")
        self.populate_overview_table()
        self.show_status("Overview comparison generated successfully!", "green")

    def generate_statistical_comparison(self):
        """Generate statistical comparison"""
        print("[INFO] -> Generating statistical comparison")
        self.populate_statistical_table()
        self.show_status("Statistical comparison generated successfully!", "green")

    def generate_visual_comparison(self):
        """Generate visual comparison"""
        print("[INFO] -> Generating visual comparison")
        
        if not hasattr(self, 'column_table'):
            self.show_status("Visual options not available", "orange")
            return

        # Get selected columns for visualization
        selected_visualizations = []
        for row in range(self.column_table.rowCount()):
            col_name = self.column_table.item(row, 0).text()
            viz_combo = self.column_table.cellWidget(row, 1)
            viz_type = viz_combo.currentText()
            if viz_type != "No":
                selected_visualizations.append((col_name, viz_type))

        if not selected_visualizations:
            self.show_status("No columns selected for visualization", "orange")
            return

        # Create visualizations
        self._create_visualizations(selected_visualizations)
        self.show_status("Visual comparison generated successfully!", "green")

    def _create_visualizations(self, selected_visualizations):
        """Create matplotlib visualizations"""
        try:
            fig = self.visual_canvas.figure
            fig.clear()

            n_plots = len(selected_visualizations)
            if n_plots == 0:
                return

            # Calculate subplot layout
            cols = min(2, n_plots)
            rows = (n_plots + cols - 1) // cols

            original_df = self.dataset_info.get_dataframe_original()
            current_df = self.dataset_info.get_dataframe()

            for i, (col_name, viz_type) in enumerate(selected_visualizations):
                ax = fig.add_subplot(rows, cols, i + 1)

                if viz_type == "Histogram":
                    self._create_histogram(ax, original_df, current_df, col_name)
                elif viz_type == "Box Plot":
                    self._create_boxplot(ax, original_df, current_df, col_name)
                elif viz_type == "Scatter Plot":
                    self._create_scatterplot(ax, original_df, current_df, col_name)

                ax.set_title(f"{col_name} - {viz_type}")

            fig.tight_layout()
            self.visual_canvas.draw()

        except Exception as e:
            print(f"[ERROR] -> Failed to create visualizations: {e}")

    def _create_histogram(self, ax, original_df, current_df, col_name):
        """Create histogram comparison"""
        try:
            # Convert to pandas for easier plotting
            original_data = original_df.select(col_name).toPandas()[col_name].dropna()
            current_data = current_df.select(col_name).toPandas()[col_name].dropna()

            ax.hist(original_data, alpha=0.7, label='Original', bins=30)
            ax.hist(current_data, alpha=0.7, label='Processed', bins=30)
            ax.legend()
            ax.set_xlabel(col_name)
            ax.set_ylabel('Frequency')
        except Exception as e:
            ax.text(0.5, 0.5, f"Error: {str(e)}", transform=ax.transAxes, ha='center')

    def _create_boxplot(self, ax, original_df, current_df, col_name):
        """Create box plot comparison"""
        try:
            # Convert to pandas for easier plotting
            original_data = original_df.select(col_name).toPandas()[col_name].dropna()
            current_data = current_df.select(col_name).toPandas()[col_name].dropna()

            ax.boxplot([original_data, current_data], labels=['Original', 'Processed'])
            ax.set_ylabel(col_name)
        except Exception as e:
            ax.text(0.5, 0.5, f"Error: {str(e)}", transform=ax.transAxes, ha='center')

    def _create_scatterplot(self, ax, original_df, current_df, col_name):
        """Create scatter plot comparison"""
        try:
            # For scatter plot, we'll plot original vs processed values
            original_data = original_df.select(col_name).toPandas()[col_name].dropna()
            current_data = current_df.select(col_name).toPandas()[col_name].dropna()

            # Ensure same length
            min_len = min(len(original_data), len(current_data))
            original_data = original_data[:min_len]
            current_data = current_data[:min_len]

            ax.scatter(original_data, current_data, alpha=0.6)
            ax.plot([original_data.min(), original_data.max()], 
                   [original_data.min(), original_data.max()], 'r--', alpha=0.8)
            ax.set_xlabel(f'{col_name} (Original)')
            ax.set_ylabel(f'{col_name} (Processed)')
        except Exception as e:
            ax.text(0.5, 0.5, f"Error: {str(e)}", transform=ax.transAxes, ha='center')

    # Helper methods
    def _count_total_nulls(self, info):
        """Count total null values in dataset"""
        try:
            if "column_null_counts" in info:
                return sum(row["null_count"] for row in info["column_null_counts"].collect())
            return 0
        except:
            return 0

    def _is_numerical_column(self, df, col):
        """Check if column is numerical"""
        try:
            dtype = dict(df.dtypes)[col]
            return dtype in ['int', 'bigint', 'float', 'double', 'decimal']
        except:
            return False

    def _get_numerical_columns(self, df):
        """Get list of numerical columns"""
        try:
            return [col for col, dtype in df.dtypes if dtype in ['int', 'bigint', 'float', 'double', 'decimal']]
        except:
            return []

    def _get_column_stats(self, df, col):
        """Get basic statistics for a column"""
        try:
            stats = df.select(col).describe().collect()
            return f"Mean: {stats[1][1]:.2f}, Std: {stats[2][1]:.2f}"
        except:
            return "N/A"

    def _calculate_change(self, original, current):
        """Calculate change between original and current stats"""
        try:
            # Simple implementation - could be enhanced
            if original == current:
                return "No change"
            else:
                return "Changed"
        except:
            return "N/A"

    def show_status(self, message, color):
        """Show status message with color"""
        self.status_label.setText(message)
        self.status_label.setStyleSheet(f"color: {color}; font-weight: bold;")
        QTimer.singleShot(5000, lambda: self.status_label.setText(""))

    def set_dataset_info(self, dataset_info):
        """Set the dataset info"""
        self.dataset_info = dataset_info
        # Auto-populate if data overview is selected
        if self.data_overview.isChecked():
            self.populate_overview_table()

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

        # Apply table styles
        for table in [self.overview_table, self.statistical_table]:
            if table:
                table.setStyleSheet(table_style)
        
        if hasattr(self, 'column_table'):
            self.column_table.setStyleSheet(table_style)

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