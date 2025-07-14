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
    """
    UI for data comparison and visualization phase including:
    - Data overview comparison (row counts, column counts, null values)
    - Statistical comparison (column-wise statistics)
    - Visual comparison (histograms, box plots, scatter plots)
    """
    
    def __init__(self):
        """Initialize all UI components"""
        self.MainWindow = None
        self.dataset_info = None
        
        # UI Components
        self.centralwidget = None
        self.verticalLayout = None
        self.title_label = None
        self.subtitle_label = None
        self.status_label = None
        self.utils = None
        
        # Radio buttons
        self.data_overview = None
        self.statistical_comparison = None
        self.visual_comparison = None
        
        # Containers and tables
        self.overview_container = None
        self.statistical_container = None
        self.visual_container = None
        self.overview_table = None
        self.statistical_table = None
        self.visual_canvas = None
        
        # Buttons
        self.pushButton = None
        self.pushButton2 = None
        self.pushButton3 = None

    # === SETUP METHODS ===
    
    def setupUi(self, MainWindow):
        """Initialize the main UI components and layout"""
        self.MainWindow = MainWindow
        self._configure_main_window()
        self._create_main_layout()
        self._create_comparison_options()
        self._create_containers()
        self._create_buttons()
        self._finalize_setup()

    def _configure_main_window(self):
        """Configure main window properties"""
        if not self.MainWindow.objectName():
            self.MainWindow.setObjectName(u"MainWindow")
        self.MainWindow.resize(900, 600)
        self.MainWindow.setMinimumSize(600, 400)
        self.dataset_info = None

    def _create_main_layout(self):
        """Create the main central widget and layout"""
        self.centralwidget = QWidget(self.MainWindow)
        self.centralwidget.setObjectName(u"centralwidget")

        self.verticalLayout = QVBoxLayout(self.centralwidget)
        self.verticalLayout.setContentsMargins(20, 20, 20, 20)
        self.verticalLayout.setSpacing(20)

        # Title and subtitle
        self.title_label = QLabel("Comparison and Visualization", self.centralwidget)
        self.subtitle_label = QLabel("Compare original and processed datasets", self.centralwidget)

    def _create_comparison_options(self):
        """Create radio buttons for comparison options"""
        self.data_overview = QRadioButton("Data Overview Comparison", self.centralwidget)
        self.statistical_comparison = QRadioButton("Statistical Comparison", self.centralwidget)
        self.visual_comparison = QRadioButton("Visual Comparison", self.centralwidget)

        # Connect radio buttons to toggle function
        self.data_overview.toggled.connect(self.toggle_comparison_options)
        self.statistical_comparison.toggled.connect(self.toggle_comparison_options)
        self.visual_comparison.toggled.connect(self.toggle_comparison_options)

    def _create_containers(self):
        """Create containers for each comparison type"""
        self._create_overview_container()
        self._create_statistical_container()
        self._create_visual_container()

    def _create_buttons(self):
        """Create action buttons"""
        self.pushButton = QPushButton("Continue", self.centralwidget)
        self.pushButton2 = QPushButton("Back", self.centralwidget)
        self.pushButton3 = QPushButton("Generate", self.centralwidget)

    def _finalize_setup(self):
        """Finalize UI setup with styling and event handling"""
        self._setup_widget_properties()
        self._setup_layout()

        self.utils = Utils(None, self.centralwidget, self.title_label, self.subtitle_label, self.pushButton,
                           self.pushButton2, self.pushButton3, None, None)
        self.setup_styles()

        self.pushButton3.clicked.connect(self.generate_comparison)
        self.MainWindow.setCentralWidget(self.centralwidget)
        self._install_resize_event()

    # === CONTAINER CREATION METHODS ===

    def _create_overview_container(self):
        """Create container for data overview comparison"""
        self.overview_container = QWidget(self.centralwidget)
        overview_layout = QVBoxLayout(self.overview_container)

        overview_label = QLabel("Dataset Overview Comparison:", self.overview_container)
        self.overview_table = self._create_table(["Metric", "Original", "Processed"])

        overview_layout.addWidget(overview_label)
        overview_layout.addWidget(self.overview_table)

    def _create_statistical_container(self):
        """Create container for statistical comparison"""
        self.statistical_container = QWidget(self.centralwidget)
        statistical_layout = QVBoxLayout(self.statistical_container)

        statistical_label = QLabel("Statistical Comparison by Column:", self.statistical_container)
        self.statistical_table = self._create_table(["Column", "Original Stats", "Processed Stats", "Change"])

        statistical_layout.addWidget(statistical_label)
        statistical_layout.addWidget(self.statistical_table)

    def _create_visual_container(self):
        """Create container for visual comparison"""
        self.visual_container = QWidget(self.centralwidget)
        visual_layout = QVBoxLayout(self.visual_container)

        visual_label = QLabel("Visual Comparison - Select columns to visualize:", self.visual_container)
        
        # Create splitter for column selection and visualization
        splitter = self._create_visual_splitter()
        
        visual_layout.addWidget(visual_label)
        visual_layout.addWidget(splitter)

    def _create_table(self, headers):
        """Create a standardized table widget"""
        table = QTableWidget(0, len(headers))
        table.setHorizontalHeaderLabels(headers)
        table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        table.verticalHeader().setVisible(False)
        table.setSelectionBehavior(QAbstractItemView.SelectRows)
        return table

    def _create_visual_splitter(self):
        """Create splitter for visual comparison layout"""
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
        
        return splitter

    def _setup_widget_properties(self):
        """Configure widget properties and initial states"""
        self.title_label.setAlignment(Qt.AlignCenter)
        self.subtitle_label.setAlignment(Qt.AlignCenter)

        # Set initial radio button states
        self.data_overview.setChecked(True)
        self.statistical_comparison.setChecked(False)
        self.visual_comparison.setChecked(False)
        
        # Set initial container visibility
        self.overview_container.setVisible(True)
        self.statistical_container.setVisible(False)
        self.visual_container.setVisible(False)

        # Configure button size policies
        for button in [self.pushButton, self.pushButton2, self.pushButton3]:
            button.setSizePolicy(QSizePolicy.Fixed, QSizePolicy.Fixed)

    def _setup_layout(self):
        """Arrange widgets in the main layout"""
        # Add title and subtitle
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

        # Create button layout
        self._create_button_layout()

    def _create_button_layout(self):
        """Create the bottom button layout with status label"""
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

    # === UI INTERACTION METHODS ===

    def toggle_comparison_options(self):
        """Toggle container visibility based on selected radio button"""
        self.overview_container.setVisible(self.data_overview.isChecked())
        self.statistical_container.setVisible(self.statistical_comparison.isChecked())
        self.visual_container.setVisible(self.visual_comparison.isChecked())

        # Populate tables/canvas when containers become visible
        self._populate_active_content()

    def _populate_active_content(self):
        """Populate the currently active content based on selected option"""
        if self.data_overview.isChecked() and self.dataset_info:
            self.populate_overview_table()
        elif self.statistical_comparison.isChecked() and self.dataset_info:
            self.populate_statistical_table()
        elif self.visual_comparison.isChecked() and self.dataset_info:
            self.populate_visual_options()

    # === TABLE POPULATION METHODS ===
    
    def populate_overview_table(self):
        """Populate overview comparison table"""
        self.overview_table.setRowCount(0)

        if not self.dataset_info:
            return

        try:
            original_info = self.dataset_info.get_general_info_original()
            current_info = self.dataset_info.get_general_info()

            metrics = self._get_overview_metrics(original_info, current_info)
            for metric, original, processed in metrics:
                self._add_overview_row(metric, original, processed)

        except Exception as e:
            print(f"[ERROR] -> Failed to populate overview table: {e}")

    def _get_overview_metrics(self, original_info, current_info):
        """Get overview metrics for comparison"""
        return [
            ("Number of Rows", original_info.get("num_rows", 0), current_info.get("num_rows", 0)),
            ("Number of Columns", original_info.get("num_columns", 0), current_info.get("num_columns", 0)),
            ("Total Null Values", self._count_total_nulls(original_info), self._count_total_nulls(current_info)),
            ("Memory Usage", "N/A", "N/A"),
        ]

    def _add_overview_row(self, metric, original, processed):
        """Add a row to the overview table"""
        row = self.overview_table.rowCount()
        self.overview_table.insertRow(row)

        self.overview_table.setItem(row, 0, QTableWidgetItem(metric))
        self.overview_table.setItem(row, 1, QTableWidgetItem(str(original)))
        self.overview_table.setItem(row, 2, QTableWidgetItem(str(processed)))

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

            # Get common numerical columns
            common_cols = self._get_common_numerical_columns(original_df, current_df)
            
            for col in common_cols:
                self._add_statistical_row(original_df, current_df, col)

        except Exception as e:
            print(f"[ERROR] -> Failed to populate statistical table: {e}")

    def _get_common_numerical_columns(self, original_df, current_df):
        """Get numerical columns that exist in both dataframes"""
        original_cols = set(original_df.columns)
        current_cols = set(current_df.columns)
        common_cols = original_cols.intersection(current_cols)
        
        return [col for col in common_cols if self._is_numerical_column(original_df, col)]

    def _add_statistical_row(self, original_df, current_df, col):
        """Add a statistical comparison row"""
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

    def populate_visual_options(self):
        """Populate visual comparison options"""
        if not hasattr(self, 'column_table'):
            return

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
                self._add_visual_option_row(col)

        except Exception as e:
            print(f"[ERROR] -> Failed to populate visual options: {e}")

    def _add_visual_option_row(self, col):
        """Add a row to the visual options table"""
        row = self.column_table.rowCount()
        self.column_table.insertRow(row)

        col_item = QTableWidgetItem(col)
        col_item.setFlags(col_item.flags() & ~Qt.ItemIsEditable)
        self.column_table.setItem(row, 0, col_item)

        visualize_combo = QComboBox()
        visualize_combo.addItems(["No", "Histogram", "Box Plot", "Scatter Plot"])
        visualize_combo.setStyleSheet(self._combo_style())
        self.column_table.setCellWidget(row, 1, visualize_combo)

    # === COMPARISON GENERATION METHODS ===

    def generate_comparison(self):
        """Generate comparison based on selected option"""
        if not self.dataset_info:
            self.show_status("No dataset available for comparison", "red")
            return

        try:
            if self.data_overview.isChecked():
                self._generate_overview_comparison()
            elif self.statistical_comparison.isChecked():
                self._generate_statistical_comparison()
            elif self.visual_comparison.isChecked():
                self._generate_visual_comparison()
            else:
                self.show_status("Please select a comparison option", "orange")

        except Exception as e:
            print(f"[ERROR] -> Failed to generate comparison: {e}")
            self.show_status(f"Error generating comparison: {e}", "red")

    def _generate_overview_comparison(self):
        """Generate overview comparison"""
        print("[INFO] -> Generating overview comparison")
        if self.overview_table.rowCount() == 0:
            self.populate_overview_table()
        
        self.show_status("Overview comparison generated successfully", "green")

    def _generate_statistical_comparison(self):
        """Generate statistical comparison"""
        print("[INFO] -> Generating statistical comparison")
        if self.statistical_table.rowCount() == 0:
            self.populate_statistical_table()
        
        self.show_status("Statistical comparison generated successfully", "green")

    def _generate_visual_comparison(self):
        """Generate visual comparison"""
        print("[INFO] -> Generating visual comparison")
        
        if not hasattr(self, 'column_table'):
            self.show_status("Visual comparison not available", "orange")
            return

        # Get selected visualization options
        visualizations = self._get_selected_visualizations()
        
        if not visualizations:
            self.show_status("No columns selected for visualization", "orange")
            return

        # Create visualizations
        self._create_visualizations(visualizations)
        self.show_status("Visual comparison generated successfully", "green")

    def _get_selected_visualizations(self):
        """Get selected visualization options from table"""
        visualizations = []
        
        for row in range(self.column_table.rowCount()):
            col_item = self.column_table.item(row, 0)
            combo_widget = self.column_table.cellWidget(row, 1)
            
            if col_item and combo_widget:
                column = col_item.text()
                viz_type = combo_widget.currentText()
                
                if viz_type != "No":
                    visualizations.append((column, viz_type))
        
        return visualizations

    def _create_visualizations(self, visualizations):
        """Create visualizations based on selected options"""
        try:
            original_df = self.dataset_info.get_dataframe_original()
            current_df = self.dataset_info.get_dataframe()
            
            if original_df is None or current_df is None:
                self.show_status("Original or current dataframe not available", "orange")
                return

            # Clear previous plots
            self.visual_canvas.figure.clear()
            
            # Create subplots based on number of visualizations
            num_plots = len(visualizations)
            cols = min(2, num_plots)
            rows = (num_plots + cols - 1) // cols
            
            for i, (column, viz_type) in enumerate(visualizations):
                ax = self.visual_canvas.figure.add_subplot(rows, cols, i + 1)
                self._create_single_visualization(ax, original_df, current_df, column, viz_type)
            
            self.visual_canvas.figure.tight_layout()
            self.visual_canvas.draw()

        except Exception as e:
            print(f"[ERROR] -> Failed to create visualizations: {e}")
            self.show_status(f"Error creating visualizations: {e}", "red")

    def _create_single_visualization(self, ax, original_df, current_df, column, viz_type):
        """Create a single visualization"""
        if viz_type == "Histogram":
            self._create_histogram(ax, original_df, current_df, column)
        elif viz_type == "Box Plot":
            self._create_boxplot(ax, original_df, current_df, column)
        elif viz_type == "Scatter Plot":
            self._create_scatterplot(ax, original_df, current_df, column)

    def _create_histogram(self, ax, original_df, current_df, column):
        """Create histogram comparison"""
        try:
            # Convert to pandas for easier plotting
            original_data = original_df.select(column).toPandas()[column].dropna()
            current_data = current_df.select(column).toPandas()[column].dropna()

            ax.hist(original_data, alpha=0.7, label='Original', bins=30, color='blue')
            ax.hist(current_data, alpha=0.7, label='Processed', bins=30, color='red')
            ax.legend()
            ax.set_xlabel(column)
            ax.set_ylabel('Frequency')
            ax.set_title(f'{column} - Histogram')
        except Exception as e:
            ax.text(0.5, 0.5, f"Error: {str(e)}", transform=ax.transAxes, ha='center')

    def _create_boxplot(self, ax, original_df, current_df, column):
        """Create box plot comparison"""
        try:
            # Convert to pandas for easier plotting
            original_data = original_df.select(column).toPandas()[column].dropna()
            current_data = current_df.select(column).toPandas()[column].dropna()

            ax.boxplot([original_data, current_data], labels=['Original', 'Processed'])
            ax.set_ylabel(column)
            ax.set_title(f'{column} - Box Plot')
        except Exception as e:
            ax.text(0.5, 0.5, f"Error: {str(e)}", transform=ax.transAxes, ha='center')

    def _create_scatterplot(self, ax, original_df, current_df, column):
        """Create scatter plot comparison"""
        try:
            # For scatter plot, we'll plot original vs processed values
            original_data = original_df.select(column).toPandas()[column].dropna()
            current_data = current_df.select(column).toPandas()[column].dropna()

            # Ensure same length
            min_len = min(len(original_data), len(current_data))
            original_data = original_data[:min_len]
            current_data = current_data[:min_len]

            ax.scatter(original_data, current_data, alpha=0.6)
            ax.plot([original_data.min(), original_data.max()], 
                   [original_data.min(), original_data.max()], 'r--', alpha=0.8)
            ax.set_xlabel(f'{column} (Original)')
            ax.set_ylabel(f'{column} (Processed)')
            ax.set_title(f'{column} - Original vs Processed')
        except Exception as e:
            ax.text(0.5, 0.5, f"Error: {str(e)}", transform=ax.transAxes, ha='center')

    # === UTILITY METHODS ===
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
        """Set the dataset info and trigger auto-population if needed"""
        self.dataset_info = dataset_info
        # Auto-populate if data overview is selected
        if self.data_overview.isChecked():
            self.populate_overview_table()

    # === STYLING METHODS ===

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
        """Setup styles for all UI components"""
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
        """Install responsive resize event handler"""
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
        """Set UI text translations"""
        MainWindow.setWindowTitle(QCoreApplication.translate("MainWindow", u"Dataset Preprocessor", None))