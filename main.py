import sys
import pandas as pd
from pyspark.sql import SparkSession
from PySide6.QtWidgets import QApplication, QMainWindow, QStackedWidget
from src.AppUi.initial_window import *
from src.AppUi.preview_phase import *
from src.AppUi.cleaning_phase import *
from src.AppUi.imputation_phase import *
from src.AppUi.transformation_phase import *
from src.AppUi.data_splitting_phase import *
from src.AppUi.comparison_phase import *
from src.AppUi.final_window import *
from src.utils import *

class MainController:
    def __init__(self):
        self.app = QApplication(sys.argv)
        self.app.setApplicationName("Dataset Preprocesor")
        
        # Initializing SparkSession
        self.spark = SparkSession.builder \
            .appName("Dataset Preprocessor") \
            .getOrCreate()
        
        
        # Initializing the stacked widget
        self.stacked_widget = QStackedWidget()
        
        # Initializing the main window
        self.initial_window = QMainWindow()
        self.preview_window = QMainWindow()
        self.cleaning_window = QMainWindow()
        self.imputation_window = QMainWindow()
        self.transformation_window = QMainWindow()
        self.data_splitting_window = QMainWindow()
        self.comparison_window = QMainWindow()
        self.final_window = QMainWindow()

        # Creating the instances of the UI classes
        self.initial_ui = Ui_initial_phase()
        self.preview_ui = Ui_preview_phase()
        self.cleaning_ui = Ui_cleaning_phase()
        self.imputation_ui = Ui_imputation_phase()
        self.transformation_ui = Ui_transformation_phase()
        self.data_splitting_ui = Ui_data_phase()
        self.comparison_ui = Ui_comparison_phase()
        self.final_ui = Ui_final_window()

        # Configuring the interfaces
        self.initial_ui.setupUi(self.initial_window)
        self.preview_ui.setupUi(self.preview_window)
        self.cleaning_ui.setupUi(self.cleaning_window)
        self.imputation_ui.setupUi(self.imputation_window)
        self.transformation_ui.setupUi(self.transformation_window)
        self.data_splitting_ui.setupUi(self.data_splitting_window)
        self.comparison_ui.setupUi(self.comparison_window)
        self.final_ui.setupUi(self.final_window)

        # Adding windows to the stacked widget
        self.stacked_widget.addWidget(self.initial_window)
        self.stacked_widget.addWidget(self.preview_window)
        self.stacked_widget.addWidget(self.cleaning_window)
        self.stacked_widget.addWidget(self.imputation_window)
        self.stacked_widget.addWidget(self.transformation_window)
        self.stacked_widget.addWidget(self.data_splitting_window)
        self.stacked_widget.addWidget(self.comparison_window)
        self.stacked_widget.addWidget(self.final_window)

        # Connecting signals
        self.initial_ui.pushButton.clicked.connect(self.show_preview)
        self.initial_ui.pushButton.clicked.connect(self.load_dataset)

        self.preview_ui.pushButton.clicked.connect(self.show_cleaning)
        self.preview_ui.pushButton2.clicked.connect(self.show_initial)

        self.cleaning_ui.pushButton.clicked.connect(self.show_imputation)
        self.cleaning_ui.pushButton2.clicked.connect(self.show_preview)
        
        self.imputation_ui.pushButton.clicked.connect(self.show_transformation)
        self.imputation_ui.pushButton2.clicked.connect(self.show_cleaning)
        
        self.transformation_ui.pushButton.clicked.connect(self.show_data_splitting)
        self.transformation_ui.pushButton2.clicked.connect(self.show_imputation)
        
        self.data_splitting_ui.pushButton.clicked.connect(self.show_comparison)
        self.data_splitting_ui.pushButton2.clicked.connect(self.show_transformation)
        
        self.comparison_ui.pushButton.clicked.connect(self.show_final)
        self.comparison_ui.pushButton2.clicked.connect(self.show_data_splitting)

        self.final_ui.pushButton.clicked.connect(self.app.quit)
        self.final_ui.pushButton2.clicked.connect(self.show_comparison)
        
        self.df_spark = None
        

    """
    TO CHANGE BETWEEN WINDOWS
    """
    def show_initial(self):
        self.stacked_widget.setCurrentIndex(0)

    def show_preview(self):
        self.stacked_widget.setCurrentIndex(1)

    def show_cleaning(self):
        self.stacked_widget.setCurrentIndex(2)
    
    def show_imputation(self):
        self.stacked_widget.setCurrentIndex(3)

    def show_transformation(self):
        self.stacked_widget.setCurrentIndex(4)

    def show_data_splitting(self):
        self.stacked_widget.setCurrentIndex(5)

    def show_comparison(self):
        self.stacked_widget.setCurrentIndex(6)

    def show_final(self):
        self.stacked_widget.setCurrentIndex(7)  


    """
    TO LOAD DATASET
    """
    def load_dataset(self):
        file_path = self.initial_ui.file_path
        file_name = self.initial_ui.file_name
        if file_path:
            try:
                self.df_spark = self.spark.read.option("header", "true").csv(file_path, inferSchema=True)
                print(f"[INFO]: Dataset loaded correctly in Spark: {file_name}")
                
                if hasattr(self.preview_ui, 'populate_table'):
                    self.preview_ui.populate_table(self.df_spark)
                    self.cleaning_ui.populate_table(self.df_spark)
                    self.cleaning_ui.setup_connections(self.df_spark)
                    self.cleaning_ui.apply_null_filter(self.df_spark)

            except Exception as e:
                print(f"[ERROR]: When trying to load the dataset with Spark: {e}")
        else:
            print("[INFO]: No file selected")
        

    def run(self):
        self.stacked_widget.show()
        sys.exit(self.app.exec())

if __name__ == "__main__":
    main = MainController()
    main.run()