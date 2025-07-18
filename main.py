import sys
import pandas as pd
from pyspark.sql import SparkSession
from PySide6.QtWidgets import QApplication, QMainWindow, QStackedWidget

"""
UI IMPORTS
"""
from src.AppUi.initial_window import *
from src.AppUi.preview_phase import *

from src.AppUi.cleaning_phase import *
from src.AppUi.cleaning.delete_ui import *

from src.AppUi.imputation_phase import *
from src.AppUi.transformation_phase import *
from src.AppUi.data_splitting_phase import *
from src.AppUi.comparison_phase import *
from src.AppUi.final_window import *
from src.utils import *
from src.logic.transformation import Transformation


"""
LOGIC IMPORTS
"""
from src.logic.cleaning import *
from src.logic.dataset_info import *


class MainController:
    def __init__(self):
        self.app = QApplication(sys.argv)
        self.app.setApplicationName("Dataset Preprocesor")
        
        # Initializing SparkSession
        self.spark = SparkSession.builder \
            .appName("Dataset Preprocessor") \
            .master("local[*]") \
            .getOrCreate()
                
        self.cleaning_logic = None
        self.stacked_widget = QStackedWidget()
        
        # Initializing the main window
        self.initial_window = QMainWindow()
        self.preview_window = QMainWindow()
        
        self.cleaning_window = QMainWindow()
        self.delete_window = QMainWindow()

        self.transformation_window = QMainWindow()
        self.imputation_window = QMainWindow()

        self.data_splitting_window = QMainWindow()
        self.comparison_window = QMainWindow()
        self.final_window = QMainWindow()

        # Creating the instances of the UI classes
        self.initial_ui = Ui_initial_phase()
        self.preview_ui = Ui_preview_phase()
        self.cleaning_ui = Ui_cleaning_phase()
        self.delete_ui = Ui_delete()
        self.transformation_ui = Ui_transformation_phase()
        self.imputation_ui = Ui_imputation_phase()
        self.data_splitting_ui = Ui_data_phase()
        self.comparison_ui = Ui_comparison_phase()
        self.final_ui = Ui_final_window()

        # Configuring the interfaces
        self.initial_ui.setupUi(self.initial_window)
        self.preview_ui.setupUi(self.preview_window)
        self.cleaning_ui.setupUi(self.cleaning_window)
        self.delete_ui.setupUi(self.delete_window)
        self.delete_window.hide()
        self.transformation_ui.setupUi(self.transformation_window)
        self.imputation_ui.setupUi(self.imputation_window)
        self.data_splitting_ui.setupUi(self.data_splitting_window)
        self.comparison_ui.setupUi(self.comparison_window)
        self.final_ui.setupUi(self.final_window)

        # Adding windows to the stacked widget
        self.stacked_widget.addWidget(self.initial_window)
        self.stacked_widget.addWidget(self.preview_window)
        self.stacked_widget.addWidget(self.cleaning_window)
        self.stacked_widget.addWidget(self.transformation_window)
        self.stacked_widget.addWidget(self.imputation_window)
        self.stacked_widget.addWidget(self.data_splitting_window)
        self.stacked_widget.addWidget(self.comparison_window)
        self.stacked_widget.addWidget(self.final_window)

        # Connecting signals
        self.initial_ui.pushButton.clicked.connect(self.show_preview)
        self.initial_ui.pushButton.clicked.connect(self.load_dataset)

        self.preview_ui.pushButton.clicked.connect(self.show_cleaning)
        self.preview_ui.pushButton2.clicked.connect(self.show_initial)


        self.cleaning_ui.pushButton.clicked.connect(self.show_transformation)
        self.cleaning_ui.pushButton2.clicked.connect(self.show_preview)
        self.cleaning_ui.pushButton3.clicked.connect(lambda: self.show_delete(True))
        
        self.delete_ui.pushButton2.clicked.connect(lambda: self.show_delete(False))


        self.transformation_ui.pushButton.clicked.connect(self.show_imputation)
        self.transformation_ui.pushButton2.clicked.connect(self.show_cleaning)

        self.imputation_ui.pushButton.clicked.connect(self.show_data_splitting)
        self.imputation_ui.pushButton2.clicked.connect(self.show_transformation)
        

        self.data_splitting_ui.pushButton.clicked.connect(self.show_comparison)
        self.data_splitting_ui.pushButton2.clicked.connect(self.show_imputation)
        
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
        # Set reference to cleaning_logic in the UI
        if hasattr(self.cleaning_ui, 'cleaning_logic'):
            self.cleaning_ui.cleaning_logic = self.cleaning_logic
        
        # Make sure to apply sorting if the user has changed the sort option
        if hasattr(self.cleaning_ui, 'sort_table_by_nulls'):
            self.cleaning_ui.sort_table_by_nulls()

    def show_delete(self, value):
        if value == True:
            self.delete_window.show()
            

        else:
            self.delete_window.hide()


    def show_transformation(self):
        try:
            self.transformation_ui.set_dataset_info(self.delete_ui.dataset_info)
        except Exception as e:
            print(f"[ERROR] -> [When trying to set dataset info in imputation UI] {e}")
        self.stacked_widget.setCurrentIndex(3)


    def show_imputation(self):
        try:
            self.imputation_ui.set_dataset_info(self.transformation_ui.dataset_info)
        except Exception as e:
            print(f"[ERROR] -> [When trying to set dataset info in imputation UI] {e}")
        self.stacked_widget.setCurrentIndex(4)
        



    def show_data_splitting(self):
        try:
            self.data_splitting_ui.set_dataset_info(self.imputation_ui.dataset_info)
        except Exception as e:
            print(f"[ERROR] -> [When trying to set dataset info in data splitting UI] {e}")
        self.stacked_widget.setCurrentIndex(5)

    def show_comparison(self):
        try:
            self.comparison_ui.set_dataset_info(self.data_splitting_ui.dataset_info)
        except Exception as e:
            print(f"[ERROR] -> [When trying to set dataset info in data comparison UI] {e}")
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
                #self.df_spark.count()
                
                self.dataset_info = DatasetInfo(self.spark, self.df_spark)
                self.dataset_info.set_dataframe_original(self.df_spark)
                self.dataset_info.set_general_info()
                self.dataset_info.set_general_info_original()
                
                self.cleaning_logic = cleaning(self.dataset_info)
                
                print(f"[INFO] -> [Dataset loaded correctly in Spark] {file_name}")
                
                if hasattr(self.preview_ui, 'populate_table'):
                    self.preview_ui.populate_table(self.dataset_info)
                    self.cleaning_ui.populate_table(self.dataset_info)
                    
                    
                    if hasattr(self.cleaning_ui, 'dataset_info'):
                        self.cleaning_ui.dataset_info = self.dataset_info
                
                if hasattr(self.delete_ui, 'cleaning_logic'):
                    self.delete_ui.cleaning_logic = self.cleaning_logic
                    self.delete_ui.dataset_info = self.cleaning_ui.dataset_info
                    self.delete_ui.set_utils_cleaning_phase(self.cleaning_ui.get_utils())

                if hasattr(self.transformation_ui, 'set_dataset_info'):
                    pass
                if hasattr(self.imputation_ui, 'set_dataset_info'):
                    pass



            except Exception as e:
                print(f"[ERROR] -> [When trying to load the dataset with Spark] {e}")
        else:
            print("[INFO]: No file selected")
        

    def run(self):
        self.stacked_widget.show()
        sys.exit(self.app.exec())

if __name__ == "__main__":
    main = MainController()
    main.run()