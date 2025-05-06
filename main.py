import sys
from PySide6.QtWidgets import QApplication, QMainWindow, QStackedWidget
from AppUi.initial_window import *
from AppUi.cleaning_phase import *
from AppUi.imputation_phase import *
from AppUi.transformation_phase import *
from AppUi.data_splitting_phase import *
from AppUi.comparison_phase import *
from AppUi.final_window import *

class MainController:
    def __init__(self):
        self.app = QApplication(sys.argv)
        self.app.setApplicationName("Dataset Preprocesor")
        
        # Configurar el widget apilado
        self.stacked_widget = QStackedWidget()
        
        # Inicializar ventanas y UIs
        self.initial_window = QMainWindow()
        self.cleaning_window = QMainWindow()
        self.imputation_window = QMainWindow()
        self.transformation_window = QMainWindow()
        self.data_splitting_window = QMainWindow()
        self.comparison_window = QMainWindow()
        self.final_window = QMainWindow()

        # Crear instancias de las interfaces
        self.initial_ui = Ui_initial_phase()
        self.cleaning_ui = Ui_cleaning_phase()
        self.imputation_ui = Ui_imputation_phase()
        self.transformation_ui = Ui_transformation_phase()
        self.data_splitting_ui = Ui_data_phase()
        self.comparison_ui = Ui_comparison_phase()
        self.final_ui = Ui_final_window()

        # Configurar las interfaces en las ventanas
        self.initial_ui.setupUi(self.initial_window)
        self.cleaning_ui.setupUi(self.cleaning_window)
        self.imputation_ui.setupUi(self.imputation_window)
        self.transformation_ui.setupUi(self.transformation_window)
        self.data_splitting_ui.setupUi(self.data_splitting_window)
        self.comparison_ui.setupUi(self.comparison_window)
        self.final_ui.setupUi(self.final_window)

        # Agregar ventanas al stacked widget
        self.stacked_widget.addWidget(self.initial_window)
        self.stacked_widget.addWidget(self.cleaning_window)
        self.stacked_widget.addWidget(self.imputation_window)
        self.stacked_widget.addWidget(self.transformation_window)
        self.stacked_widget.addWidget(self.data_splitting_window)
        self.stacked_widget.addWidget(self.comparison_window)
        self.stacked_widget.addWidget(self.final_window)

        # Conectar señales
        self.initial_ui.pushButton.clicked.connect(self.show_cleaning)

        self.cleaning_ui.pushButton.clicked.connect(self.show_imputation)
        self.cleaning_ui.pushButton2.clicked.connect(self.show_initial)
        
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

    def show_initial(self):
        self.stacked_widget.setCurrentIndex(0)

    def show_cleaning(self):
        self.stacked_widget.setCurrentIndex(1)
    
    def show_imputation(self):
        self.stacked_widget.setCurrentIndex(2)

    def show_transformation(self):
        self.stacked_widget.setCurrentIndex(3)

    def show_data_splitting(self):
        self.stacked_widget.setCurrentIndex(4)

    def show_comparison(self):
        self.stacked_widget.setCurrentIndex(5)

    def show_final(self):
        self.stacked_widget.setCurrentIndex(6)  # Corregido el índice

    def run(self):
        self.stacked_widget.show()
        sys.exit(self.app.exec())

if __name__ == "__main__":
    main = MainController()
    main.run()