import sys
from PySide6.QtWidgets import QApplication, QMainWindow
from AppUi.initial_window import *


if __name__ == "__main__":
    app = QApplication(sys.argv)
    app.setApplicationName("Dataset Preprocesor")  # <-- Añade esto
    window = QMainWindow()
    ui = Ui_MainWindow()
    ui.setupUi(window)
    window.show()
    sys.exit(app.exec())