# -*- coding: utf-8 -*-

from PySide6.QtCore import (QCoreApplication, QDate, QDateTime, QLocale,
    QMetaObject, QObject, QPoint, QRect,
    QSize, QTime, QUrl, Qt)
from PySide6.QtGui import (QBrush, QColor, QConicalGradient, QCursor,
    QFont, QFontDatabase, QGradient, QIcon,
    QImage, QKeySequence, QLinearGradient, QPainter,
    QPalette, QPixmap, QRadialGradient, QTransform)
from PySide6.QtWidgets import (
    QApplication, QMainWindow, QMenuBar, QPushButton,
    QSizePolicy, QStatusBar, QWidget, QVBoxLayout, QSpacerItem, QLabel
)

background_image = "./AppUi/Images/sky_image.jpg"

class Ui_cleaning_phase(object):
    def setupUi(self, MainWindow):
        self.MainWindow = MainWindow
        if not MainWindow.objectName():
            MainWindow.setObjectName(u"MainWindow")
        MainWindow.resize(800, 600)
        MainWindow.setMinimumSize(400, 300)

        self.centralwidget = QWidget(MainWindow)
        self.centralwidget.setObjectName(u"centralwidget")

        # Layout principal
        self.verticalLayout = QVBoxLayout(self.centralwidget)
        self.verticalLayout.setContentsMargins(0, 0, 0, 0)
        self.verticalLayout.setSpacing(0)

        # Fondo de imagen
        self.background_label = QLabel(self.centralwidget)
        self.background_label.setObjectName("background_label")
        self.background_label.setScaledContents(True)
        self.background_label.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        self.verticalLayout.addWidget(self.background_label)
        self.background_label.setPixmap(QPixmap(background_image))

        # Layout superpuesto para elementos
        self.overlay_layout = QVBoxLayout(self.background_label)
        self.overlay_layout.setContentsMargins(20, 20, 20, 20)
        
        # Label cleaning phase
        self.cleaning_phase = QLabel("Cleaning", self.background_label)
        self.cleaning_phase.setAlignment(Qt.AlignCenter)
        self.cleaning_phase.setStyleSheet("color: white;")
        self.overlay_layout.addWidget(self.cleaning_phase, 0.5)  # 1/4 del espacio

        # Espaciador flexible
        self.overlay_layout.addStretch(2)  # 2/4 del espacio

        # Botón
        self.pushButton = QPushButton("Continue", self.background_label)
        self.pushButton.setStyleSheet("""
            QPushButton {
                border: 2px solid white;
                border-radius: 5px;
                background-color: transparent;
                color: white;
                min-width: 120px;
                max-width: 180px;
                min-height: 35px;
                padding: 8px;
            }
            QPushButton:hover {
                background-color: rgba(255, 255, 255, 30);
            }
        """)
        self.pushButton.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        self.overlay_layout.addWidget(self.pushButton, 1, Qt.AlignHCenter)  # 1/4 del espacio

        MainWindow.setCentralWidget(self.centralwidget)
        
        # Configurar fuente responsive
        self._install_resize_event()

    def _install_resize_event(self):
        original_resize_event = self.MainWindow.resizeEvent
        
        def new_resize_event(event):
            # Ajustar tamaño de fuentes
            new_size = int(self.MainWindow.height() * 0.05)
            self.cleaning_phase.setStyleSheet(f"""
                color: white;
                font-size: {new_size}px;
                font-weight: bold;
            """)
            
            btn_size = int(self.MainWindow.height() * 0.025)
            self.pushButton.setStyleSheet(f"""
                QPushButton {{
                    border: 2px solid white;
                    border-radius: 5px;
                    background-color: transparent;
                    color: white;
                    font-size: {btn_size}px;
                    min-width: {int(self.MainWindow.width()//5)}px;  
                    max-width: {int(self.MainWindow.width()//2.5)}px;  
                    min-height: {int(self.MainWindow.height()//18)}px;  
                    padding: {max(5, int(btn_size*0.4))}px;  
                }}
                QPushButton:hover {{
                    background-color: rgba(255, 255, 255, 30);
                }}
            """)
            original_resize_event(event)
            
        self.MainWindow.resizeEvent = new_resize_event

    def retranslateUi(self, MainWindow):
        MainWindow.setWindowTitle(QCoreApplication.translate("MainWindow", u"Dataset Preprocesor", None))