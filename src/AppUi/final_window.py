# -*- coding: utf-8 -*-

import os
import sys
from PySide6.QtCore import QCoreApplication, Qt
from PySide6.QtGui import QIcon, QPixmap
from PySide6.QtWidgets import QMainWindow, QLabel, QPushButton, QSizePolicy, QVBoxLayout, QWidget, QHBoxLayout, QFileDialog


def resource_path(relative_path):
    """Get the absolute path to a resource, works for dev and packaged apps"""
    if getattr(sys, 'frozen', False):  # If running as a PyInstaller bundle
        base_path = sys._MEIPASS
    else:  # If running in a normal Python environment
        base_path = os.path.abspath(".")
    
    return os.path.join(base_path, relative_path)


class Ui_final_window(object):
    """
    Final window UI class for the Dataset Preprocessor application.
    
    This class provides the completion screen shown after successful dataset preprocessing,
    displaying congratulations message and navigation options.
    """
    
    def setupUi(self, MainWindow):
        """Initialize and configure the final window UI"""
        self.MainWindow = MainWindow
        
        # Configure main window properties
        self._setup_main_window()
        
        # Create central widget and layout
        self._create_central_widget()
        
        # Create UI components
        self._create_widgets()
        
        # Configure widget properties and layout
        self._setup_widget_properties()
        self._setup_layout()
        
        # Apply styling and responsive design
        self.setup_styles()
        self._install_resize_event()
        
        # Set central widget
        MainWindow.setCentralWidget(self.centralwidget)

    # === SETUP METHODS ===

    def _setup_main_window(self):
        """Configure main window properties"""
        if not self.MainWindow.objectName():
            self.MainWindow.setObjectName(u"MainWindow")
        self.MainWindow.resize(900, 600)
        self.MainWindow.setMinimumSize(600, 400)

    def _create_central_widget(self):
        """Create and configure the central widget"""
        self.centralwidget = QWidget(self.MainWindow)
        self.centralwidget.setObjectName(u"centralwidget")
        
        # Main vertical layout with proper spacing
        self.verticalLayout = QVBoxLayout(self.centralwidget)
        self.verticalLayout.setContentsMargins(20, 20, 20, 20)
        self.verticalLayout.setSpacing(20)

    def _create_widgets(self):
        """Create all UI widgets"""
        # Title and subtitle labels
        self.title_label = QLabel("Finished", self.centralwidget)
        self.subtitle_label = QLabel("Congratulations, you've finished preprocessing your dataset", self.centralwidget)
        
        # Action buttons
        self.pushButton = QPushButton("Terminate", self.centralwidget)
        self.pushButton2 = QPushButton("Back", self.centralwidget)

    def _setup_widget_properties(self):
        """Configure widget properties and alignment"""
        # Center-align title and subtitle
        self.title_label.setAlignment(Qt.AlignCenter)
        self.subtitle_label.setAlignment(Qt.AlignCenter)
        
        # Set button size policies for consistent appearance
        self.pushButton.setSizePolicy(QSizePolicy.Fixed, QSizePolicy.Fixed)
        self.pushButton2.setSizePolicy(QSizePolicy.Fixed, QSizePolicy.Fixed)

    def _setup_layout(self):
        """Arrange widgets in the main layout"""
        # Add title and subtitle at the top
        self.verticalLayout.addWidget(self.title_label)
        self.verticalLayout.addWidget(self.subtitle_label)
        
        # Add stretch to push buttons to bottom
        self.verticalLayout.addStretch(1)
        
        # Create and configure button layout
        self._create_button_layout()

    def _create_button_layout(self):
        """Create the bottom button layout"""
        # Create horizontal layout for buttons
        button_container = QHBoxLayout()
        button_container.setContentsMargins(0, 0, 0, 0)
        button_container.setSpacing(10)
        
        # Add stretch to push buttons to the right
        button_container.addStretch()
        
        # Add buttons in order (Back first, then Terminate)
        button_container.addWidget(self.pushButton2)
        button_container.addWidget(self.pushButton)
        
        # Add button container to main layout
        self.verticalLayout.addLayout(button_container)

    # === STYLING METHODS ===

    def setup_styles(self, title_size=36, subtitle_size=18, button_size=4):
        """Setup styles for all UI components"""
        # Define color palette
        colors = self._get_color_palette()
        
        # Apply background style
        self._apply_background_style(colors)
        
        # Create and apply component styles
        title_style = self._create_title_style(colors, title_size)
        subtitle_style = self._create_subtitle_style(colors, subtitle_size)
        button_style = self._create_button_style(colors, button_size)
        button_back_style = self._create_button_back_style(colors, button_size)
        
        # Apply styles to components
        self.title_label.setStyleSheet(title_style)
        self.subtitle_label.setStyleSheet(subtitle_style)
        self.pushButton.setStyleSheet(button_style)
        self.pushButton2.setStyleSheet(button_back_style)

    def _get_color_palette(self):
        """Get the application color palette"""
        return {
            'background': '#f5f5f5',
            'title': '#333333',
            'subtitle': '#555555',
            'button': '#0078d7',
            'button_back': '#F7F7F7',
            'button_hover': '#005a9e',
            'text_white': 'white',
            'text_black': 'black',
            'border_gray': '#808080',
            'hover_gray': '#e0e0e0'
        }

    def _apply_background_style(self, colors):
        """Apply background styling to central widget"""
        self.centralwidget.setStyleSheet(f"""
            QWidget {{
                background-color: {colors['background']};
            }}
        """)

    def _create_title_style(self, colors, title_size):
        """Create title label style"""
        return f"""
            QLabel {{
                color: {colors['title']};
                font-size: {title_size}px;
                font-weight: bold;
            }}
        """

    def _create_subtitle_style(self, colors, subtitle_size):
        """Create subtitle label style"""
        return f"""
            QLabel {{
                color: {colors['subtitle']};
                font-size: {subtitle_size}px;
            }}
        """

    def _create_button_style(self, colors, button_size):
        """Create primary button style"""
        padding_v = max(2, int(button_size * 0.5))
        padding_h = max(5, int(button_size * 1.5))
        
        return f"""
            QPushButton {{
                background-color: {colors['button']};
                color: {colors['text_white']};
                font-size: {button_size}px;
                font-weight: bold;
                border: none;
                border-radius: 5px;
                padding: {padding_v}px {padding_h}px;
                min-width: 50px;
            }}
            QPushButton:hover {{
                background-color: {colors['button_hover']};
            }}
        """

    def _create_button_back_style(self, colors, button_size):
        """Create secondary button style"""
        padding_v = max(2, int(button_size * 0.5))
        padding_h = max(5, int(button_size * 1.5))
        
        return f"""
            QPushButton {{
                background-color: {colors['button_back']};
                color: {colors['text_black']};
                font-size: {button_size}px;
                font-weight: bold;
                border: 1px solid {colors['border_gray']};
                border-radius: 5px;
                padding: {padding_v}px {padding_h}px;
                min-width: 50px;
            }}
            QPushButton:hover {{
                background-color: {colors['hover_gray']};
                border: 1px solid {colors['border_gray']};
            }}
        """

    def _install_resize_event(self):
        """Install responsive resize event handler"""
        original_resize_event = self.MainWindow.resizeEvent

        def new_resize_event(event):
            # Calculate responsive sizes based on window height
            new_title_size = int(self.MainWindow.height() * 0.06)
            new_subtitle_size = int(self.MainWindow.height() * 0.03)
            new_button_size = int(self.MainWindow.height() * 0.021)

            # Apply responsive styling
            self.setup_styles(
                title_size=new_title_size,
                subtitle_size=new_subtitle_size,
                button_size=new_button_size,
            )
            
            # Call original resize event
            original_resize_event(event)

        self.MainWindow.resizeEvent = new_resize_event

    def retranslateUi(self, MainWindow):
        """Set UI text translations"""
        MainWindow.setWindowTitle(QCoreApplication.translate("MainWindow", u"Dataset Preprocessor", None))