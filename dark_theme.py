#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Dark Theme Stylesheet for DICOM-RT-Station
Modern dark mode styling with improved aesthetics
"""

DARK_THEME = """
/* Main Window and Base Widgets */
QMainWindow, QDialog, QWidget {
    background-color: #1e1e1e;
    color: #e0e0e0;
    font-family: 'Segoe UI', Arial, sans-serif;
    font-size: 9pt;
}

/* Group Boxes */
QGroupBox {
    background-color: #252525;
    border: 1px solid #3d3d3d;
    border-radius: 6px;
    margin-top: 12px;
    padding-top: 8px;
    font-weight: bold;
    color: #ffffff;
}

QGroupBox::title {
    subcontrol-origin: margin;
    subcontrol-position: top left;
    padding: 4px 8px;
    background-color: #2d2d2d;
    border-radius: 4px;
    color: #4fc3f7;
}

/* Buttons */
QPushButton {
    background-color: #2d2d2d;
    color: #e0e0e0;
    border: 1px solid #3d3d3d;
    border-radius: 4px;
    padding: 6px 16px;
    font-weight: 500;
    min-height: 24px;
}

QPushButton:hover {
    background-color: #3d3d3d;
    border: 1px solid #4fc3f7;
}

QPushButton:pressed {
    background-color: #1e1e1e;
    border: 1px solid #4fc3f7;
}

QPushButton:disabled {
    background-color: #1e1e1e;
    color: #666666;
    border: 1px solid #2d2d2d;
}

/* Primary Action Buttons */
QPushButton#sendButton, QPushButton#receiverButton {
    background-color: #0d47a1;
    color: #ffffff;
    font-weight: bold;
}

QPushButton#sendButton:hover, QPushButton#receiverButton:hover {
    background-color: #1565c0;
}

/* Tree Widget */
QTreeWidget {
    background-color: #252525;
    color: #e0e0e0;
    border: 1px solid #3d3d3d;
    border-radius: 4px;
    selection-background-color: #0d47a1;
    selection-color: #ffffff;
    outline: none;
}

QTreeWidget::item {
    padding: 4px;
    border-radius: 2px;
}

QTreeWidget::item:hover {
    background-color: #2d2d2d;
}

QTreeWidget::item:selected {
    background-color: #0d47a1;
    color: #ffffff;
}

QTreeWidget::branch {
    background-color: #252525;
}

/* Headers */
QHeaderView::section {
    background-color: #2d2d2d;
    color: #e0e0e0;
    padding: 6px;
    border: none;
    border-right: 1px solid #3d3d3d;
    border-bottom: 1px solid #3d3d3d;
    font-weight: bold;
}

/* Labels */
QLabel {
    color: #e0e0e0;
    background-color: transparent;
}

/* Line Edits */
QLineEdit {
    background-color: #2d2d2d;
    color: #e0e0e0;
    border: 1px solid #3d3d3d;
    border-radius: 4px;
    padding: 4px 8px;
    selection-background-color: #0d47a1;
}

QLineEdit:focus {
    border: 1px solid #4fc3f7;
}

QLineEdit:disabled {
    background-color: #1e1e1e;
    color: #666666;
}

/* Checkboxes */
QCheckBox {
    color: #e0e0e0;
    spacing: 6px;
}

QCheckBox::indicator {
    width: 18px;
    height: 18px;
    border: 2px solid #3d3d3d;
    border-radius: 3px;
    background-color: #2d2d2d;
}

QCheckBox::indicator:hover {
    border: 2px solid #4fc3f7;
}

QCheckBox::indicator:checked {
    background-color: #0d47a1;
    border: 2px solid #0d47a1;
    image: url(data:image/svg+xml;base64,PHN2ZyB3aWR0aD0iMTYiIGhlaWdodD0iMTYiIHZpZXdCb3g9IjAgMCAxNiAxNiIgZmlsbD0ibm9uZSIgeG1sbnM9Imh0dHA6Ly93d3cudzMub3JnLzIwMDAvc3ZnIj4KPHBhdGggZD0iTTEzLjMzMzMgNEw2IDExLjMzMzNMMi42NjY2NyA4IiBzdHJva2U9IndoaXRlIiBzdHJva2Utd2lkdGg9IjIiIHN0cm9rZS1saW5lY2FwPSJyb3VuZCIgc3Ryb2tlLWxpbmVqb2luPSJyb3VuZCIvPgo8L3N2Zz4K);
}

QCheckBox::indicator:disabled {
    background-color: #1e1e1e;
    border: 2px solid #2d2d2d;
}

/* Progress Bar */
QProgressBar {
    background-color: #2d2d2d;
    border: 1px solid #3d3d3d;
    border-radius: 4px;
    text-align: center;
    color: #e0e0e0;
    height: 20px;
}

QProgressBar::chunk {
    background-color: qlineargradient(x1:0, y1:0, x2:1, y2:0,
                                      stop:0 #0d47a1, stop:1 #1565c0);
    border-radius: 3px;
}

/* Tabs */
QTabWidget::pane {
    border: 1px solid #3d3d3d;
    background-color: #252525;
    border-radius: 4px;
}

QTabBar::tab {
    background-color: #2d2d2d;
    color: #e0e0e0;
    padding: 8px 16px;
    border: 1px solid #3d3d3d;
    border-bottom: none;
    border-top-left-radius: 4px;
    border-top-right-radius: 4px;
    margin-right: 2px;
}

QTabBar::tab:selected {
    background-color: #0d47a1;
    color: #ffffff;
}

QTabBar::tab:hover:!selected {
    background-color: #3d3d3d;
}

/* Scroll Bars */
QScrollBar:vertical {
    background-color: #1e1e1e;
    width: 12px;
    border-radius: 6px;
}

QScrollBar::handle:vertical {
    background-color: #3d3d3d;
    border-radius: 6px;
    min-height: 20px;
}

QScrollBar::handle:vertical:hover {
    background-color: #4d4d4d;
}

QScrollBar::add-line:vertical, QScrollBar::sub-line:vertical {
    height: 0px;
}

QScrollBar:horizontal {
    background-color: #1e1e1e;
    height: 12px;
    border-radius: 6px;
}

QScrollBar::handle:horizontal {
    background-color: #3d3d3d;
    border-radius: 6px;
    min-width: 20px;
}

QScrollBar::handle:horizontal:hover {
    background-color: #4d4d4d;
}

QScrollBar::add-line:horizontal, QScrollBar::sub-line:horizontal {
    width: 0px;
}

/* Menu Bar */
QMenuBar {
    background-color: #252525;
    color: #e0e0e0;
    border-bottom: 1px solid #3d3d3d;
}

QMenuBar::item {
    padding: 6px 12px;
    background-color: transparent;
}

QMenuBar::item:selected {
    background-color: #0d47a1;
    color: #ffffff;
}

QMenuBar::item:pressed {
    background-color: #1565c0;
}

/* Menu */
QMenu {
    background-color: #252525;
    color: #e0e0e0;
    border: 1px solid #3d3d3d;
    border-radius: 4px;
    padding: 4px;
}

QMenu::item {
    padding: 6px 24px 6px 12px;
    border-radius: 2px;
}

QMenu::item:selected {
    background-color: #0d47a1;
    color: #ffffff;
}

QMenu::separator {
    height: 1px;
    background-color: #3d3d3d;
    margin: 4px 8px;
}

/* Splitter */
QSplitter::handle {
    background-color: #3d3d3d;
}

QSplitter::handle:hover {
    background-color: #4fc3f7;
}

/* List Widget */
QListWidget {
    background-color: #252525;
    color: #e0e0e0;
    border: 1px solid #3d3d3d;
    border-radius: 4px;
    outline: none;
}

QListWidget::item {
    padding: 6px;
    border-radius: 2px;
}

QListWidget::item:hover {
    background-color: #2d2d2d;
}

QListWidget::item:selected {
    background-color: #0d47a1;
    color: #ffffff;
}

/* Combo Box */
QComboBox {
    background-color: #2d2d2d;
    color: #e0e0e0;
    border: 1px solid #3d3d3d;
    border-radius: 4px;
    padding: 4px 8px;
    min-height: 24px;
}

QComboBox:hover {
    border: 1px solid #4fc3f7;
}

QComboBox::drop-down {
    border: none;
    width: 20px;
}

QComboBox::down-arrow {
    image: url(data:image/svg+xml;base64,PHN2ZyB3aWR0aD0iMTIiIGhlaWdodD0iOCIgdmlld0JveD0iMCAwIDEyIDgiIGZpbGw9Im5vbmUiIHhtbG5zPSJodHRwOi8vd3d3LnczLm9yZy8yMDAwL3N2ZyI+CjxwYXRoIGQ9Ik0xIDFMNiA2TDExIDEiIHN0cm9rZT0iI2UwZTBlMCIgc3Ryb2tlLXdpZHRoPSIyIiBzdHJva2UtbGluZWNhcD0icm91bmQiIHN0cm9rZS1saW5lam9pbj0icm91bmQiLz4KPC9zdmc+Cg==);
}

QComboBox QAbstractItemView {
    background-color: #252525;
    color: #e0e0e0;
    border: 1px solid #3d3d3d;
    selection-background-color: #0d47a1;
    selection-color: #ffffff;
}

/* Scroll Area */
QScrollArea {
    background-color: transparent;
    border: none;
}

/* Status Frame */
QFrame#statusFrame {
    background-color: #252525;
    border: 1px solid #3d3d3d;
    border-radius: 4px;
}

/* Message Box */
QMessageBox {
    background-color: #1e1e1e;
}

QMessageBox QLabel {
    color: #e0e0e0;
}

QMessageBox QPushButton {
    min-width: 80px;
}
"""

def apply_dark_theme(app):
    """Apply the dark theme to the application"""
    app.setStyleSheet(DARK_THEME)
