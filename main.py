#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
DICOM-RT-Kaffee: Ein DICOM-Plan-Manager mit GUI

Diese Anwendung ermöglicht es, DICOM-RT-Pläne zu empfangen, zu organisieren 
und gezielt an verschiedene DICOM-Knoten zu senden.
"""

import os
import sys
import time
import logging
import threading
import configparser
import socket
import shutil
from datetime import datetime
from pathlib import Path
from PyQt5.QtCore import QThread, pyqtSignal, pyqtSlot, QTimer, Qt, QMetaObject, Q_ARG
from PyQt5.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout, 
    QPushButton, QLabel, QTreeWidget, QTreeWidgetItem, QCheckBox, 
    QGroupBox, QFormLayout, QLineEdit, QMessageBox, QMenu, QAction,
    QDialog, QTabWidget, QFileDialog, QSplitter, QProgressBar, QFrame,
    QComboBox, QListWidget, QListWidgetItem, QScrollArea
)
from PyQt5.QtCore import Qt, QThread, pyqtSignal, QTimer, QSize
from PyQt5.QtGui import QIcon, QFont, QPixmap, QColor, QPainter, QIntValidator, QBrush, QPen

# Eigene Module importieren
from dicom_processor import DicomProcessor
from settings_dialog import SettingsDialog
from rules_dialog import RulesDialog
from rules_manager import RulesManager

# ... (rest of imports)


# In main.py, add this new class
class SenderThread(QThread):
    """Thread zum sicheren Senden von DICOM-Plänen mit GUI-Feedback."""
    # Define signals to communicate with the main thread
    # Signal: (status_text)
    update_status_signal = pyqtSignal(str)
    # Signal: (current_operation, total_operations)
    update_progress_signal = pyqtSignal(int, int)
    # Signal: (message)
    finished_signal = pyqtSignal(str)

    def __init__(self, dicom_processor, plan_data_list, nodes_to_send_to, delete_after=False, parent=None):
        """Initialisiert den SenderThread mit einfachen Daten statt Qt-Objekten
        
        Args:
            dicom_processor: DicomProcessor-Instanz
            plan_data_list: Liste von Tupeln (plan_name, plan_path)
            nodes_to_send_to: Liste von Knoten-Tupeln (node_id, node_info)
            delete_after: Ob Dateien nach dem Senden gelöscht werden sollen
            parent: Elternobjekt
        """
        super().__init__(parent)
        self.processor = dicom_processor
        self.plans = plan_data_list  # Liste von Tupeln (plan_name, plan_path)
        self.nodes = nodes_to_send_to
        self.delete_after = delete_after
        self.is_running = True

    def run(self):
        """This method is executed in the new thread."""
        total_operations = len(self.plans) * len(self.nodes)
        completed_operations = 0
        successful_sends = 0

        try:
            # For each plan...
            for item_index, (plan_name, plan_path) in enumerate(self.plans):
                if not self.is_running:
                    break
                
                self.update_status_signal.emit(f"Verarbeite Plan: {plan_name}")

                # For each selected node...
                all_sends_successful = True
                for node_index, (node_id, node_info) in enumerate(self.nodes):
                    if not self.is_running:
                        break
                    
                    operation_number = item_index * len(self.nodes) + node_index
                    self.update_progress_signal.emit(operation_number, total_operations)
                    self.update_status_signal.emit(f"Sende {plan_name} an {node_info.get('name')}...")
                    
                    # Niemals während der Schleife löschen, nur am Ende des gesamten Prozesses
                    success = self.processor.send_plan_to_node(plan_path, node_info, delete_after=False)
                    
                    if success:
                        logger.info(f"Plan {plan_name} erfolgreich an {node_info.get('name')} gesendet")
                        successful_sends += 1
                    else:
                        logger.error(f"Fehler beim Senden von {plan_name} an {node_info.get('name')}")
                        all_sends_successful = False
                    
                    completed_operations += 1
                
                # Dateien nur löschen, wenn alle Sendungen für diesen Plan erfolgreich waren und Löschen aktiviert ist
                if self.delete_after and all_sends_successful and self.is_running:
                    self.update_status_signal.emit(f"Lösche Plan-Dateien: {plan_name}")
                    try:
                        self.processor.delete_plan_files(plan_path)
                        logger.info(f"Plan-Dateien für {plan_name} wurden gelöscht")
                    except Exception as e:
                        logger.error(f"Fehler beim Löschen der Plan-Dateien für {plan_name}: {str(e)}")
                elif self.delete_after:
                    logger.info(f"Plan-Dateien für {plan_name} wurden NICHT gelöscht, da nicht alle Sendungen erfolgreich waren")

            # Final progress update
            self.update_progress_signal.emit(total_operations, total_operations)
            final_message = f"{successful_sends} von {completed_operations} Sendeoperationen erfolgreich abgeschlossen."
            self.finished_signal.emit(final_message)

        except Exception as e:
            logger.error(f"Schwerer Fehler im Sender-Thread: {str(e)}")
            self.finished_signal.emit(f"Fehler: {str(e)}")
            
    def stop(self):
        self.is_running = False
# Status-Lampen-Klasse
class StatusLamp(QFrame):
    """Ein einfaches Status-Indikator-Widget, das den Zustand durch Farbe anzeigt"""
    # Farbdefinitionen
    COLOR_GREEN = QColor(0, 170, 0)    # Bereit
    COLOR_BLUE = QColor(0, 100, 220)   # Empfängt
    COLOR_RED = QColor(220, 0, 0)      # Aus
    
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setFixedSize(20, 20)  # Größe der Lampe festlegen
        self.setFrameShape(QFrame.Box)  # Rahmenform
        self.color = self.COLOR_RED     # Standardfarbe: Rot (Aus)
    
    def set_status(self, status):
        """Setzt den Status der Lampe: 'ready', 'receiving' oder 'off'"""
        if status == 'ready':
            self.color = self.COLOR_GREEN
        elif status == 'receiving':
            self.color = self.COLOR_BLUE
        else:  # 'off' oder andere
            self.color = self.COLOR_RED
        self.update()  # Widget neu zeichnen
    
    def paintEvent(self, event):
        """Zeichnet die Lampe mit der aktuellen Farbe"""
        painter = QPainter(self)
        painter.setRenderHint(QPainter.Antialiasing)
        
        # Kreis füllen
        painter.setBrush(self.color)
        painter.setPen(Qt.NoPen)
        painter.drawEllipse(2, 2, self.width()-4, self.height()-4)

# Logging konfigurieren
log_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'logs')
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, f'dicom_rt_kaffee_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')

# Standardmäßig INFO-Level verwenden
log_level = logging.INFO

# Logging-Konfiguration
logging.basicConfig(
    level=log_level,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger('DICOM-RT-Kaffee')

# Logger für DICOM-Processor separat konfigurieren
dicom_processor_logger = logging.getLogger('DICOM-Processor')

class SettingsManager:
    """Verwaltet die Anwendungseinstellungen"""
    
    def __init__(self):
        self.config_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'settings.ini')
        self.config = configparser.ConfigParser()
        self.create_default_settings_file()
        self.load_config()
        self.configure_logging()  # Logging nach dem Laden der Konfiguration einrichten
        self.system_ip = self.get_system_ip()

    def create_default_settings_file(self):
        """Erstellt eine Default-settings.ini, falls sie fehlt."""
        if not os.path.exists(self.config_file):
            default_content = (
                "[General]\n"
                "clear_import_folder_after_import = False\n"
                "auto_start_receiver = True\n"
                f"receivedplansfolder = {os.path.join(os.path.dirname(os.path.abspath(__file__)), 'received_plans')}\n"
                f"importfolder = {os.path.join(os.path.dirname(os.path.abspath(__file__)), 'import')}\n"

                "[LocalNode]\n"
                "aet = DICOM-RT-KAFFEE\n"
                "receiveport = 1334\n\n"
                "[DicomNode1]\n"
                "name = BL_IMPORT_ARC\n"
                "aet = BL_IMPORT_ARC\n"
                "ip = 10.23.112.2\n"
                "port = 104\n"
                "enabled = False\n\n"
                "[DicomNode2]\n"
                "name = ORGANO\n"
                "aet = ORGANO\n"
                "ip = 192.168.178.55\n"
                "port = 1333\n"
                "enabled = True\n\n"
                "[DicomNode3]\n"
                "name = FOLLOW\n"
                "aet = FOLLOW\n"
                "ip = 192.168.178.55\n"
                "port = 1335\n"
                "enabled = False\n\n"
                "[DicomNode4]\n"
                "name = Eclipse\n"
                "aet = VMSDBD1\n"
                "ip = 10.23.116.195\n"
                "port = 51402\n"
                "enabled = False\n\n"
                "[SendOptions]\n"
                "delete_after_send = True\n\n"
                "[Logging]\n"
                "log_level = 20\n"
                "verbose_info_logging = True\n"
            )
            with open(self.config_file, 'w', encoding='utf-8') as f:
                f.write(default_content)
            logger.info("Default settings.ini automatisch erstellt.")
        
    def get_system_ip(self):
        """Ermittelt die IP-Adresse des Systems"""
        try:
            # Methode, um die aktuelle IP-Adresse des Systems zu ermitteln
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            # Verbindung zu einer externen Adresse (keine tatsächliche Verbindung nötig)
            s.connect(("8.8.8.8", 80))
            ip = s.getsockname()[0]
            s.close()
            return ip
        except Exception as e:
            logger.error(f"Fehler beim Ermitteln der IP-Adresse: {str(e)}")
            return "127.0.0.1"  # Fallback auf localhost
        
    def configure_logging(self):
        """Konfiguriert das Logging basierend auf den Einstellungen in der settings.ini"""
        try:
            # Log-Level aus der Konfiguration lesen (Standard: INFO = 20)
            log_level = int(self.config.get('Logging', 'log_level', fallback='20'))
            # Verbose INFO-Logging Option (Standard: True)
            verbose_info_logging = self.config.getboolean('Logging', 'verbose_info_logging', fallback=True)
            
            # Root-Logger und Haupt-Logger konfigurieren
            logging.getLogger().setLevel(log_level)
            logger.setLevel(log_level)
            
            # DICOM-Processor Logger konfigurieren
            if verbose_info_logging:
                dicom_processor_logger.setLevel(log_level)
            else:
                # Bei deaktiviertem verbose_info_logging nur WARNING und höher anzeigen
                dicom_processor_logger.setLevel(logging.WARNING)
                
            logger.info(f"Logging konfiguriert: log_level={log_level}, verbose_info_logging={verbose_info_logging}")
        except Exception as e:
            logger.error(f"Fehler bei der Logging-Konfiguration: {str(e)}")
    
    def load_config(self):
        """Lädt Konfiguration aus der settings.ini, erstellt sie falls nötig"""
        if os.path.exists(self.config_file):
            self.config.read(self.config_file)
        else:
            # Standardkonfiguration erstellen
            self.config['General'] = {
                'ReceivePort': '1334',

            }
            
            # Lokalen DICOM-Knoten konfigurieren
            self.config['LocalNode'] = {
                'AET': 'DICOM-RT-KAFFEE',
                'ReceivePort': '1334'
            }
            
            # Standardknoten konfigurieren
            self.config['DicomNode1'] = {
                'Name': 'BL_IMPORT_ARC',
                'AET': 'BL_IMPORT_ARC',
                'IP': '10.23.112.2',
                'Port': '104',
                'Enabled': 'True'
            }
            
            self.config['DicomNode2'] = {
                'Name': 'FOLLOW',
                'AET': 'FOLLOW',
                'IP': '10.23.116.189',
                'Port': '1334', 
                'Enabled': 'True'
            }
            
            self.config['DicomNode3'] = {
                'Name': 'Eclipse',
                'AET': 'VMSDBD1',
                'IP': '10.23.116.195',
                'Port': '51402',
                'Enabled': 'True'
            }
            
            # Speichern
            with open(self.config_file, 'w') as f:
                self.config.write(f)
    
    def save_config(self):
        """Speichert die aktuelle Konfiguration"""
        with open(self.config_file, 'w') as f:
            self.config.write(f)
            
    def get_dicom_nodes(self):
        """Gibt alle konfigurierten DICOM-Knoten zurück"""
        nodes = []
        for section in self.config.sections():
            if section.startswith('DicomNode'):
                node = {
                    'name': self.config[section].get('Name', 'Unbekannt'),
                    'aet': self.config[section].get('AET', ''),
                    'ip': self.config[section].get('IP', ''),
                    'port': self.config[section].get('Port', '104'),
                    'enabled': self.config[section].getboolean('Enabled', False)
                }
                nodes.append(node)
        return nodes

    def get_node_info(self, node_name):
        """Gibt die Informationen eines bestimmten DICOM-Knotens anhand des Namens zurück."""
        nodes = self.get_dicom_nodes()
        for node in nodes:
            if node['name'] == node_name:
                return node
        logger.warning(f"DICOM-Knoten mit Name '{node_name}' nicht gefunden.")
        return None
    
    def update_node(self, index, node_data):
        """Aktualisiert einen DICOM-Knoten"""
        section = f'DicomNode{index+1}'
        if not self.config.has_section(section):
            self.config.add_section(section)
        
        self.config[section]['Name'] = node_data['name']
        self.config[section]['AET'] = node_data['aet']
        self.config[section]['IP'] = node_data['ip']
        self.config[section]['Port'] = node_data['port']
        self.config[section]['Enabled'] = str(node_data['enabled'])
        
        self.save_config()
        
    def get_received_plans_folder(self):
        """Gibt den Pfad zum received_plans-Ordner zurück (aus settings.ini, fallback: ./received_plans)"""
        folder = self.config['General'].get('receivedplansfolder', '')
        if not folder:
            folder = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'received_plans')
            folder = self.settings_manager.get_received_plans_folder()
            self.config['General']['receivedplansfolder'] = folder
            self.save_config()
        os.makedirs(folder, exist_ok=True)
        return folder

    def get_import_folder(self):
        """Gibt den Pfad zum Import-Ordner zurück (aus settings.ini, fallback: ./import)"""
        folder = self.config['General'].get('importfolder', '')
        if not folder:
            folder = self.settings_manager.get_import_folder()
            self.config['General']['importfolder'] = folder
            self.save_config()
        os.makedirs(folder, exist_ok=True)
        return folder

    def get_auto_start_receiver(self):
        """Gibt zurück, ob der DICOM-Empfänger beim Start automatisch aktiviert wird (aus settings.ini, fallback: False)"""
        return self.config['General'].get('auto_start_receiver', 'False').lower() == 'true'

    def get_clear_import_folder_after_import(self):
        """Gibt zurück, ob der Import-Ordner nach dem Import gelöscht werden soll (aus settings.ini, fallback: False)"""
        return self.config['General'].get('clear_import_folder_after_import', 'False').lower() == 'true'

    def get_delete_after_send(self):
        """Gibt zurück, ob Pläne nach dem Senden gelöscht werden sollen (aus settings.ini, [SendOptions], fallback: False)"""
        return self.config['SendOptions'].get('delete_after_send', 'False').lower() == 'true'


class DicomSenderThread(QThread):
    """Thread zum Senden von DICOM-Daten"""
    progress_signal = pyqtSignal(int, int)  # (current, total)
    status_signal = pyqtSignal(str, str)  # (plan_name, status)
    finished_signal = pyqtSignal(bool, str, str)  # (success, plan_name, node_name)
    
    def __init__(self, processor, plan_path, node_info, delete_after=False):
        super().__init__()
        self.processor = processor
        self.plan_path = plan_path
        self.node_info = node_info
        self.delete_after = delete_after
        
    def run(self):
        """Führt den Sendevorgang durch"""
        plan_name = os.path.basename(self.plan_path)
        try:
            self.status_signal.emit(plan_name, f"Sende an {self.node_info['name']}...")
            
            # Senden mit Fortschrittsrückmeldung
            success = self.processor.send_plan_to_node(
                self.plan_path, 
                self.node_info,
                progress_callback=self.progress_callback,
                delete_after=self.delete_after
            )
            
            if success:
                self.status_signal.emit(plan_name, f"Erfolgreich an {self.node_info['name']} gesendet")
                self.finished_signal.emit(True, plan_name, self.node_info['name'])
            else:
                self.status_signal.emit(plan_name, f"Fehler beim Senden an {self.node_info['name']}")
                self.finished_signal.emit(False, plan_name, self.node_info['name'])
                
        except Exception as e:
            logger.error(f"Fehler beim Senden von {plan_name}: {str(e)}")
            self.status_signal.emit(plan_name, f"Fehler: {str(e)}")
            self.finished_signal.emit(False, plan_name, self.node_info['name'])
    
    def progress_callback(self, current, total):
        """Callback für Fortschrittsrückmeldung"""
        self.progress_signal.emit(current, total)


class DicomReceiverThread(QThread):
    """Thread zum Empfangen von DICOM-Daten"""
    new_plan_signal = pyqtSignal(str)  # (plan_path)
    status_signal = pyqtSignal(str)  # (status)
    
    def __init__(self, processor, port=1334):
        super().__init__()
        self.processor = processor
        self.port = port
        self.running = False
        
    def run(self):
        """Startet den DICOM-Empfänger"""
        self.running = True
        self.status_signal.emit("DICOM-Empfänger gestartet...")
        
        try:
            self.processor.start_receiver(
                port=self.port,
                new_plan_callback=self.new_plan_callback
            )
            
            # Wartet, bis der Thread angehalten wird
            while self.running:
                self.msleep(100)
                
        except Exception as e:
            logger.error(f"Fehler im DICOM-Empfänger: {str(e)}")
            self.status_signal.emit(f"Fehler im DICOM-Empfänger: {str(e)}")
        
        self.processor.stop_receiver()
        self.status_signal.emit("DICOM-Empfänger gestoppt.")
        
    def stop(self):
        """Stoppt den DICOM-Empfänger"""
        self.running = False
        
    def new_plan_callback(self, plan_path):
        """Callback für neue Pläne"""
        self.new_plan_signal.emit(plan_path)


class LocalNodeSettingsDialog(QDialog):
    """Dialog zur Konfiguration des lokalen DICOM-Knotens (DICOM-RT-Kaffee)"""
    
    def __init__(self, settings_manager, parent=None):
        super().__init__(parent)
        self.settings_manager = settings_manager
        
        self.setWindowTitle("Lokalen DICOM-Knoten konfigurieren")
        self.setMinimumWidth(400)
        
        # Layout erstellen
        layout = QFormLayout()
        
        # Aktuelle System-IP anzeigen (nicht editierbar)
        ip_label = QLabel(f"<b>System IP:</b> {settings_manager.system_ip}")
        # Port editierbar machen
        self.port_edit = QLineEdit(settings_manager.config.get('LocalNode', 'ReceivePort', fallback='1334'))
        self.port_edit.setValidator(QIntValidator(1, 65535, self))
        # AET eingeben
        self.aet_edit = QLineEdit(settings_manager.config.get('LocalNode', 'AET', fallback='DICOM-RT-KAFFEE'))
        # Felder zum Layout hinzufügen
        layout.addRow(ip_label)
        layout.addRow("Port:", self.port_edit)
        layout.addRow("AE Title:", self.aet_edit)
        
        # Hinweistext
        hint_label = QLabel("<i>Hinweis: Diese Einstellungen werden für die Konfiguration des lokalen DICOM-Empfängers verwendet. "
                           "Verwenden Sie diese Werte, um den DICOM-RT-Kaffee in Ihrem TPS zu konfigurieren.</i>")
        hint_label.setWordWrap(True)
        layout.addRow(hint_label)
        
        # Buttons
        button_layout = QHBoxLayout()
        save_button = QPushButton("Speichern")
        save_button.clicked.connect(self.save_settings)
        cancel_button = QPushButton("Abbrechen")
        cancel_button.clicked.connect(self.reject)
        
        button_layout.addWidget(save_button)
        button_layout.addWidget(cancel_button)
        
        # Vertikales Layout für alle Elemente
        main_layout = QVBoxLayout()
        main_layout.addLayout(layout)
        main_layout.addLayout(button_layout)
        
        self.setLayout(main_layout)
    
    def save_settings(self):
        """Speichert die lokalen Knoteneinstellungen"""
        try:
            if 'LocalNode' not in self.settings_manager.config:
                self.settings_manager.config['LocalNode'] = {}
            
            self.settings_manager.config['LocalNode']['AET'] = self.aet_edit.text()
            self.settings_manager.config['LocalNode']['ReceivePort'] = self.port_edit.text()
            # Konfiguration speichern
            with open(self.settings_manager.config_file, 'w') as f:
                self.settings_manager.config.write(f)
            
            logger.info("Lokale DICOM-Knoten-Einstellungen gespeichert")
            QMessageBox.information(self, "Einstellungen gespeichert", "Die Einstellungen wurden erfolgreich gespeichert.")
            self.accept()
        except Exception as e:
            logger.error(f"Fehler beim Speichern der Einstellungen: {str(e)}")
            QMessageBox.critical(self, "Fehler", f"Die Einstellungen konnten nicht gespeichert werden: {str(e)}")

class NodeSettingsDialog(QDialog):
    """Dialog zur Konfiguration der DICOM-Knoten"""
    
    def __init__(self, settings_manager, parent=None):
        super().__init__(parent)
        self.settings_manager = settings_manager
        self.nodes = settings_manager.get_dicom_nodes()
        
        self.setWindowTitle("DICOM-Knoten konfigurieren")
        self.setMinimumWidth(500)
        
        # Layout erstellen
        layout = QVBoxLayout()
        
        self.tabs = QTabWidget()
        
        # Tab für jeden Knoten erstellen
        for i, node in enumerate(self.nodes):
            node_tab = QWidget()
            tab_layout = QFormLayout()
            
            name_edit = QLineEdit(node['name'])
            aet_edit = QLineEdit(node['aet'])
            ip_edit = QLineEdit(node['ip'])
            port_edit = QLineEdit(node['port'])
            enabled_checkbox = QCheckBox("Aktiviert")
            enabled_checkbox.setChecked(node['enabled'])
            
            tab_layout.addRow("Name:", name_edit)
            tab_layout.addRow("AE Title:", aet_edit)
            tab_layout.addRow("IP-Adresse:", ip_edit)
            tab_layout.addRow("Port:", port_edit)
            tab_layout.addRow("", enabled_checkbox)
            
            node_tab.setLayout(tab_layout)
            self.tabs.addTab(node_tab, f"Knoten {i+1}")
            
            # Referenzen speichern
            setattr(self, f"name_edit_{i}", name_edit)
            setattr(self, f"aet_edit_{i}", aet_edit)
            setattr(self, f"ip_edit_{i}", ip_edit)
            setattr(self, f"port_edit_{i}", port_edit)
            setattr(self, f"enabled_checkbox_{i}", enabled_checkbox)
        
        layout.addWidget(self.tabs)
        
        # Buttons
        button_layout = QHBoxLayout()
        save_button = QPushButton("Speichern")
        save_button.clicked.connect(self.save_settings)
        cancel_button = QPushButton("Abbrechen")
        cancel_button.clicked.connect(self.reject)
        
        button_layout.addWidget(save_button)
        button_layout.addWidget(cancel_button)
        layout.addLayout(button_layout)
        
        self.setLayout(layout)
    
    def save_settings(self):
        """Speichert die Knoteneinstellungen"""
        for i in range(len(self.nodes)):
            node_data = {
                'name': getattr(self, f"name_edit_{i}").text(),
                'aet': getattr(self, f"aet_edit_{i}").text(),
                'ip': getattr(self, f"ip_edit_{i}").text(),
                'port': getattr(self, f"port_edit_{i}").text(),
                'enabled': getattr(self, f"enabled_checkbox_{i}").isChecked()
            }
            self.settings_manager.update_node(i, node_data)
        
        self.accept()


class MainWindow(QMainWindow):
    """Hauptfenster der Anwendung"""
    
    def __init__(self):
        super().__init__()
        
        # Einstellungen laden
        self.settings_manager = SettingsManager()
        
        # Rules Manager initialisieren
        self.rules_manager = RulesManager()
        
        # DICOM-Prozessor initialisieren
        self.dicom_processor = DicomProcessor(self.settings_manager)
        # Der Empfangsordner ist jetzt immer:
        received_folder = self.settings_manager.get_received_plans_folder()
        
        # UI einrichten
        self.setup_ui()
        
        # Icon setzen
        self.set_application_icon()
        
        # Timer für regelmäßiges Aktualisieren der Planliste
        self.refresh_timer = QTimer()
        self.refresh_timer.timeout.connect(self.periodic_refresh)
        self.refresh_timer.start(5000)  # alle 5 Sekunden
        
        # DICOM-Empfänger-Thread
        self.receiver_thread = None
        
        # Aktive Sende-Threads
        self.send_threads = {}

        # Automatischer Start des DICOM-Empfängers, wenn in den Settings aktiviert
        auto_start = self.settings_manager.get_auto_start_receiver()
        if auto_start:
            try:
                self.toggle_receiver()
                logger.info("DICOM-Empfänger wurde beim Start automatisch aktiviert (auto_start_receiver=True)")
            except Exception as e:
                logger.error(f"Automatischer Start des DICOM-Empfängers fehlgeschlagen: {e}")
        
    def setup_ui(self):
        """Richtet die Benutzeroberfläche ein"""
        self.setWindowTitle("DICOM-RT-Kaffee")
        self.setMinimumSize(800, 600)
        
        # Zentrales Widget
        central_widget = QWidget()
        main_layout = QVBoxLayout()
        
        # Splitter für die Hauptbereiche
        splitter = QSplitter(Qt.Horizontal)
        
        # Linke Seite: Plan-Liste
        left_widget = QWidget()
        left_layout = QVBoxLayout()
        
        # Header
        plan_header = QLabel("Verfügbare RT-Pläne")
        plan_header.setFont(QFont('Arial', 12, QFont.Bold))
        left_layout.addWidget(plan_header)
        
        # Plan-Baum
        self.plan_tree = QTreeWidget()
        self.plan_tree.setHeaderLabels(["Patienten & Pläne"])
        self.plan_tree.setSelectionMode(QTreeWidget.ExtendedSelection)
        self.plan_tree.itemSelectionChanged.connect(self.update_buttons)
        left_layout.addWidget(self.plan_tree)
        
        # Buttons für Pläne
        plan_buttons_layout = QHBoxLayout()
        self.refresh_button = QPushButton("Aktualisieren")
        self.refresh_button.clicked.connect(self.refresh_plan_list)
        self.delete_button = QPushButton("Ausgewählte löschen")
        self.delete_button.clicked.connect(self.delete_selected_plans)
        self.delete_button.setEnabled(False)
        self.delete_all_button = QPushButton("Alle löschen")
        self.delete_all_button.clicked.connect(self.delete_all_plans)
        
        plan_buttons_layout.addWidget(self.refresh_button)
        plan_buttons_layout.addWidget(self.delete_button)
        plan_buttons_layout.addWidget(self.delete_all_button)
        left_layout.addLayout(plan_buttons_layout)
        
        left_widget.setLayout(left_layout)
        
        # Rechte Seite: DICOM-Knoten und Aktionen
        right_widget = QWidget()
        right_layout = QVBoxLayout()
        
        # DICOM-Knoten-Gruppe
        nodes_group = QGroupBox("DICOM-Knoten")
        nodes_layout = QVBoxLayout()
        
        # Knoten-Checkboxes
        self.node_checkboxes = []
        for i, node in enumerate(self.settings_manager.get_dicom_nodes(), 1):
            checkbox = QCheckBox(f"{node['name']} ({node['ip']}:{node['port']})")
            checkbox.setChecked(node['enabled'])
            nodes_layout.addWidget(checkbox)
            self.node_checkboxes.append(checkbox)
            # Checkbox auch als Attribut setzen für einfacheren Zugriff
            setattr(self, f"node{i}_checkbox", checkbox)
        
        # Knoten-Einstellungen-Button
        node_settings_button = QPushButton("Externe Knoten konfigurieren...")
        node_settings_button.clicked.connect(self.show_node_settings)
        nodes_layout.addWidget(node_settings_button)
        
        # Lokaler Knoten-Einstellungen-Button
        local_node_settings_button = QPushButton("Lokalen Knoten konfigurieren...")
        local_node_settings_button.clicked.connect(self.show_local_node_settings)
        nodes_layout.addWidget(local_node_settings_button)
        
        nodes_group.setLayout(nodes_layout)
        right_layout.addWidget(nodes_group)
        
        # Aktionen-Gruppe
        actions_group = QGroupBox("Aktionen")
        actions_layout = QVBoxLayout()
        
        self.send_button = QPushButton("Ausgewählte Pläne senden")
        self.send_button.clicked.connect(self.send_selected_plans)
        self.send_button.setEnabled(False)
        
        self.receiver_button = QPushButton("DICOM-Empfänger starten")
        self.receiver_button.clicked.connect(self.toggle_receiver)
        
        self.import_button = QPushButton("Import-Ordner verarbeiten")
        self.import_button.clicked.connect(self.process_import_folder)
        self.import_button.setToolTip("DICOM-Dateien aus dem Import-Ordner verarbeiten und sortieren")
        
        
        actions_layout.addWidget(self.send_button)
        actions_layout.addWidget(self.receiver_button)
        actions_layout.addWidget(self.import_button)
        
        actions_group.setLayout(actions_layout)
        right_layout.addWidget(actions_group)
        
        # Status-Gruppe
        status_group = QGroupBox("Status")
        status_layout = QVBoxLayout()
        
        # Status-Anzeige mit Lampe
        status_header_layout = QHBoxLayout()
        
        # Status-Lampe
        self.status_lamp = StatusLamp()
        self.status_lamp.set_status('off')  # Initial ausgeschaltet
        
        # Status-Text
        self.status_label = QLabel("Bereit.")
        
        status_header_layout.addWidget(self.status_lamp)
        status_header_layout.addWidget(self.status_label, 1)  # 1 = Stretchfaktor
        status_layout.addLayout(status_header_layout)
        
        self.progress_bar = QProgressBar()
        self.progress_bar.setVisible(False)
        status_layout.addWidget(self.progress_bar)
        
        status_group.setLayout(status_layout)
        right_layout.addWidget(status_group)
        
        right_widget.setLayout(right_layout)
        
        # Splitter hinzufügen
        splitter.addWidget(left_widget)
        splitter.addWidget(right_widget)
        splitter.setSizes([400, 400])
        
        main_layout.addWidget(splitter)
        
        central_widget.setLayout(main_layout)
        self.setCentralWidget(central_widget)
        
        # Menüleiste
        self.setup_menu()
        
        # Erste Aktualisierung der Planliste
        self.refresh_plan_list()
    
    def setup_menu(self):
        """Erstellt die Menüleiste"""
        menu_bar = self.menuBar()
        
        # Datei-Menü
        file_menu = menu_bar.addMenu("&Datei")
        
        settings_action = QAction("&Einstellungen", self)
        settings_action.triggered.connect(self.open_settings_dialog)
        file_menu.addAction(settings_action)
        
        rules_action = QAction("&Weiterleitungsregeln", self)
        rules_action.triggered.connect(self.show_rules_dialog)
        file_menu.addAction(rules_action)
        
        exit_action = QAction("&Beenden", self)
        exit_action.triggered.connect(self.close)
        file_menu.addAction(exit_action)
        
        # Hilfe-Menü
        help_menu = menu_bar.addMenu("&Hilfe")
        
        about_action = QAction("Ü&ber", self)
        about_action.triggered.connect(self.show_about)
        help_menu.addAction(about_action)
    
    def set_application_icon(self):
        """Setzt das Anwendungsicon für Fenster und Taskleiste"""
        try:
            # Pfad zum Icon-File
            icon_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'icons', 'DICOM-RT-Station.ico')
            
            if os.path.exists(icon_path):
                # Icon für das Fenster setzen
                icon = QIcon(icon_path)
                self.setWindowIcon(icon)
                
                # Icon auch für die gesamte Anwendung setzen (Taskleiste)
                QApplication.instance().setWindowIcon(icon)
                
                logger.info(f"Anwendungsicon erfolgreich geladen: {icon_path}")
            else:
                logger.warning(f"Icon-Datei nicht gefunden: {icon_path}")
                
        except Exception as e:
            logger.error(f"Fehler beim Laden des Anwendungsicons: {str(e)}")
    
    def refresh_plan_list(self):
        """Aktualisiert die hierarchische Liste der verfügbaren Pläne und erhält die Auswahl"""
        watch_folder = self.settings_manager.get_received_plans_folder()
        patient_dict = {}  # Dictionary für Patientenordner -> Plan-Ordner
        plan_count = 0
        
        # Aktuelle Auswahl speichern
        selected_paths = []
        for item in self.plan_tree.selectedItems():
            # Nur Plan-Elemente (nicht Patienten) berücksichtigen
            if item.data(0, Qt.UserRole) is not None:
                selected_paths.append(item.data(0, Qt.UserRole))
        
        # Den Baum leeren
        self.plan_tree.clear()
        
        # Patientenordner durchsuchen
        if os.path.exists(watch_folder):
            # Nur Verzeichnisse berücksichtigen und den failed-Ordner ignorieren
            patient_folders = [
                d for d in os.listdir(watch_folder)
                if os.path.isdir(os.path.join(watch_folder, d)) and d != "failed"
            ]
            
            # Für jeden Patientenordner
            for patient_folder in patient_folders:
                patient_path = os.path.join(watch_folder, patient_folder)
                plan_folders = []
                
                # Für jeden Plan im Patientenordner
                for plan_folder in os.listdir(patient_path):
                    plan_path = os.path.join(patient_path, plan_folder)
                    if os.path.isdir(plan_path):
                        plan_folders.append(plan_folder)
                        plan_count += 1
                
                # Nur Patienten mit mindestens einem Plan hinzufügen
                if plan_folders:
                    patient_dict[patient_folder] = plan_folders
        
        # Hierarchischen Baum erstellen
        plan_items_map = {}  # Dictionary für Pfad -> Plan-Item (zur Wiederherstellung der Auswahl)
        
        for patient_name, plan_folders in patient_dict.items():
            # Patienten-Element erstellen
            patient_item = QTreeWidgetItem([patient_name])
            patient_item.setData(0, Qt.UserRole, None)  # Kein Pfad für Patientenelemente
            patient_item.setFlags(patient_item.flags() & ~Qt.ItemIsSelectable)  # Patienten nicht selektierbar
            self.plan_tree.addTopLevelItem(patient_item)
            
            # Plan-Elemente als Kind-Elemente hinzufügen
            for plan_name in plan_folders:
                # Study-Nummer aus der Anzeige entfernen, falls vorhanden
                display_name = plan_name
                if "_" in display_name:
                    # Versuchen, die Study-Nummer zu entfernen (typischerweise nach einem Unterstrich)
                    parts = display_name.split("_")
                    if len(parts) > 1 and any(part.isdigit() for part in parts[1:]):
                        # Wenn nach dem Unterstrich eine Zahl steht, diese entfernen
                        display_name = parts[0]
                
                plan_item = QTreeWidgetItem(["  " + display_name])  # Eingerückt für visuelle Hierarchie
                plan_path = os.path.join(watch_folder, patient_name, plan_name)
                plan_item.setData(0, Qt.UserRole, plan_path)  # Vollständigen Pfad speichern
                patient_item.addChild(plan_item)
                plan_items_map[plan_path] = plan_item
            
            # Patienten- und Plan-Elemente standardmäßig expandieren
            patient_item.setExpanded(True)
            
            # Alle Kind-Elemente (Pläne) ebenfalls expandieren
            for i in range(patient_item.childCount()):
                patient_item.child(i).setExpanded(True)
        
        # Auswahl wiederherstellen, falls möglich
        if selected_paths:
            # Signalverbindung temporär deaktivieren, um unnötige Ereignisse zu vermeiden
            self.plan_tree.itemSelectionChanged.disconnect(self.update_buttons)
            
            for path in selected_paths:
                if path in plan_items_map:
                    plan_items_map[path].setSelected(True)
            
            # Signalverbindung wiederherstellen
            self.plan_tree.itemSelectionChanged.connect(self.update_buttons)
            
            # Button-Status manuell aktualisieren, da das Signal unterdrückt wurde
            self.update_buttons()
            
        # Status aktualisieren
        self.status_label.setText(f"{plan_count} Pläne in {len(patient_dict)} Patienten verfügbar.")
        
        # "Alle löschen"-Button nur aktivieren, wenn Pläne vorhanden sind
        self.delete_all_button.setEnabled(plan_count > 0)
        
    def delete_all_plans(self):
        """Löscht alle verfügbaren Pläne nach Bestätigung"""
        watch_folder = self.settings_manager.get_received_plans_folder()
        plans = []
        
        # Alle Unterordner im Watch-Folder sind Pläne (außer 'failed')
        if os.path.exists(watch_folder):
            plans = [
                d for d in os.listdir(watch_folder) 
                if os.path.isdir(os.path.join(watch_folder, d)) 
                and d != "failed"
            ]
        
        if not plans:
            self.status_label.setText("Keine Pläne zum Löschen vorhanden.")
            return
            
        # Bestätigung einholen mit Warnung
        count = len(plans)
        confirm_message = (f"ACHTUNG: Alle {count} Pläne werden unwiderruflich gelöscht!\n\n"
                          f"Möchten Sie wirklich ALLE {count} Pläne löschen?")
        
        confirm = QMessageBox.question(
            self,
            "Alle Pläne löschen",
            confirm_message,
            QMessageBox.Yes | QMessageBox.No,
            QMessageBox.No
        )
        
        if confirm != QMessageBox.Yes:
            return
            
        # Zweite Bestätigung einholen
        second_confirm = QMessageBox.warning(
            self,
            "Letzte Warnung",
            "Dies ist Ihre letzte Warnung!\n\nAlle Pläne werden gelöscht und können nicht wiederhergestellt werden!",
            QMessageBox.Yes | QMessageBox.Cancel,
            QMessageBox.Cancel
        )
        
        if second_confirm != QMessageBox.Yes:
            return
        
        # Fortschrittsanzeige vorbereiten    
        self.progress_bar.setVisible(True)
        self.progress_bar.setMaximum(count)
        self.progress_bar.setValue(0)
        
        # Pläne löschen
        success_count = 0
        failed_plans = []
        
        for i, plan_name in enumerate(plans):
            plan_path = os.path.join(watch_folder, plan_name)
            
            # Status aktualisieren
            self.status_label.setText(f"Lösche Plan {i+1}/{count}: {plan_name}...")
            self.progress_bar.setValue(i + 1)
            QApplication.processEvents()  # UI-Updates zulassen
            
            try:
                import shutil
                if os.path.exists(plan_path):
                    shutil.rmtree(plan_path)
                    logger.info(f"Plan gelöscht: {plan_name}")
                    success_count += 1
                else:
                    logger.warning(f"Plan nicht gefunden: {plan_name}")
                    failed_plans.append((plan_name, "Plan nicht gefunden"))
            except Exception as e:
                logger.error(f"Fehler beim Löschen von {plan_name}: {str(e)}")
                failed_plans.append((plan_name, str(e)))
        
        # Zusammenfassung anzeigen
        if failed_plans:
            error_details = "\n- ".join([f"{name}: {error}" for name, error in failed_plans])
            QMessageBox.warning(
                self,
                "Fehler beim Löschen",
                f"{len(failed_plans)} von {count} Plänen konnten nicht gelöscht werden:\n\n- {error_details}"
            )
        
        if success_count > 0:
            self.status_label.setText(f"Alle Pläne gelöscht: {success_count} von {count} erfolgreich.")
        
        # Liste aktualisieren und Fortschrittsanzeige ausblenden
        self.refresh_plan_list()
        self.progress_bar.setVisible(False)
        
    def update_buttons(self):
        """Aktualisiert den Zustand der Buttons basierend auf der Auswahl"""
        # Nur Planelemente zählen (keine Patientenordner)
        selected_plan_items = []
        for item in self.plan_tree.selectedItems():
            # Wenn es sich um ein Plan-Element handelt (hat einen Pfad gespeichert)
            if item.data(0, Qt.UserRole) is not None:
                selected_plan_items.append(item)
        
        has_selection = len(selected_plan_items) > 0
        self.delete_button.setEnabled(has_selection)
        self.send_button.setEnabled(has_selection and any([cb.isChecked() for cb in self.node_checkboxes]))
        
    def delete_selected_plans(self):
        """Löscht die ausgewählten Pläne"""
        selected_items = self.plan_tree.selectedItems()
        
        # Nur Plan-Elemente auswählen (keine Patientenordner)
        plan_items = []
        for item in selected_items:
            # Plan-Elemente haben einen gespeicherten Pfad
            if item.data(0, Qt.UserRole) is not None:
                plan_items.append(item)
        
        if not plan_items:
            return
            
        # Bestätigung einholen mit detaillierten Informationen
        count = len(plan_items)
        plan_names = [item.text(0).strip() for item in plan_items]  # Führende Leerzeichen entfernen
        plan_list_str = "\n- ".join(plan_names)
        
        confirm_message = f"Sind Sie sicher, dass Sie folgende {count} {'Plan' if count == 1 else 'Pläne'} löschen möchten?\n\n- {plan_list_str}"
        
        confirm = QMessageBox.question(
            self,
            "Pläne löschen",
            confirm_message,
            QMessageBox.Yes | QMessageBox.No,
            QMessageBox.No
        )
        
        if confirm != QMessageBox.Yes:
            return
        
        # Fortschrittsanzeige vorbereiten    
        self.progress_bar.setVisible(True)
        self.progress_bar.setMaximum(count)
        self.progress_bar.setValue(0)
        
        # Pläne löschen
        success_count = 0
        failed_plans = []
        
        for i, item in enumerate(plan_items):
            plan_name = item.text(0).strip()
            plan_path = item.data(0, Qt.UserRole)  # Hole gespeicherten Pfad
            
            # Status aktualisieren
            self.status_label.setText(f"Lösche Plan: {plan_name}...")
            self.progress_bar.setValue(i + 1)
            QApplication.processEvents()  # UI-Updates zulassen
            
            try:
                import shutil
                if os.path.exists(plan_path):
                    shutil.rmtree(plan_path)
                    logger.info(f"Plan gelöscht: {plan_name}")
                    success_count += 1
                else:
                    logger.warning(f"Plan nicht gefunden: {plan_name}")
                    failed_plans.append((plan_name, "Plan nicht gefunden"))
            except Exception as e:
                logger.error(f"Fehler beim Löschen von {plan_name}: {str(e)}")
                failed_plans.append((plan_name, str(e)))
        
        # Zusammenfassung anzeigen
        if failed_plans:
            error_details = "\n- ".join([f"{name}: {error}" for name, error in failed_plans])
            QMessageBox.warning(
                self,
                "Fehler beim Löschen",
                f"{len(failed_plans)} von {count} Plänen konnten nicht gelöscht werden:\n\n- {error_details}"
            )
        
        if success_count > 0:
            self.status_label.setText(f"{success_count} Pläne erfolgreich gelöscht.")
        
        # Liste aktualisieren und Fortschrittsanzeige ausblenden
        # Liste aktualisieren und Fortschrittsanzeige ausblenden
        self.refresh_plan_list()
        self.progress_bar.setVisible(False)
    
    def send_selected_plans(self):
        """Startet den Sendevorgang für ausgewählte Pläne in einem sicheren Worker-Thread."""
        selected_items = [item for item in self.plan_tree.selectedItems() if item.data(0, Qt.UserRole) is not None]
        if not selected_items:
            return

        # Konvertiere QTreeWidgetItems in einfache Tupel (plan_name, plan_path)
        plan_data_list = []
        for item in selected_items:
            plan_name = item.text(0).strip()
            plan_path = item.data(0, Qt.UserRole)
            plan_data_list.append((plan_name, plan_path))
            logger.info(f"Plan zum Senden vorbereitet: {plan_name}, Pfad: {plan_path}")

        # Ausgewählte Knoten dynamisch ermitteln
        enabled_nodes = []
        for i, checkbox in enumerate(self.node_checkboxes):
            if checkbox.isChecked():
                # Extract only the node name (before any parenthesis)
                node_label = checkbox.text().strip()
                node_name = node_label.split('(')[0].strip()
                logger.info(f"Node-Checkbox aktiviert: {node_name} (Label: {node_label})")
                node_info = self.settings_manager.get_node_info(node_name)
                if node_info:
                    enabled_nodes.append((node_name, node_info))
                else:
                    logger.warning(f"Knoten mit Name '{node_name}' nicht gefunden!")

        if not enabled_nodes:
            QMessageBox.warning(self, "Keine Knoten ausgewählt", "Bitte wählen Sie mindestens einen DICOM-Knoten aus.")
            return
            
        # Disable buttons during send
        self.send_button.setEnabled(False)
        self.delete_button.setEnabled(False)
        self.delete_all_button.setEnabled(False)
        self.progress_bar.setValue(0)
        self.progress_bar.setMaximum(len(plan_data_list) * len(enabled_nodes))
        self.progress_bar.setVisible(True)

        # Determine if plans should be deleted after sending
        delete_after = self.settings_manager.get_delete_after_send()

        # Pass delete_after flag to SenderThread with plain data instead of QTreeWidgetItems
        self.sender_thread = SenderThread(self.dicom_processor, plan_data_list, enabled_nodes, delete_after=delete_after)
        # Connect signals from the thread to slots in the MainWindow
        self.sender_thread.update_status_signal.connect(self.status_label.setText)
        self.sender_thread.update_progress_signal.connect(self.progress_bar.setValue)
        self.sender_thread.finished.connect(self.on_send_finished) # Use the built-in finished signal
        self.sender_thread.finished_signal.connect(self.on_send_finished_message)

        self.sender_thread.start()

    # Add these two new methods (slots) to your MainWindow class
    def on_send_finished(self):
        """Slot, der aufgerufen wird, wenn der Sender-Thread beendet ist."""
        self.progress_bar.setVisible(False)
        self.send_button.setEnabled(True)
        self.delete_button.setEnabled(True)
        self.delete_all_button.setEnabled(True)
        self.cleanup_empty_plan_folders()
        self.refresh_plan_list()
        self.sender_thread = None # Clean up thread reference

    def cleanup_empty_plan_folders(self):
        """Löscht leere Plan-Ordner und Patienten-Ordner aus received_plans."""
        watch_folder = self.settings_manager.get_received_plans_folder()
        if not os.path.exists(watch_folder):
            return
        for patient_folder in os.listdir(watch_folder):
            patient_path = os.path.join(watch_folder, patient_folder)
            if not os.path.isdir(patient_path) or patient_folder == "failed":
                continue
            # Remove empty plan folders
            for plan_folder in os.listdir(patient_path):
                plan_path = os.path.join(patient_path, plan_folder)
                if os.path.isdir(plan_path) and not os.listdir(plan_path):
                    try:
                        os.rmdir(plan_path)
                    except Exception:
                        pass
            # Remove patient folder if now empty
            if not os.listdir(patient_path):
                try:
                    os.rmdir(patient_path)
                except Exception:
                    pass

    def on_send_finished_message(self, message):
        """Slot, um die finale Statusmeldung anzuzeigen."""
        self.status_label.setText(message)

    
    def send_plans_thread(self, plan_infos, enabled_nodes):
        """Thread-Funktion zum Senden von Plänen (nur Python-Objekte, keine Qt-Objekte!)"""
        import traceback
        total_operations = len(plan_infos) * len(enabled_nodes)
        completed_operations = 0
        try:
            logger.info(f"Starte send_plans_thread mit {len(plan_infos)} Plänen und {len(enabled_nodes)} Knoten.")
            # Für jeden Plan...
            for item_index, plan in enumerate(plan_infos):
                plan_name = plan["plan_name"].strip()
                plan_path = plan["plan_path"]
                logger.info(f"Beginne mit Plan: {plan_name}, Pfad: {plan_path}")
                self.status_label.setText(f"Verarbeite Plan: {plan_name}")
                QApplication.processEvents()
                # An alle Knoten senden
                for node_index, (node_id, node_info) in enumerate(enabled_nodes):
                    is_last_node = (node_index == len(enabled_nodes) - 1)
                    logger.info(f"Sende {plan_name} an {node_info.get('name', f'Node {node_id}')} (is_last_node={is_last_node})")
                    logger.info(f"Node Info: {node_info}")
                    logger.info(f"Plan Path: {plan_path}")
                    import os
                    if not os.path.exists(plan_path):
                        logger.error(f"Plan-Pfad existiert nicht: {plan_path}")
                    else:
                        logger.info(f"Plan-Pfad existiert: {plan_path}")
                    self.status_label.setText(f"Sende {plan_name} an {node_info.get('name', f'Node {node_id}')}...")
                    QApplication.processEvents()
                    operation_number = item_index * len(enabled_nodes) + node_index + 1
                    self.progress_bar.setMaximum(total_operations)
                    self.progress_bar.setValue(operation_number)
                    try:
                        logger.info(f"Vor send_plan_to_node() für {plan_name} -> {node_info.get('name')}")
                        success = self.dicom_processor.send_plan_to_node(
                            plan_path,
                            node_info,
                            progress_callback=self.update_send_progress,
                            delete_after=is_last_node
                        )
                        logger.info(f"Nach send_plan_to_node() für {plan_name} -> {node_info.get('name')}, Erfolg: {success}")
                        if success:
                            logger.info(f"Plan {plan_name} erfolgreich an {node_info.get('name')} gesendet")
                        else:
                            logger.error(f"Fehler beim Senden von {plan_name} an {node_info.get('name')}")
                    except Exception as send_exc:
                        logger.error(f"Exception beim Senden von {plan_name} an {node_info.get('name')}: {send_exc}\n{traceback.format_exc()}")
                        self.status_label.setText(f"Fehler beim Senden: {send_exc}")
                        # Show error in UI
                        try:
                            QMessageBox.critical(self, "Fehler beim Senden", f"Fehler beim Senden von {plan_name} an {node_info.get('name')}:\n{send_exc}")
                        except Exception:
                            pass
                    completed_operations += 1
            self.status_label.setText(f"{completed_operations} von {total_operations} Sendeoperationen abgeschlossen")
        except Exception as e:
            logger.error(f"Fehler beim Senden von Plänen: {str(e)}\n{traceback.format_exc()}")
            self.status_label.setText(f"Fehler: {str(e)}")
            try:
                QMessageBox.critical(self, "Fehler beim Senden", f"Fehler beim Senden von Plänen:\n{e}\n{traceback.format_exc()}")
            except Exception:
                pass
        finally:
            self.send_button.setEnabled(True)
            self.delete_button.setEnabled(True)
            self.delete_all_button.setEnabled(True)
            self.progress_bar.setVisible(False)
            self.refresh_plan_list()


    def update_send_progress(self, current, total):
        """Aktualisiert die Fortschrittsanzeige beim Dateiübertragung"""
        if total > 0:
            # Fortschritt für die aktuelle Dateiübertragung anzeigen
            percentage = int(current / total * 100)
            self.status_label.setText(f"{self.status_label.text()} - {percentage}% ({current}/{total} Dateien)")
            QApplication.processEvents()  # UI-Updates zulassen
    
    def update_status(self, plan_name, status):
        """Aktualisiert die Statusanzeige"""
        self.status_label.setText(f"{plan_name}: {status}")
        QApplication.processEvents()  # UI-Updates zulassen
    
    def handle_send_finished(self, success, plan_name, node_name):
        """Behandelt das Ende eines Sendevorgangs"""
        key = f"{plan_name}_{node_name}"
        if hasattr(self, 'send_threads') and key in self.send_threads:
            del self.send_threads[key]
        
        # Wenn keine Threads mehr laufen, Fortschrittsanzeige zurücksetzen
        if not hasattr(self, 'send_threads') or not self.send_threads:
            self.progress_bar.setVisible(False)
            self.refresh_plan_list()
    
    def toggle_receiver(self):
        """Startet oder stoppt den DICOM-Empfänger"""
        if self.receiver_thread is None or not self.receiver_thread.isRunning():
            # Empfänger starten
            port = int(self.settings_manager.config['General'].get('ReceivePort', '1334'))
            self.receiver_thread = DicomReceiverThread(self.dicom_processor, port)
            self.receiver_thread.new_plan_signal.connect(self.handle_new_plan)
            self.receiver_thread.status_signal.connect(self.update_receiver_status)
            self.receiver_thread.start()
            
            self.receiver_button.setText("DICOM-Empfänger stoppen")
            logger.info("DICOM-Empfänger gestartet")
            
            # Statuslampe auf grün (bereit) setzen
            self.status_lamp.set_status('ready')
        else:
            # Empfänger stoppen
            self.receiver_thread.stop()
            self.receiver_button.setText("DICOM-Empfänger starten")
            logger.info("DICOM-Empfänger gestoppt")
            
            # Statuslampe auf rot (aus) setzen
            self.status_lamp.set_status('off')
    
    def update_receiver_status(self, status):
        """Aktualisiert den Empfängerstatus"""
        self.status_label.setText(status)
        
        # Status-Lampe aktualisieren
        if "empfange" in status.lower() or "receiving" in status.lower():
            self.status_lamp.set_status('receiving')
        elif self.receiver_thread is not None and self.receiver_thread.isRunning():
            self.status_lamp.set_status('ready')
    
    def handle_new_plan(self, plan_path):
        """Behandelt einen neu empfangenen Plan
        
        Args:
            plan_path (str): Pfad zum neuen Plan
        """
        self.refresh_plan_list()
        self.update_receiver_status(f"Neuer Plan empfangen: {os.path.basename(plan_path)}")
        
    def periodic_refresh(self):
        """Führt eine regelmäßige Aktualisierung durch und stellt sicher, dass alle Pläne aufgeklappt bleiben"""
        # Planliste aktualisieren
        self.refresh_plan_list()
        
        # Sicherstellen, dass alle Pläne aufgeklappt sind
        root = self.plan_tree.invisibleRootItem()
        for i in range(root.childCount()):
            patient_item = root.child(i)
            patient_item.setExpanded(True)
            
            # Alle Pläne des Patienten aufklappen
            for j in range(patient_item.childCount()):
                patient_item.child(j).setExpanded(True)

    def process_import_folder(self):
        """Verarbeitet alle DICOM-Dateien im Import-Ordner und sortiert sie in die richtige Struktur"""
        # Status aktualisieren
        self.status_label.setText("Verarbeite Import-Ordner...")
        self.import_button.setEnabled(False)
        
        # Thread starten, um die UI nicht zu blockieren
        threading.Thread(target=self._process_import_folder_thread, daemon=True).start()
    
    def _process_import_folder_thread(self):
        """Thread-Funktion für Import-Ordner-Verarbeitung"""
        logger.info("=== IMPORT THREAD GESTARTET ===")
        try:
            # Import-Ordner verarbeiten
            success, count, message = self.dicom_processor.process_import_folder(
                status_callback=lambda s: self.update_status_threadsafe("Import", s)
            )
            logger.info(f"=== IMPORT ERGEBNIS: success={success}, count={count}, message={message} ===")
            
            # Ergebnis anzeigen
            if success:
                self.update_status_threadsafe("Import", message)
                # Plan-Liste aktualisieren - thread-safe approach
                QTimer.singleShot(0, self.refresh_plan_list)
            else:
                self.update_status_threadsafe("Import", f"Fehler: {message}")
        except Exception as e:
            self.update_status_threadsafe("Import", f"Fehler bei Import: {str(e)}")
        finally:
            # Button wieder aktivieren
            QMetaObject.invokeMethod(self.import_button, "setEnabled", 
                                   Qt.QueuedConnection,
                                   Q_ARG(bool, True))
            
            # Weiterleitungsregeln prüfen, nachdem der Button wieder aktiviert wurde
            threading.Thread(target=self._check_forwarding_rules_thread, daemon=True).start()
            
            # Import-Ordner leeren, nachdem der Button wieder aktiviert wurde, aber nur wenn die Einstellung aktiviert ist
            if self.settings_manager.get_clear_import_folder_after_import():
                # Dies wird in einem separaten Thread ausgeführt, um die UI nicht zu blockieren
                threading.Thread(target=self._clear_import_folder_thread, daemon=True).start()
                logger.info("Import-Ordner wird nach Import gelöscht (clear_import_folder_after_import=True)")
            else:
                logger.info("Import-Ordner wird nicht gelöscht (clear_import_folder_after_import=False)")
    
    def _check_forwarding_rules_thread(self):
        """Prüft Weiterleitungsregeln für alle importierten Pläne in einem separaten Thread"""
        try:
            logger.info("=== STARTE WEITERLEITUNGSREGELN-PRÜFUNG ===")
            self.update_status_threadsafe("Import", "Prüfe Weiterleitungsregeln...")
            
            from rules_manager import RulesManager
            rules_manager = RulesManager()
            
            # Alle importierten Pläne durchgehen
            received_plans_folder = self.settings_manager.get_received_plans_folder()
            forwarded_count = 0
            
            logger.info(f"Durchsuche received_plans Ordner: {received_plans_folder}")
            
            for patient_folder in os.listdir(received_plans_folder):
                patient_path = os.path.join(received_plans_folder, patient_folder)
                if os.path.isdir(patient_path):
                    logger.info(f"Patient-Ordner gefunden: {patient_folder}")
                    for plan_folder in os.listdir(patient_path):
                        plan_path = os.path.join(patient_path, plan_folder)
                        if os.path.isdir(plan_path):
                            # Prüfe Weiterleitungsregeln für diesen Plan
                            plan_name = plan_folder.split('_')[0] if '_' in plan_folder else plan_folder
                            source_ae = "IMPORT_FOLDER"  # Spezielle AE für Import-Ordner
                            
                            logger.info(f"Prüfe Weiterleitungsregeln für importierten Plan {plan_name} (Ordner: {plan_folder})")
                            target_nodes = rules_manager.check_forwarding_rules(source_ae, plan_name, self.settings_manager)
                            
                            if target_nodes:
                                logger.info(f"Plan {plan_name} entspricht {len(target_nodes)} Weiterleitungsregeln")
                                for node_name, node_info in target_nodes:
                                    try:
                                        self.update_status_threadsafe("Import", f"Leite Plan {plan_name} an {node_name} weiter...")
                                        logger.info(f"Leite Plan {plan_name} an {node_name} weiter")
                                        success = self.dicom_processor.send_plan_to_node(plan_path, node_info)
                                        if success:
                                            logger.info(f"Plan {plan_name} erfolgreich an {node_name} weitergeleitet")
                                            forwarded_count += 1
                                        else:
                                            logger.error(f"Fehler beim Weiterleiten von Plan {plan_name} an {node_name}")
                                    except Exception as e:
                                        logger.error(f"Fehler beim Weiterleiten von Plan {plan_name} an {node_name}: {str(e)}")
                            else:
                                logger.info(f"Keine passenden Weiterleitungsregeln für Plan {plan_name} gefunden")
            
            if forwarded_count > 0:
                self.update_status_threadsafe("Import", f"{forwarded_count} Pläne weitergeleitet")
                logger.info(f"=== WEITERLEITUNGSREGELN-PRÜFUNG ABGESCHLOSSEN: {forwarded_count} Pläne weitergeleitet ===")
            else:
                self.update_status_threadsafe("Import", "Keine Pläne weitergeleitet")
                logger.info("=== WEITERLEITUNGSREGELN-PRÜFUNG ABGESCHLOSSEN: Keine Pläne weitergeleitet ===")
                
        except Exception as e:
            logger.error(f"Fehler beim Prüfen der Weiterleitungsregeln: {str(e)}")
            self.update_status_threadsafe("Import", f"Fehler bei Weiterleitungsregeln: {str(e)}")

    def _clear_import_folder_thread(self):
        """Löscht den Import-Ordner komplett in einem separaten Thread"""
        try:
            import_folder = self.settings_manager.get_import_folder()
            self.update_status_threadsafe("Import", "Lösche Import-Ordner...")
            
            # Alle Dateien im Import-Ordner finden
            all_files = []
            for root, _, files in os.walk(import_folder):
                for file in files:
                    all_files.append(os.path.join(root, file))
            
            # Dateien löschen
            for file_path in all_files:
                try:
                    if os.path.exists(file_path):
                        os.remove(file_path)
                except Exception as e:
                    logger.error(f"Fehler beim Löschen von {file_path}: {str(e)}")
            
            # Alle Unterordner finden und sortieren (tiefste zuerst)
            all_dirs = []
            for root, dirs, _ in os.walk(import_folder):
                for dir_name in dirs:
                    dir_path = os.path.join(root, dir_name)
                    all_dirs.append(dir_path)
            
            # Sortiere Verzeichnisse nach Tiefe (tiefste zuerst)
            all_dirs.sort(key=lambda x: x.count(os.sep), reverse=True)
            
            # Verzeichnisse löschen
            for dir_path in all_dirs:
                try:
                    if os.path.exists(dir_path) and os.path.isdir(dir_path):
                        os.rmdir(dir_path)
                except Exception as e:
                    logger.error(f"Fehler beim Löschen von Verzeichnis {dir_path}: {str(e)}")
            
            # Kompletten Import-Ordner löschen und neu erstellen
            try:
                shutil.rmtree(import_folder, ignore_errors=True)
                os.makedirs(import_folder, exist_ok=True)
                self.update_status_threadsafe("Import", "Import-Ordner wurde geleert.")
                logger.info(f"Import-Ordner wurde komplett geleert: {import_folder}")
            except Exception as e:
                logger.error(f"Fehler beim kompletten Löschen des Import-Ordners: {str(e)}")
                self.update_status_threadsafe("Import", f"Fehler beim Löschen des Import-Ordners: {str(e)}")
        except Exception as e:
            logger.error(f"Unerwarteter Fehler beim Löschen des Import-Ordners: {str(e)}")
            self.update_status_threadsafe("Import", f"Fehler: {str(e)}")

    
    def update_status_threadsafe(self, prefix, status):
        """Aktualisiert den Status-Text thread-sicher
        
        Args:
            prefix (str): Prefix für die Statusmeldung
            status (str): Statusmeldung
        """
        QMetaObject.invokeMethod(self.status_label, "setText", 
                               Qt.QueuedConnection,
                               Q_ARG(str, f"{prefix}: {status}"))

        # Use QTimer.singleShot for thread-safe UI updates
        QTimer.singleShot(0, self.refresh_plan_list)
        
    def show_node_settings(self):
        """Zeigt den Dialog zur Konfiguration der DICOM-Knoten"""
        dialog = NodeSettingsDialog(self.settings_manager, self)
        if dialog.exec_():
            # Knoten-Checkboxes aktualisieren
            nodes = self.settings_manager.get_dicom_nodes()
            for i, checkbox in enumerate(self.node_checkboxes):
                if i < len(nodes):
                    checkbox.setText(f"{nodes[i]['name']} ({nodes[i]['ip']}:{nodes[i]['port']})") 
                    checkbox.setChecked(nodes[i]['enabled'])
    
    def show_local_node_settings(self):
        """Zeigt den Dialog zur Konfiguration des lokalen DICOM-Knotens"""
        dialog = LocalNodeSettingsDialog(self.settings_manager, self)
        dialog.exec_()
    
    
    
    def open_settings_dialog(self):
        """Zeigt den Dialog für allgemeine Einstellungen"""
        from settings_dialog import SettingsDialog
        dlg = SettingsDialog(self.settings_manager, self)
        dlg.exec_()
    
    def show_rules_dialog(self):
        """Zeigt den Dialog zur Konfiguration der Weiterleitungsregeln"""
        dialog = RulesDialog(self.rules_manager, self.settings_manager, self)
        dialog.exec_()
    
    def show_about(self):
        """Zeigt Informationen über die Anwendung"""
        QMessageBox.about(
            self,
            "Über DICOM-RT-Kaffee",
            "<h3>DICOM-RT-Kaffee</h3>"
            "<p>Ein DICOM-Plan-Manager mit GUI</p>"
            "<p>Version 1.0</p>"
        )
    
    def closeEvent(self, event):
        """Behandelt das Schließen des Fensters"""
        # Alle laufenden Threads stoppen
        if self.receiver_thread and self.receiver_thread.isRunning():
            self.receiver_thread.stop()
        
        for thread in self.send_threads.values():
            if thread.isRunning():
                thread.wait()
        
        event.accept()


def main():
    """Haupteinstiegspunkt der Anwendung"""
    app = QApplication(sys.argv)
    
    # Windows-spezifische Taskbar-Icon-Konfiguration
    try:
        import ctypes
        # App-ID für Windows Taskbar setzen
        ctypes.windll.shell32.SetCurrentProcessExplicitAppUserModelID('DICOM.RT.Station.1.0')
    except:
        pass  # Ignoriere Fehler auf anderen Betriebssystemen
    
    # Icon für die gesamte Anwendung setzen (vor dem Erstellen des Fensters)
    icon_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'icons', 'DICOM-RT-Station.ico')
    if os.path.exists(icon_path):
        app.setWindowIcon(QIcon(icon_path))
    
    # Verzeichnisse erstellen
    app_dir = os.path.dirname(os.path.abspath(__file__))
    os.makedirs(os.path.join(app_dir, 'logs'), exist_ok=True)
    os.makedirs(os.path.join(app_dir, 'received_plans'), exist_ok=True)
    
    window = MainWindow()
    window.show()
    
    sys.exit(app.exec_())


if __name__ == "__main__":
    main()
