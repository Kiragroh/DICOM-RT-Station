#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Rules Dialog für DICOM-RT-Kaffee

Dialog zur Konfiguration von Weiterleitungsregeln.
"""

import os
import logging
from PyQt5.QtWidgets import (
    QDialog, QVBoxLayout, QHBoxLayout, QFormLayout, QLabel, QLineEdit, 
    QPushButton, QCheckBox, QComboBox, QListWidget, QListWidgetItem, 
    QTabWidget, QWidget, QMessageBox, QGroupBox, QScrollArea
)
from PyQt5.QtCore import Qt, QSize

logger = logging.getLogger("DICOM-Rules")

class RuleEditDialog(QDialog):
    """Dialog zum Bearbeiten einer einzelnen Regel."""
    
    def __init__(self, rules_manager, settings_manager, rule_id=None, parent=None):
        """Initialisiert den Dialog.
        
        Args:
            rules_manager: RulesManager-Instanz
            settings_manager: SettingsManager-Instanz für Zugriff auf Knoten-Informationen
            rule_id: ID der zu bearbeitenden Regel oder None für neue Regel
            parent: Elternobjekt
        """
        super().__init__(parent)
        self.rules_manager = rules_manager
        self.settings_manager = settings_manager
        self.rule_id = rule_id
        self.is_new = rule_id is None
        
        if self.is_new:
            self.setWindowTitle("Neue Regel erstellen")
            self.rule = {
                'name': '',
                'enabled': True,
                'source_ae': '',
                'target_nodes': [],
                'plan_label_match': ''
            }
        else:
            self.setWindowTitle("Regel bearbeiten")
            self.rule = self.rules_manager.get_rule(rule_id)
        
        self.setup_ui()
        self.load_rule_data()
    
    def setup_ui(self):
        """Richtet die Benutzeroberfläche ein."""
        self.setMinimumWidth(400)
        
        # Layout erstellen
        layout = QFormLayout()
        
        # Regelname
        self.name_edit = QLineEdit()
        layout.addRow("Regelname:", self.name_edit)
        
        # Aktiviert
        self.enabled_checkbox = QCheckBox("Regel aktivieren")
        layout.addRow("", self.enabled_checkbox)
        
        # Quell-AE
        self.source_ae_edit = QLineEdit()
        layout.addRow("Quell-AE Title:", self.source_ae_edit)
        
        # Plan-Label-Match
        self.plan_label_match_edit = QLineEdit()
        layout.addRow("Plan-Label enthält:", self.plan_label_match_edit)
        
        # Zielknoten
        self.target_nodes_group = QGroupBox("Zielknoten")
        target_nodes_layout = QVBoxLayout()
        
        # Liste der verfügbaren Knoten
        self.node_checkboxes = []
        nodes = self.settings_manager.get_dicom_nodes()
        
        for node in nodes:
            checkbox = QCheckBox(node['name'])
            checkbox.setChecked(False)  # Standardmäßig nicht ausgewählt
            target_nodes_layout.addWidget(checkbox)
            self.node_checkboxes.append((node['name'], checkbox))
        
        self.target_nodes_group.setLayout(target_nodes_layout)
        
        # ScrollArea für die Zielknoten
        scroll_area = QScrollArea()
        scroll_area.setWidgetResizable(True)
        scroll_area.setWidget(self.target_nodes_group)
        layout.addRow("", scroll_area)
        
        # Buttons
        button_layout = QHBoxLayout()
        save_button = QPushButton("Speichern")
        save_button.clicked.connect(self.save_rule)
        cancel_button = QPushButton("Abbrechen")
        cancel_button.clicked.connect(self.reject)
        
        button_layout.addWidget(save_button)
        button_layout.addWidget(cancel_button)
        
        # Hauptlayout
        main_layout = QVBoxLayout()
        main_layout.addLayout(layout)
        main_layout.addLayout(button_layout)
        
        self.setLayout(main_layout)
    
    def load_rule_data(self):
        """Lädt die Regeldaten in die UI-Elemente."""
        if not self.is_new:
            self.name_edit.setText(self.rule['name'])
            self.enabled_checkbox.setChecked(self.rule['enabled'])
            self.source_ae_edit.setText(self.rule['source_ae'])
            self.plan_label_match_edit.setText(self.rule['plan_label_match'])
            
            # Zielknoten markieren
            for node_name, checkbox in self.node_checkboxes:
                checkbox.setChecked(node_name in self.rule['target_nodes'])
    
    def save_rule(self):
        """Speichert die Regel."""
        name = self.name_edit.text().strip()
        if not name:
            QMessageBox.warning(self, "Fehler", "Bitte geben Sie einen Namen für die Regel ein.")
            return
        
        source_ae = self.source_ae_edit.text().strip()
        plan_label_match = self.plan_label_match_edit.text().strip()
        enabled = self.enabled_checkbox.isChecked()
        
        # Ausgewählte Zielknoten sammeln
        target_nodes = []
        for node_name, checkbox in self.node_checkboxes:
            if checkbox.isChecked():
                target_nodes.append(node_name)
        
        if not target_nodes:
            QMessageBox.warning(self, "Fehler", "Bitte wählen Sie mindestens einen Zielknoten aus.")
            return
        
        # Regel speichern
        if self.is_new:
            self.rule_id = self.rules_manager.add_rule(
                name, source_ae, target_nodes, plan_label_match, enabled
            )
            if self.rule_id:
                logger.info(f"Neue Regel erstellt: {name}")
                self.accept()
            else:
                QMessageBox.critical(self, "Fehler", "Fehler beim Erstellen der Regel.")
        else:
            success = self.rules_manager.update_rule(
                self.rule_id, name, source_ae, target_nodes, plan_label_match, enabled
            )
            if success:
                logger.info(f"Regel aktualisiert: {name}")
                self.accept()
            else:
                QMessageBox.critical(self, "Fehler", "Fehler beim Aktualisieren der Regel.")


class RulesDialog(QDialog):
    """Dialog zur Konfiguration von Weiterleitungsregeln."""
    
    def __init__(self, rules_manager, settings_manager, parent=None):
        """Initialisiert den Dialog.
        
        Args:
            rules_manager: RulesManager-Instanz
            settings_manager: SettingsManager-Instanz für Zugriff auf Knoten-Informationen
            parent: Elternobjekt
        """
        super().__init__(parent)
        self.rules_manager = rules_manager
        self.settings_manager = settings_manager
        
        self.setWindowTitle("Weiterleitungsregeln konfigurieren")
        self.setMinimumWidth(600)
        self.setMinimumHeight(400)
        
        self.setup_ui()
        self.load_rules()
    
    def setup_ui(self):
        """Richtet die Benutzeroberfläche ein."""
        # Hauptlayout
        layout = QVBoxLayout()
        
        # Globaler Aktivierungsschalter
        self.global_enabled_checkbox = QCheckBox("Weiterleitungsregeln aktivieren")
        self.global_enabled_checkbox.setChecked(self.rules_manager.get_rules_enabled())
        layout.addWidget(self.global_enabled_checkbox)
        
        # Erklärungstext
        info_label = QLabel(
            "<i>Hier können Sie Regeln definieren, nach denen empfangene DICOM-Pläne "
            "automatisch an bestimmte Zielknoten weitergeleitet werden. "
            "Eine Regel wird angewendet, wenn der AE-Titel der Quelle und optional "
            "ein Teil des Plan-Labels übereinstimmen.</i>"
        )
        info_label.setWordWrap(True)
        layout.addWidget(info_label)
        
        # Liste der Regeln
        self.rules_list = QListWidget()
        self.rules_list.setSelectionMode(QListWidget.SingleSelection)
        layout.addWidget(self.rules_list)
        
        # Buttons für die Regelliste
        button_layout = QHBoxLayout()
        
        self.add_button = QPushButton("Neue Regel")
        self.add_button.clicked.connect(self.add_rule)
        
        self.edit_button = QPushButton("Bearbeiten")
        self.edit_button.clicked.connect(self.edit_rule)
        self.edit_button.setEnabled(False)
        
        self.delete_button = QPushButton("Löschen")
        self.delete_button.clicked.connect(self.delete_rule)
        self.delete_button.setEnabled(False)
        
        button_layout.addWidget(self.add_button)
        button_layout.addWidget(self.edit_button)
        button_layout.addWidget(self.delete_button)
        layout.addLayout(button_layout)
        
        # Verbindung zur Aktualisierung der Button-Aktivierung
        self.rules_list.itemSelectionChanged.connect(self.update_buttons)
        
        # OK/Abbrechen-Buttons
        dialog_buttons = QHBoxLayout()
        ok_button = QPushButton("OK")
        ok_button.clicked.connect(self.save_and_close)
        cancel_button = QPushButton("Abbrechen")
        cancel_button.clicked.connect(self.reject)
        
        dialog_buttons.addWidget(ok_button)
        dialog_buttons.addWidget(cancel_button)
        layout.addLayout(dialog_buttons)
        
        self.setLayout(layout)
    
    def load_rules(self):
        """Lädt die Regeln in die Liste."""
        self.rules_list.clear()
        
        # Erstelle ein Widget für jede Regel mit Checkbox für Aktivierung
        rules = self.rules_manager.get_all_rules()
        for rule in rules:
            # Erstelle ein Widget für das Listenelement
            item_widget = QWidget()
            item_layout = QHBoxLayout(item_widget)
            item_layout.setContentsMargins(4, 4, 4, 4)
            
            # Checkbox für Aktivierung/Deaktivierung
            enabled_checkbox = QCheckBox()
            enabled_checkbox.setChecked(rule['enabled'])
            enabled_checkbox.setToolTip("Regel aktivieren/deaktivieren")
            
            # Speichere die Regel-ID in der Checkbox für spätere Verwendung
            enabled_checkbox.setProperty("rule_id", rule['id'])
            
            # Verbinde das Signal mit der Methode zum Umschalten der Aktivierung
            enabled_checkbox.stateChanged.connect(self.toggle_rule_enabled)
            
            # Label für den Regelnamen
            name_label = QLabel(rule['name'])
            if not rule['enabled']:
                name_label.setStyleSheet("color: gray;")
            
            # Füge die Widgets zum Layout hinzu
            item_layout.addWidget(enabled_checkbox)
            item_layout.addWidget(name_label, 1)  # 1 = stretch factor
            item_layout.addStretch()
            
            # Erstelle das Listenelement
            list_item = QListWidgetItem()
            list_item.setData(Qt.UserRole, rule['id'])
            list_item.setSizeHint(item_widget.sizeHint())
            
            # Füge das Element zur Liste hinzu und setze das Widget
            self.rules_list.addItem(list_item)
            self.rules_list.setItemWidget(list_item, item_widget)
    
    def update_buttons(self):
        """Aktualisiert den Zustand der Buttons basierend auf der Auswahl."""
        selected = len(self.rules_list.selectedItems()) > 0
        self.edit_button.setEnabled(selected)
        self.delete_button.setEnabled(selected)
    
    def add_rule(self):
        """Öffnet den Dialog zum Hinzufügen einer neuen Regel."""
        dialog = RuleEditDialog(self.rules_manager, self.settings_manager, parent=self)
        if dialog.exec_():
            self.load_rules()
    
    def edit_rule(self):
        """Öffnet den Dialog zum Bearbeiten der ausgewählten Regel."""
        selected_items = self.rules_list.selectedItems()
        if not selected_items:
            return
            
        rule_id = selected_items[0].data(Qt.UserRole)
        dialog = RuleEditDialog(self.rules_manager, self.settings_manager, rule_id, parent=self)
        if dialog.exec_():
            self.load_rules()
    
    def delete_rule(self):
        """Löscht die ausgewählte Regel nach Bestätigung."""
        selected_items = self.rules_list.selectedItems()
        if not selected_items:
            return
            
        rule_id = selected_items[0].data(Qt.UserRole)
        rule = self.rules_manager.get_rule(rule_id)
        
        reply = QMessageBox.question(
            self, 
            "Regel löschen", 
            f"Möchten Sie die Regel '{rule['name']}' wirklich löschen?",
            QMessageBox.Yes | QMessageBox.No, 
            QMessageBox.No
        )
        
        if reply == QMessageBox.Yes:
            if self.rules_manager.delete_rule(rule_id):
                self.load_rules()
                logger.info(f"Regel gelöscht: {rule['name']}")
            else:
                QMessageBox.critical(self, "Fehler", "Fehler beim Löschen der Regel.")
    
    def toggle_rule_enabled(self, state):
        """Schaltet die Aktivierung einer Regel um."""
        # Ermittle die Checkbox, die das Signal gesendet hat
        checkbox = self.sender()
        if not checkbox:
            return
            
        # Hole die Regel-ID aus der Checkbox
        rule_id = checkbox.property("rule_id")
        if not rule_id:
            return
            
        # Hole die aktuelle Regel
        rule = self.rules_manager.get_rule(rule_id)
        if not rule:
            return
            
        # Aktualisiere den enabled-Status
        enabled = state == Qt.Checked
        success = self.rules_manager.update_rule(
            rule_id, 
            rule['name'], 
            rule['source_ae'], 
            rule['target_nodes'], 
            rule['plan_label_match'], 
            enabled
        )
        
        if success:
            logger.info(f"Regel '{rule['name']}' {('aktiviert' if enabled else 'deaktiviert')}")
            
            # Aktualisiere die visuelle Darstellung
            item = None
            for i in range(self.rules_list.count()):
                if self.rules_list.item(i).data(Qt.UserRole) == rule_id:
                    item = self.rules_list.item(i)
                    break
                    
            if item:
                widget = self.rules_list.itemWidget(item)
                if widget:
                    # Finde das QLabel im Widget
                    for child in widget.children():
                        if isinstance(child, QLabel):
                            if enabled:
                                child.setStyleSheet("")
                            else:
                                child.setStyleSheet("color: gray;")
                            break
        else:
            logger.error(f"Fehler beim Aktualisieren des Status für Regel '{rule['name']}'")
    
    def save_and_close(self):
        """Speichert die globale Einstellung und schließt den Dialog."""
        enabled = self.global_enabled_checkbox.isChecked()
        self.rules_manager.set_rules_enabled(enabled)
        logger.info(f"Weiterleitungsregeln {'aktiviert' if enabled else 'deaktiviert'}")
        self.accept()
