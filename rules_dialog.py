#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Rules Dialog for DICOM-RT-Station

Dialog for configuring forwarding rules.
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
    """Dialog for editing a single rule."""
    
    def __init__(self, rules_manager, settings_manager, rule_id=None, parent=None):
        """Initializes the dialog.
        
        Args:
            rules_manager: RulesManager instance
            settings_manager: SettingsManager instance for accessing node information
            rule_id: ID of the rule to edit or None for new rule
            parent: Parent object
        """
        super().__init__(parent)
        self.rules_manager = rules_manager
        self.settings_manager = settings_manager
        self.rule_id = rule_id
        self.is_new = rule_id is None
        
        if self.is_new:
            self.setWindowTitle("Create New Rule")
            self.rule = {
                'name': '',
                'enabled': True,
                'source_ae': '',
                'target_nodes': [],
                'plan_label_match': ''
            }
        else:
            self.setWindowTitle("Edit Rule")
            self.rule = self.rules_manager.get_rule(rule_id)
        
        self.setup_ui()
        self.load_rule_data()
    
    def setup_ui(self):
        """Richtet die Benutzeroberfläche ein."""
        self.setMinimumWidth(400)
        
        # Create layout
        layout = QFormLayout()
        
        # Rule name
        self.name_edit = QLineEdit()
        layout.addRow("Rule Name:", self.name_edit)
        
        # Enabled
        self.enabled_checkbox = QCheckBox("Enable Rule")
        layout.addRow("", self.enabled_checkbox)
        
        # Source AE
        self.source_ae_edit = QLineEdit()
        layout.addRow("Source AE Title:", self.source_ae_edit)
        
        # Plan label match
        self.plan_label_match_edit = QLineEdit()
        layout.addRow("Plan Label Contains:", self.plan_label_match_edit)
        
        # Target nodes
        self.target_nodes_group = QGroupBox("Target Nodes")
        target_nodes_layout = QVBoxLayout()
        
        # List of available nodes
        self.node_checkboxes = []
        nodes = self.settings_manager.get_dicom_nodes()
        
        for node in nodes:
            checkbox = QCheckBox(node['name'])
            checkbox.setChecked(False)  # Not selected by default
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
        save_button = QPushButton("Save")
        save_button.clicked.connect(self.save_rule)
        cancel_button = QPushButton("Cancel")
        cancel_button.clicked.connect(self.reject)
        
        button_layout.addWidget(save_button)
        button_layout.addWidget(cancel_button)
        
        # Main layout
        main_layout = QVBoxLayout()
        main_layout.addLayout(layout)
        main_layout.addLayout(button_layout)
        
        self.setLayout(main_layout)
    
    def load_rule_data(self):
        """Loads the rule data into the UI elements."""
        if not self.is_new:
            self.name_edit.setText(self.rule['name'])
            self.enabled_checkbox.setChecked(self.rule['enabled'])
            self.source_ae_edit.setText(self.rule['source_ae'])
            self.plan_label_match_edit.setText(self.rule['plan_label_match'])
            
            # Mark target nodes
            for node_name, checkbox in self.node_checkboxes:
                checkbox.setChecked(node_name in self.rule['target_nodes'])
    
    def save_rule(self):
        """Saves the rule."""
        name = self.name_edit.text().strip()
        if not name:
            QMessageBox.warning(self, "Error", "Please enter a name for the rule.")
            return
        
        source_ae = self.source_ae_edit.text().strip()
        plan_label_match = self.plan_label_match_edit.text().strip()
        enabled = self.enabled_checkbox.isChecked()
        
        # Collect selected target nodes
        target_nodes = []
        for node_name, checkbox in self.node_checkboxes:
            if checkbox.isChecked():
                target_nodes.append(node_name)
        
        if not target_nodes:
            QMessageBox.warning(self, "Error", "Please select at least one target node.")
            return
        
        # Save rule
        if self.is_new:
            self.rule_id = self.rules_manager.add_rule(
                name, source_ae, target_nodes, plan_label_match, enabled
            )
            if self.rule_id:
                logger.info(f"New rule created: {name}")
                self.accept()
            else:
                QMessageBox.critical(self, "Error", "Error creating the rule.")
        else:
            success = self.rules_manager.update_rule(
                self.rule_id, name, source_ae, target_nodes, plan_label_match, enabled
            )
            if success:
                logger.info(f"Rule updated: {name}")
                self.accept()
            else:
                QMessageBox.critical(self, "Error", "Error updating the rule.")


class RulesDialog(QDialog):
    """Dialog for configuring forwarding rules."""
    
    def __init__(self, rules_manager, settings_manager, parent=None):
        """Initializes the dialog.
        
        Args:
            rules_manager: RulesManager instance
            settings_manager: SettingsManager instance for accessing node information
            parent: Parent object
        """
        super().__init__(parent)
        self.rules_manager = rules_manager
        self.settings_manager = settings_manager
        
        self.setWindowTitle("Configure Forwarding Rules")
        self.setMinimumWidth(600)
        self.setMinimumHeight(400)
        
        self.setup_ui()
        self.load_rules()
    
    def setup_ui(self):
        """Sets up the user interface."""
        # Main layout
        layout = QVBoxLayout()
        
        # Global activation switch
        self.global_enabled_checkbox = QCheckBox("Enable Forwarding Rules")
        self.global_enabled_checkbox.setChecked(self.rules_manager.get_rules_enabled())
        layout.addWidget(self.global_enabled_checkbox)
        
        # Explanation text
        info_label = QLabel(
            "<i>Here you can define rules for automatically forwarding received DICOM plans "
            "to specific target nodes. "
            "A rule is applied when the source AE title and optionally "
            "part of the plan label match.</i>"
        )
        info_label.setWordWrap(True)
        layout.addWidget(info_label)
        
        # List of rules
        self.rules_list = QListWidget()
        self.rules_list.setSelectionMode(QListWidget.SingleSelection)
        layout.addWidget(self.rules_list)
        
        # Buttons for rule list
        button_layout = QHBoxLayout()
        
        self.add_button = QPushButton("New Rule")
        self.add_button.clicked.connect(self.add_rule)
        
        self.edit_button = QPushButton("Edit")
        self.edit_button.clicked.connect(self.edit_rule)
        self.edit_button.setEnabled(False)
        
        self.delete_button = QPushButton("Delete")
        self.delete_button.clicked.connect(self.delete_rule)
        self.delete_button.setEnabled(False)
        
        button_layout.addWidget(self.add_button)
        button_layout.addWidget(self.edit_button)
        button_layout.addWidget(self.delete_button)
        layout.addLayout(button_layout)
        
        # Connection for updating button activation
        self.rules_list.itemSelectionChanged.connect(self.update_buttons)
        
        # OK/Abbrechen-Buttons
        dialog_buttons = QHBoxLayout()
        ok_button = QPushButton("OK")
        ok_button.clicked.connect(self.save_and_close)
        cancel_button = QPushButton("Cancel")
        cancel_button.clicked.connect(self.reject)
        
        dialog_buttons.addWidget(ok_button)
        dialog_buttons.addWidget(cancel_button)
        layout.addLayout(dialog_buttons)
        
        self.setLayout(layout)
    
    def load_rules(self):
        """Loads the rules into the list."""
        self.rules_list.clear()
        
        # Create a widget for each rule with checkbox for activation
        rules = self.rules_manager.get_all_rules()
        for rule in rules:
            # Create a widget for the list item
            item_widget = QWidget()
            item_layout = QHBoxLayout(item_widget)
            item_layout.setContentsMargins(4, 4, 4, 4)
            
            # Checkbox for enabling/disabling
            enabled_checkbox = QCheckBox()
            enabled_checkbox.setChecked(rule['enabled'])
            enabled_checkbox.setToolTip("Enable/disable rule")
            
            # Save the rule ID in the checkbox for later use
            enabled_checkbox.setProperty("rule_id", rule['id'])
            
            # Connect the signal to the method for toggling activation
            enabled_checkbox.stateChanged.connect(self.toggle_rule_enabled)
            
            # Label for the rule name
            name_label = QLabel(rule['name'])
            if not rule['enabled']:
                name_label.setStyleSheet("color: gray;")
            
            # Add the widgets to the layout
            item_layout.addWidget(enabled_checkbox)
            item_layout.addWidget(name_label, 1)  # 1 = stretch factor
            item_layout.addStretch()
            
            # Create the list item
            list_item = QListWidgetItem()
            list_item.setData(Qt.UserRole, rule['id'])
            list_item.setSizeHint(item_widget.sizeHint())
            
            # Add the element to the list and set the widget
            self.rules_list.addItem(list_item)
            self.rules_list.setItemWidget(list_item, item_widget)
    
    def update_buttons(self):
        """Updates the button state based on selection."""
        selected = len(self.rules_list.selectedItems()) > 0
        self.edit_button.setEnabled(selected)
        self.delete_button.setEnabled(selected)
    
    def add_rule(self):
        """Opens the dialog for adding a new rule."""
        dialog = RuleEditDialog(self.rules_manager, self.settings_manager, parent=self)
        if dialog.exec_():
            self.load_rules()
    
    def edit_rule(self):
        """Opens the dialog for editing the selected rule."""
        selected_items = self.rules_list.selectedItems()
        if not selected_items:
            return
            
        rule_id = selected_items[0].data(Qt.UserRole)
        dialog = RuleEditDialog(self.rules_manager, self.settings_manager, rule_id, parent=self)
        if dialog.exec_():
            self.load_rules()
    
    def delete_rule(self):
        """Deletes the selected rule after confirmation."""
        selected_items = self.rules_list.selectedItems()
        if not selected_items:
            return
            
        rule_id = selected_items[0].data(Qt.UserRole)
        rule = self.rules_manager.get_rule(rule_id)
        
        reply = QMessageBox.question(
            self, 
            "Delete Rule", 
            f"Do you really want to delete the rule '{rule['name']}'?",
            QMessageBox.Yes | QMessageBox.No, 
            QMessageBox.No
        )
        
        if reply == QMessageBox.Yes:
            if self.rules_manager.delete_rule(rule_id):
                self.load_rules()
                logger.info(f"Rule deleted: {rule['name']}")
            else:
                QMessageBox.critical(self, "Error", "Error deleting the rule.")
    
    def toggle_rule_enabled(self, state):
        """Toggles the activation of a rule."""
        # Determine the checkbox that sent the signal
        checkbox = self.sender()
        if not checkbox:
            return
            
        # Get the rule ID from the checkbox
        rule_id = checkbox.property("rule_id")
        if not rule_id:
            return
            
        # Get the current rule
        rule = self.rules_manager.get_rule(rule_id)
        if not rule:
            return
            
        # Update the enabled status
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
            logger.info(f"Rule '{rule['name']}' {('enabled' if enabled else 'disabled')}")
            
            # Update the visual representation
            item = None
            for i in range(self.rules_list.count()):
                if self.rules_list.item(i).data(Qt.UserRole) == rule_id:
                    item = self.rules_list.item(i)
                    break
                    
            if item:
                widget = self.rules_list.itemWidget(item)
                if widget:
                    # Find the QLabel in the widget
                    for child in widget.children():
                        if isinstance(child, QLabel):
                            if enabled:
                                child.setStyleSheet("")
                            else:
                                child.setStyleSheet("color: gray;")
                            break
        else:
            logger.error(f"Error updating status for rule '{rule['name']}'")
    
    def save_and_close(self):
        """Saves the global setting and closes the dialog."""
        enabled = self.global_enabled_checkbox.isChecked()
        self.rules_manager.set_rules_enabled(enabled)
        logger.info(f"Forwarding rules {'enabled' if enabled else 'disabled'}")
        self.accept()
