from PyQt5.QtWidgets import QDialog, QVBoxLayout, QCheckBox, QPushButton, QHBoxLayout

class SettingsDialog(QDialog):
    """Central settings window for DICOM-RT-Station"""
    def __init__(self, settings_manager, parent=None):
        super().__init__(parent)
        self.settings_manager = settings_manager
        self.setWindowTitle("General Settings")
        self.setMinimumWidth(350)

        layout = QVBoxLayout()

        # Checkbox: Delete DICOMs after import
        self.delete_import_checkbox = QCheckBox("Delete DICOMs from import folder after import")
        self.delete_import_checkbox.setChecked(self.settings_manager.get_clear_import_folder_after_import())
        layout.addWidget(self.delete_import_checkbox)

        # Checkbox: Delete DICOMs after sending from received_plans
        self.delete_after_send_checkbox = QCheckBox("Delete DICOMs from receive folder after sending")
        self.delete_after_send_checkbox.setChecked(self.settings_manager.get_delete_after_send())
        layout.addWidget(self.delete_after_send_checkbox)

        # Checkbox: Automatically activate DICOM receiver at startup
        self.auto_start_checkbox = QCheckBox("Automatically activate DICOM receiver at startup")
        self.auto_start_checkbox.setChecked(self.settings_manager.get_auto_start_receiver())
        layout.addWidget(self.auto_start_checkbox)

        # Buttons
        button_layout = QHBoxLayout()
        save_button = QPushButton("Save")
        save_button.clicked.connect(self.save_settings)
        cancel_button = QPushButton("Cancel")
        cancel_button.clicked.connect(self.reject)
        button_layout.addWidget(save_button)
        button_layout.addWidget(cancel_button)
        layout.addLayout(button_layout)

        self.setLayout(layout)

    def save_settings(self):
        # Save values to settings.ini
        self.settings_manager.config['General']['clear_import_folder_after_import'] = (
            'True' if self.delete_import_checkbox.isChecked() else 'False')
        self.settings_manager.config['General']['auto_start_receiver'] = (
            'True' if self.auto_start_checkbox.isChecked() else 'False')
        if 'SendOptions' not in self.settings_manager.config:
            self.settings_manager.config['SendOptions'] = {}
        self.settings_manager.config['SendOptions']['delete_after_send'] = (
            'True' if self.delete_after_send_checkbox.isChecked() else 'False')
        with open(self.settings_manager.config_file, 'w') as f:
            self.settings_manager.config.write(f)
        self.accept()
