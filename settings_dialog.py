from PyQt5.QtWidgets import QDialog, QVBoxLayout, QCheckBox, QPushButton, QHBoxLayout

class SettingsDialog(QDialog):
    """Zentrales Einstellungsfenster für DICOM-RT-Kaffee"""
    def __init__(self, settings_manager, parent=None):
        super().__init__(parent)
        self.settings_manager = settings_manager
        self.setWindowTitle("Allgemeine Einstellungen")
        self.setMinimumWidth(350)

        layout = QVBoxLayout()

        # Checkbox: DICOMs nach Import löschen
        self.delete_import_checkbox = QCheckBox("DICOMs nach Import aus dem Import-Ordner löschen")
        self.delete_import_checkbox.setChecked(self.settings_manager.get_clear_import_folder_after_import())
        layout.addWidget(self.delete_import_checkbox)

        # Checkbox: DICOMs nach Senden aus received_plans löschen
        self.delete_after_send_checkbox = QCheckBox("DICOMs nach Senden aus dem Empfangsordner löschen")
        self.delete_after_send_checkbox.setChecked(self.settings_manager.get_delete_after_send())
        layout.addWidget(self.delete_after_send_checkbox)

        # Checkbox: DICOM-Empfänger beim Start automatisch aktivieren
        self.auto_start_checkbox = QCheckBox("DICOM-Empfänger beim Start automatisch aktivieren")
        self.auto_start_checkbox.setChecked(self.settings_manager.get_auto_start_receiver())
        layout.addWidget(self.auto_start_checkbox)

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
        # Werte in settings.ini speichern
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
