#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Rules Manager für DICOM-RT-Kaffee

Verwaltet Regeln für die automatische Weiterleitung von DICOM-Daten.
"""

import os
import configparser
import logging

logger = logging.getLogger("DICOM-Rules")

class RulesManager:
    """Verwaltet Regeln für die automatische Weiterleitung von DICOM-Daten."""
    
    def __init__(self):
        """Initialisiert den RulesManager."""
        self.config_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'rules.ini')
        self.config = configparser.ConfigParser()
        self.create_default_rules_file()
        self.load_config()
    
    def create_default_rules_file(self):
        """Erstellt eine Default-rules.ini, falls sie fehlt."""
        if not os.path.exists(self.config_file):
            self.config['General'] = {
                'rules_enabled': 'False'
            }
            
            self.config['Rule1'] = {
                'name': 'Beispiel-Regel',
                'enabled': 'False',
                'source_ae': 'TESTAE',
                'target_nodes': 'Knoten1,Knoten2',
                'plan_label_match': 'ADP'
            }
            
            # Spezielle Regel für den Import-Ordner
            self.config['Rule2'] = {
                'name': 'Import-Ordner Regel',
                'enabled': 'True',  # Standardmäßig aktiviert
                'source_ae': 'IMPORT_FOLDER',  # Spezieller AE-Titel für den Import-Ordner
                'target_nodes': '',  # Keine Zielknoten standardmäßig
                'plan_label_match': ''  # Kein Plan-Label-Match standardmäßig
            }
            
            with open(self.config_file, 'w') as configfile:
                self.config.write(configfile)
            
            logger.info(f"Default-Rules-Datei erstellt: {self.config_file}")
    
    def load_config(self):
        """Lädt die Konfiguration aus der rules.ini."""
        self.config.read(self.config_file)
        logger.debug(f"Rules-Konfiguration geladen aus: {self.config_file}")
        
        # Stelle sicher, dass die IMPORT_FOLDER-Regel vorhanden ist
        self.ensure_import_folder_rule()
    
    def ensure_import_folder_rule(self):
        """Stellt sicher, dass die spezielle IMPORT_FOLDER-Regel existiert."""
        import_folder_rule_exists = False
        import_folder_rule_id = None
        
        # Prüfe, ob bereits eine IMPORT_FOLDER-Regel existiert
        for section in self.config.sections():
            if section.startswith('Rule'):
                if self.config.get(section, 'source_ae', fallback='') == 'IMPORT_FOLDER':
                    import_folder_rule_exists = True
                    import_folder_rule_id = section
                    break
        
        # Wenn keine IMPORT_FOLDER-Regel existiert, erstelle sie
        if not import_folder_rule_exists:
            # Nächste freie Rule-ID finden
            rule_id = None
            for i in range(1, 100):
                candidate = f'Rule{i}'
                if candidate not in self.config:
                    rule_id = candidate
                    break
            
            if rule_id:
                self.config[rule_id] = {
                    'name': 'Import-Ordner Regel',
                    'enabled': 'True',  # Standardmäßig aktiviert
                    'source_ae': 'IMPORT_FOLDER',  # Spezieller AE-Titel für den Import-Ordner
                    'target_nodes': '',  # Keine Zielknoten standardmäßig
                    'plan_label_match': ''  # Kein Plan-Label-Match standardmäßig
                }
                self.save_config()
                logger.info(f"IMPORT_FOLDER-Regel erstellt mit ID {rule_id}")
    
    def save_config(self):
        """Speichert die aktuelle Konfiguration in rules.ini."""
        with open(self.config_file, 'w') as configfile:
            self.config.write(configfile)
        logger.debug(f"Rules-Konfiguration gespeichert in: {self.config_file}")
    
    def get_rules_enabled(self):
        """Gibt zurück, ob die Regeln aktiviert sind."""
        return self.config.getboolean('General', 'rules_enabled', fallback=False)
    
    def set_rules_enabled(self, enabled):
        """Setzt, ob die Regeln aktiviert sind."""
        self.config['General']['rules_enabled'] = str(enabled)
        self.save_config()
    
    def get_all_rules(self):
        """Gibt alle konfigurierten Regeln zurück."""
        rules = []
        for section in self.config.sections():
            if section.startswith('Rule'):
                rule = {
                    'id': section,
                    'name': self.config.get(section, 'name', fallback=f'Regel {section[4:]}'),
                    'enabled': self.config.getboolean(section, 'enabled', fallback=False),
                    'source_ae': self.config.get(section, 'source_ae', fallback=''),
                    'target_nodes': self.config.get(section, 'target_nodes', fallback='').split(','),
                    'plan_label_match': self.config.get(section, 'plan_label_match', fallback='')
                }
                rules.append(rule)
        return rules
    
    def get_rule(self, rule_id):
        """Gibt eine bestimmte Regel zurück."""
        if rule_id in self.config:
            return {
                'id': rule_id,
                'name': self.config.get(rule_id, 'name', fallback=f'Regel {rule_id[4:]}'),
                'enabled': self.config.getboolean(rule_id, 'enabled', fallback=False),
                'source_ae': self.config.get(rule_id, 'source_ae', fallback=''),
                'target_nodes': self.config.get(rule_id, 'target_nodes', fallback='').split(','),
                'plan_label_match': self.config.get(rule_id, 'plan_label_match', fallback='')
            }
        return None
    
    def add_rule(self, name, source_ae, target_nodes, plan_label_match='', enabled=True):
        """Fügt eine neue Regel hinzu."""
        # Nächste freie Rule-ID finden
        rule_id = None
        for i in range(1, 100):
            candidate = f'Rule{i}'
            if candidate not in self.config:
                rule_id = candidate
                break
        
        if rule_id:
            self.config[rule_id] = {
                'name': name,
                'enabled': str(enabled),
                'source_ae': source_ae,
                'target_nodes': ','.join(target_nodes) if isinstance(target_nodes, list) else target_nodes,
                'plan_label_match': plan_label_match
            }
            self.save_config()
            return rule_id
        return None
    
    def update_rule(self, rule_id, name, source_ae, target_nodes, plan_label_match='', enabled=True):
        """Aktualisiert eine bestehende Regel."""
        if rule_id in self.config:
            self.config[rule_id] = {
                'name': name,
                'enabled': str(enabled),
                'source_ae': source_ae,
                'target_nodes': ','.join(target_nodes) if isinstance(target_nodes, list) else target_nodes,
                'plan_label_match': plan_label_match
            }
            self.save_config()
            return True
        return False
    
    def delete_rule(self, rule_id):
        """Löscht eine Regel."""
        if rule_id in self.config:
            self.config.remove_section(rule_id)
            self.save_config()
            return True
        return False
    
    def check_forwarding_rules(self, source_ae, plan_name, settings_manager):
        """Überprüft, ob für einen empfangenen Plan Weiterleitungsregeln zutreffen.
        
        Args:
            source_ae (str): AE-Titel der Quelle
            plan_name (str): Name des Plans
            settings_manager: SettingsManager-Instanz für Zugriff auf Knoten-Informationen
            
        Returns:
            list: Liste von Knoten-Infos, an die der Plan weitergeleitet werden soll
        """
        logger.info(f"Prüfe Weiterleitungsregeln für Plan '{plan_name}' von AE-Titel '{source_ae}'")
        
        if not self.get_rules_enabled():
            logger.info("Weiterleitungsregeln sind global deaktiviert")
            return []
        
        target_nodes = []
        all_rules = self.get_all_rules()
        logger.info(f"Prüfe {len(all_rules)} Weiterleitungsregeln")
        
        for rule in all_rules:
            rule_id = rule['id']
            rule_name = rule['name']
            
            if not rule['enabled']:
                logger.info(f"Regel '{rule_name}' (ID: {rule_id}) ist deaktiviert, überspringe")
                continue
                
            logger.info(f"Prüfe Regel '{rule_name}' (ID: {rule_id})")
            logger.info(f"  Regel-Kriterien: source_ae='{rule['source_ae']}', plan_label_match='{rule['plan_label_match']}'")
            logger.info(f"  Plan-Daten: source_ae='{source_ae}', plan_name='{plan_name}'")
                
            # Prüfen, ob der AE-Titel übereinstimmt
            if rule['source_ae'] and rule['source_ae'] != source_ae:
                logger.info(f"  AE-Titel stimmt nicht überein: '{rule['source_ae']}' != '{source_ae}'")
                continue
                
            # Prüfen, ob der Plan-Name den Suchbegriff enthält (wenn angegeben)
            if rule['plan_label_match'] and rule['plan_label_match'] not in plan_name:
                logger.info(f"  Plan-Label enthält nicht '{rule['plan_label_match']}'")
                continue
                
            # Regel trifft zu
            logger.info(f"  Regel '{rule_name}' trifft zu!")
            
            # Zielknoten hinzufügen
            if not rule['target_nodes'] or len(rule['target_nodes']) == 0 or (len(rule['target_nodes']) == 1 and rule['target_nodes'][0] == ''):
                logger.warning(f"  Regel '{rule_name}' hat keine Zielknoten konfiguriert")
                continue
                
            logger.info(f"  Zielknoten: {rule['target_nodes']}")
            
            for node_name in rule['target_nodes']:
                node_name = node_name.strip()
                if not node_name:
                    continue
                    
                node_info = settings_manager.get_node_info(node_name)
                if node_info and node_info.get('enabled', False):
                    logger.info(f"  Zielknoten '{node_name}' hinzugefügt")
                    target_nodes.append((node_name, node_info))
                else:
                    if not node_info:
                        logger.warning(f"  Zielknoten '{node_name}' nicht gefunden")
                    else:
                        logger.warning(f"  Zielknoten '{node_name}' ist deaktiviert")
        
        if target_nodes:
            logger.info(f"Insgesamt {len(target_nodes)} Zielknoten für Weiterleitung gefunden")
        else:
            logger.info("Keine passenden Weiterleitungsregeln gefunden")
            
        return target_nodes
