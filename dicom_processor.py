#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
DICOM-Prozessor-Modul für DICOM-RT-Kaffee

Dieses Modul verarbeitet DICOM-Dateien, organisiert sie in Ordner
und sendet sie an verschiedene DICOM-Knoten.
"""

import os
import sys
import time
import logging
import threading
import shutil
from pathlib import Path
from datetime import datetime
from collections import defaultdict
import re

# PyDICOM-Bibliotheken
import pydicom
from pynetdicom import AE, debug_logger, StoragePresentationContexts, evt
from pynetdicom.sop_class import (
    CTImageStorage, 
    RTStructureSetStorage,
    RTPlanStorage, 
    RTDoseStorage,
    RTIonPlanStorage,
    RTIonBeamsTreatmentRecordStorage,
    RTBeamsTreatmentRecordStorage,
    UID
)
from pydicom.uid import ImplicitVRLittleEndian, ExplicitVRLittleEndian
# Eigene RTPLAN UID definieren
MyPrivateRTPlanStorage = UID('1.2.246.352.70.1.70')

# Watchdog für Dateiüberwachung
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

# Logging-Konfiguration
logger = logging.getLogger('DICOM-Processor')


class DicomProcessor:
    """Hauptklasse für die Verarbeitung von DICOM-Dateien"""
    
    def __init__(self, settings_manager):
        """Initialisiert den DICOM-Prozessor
        
        Args:
            settings_manager (SettingsManager): Instanz für dynamische Einstellungen
        """
        self.settings_manager = settings_manager
        self.watch_folder = self.settings_manager.get_received_plans_folder()
        self.import_folder = self.settings_manager.get_import_folder()
        os.makedirs(self.watch_folder, exist_ok=True)
        os.makedirs(self.import_folder, exist_ok=True)
        # Sicherstellen, dass der Failed-Ordner existiert
        self.failed_folder = os.path.join(self.watch_folder, "failed")
        os.makedirs(self.failed_folder, exist_ok=True)
        
        # Watchdog-Komponenten
        self.observer = None
        self.event_handler = None
        
        # DICOM-Empfänger-Komponenten
        self.ae = None
        self.server = None
        
        # Timer für Inaktivitätserkennung
        self.folder_timers = {}
        self.pending_files = {}
        self.timer_lock = threading.Lock()
        
        # DICOM-Modalitätsreihenfolge für geordnetes Senden
        self.modality_order = {
            "CT": 1, 
            "RTSTRUCT": 2, 
            "RTPLAN": 3, 
            "RTDOSE": 4
        }
    
    def start_receiver(self, port=1334, new_plan_callback=None):
        """Startet den DICOM-Empfänger
        
        Args:
            port (int): Port für DICOM-Empfang
            new_plan_callback (callable): Funktion, die bei neuen Plänen aufgerufen wird
        
        Returns:
            bool: True bei erfolgreichem Start, sonst False
        """
        try:
            # Application Entity erstellen
            self.ae = AE(ae_title=b"DICOM-RT-KAFFEE")
            
            # Storage SOP Classes für alle unterstützten Modalitäten
            self.ae.supported_contexts = StoragePresentationContexts
            # Eigene RTPLAN UID explizit als unterstützten Kontext hinzufügen
            self.ae.add_supported_context(MyPrivateRTPlanStorage, [ImplicitVRLittleEndian, ExplicitVRLittleEndian])
            
            # Handler für eingehende Daten
            self.new_plan_callback = new_plan_callback
            
            # Ereignishandler registrieren
            handlers = [
                (evt.EVT_C_STORE, self.handle_store)
            ]
            
            # Server starten
            self.server = self.ae.start_server(
                ("0.0.0.0", port), 
                block=False,
                evt_handlers=handlers
            )
            
            logger.info(f"DICOM-Empfänger gestartet auf Port {port}")
            return True
            
        except Exception as e:
            logger.error(f"Fehler beim Starten des DICOM-Empfängers: {str(e)}")
            return False
    
    def stop_receiver(self):
        """Stoppt den DICOM-Empfänger"""
        if self.server:
            self.server.shutdown()
            self.server = None
            logger.info("DICOM-Empfänger gestoppt")
    
    def handle_store(self, event):
        """Handler für eingehende DICOM-Daten mit gepuffertem Empfang und Plan-Gruppierung."""
        try:
            import threading, tempfile, uuid, time, os, pydicom
            ds = event.dataset
            ds.file_meta = event.file_meta
            
            # Speichere den AE-Titel der sendenden Station in den Metadaten
            if hasattr(event, 'assoc') and hasattr(event.assoc, 'requestor') and hasattr(event.assoc.requestor, 'ae_title'):
                # Behandle sowohl bytes als auch str Typen
                ae_title = event.assoc.requestor.ae_title
                if isinstance(ae_title, bytes):
                    source_ae_title = ae_title.decode('ascii', 'ignore')
                else:
                    source_ae_title = str(ae_title)
                
                logger.info(f"Empfange DICOM-Daten von AE-Titel: {source_ae_title}")
                
                # Speichere den AE-Titel in den Metadaten
                if not hasattr(ds, 'file_meta') or not ds.file_meta:
                    ds.file_meta = pydicom.dataset.FileMetaDataset()
                ds.file_meta.SourceApplicationEntityTitle = source_ae_title

            # Buffer key: (patient_id, study_uid)
            patient_id = getattr(ds, "PatientID", "unknown")
            study_uid = getattr(ds, "StudyInstanceUID", "unknown")
            modality = getattr(ds, "Modality", "unknown")
            buffer_key = (str(patient_id), str(study_uid))

            # Setup buffer and locks
            if not hasattr(self, '_receive_buffer_lock'):
                self._receive_buffer_lock = threading.Lock()
            if not hasattr(self, '_receive_buffer'):
                self._receive_buffer = {}
            if not hasattr(self, '_receive_time'):
                self._receive_time = {}
            if not hasattr(self, '_receive_temp_dir_base'):
                self._receive_temp_dir_base = tempfile.mkdtemp(prefix="dicom_receive_")

            buffer_lock = self._receive_buffer_lock
            receive_buffer = self._receive_buffer
            receive_time = self._receive_time
            temp_dir_base = self._receive_temp_dir_base

            # Save incoming file to temp dir
            with buffer_lock:
                key_dir = os.path.join(temp_dir_base, f"{patient_id}_{study_uid}")
                os.makedirs(key_dir, exist_ok=True)
                unique_name = f"{modality}_{uuid.uuid4().hex}.dcm"
                temp_file_path = os.path.join(key_dir, unique_name)
                
                # Ensure SOPInstanceUID is present and properly set in file_meta
                if not hasattr(ds, 'SOPInstanceUID') or not ds.SOPInstanceUID:
                    if hasattr(ds.file_meta, 'MediaStorageSOPInstanceUID'):
                        ds.SOPInstanceUID = ds.file_meta.MediaStorageSOPInstanceUID
                    else:
                        # Generate a new UID if none exists
                        from pydicom.uid import generate_uid
                        new_uid = generate_uid()
                        ds.SOPInstanceUID = new_uid
                        if hasattr(ds, 'file_meta'):
                            ds.file_meta.MediaStorageSOPInstanceUID = new_uid
                
                # Ensure file_meta is complete
                if not hasattr(ds, 'file_meta') or not ds.file_meta:
                    ds.file_meta = pydicom.dataset.FileMetaDataset()
                
                # Ensure MediaStorageSOPInstanceUID matches SOPInstanceUID
                if hasattr(ds, 'SOPInstanceUID') and ds.SOPInstanceUID:
                    ds.file_meta.MediaStorageSOPInstanceUID = ds.SOPInstanceUID
                
                # Special handling for RTDOSE files to preserve pixel data
                if modality == "RTDOSE":
                    # For RTDOSE, write the raw bytes directly to preserve all data
                    with open(temp_file_path, 'wb') as f:
                        f.write(event.request.DataSet.getvalue())
                    logger.info(f"RTDOSE received and saved directly: {temp_file_path} ({os.path.getsize(temp_file_path)} bytes)")
                else:
                    # For other modalities, use pydicom with write_like_original=False
                    pydicom.dcmwrite(temp_file_path, ds, write_like_original=False)
                
                if buffer_key not in receive_buffer:
                    receive_buffer[buffer_key] = []
                receive_buffer[buffer_key].append(temp_file_path)
                receive_time[buffer_key] = time.time()

            # Start/refresh inactivity timer thread for this buffer key
            def flush_buffer_after_timeout(buffer_key, timeout=2.0):
                time.sleep(timeout)
                with buffer_lock:
                    now = time.time()
                    last_time = receive_time.get(buffer_key, 0)
                    if now - last_time < timeout - 0.1:
                        return
                    file_list = receive_buffer.pop(buffer_key, [])
                    receive_time.pop(buffer_key, None)
                if not file_list:
                    return
                try:
                    self._group_and_move_received_files(file_list)
                except Exception as e:
                    logger.error(f"Fehler beim Gruppieren und Verschieben empfangener DICOM-Dateien: {str(e)}")

            flush_thread_name = f"flush_buffer_{patient_id}_{study_uid}"
            if hasattr(self, f'_{flush_thread_name}_thread'):
                old_thread = getattr(self, f'_{flush_thread_name}_thread')
                if old_thread.is_alive():
                    pass
            flush_thread = threading.Thread(target=flush_buffer_after_timeout, args=(buffer_key,), daemon=True)
            setattr(self, f'_{flush_thread_name}_thread', flush_thread)
            flush_thread.start()

            return 0x0000  # Success
        except Exception as e:
            logger.error(f"Fehler beim Verarbeiten einer eingehenden DICOM-Datei: {str(e)}")
            return 0xC001  # Failure

    def _group_and_move_received_files(self, file_list):
        """Group and move received DICOM files into unified plan folder, mimicking import logic."""
        import shutil
        import pydicom
        file_data = {}
        plan_files = []
        ct_files = []
        structure_files = []
        other_files = []
        for file_path in file_list:
            try:
                ds = pydicom.dcmread(file_path, force=True, stop_before_pixels=True)
                modality = getattr(ds, "Modality", "UNKNOWN")
                file_data[file_path] = {
                    'ds': ds,
                    'modality': modality,
                    'patient_id': getattr(ds, "PatientID", "unknown"),
                    'patient_name': getattr(ds, "PatientName", "unknown"),
                    'sop_instance_uid': getattr(ds, "SOPInstanceUID", "unknown")
                }
                if modality == "RTPLAN":
                    plan_files.append(file_path)
                elif modality == "CT":
                    ct_files.append(file_path)
                elif modality == "RTSTRUCT":
                    structure_files.append(file_path)
                else:
                    other_files.append(file_path)
            except Exception as e:
                logger.error(f"Fehler beim Lesen von DICOM-Datei {file_path}: {str(e)}")
                continue
        # --- Grouping logic as in process_import_folder ---
        plan_ref_map = {}
        frame_ref_map = {}
        for plan_file in plan_files:
            plan_ds = file_data[plan_file]['ds']
            plan_sop_uid = getattr(plan_ds, "SOPInstanceUID", "unknown")
            plan_ref_map[plan_sop_uid] = []
            for dose_file in other_files:
                dose_ds = file_data[dose_file]['ds']
                try:
                    dose_ref_uid = None
                    if hasattr(dose_ds, "ReferencedRTPlanSequence") and dose_ds.ReferencedRTPlanSequence and hasattr(dose_ds.ReferencedRTPlanSequence[0], "ReferencedSOPInstanceUID"):
                        dose_ref_uid = dose_ds.ReferencedRTPlanSequence[0].ReferencedSOPInstanceUID
                except Exception as e:
                    dose_ref_uid = None
                if dose_ref_uid == plan_sop_uid:
                    plan_ref_map[plan_sop_uid].append(dose_file)
            frame_ref_uid = getattr(plan_ds, "FrameOfReferenceUID", "unknown")
            frame_ref_map[frame_ref_uid] = []
            for ct_file in ct_files:
                ct_ds = file_data[ct_file]['ds']
                ct_frame_ref_uid = getattr(ct_ds, "FrameOfReferenceUID", "unknown")
                if ct_frame_ref_uid == frame_ref_uid:
                    frame_ref_map[frame_ref_uid].append(ct_file)
            for struct_file in structure_files:
                struct_ds = file_data[struct_file]['ds']
                struct_frame_ref_uid = getattr(struct_ds, "FrameOfReferenceUID", "unknown")
                if struct_frame_ref_uid == frame_ref_uid:
                    frame_ref_map[frame_ref_uid].append(struct_file)
        # --- Move grouped files to unified plan folder ---
        for plan_file_path in plan_files:
            try:
                plan_ds = file_data[plan_file_path]['ds']
                patient_id = getattr(plan_ds, "PatientID", "unknown")
                patient_name = getattr(plan_ds, "PatientName", "unknown")
                plan_name = getattr(plan_ds, "RTPlanLabel", "unknown")
                study_id = getattr(plan_ds, "StudyInstanceUID", "unknown").split('.')[-1]
                safe_patient_name = self.sanitize_path_component(patient_name)
                safe_patient_id = self.sanitize_path_component(patient_id)
                safe_plan_name = self.sanitize_path_component(plan_name)
                safe_study_id = self.sanitize_path_component(study_id)
                patient_folder = os.path.join(self.watch_folder, f"{safe_patient_name} ({safe_patient_id})")
                plan_folder = os.path.join(patient_folder, f"{safe_plan_name}_{safe_study_id}")
                os.makedirs(plan_folder, exist_ok=True)
                # Plan-Datei
                plan_filename = f"RTPLAN_{safe_plan_name}.dcm"
                dest_path = os.path.join(plan_folder, plan_filename)
                if os.path.exists(dest_path):
                    os.remove(dest_path)
                
                # Speichere den AE-Titel der Quelle für spätere Weiterleitungsregeln
                source_ae = getattr(plan_ds, "SourceApplicationEntityTitle", "UNKNOWN")
                if not source_ae or source_ae == "UNKNOWN":
                    # Versuche, den AE-Titel aus den Metadaten zu holen
                    if hasattr(plan_ds, "file_meta") and hasattr(plan_ds.file_meta, "SourceApplicationEntityTitle"):
                        source_ae = plan_ds.file_meta.SourceApplicationEntityTitle
                    
                # Ensure SOPInstanceUID is preserved when moving the file
                # Instead of moving, read the file, ensure UIDs, and write to destination
                try:
                    # Read with force=True but don't skip pixels to preserve RTPLAN data
                    ds = pydicom.dcmread(plan_file_path, force=True, stop_before_pixels=False)
                    
                    # Ensure SOPInstanceUID is present
                    if not hasattr(ds, 'SOPInstanceUID') or not ds.SOPInstanceUID:
                        if hasattr(ds.file_meta, 'MediaStorageSOPInstanceUID'):
                            ds.SOPInstanceUID = ds.file_meta.MediaStorageSOPInstanceUID
                        else:
                            # Generate a new UID if none exists
                            from pydicom.uid import generate_uid
                            new_uid = generate_uid()
                            ds.SOPInstanceUID = new_uid
                            if hasattr(ds, 'file_meta'):
                                ds.file_meta.MediaStorageSOPInstanceUID = new_uid
                    
                    # Ensure file_meta is complete
                    if not hasattr(ds, 'file_meta') or not ds.file_meta:
                        ds.file_meta = pydicom.dataset.FileMetaDataset()
                    
                    # Ensure MediaStorageSOPInstanceUID matches SOPInstanceUID
                    if hasattr(ds, 'SOPInstanceUID') and ds.SOPInstanceUID:
                        ds.file_meta.MediaStorageSOPInstanceUID = ds.SOPInstanceUID
                    
                    # Write with explicit file_meta and preserve original pixel data
                    pydicom.dcmwrite(dest_path, ds, write_like_original=False)
                    os.remove(plan_file_path)  # Remove the original file after successful write
                except Exception as e:
                    logger.error(f"Error ensuring SOPInstanceUID in {plan_file_path}: {str(e)}")
                    # Fall back to simple move if the above fails
                    shutil.move(plan_file_path, dest_path)
                # Dosis-Dateien
                plan_sop_uid = getattr(plan_ds, "SOPInstanceUID", "unknown")
                if plan_sop_uid in plan_ref_map:
                    for dose_file_path in plan_ref_map[plan_sop_uid]:
                        dose_ds = file_data[dose_file_path]['ds']
                        dose_filename = f"RTDOSE_{safe_plan_name}.dcm"
                        dose_dest_path = os.path.join(plan_folder, dose_filename)
                        if os.path.exists(dose_dest_path):
                            os.remove(dose_dest_path)
                            
                        # For RTDOSE files, use direct file copy to preserve all data exactly as received
                        try:
                            # First, read just the header to check/fix SOPInstanceUID
                            ds = pydicom.dcmread(dose_file_path, force=True, stop_before_pixels=True)
                            
                            # Check if SOPInstanceUID is present
                            sop_uid_fixed = False
                            if not hasattr(ds, 'SOPInstanceUID') or not ds.SOPInstanceUID:
                                if hasattr(ds.file_meta, 'MediaStorageSOPInstanceUID'):
                                    # Only log that we would fix it, but don't actually modify the file
                                    logger.info(f"RTDOSE file missing SOPInstanceUID, would use MediaStorageSOPInstanceUID: {ds.file_meta.MediaStorageSOPInstanceUID}")
                                    sop_uid_fixed = True
                                else:
                                    # Only log that we would generate a new one, but don't actually modify the file
                                    logger.info(f"RTDOSE file missing SOPInstanceUID and MediaStorageSOPInstanceUID, would generate new UID")
                                    sop_uid_fixed = True
                            
                            # If we would need to fix UIDs, log a warning but proceed with direct copy
                            if sop_uid_fixed:
                                logger.warning(f"RTDOSE file {dose_file_path} has UID issues, but using direct copy to preserve pixel data")
                            
                            # Use direct file copy to preserve all data exactly as received
                            shutil.copy2(dose_file_path, dose_dest_path)
                            
                            # Log file sizes to help diagnose issues
                            src_size = os.path.getsize(dose_file_path)
                            dest_size = os.path.getsize(dose_dest_path)
                            logger.info(f"RTDOSE copied: {dose_file_path} ({src_size} bytes) -> {dose_dest_path} ({dest_size} bytes)")
                            
                            # Remove original file after successful copy
                            os.remove(dose_file_path)
                        except Exception as e:
                            logger.error(f"Error copying RTDOSE file {dose_file_path}: {str(e)}")
                            # Fall back to simple copy if the above fails
                            try:
                                shutil.copy2(dose_file_path, dose_dest_path)
                                os.remove(dose_file_path)
                            except Exception as e2:
                                logger.error(f"Fallback copy also failed: {str(e2)}")
                                # If all else fails, try to move the file
                                shutil.move(dose_file_path, dose_dest_path)
                # CT/Structure-Dateien
                frame_ref_uid = getattr(plan_ds, "FrameOfReferenceUID", "unknown")
                if frame_ref_uid and frame_ref_uid in frame_ref_map:
                    for related_file_path in frame_ref_map[frame_ref_uid]:
                        related_data = file_data[related_file_path]
                        related_modality = related_data['modality']
                        if related_modality == "CT":
                            sop_instance = related_data['sop_instance_uid']
                            safe_sop_instance = self.sanitize_path_component(sop_instance)
                            ct_filename = f"CT.{safe_sop_instance}.dcm"
                            ct_dest_path = os.path.join(plan_folder, ct_filename)
                            if os.path.exists(ct_dest_path):
                                os.remove(ct_dest_path)
                                
                            # Ensure SOPInstanceUID is preserved when moving the file
                            try:
                                # Read with force=True but don't skip pixels to preserve CT image data
                                ds = pydicom.dcmread(related_file_path, force=True, stop_before_pixels=False)
                                
                                # Ensure SOPInstanceUID is present
                                if not hasattr(ds, 'SOPInstanceUID') or not ds.SOPInstanceUID:
                                    if hasattr(ds.file_meta, 'MediaStorageSOPInstanceUID'):
                                        ds.SOPInstanceUID = ds.file_meta.MediaStorageSOPInstanceUID
                                    else:
                                        # Generate a new UID if none exists
                                        from pydicom.uid import generate_uid
                                        new_uid = generate_uid()
                                        ds.SOPInstanceUID = new_uid
                                        if hasattr(ds, 'file_meta'):
                                            ds.file_meta.MediaStorageSOPInstanceUID = new_uid
                                
                                # Ensure file_meta is complete
                                if not hasattr(ds, 'file_meta') or not ds.file_meta:
                                    ds.file_meta = pydicom.dataset.FileMetaDataset()
                                
                                # Ensure MediaStorageSOPInstanceUID matches SOPInstanceUID
                                if hasattr(ds, 'SOPInstanceUID') and ds.SOPInstanceUID:
                                    ds.file_meta.MediaStorageSOPInstanceUID = ds.SOPInstanceUID
                                
                                # Write with explicit file_meta and preserve original pixel data
                                pydicom.dcmwrite(ct_dest_path, ds, write_like_original=False)
                                # Don't remove CT files as they might be needed by other plans
                            except Exception as e:
                                logger.error(f"Error ensuring SOPInstanceUID in {related_file_path}: {str(e)}")
                                # Fall back to simple copy if the above fails
                                shutil.copy2(related_file_path, ct_dest_path)
                        elif related_modality == "RTSTRUCT":
                            struct_filename = f"RTSTRUCT_{safe_plan_name}.dcm"
                            struct_dest_path = os.path.join(plan_folder, struct_filename)
                            if os.path.exists(struct_dest_path):
                                os.remove(struct_dest_path)
                                
                            # Ensure SOPInstanceUID is preserved when moving the file
                            try:
                                # Read with force=True but don't skip pixels to preserve RTSTRUCT data
                                ds = pydicom.dcmread(related_file_path, force=True, stop_before_pixels=False)
                                
                                # Ensure SOPInstanceUID is present
                                if not hasattr(ds, 'SOPInstanceUID') or not ds.SOPInstanceUID:
                                    if hasattr(ds.file_meta, 'MediaStorageSOPInstanceUID'):
                                        ds.SOPInstanceUID = ds.file_meta.MediaStorageSOPInstanceUID
                                    else:
                                        # Generate a new UID if none exists
                                        from pydicom.uid import generate_uid
                                        new_uid = generate_uid()
                                        ds.SOPInstanceUID = new_uid
                                        if hasattr(ds, 'file_meta'):
                                            ds.file_meta.MediaStorageSOPInstanceUID = new_uid
                                
                                # Ensure file_meta is complete
                                if not hasattr(ds, 'file_meta') or not ds.file_meta:
                                    ds.file_meta = pydicom.dataset.FileMetaDataset()
                                
                                # Ensure MediaStorageSOPInstanceUID matches SOPInstanceUID
                                if hasattr(ds, 'SOPInstanceUID') and ds.SOPInstanceUID:
                                    ds.file_meta.MediaStorageSOPInstanceUID = ds.SOPInstanceUID
                                
                                # Write with explicit file_meta and preserve original pixel data
                                pydicom.dcmwrite(struct_dest_path, ds, write_like_original=False)
                                # Don't remove RTSTRUCT files as they might be needed by other plans
                            except Exception as e:
                                logger.error(f"Error ensuring SOPInstanceUID in {related_file_path}: {str(e)}")
                                # Fall back to simple copy if the above fails
                                shutil.copy2(related_file_path, struct_dest_path)
            except Exception as e:
                logger.error(f"Fehler beim Gruppieren/Verschieben des Plans {plan_file_path}: {str(e)}")
        # Remove temp files for any files not grouped (orphans)
        for file_path in file_list:
            if os.path.exists(file_path):
                try:
                    os.remove(file_path)
                except Exception as e:
                    logger.warning(f"Konnte temporäre Datei {file_path} nicht entfernen: {str(e)}")
        
        # Prüfe Weiterleitungsregeln für jeden verarbeiteten Plan
        try:
            from rules_manager import RulesManager
            rules_manager = RulesManager()
            
            for plan_file_path in plan_files:
                try:
                    plan_ds = file_data[plan_file_path]['ds']
                    patient_id = getattr(plan_ds, "PatientID", "unknown")
                    patient_name = getattr(plan_ds, "PatientName", "unknown")
                    plan_name = getattr(plan_ds, "RTPlanLabel", "unknown")
                    study_id = getattr(plan_ds, "StudyInstanceUID", "unknown").split('.')[-1]
                    safe_patient_name = self.sanitize_path_component(patient_name)
                    safe_patient_id = self.sanitize_path_component(patient_id)
                    safe_plan_name = self.sanitize_path_component(plan_name)
                    safe_study_id = self.sanitize_path_component(study_id)
                    
                    # Bestimme den AE-Titel der Quelle
                    source_ae = getattr(plan_ds, "SourceApplicationEntityTitle", "UNKNOWN")
                    if not source_ae or source_ae == "UNKNOWN":
                        # Versuche, den AE-Titel aus den Metadaten zu holen
                        if hasattr(plan_ds, "file_meta") and hasattr(plan_ds.file_meta, "SourceApplicationEntityTitle"):
                            source_ae = plan_ds.file_meta.SourceApplicationEntityTitle
                    
                    # Pfad zum Plan-Ordner
                    patient_folder = os.path.join(self.watch_folder, f"{safe_patient_name} ({safe_patient_id})")
                    plan_folder = os.path.join(patient_folder, f"{safe_plan_name}_{safe_study_id}")
                    
                    # Prüfe Weiterleitungsregeln für empfangene DICOM-Dateien
                    if os.path.exists(plan_folder):
                        logger.info(f"Prüfe Weiterleitungsregeln für Plan {plan_name} von {source_ae}")
                        target_nodes = rules_manager.check_forwarding_rules(source_ae, plan_name, self.settings_manager)
                        
                        if target_nodes:
                            logger.info(f"Plan {plan_name} entspricht {len(target_nodes)} Weiterleitungsregeln")
                            for node_name, node_info in target_nodes:
                                try:
                                    logger.info(f"Leite Plan {plan_name} an {node_name} weiter")
                                    success = self.send_plan_to_node(plan_folder, node_info)
                                    if success:
                                        logger.info(f"Plan {plan_name} erfolgreich an {node_name} weitergeleitet")
                                    else:
                                        logger.error(f"Fehler beim Weiterleiten von Plan {plan_name} an {node_name}")
                                except Exception as e:
                                    logger.error(f"Fehler beim Weiterleiten von Plan {plan_name} an {node_name}: {str(e)}")
                        else:
                            logger.info(f"Keine passenden Weiterleitungsregeln für Plan {plan_name} von {source_ae} gefunden")
                except Exception as e:
                    logger.error(f"Fehler beim Prüfen der Weiterleitungsregeln für Plan {plan_file_path}: {str(e)}")
        except Exception as e:
            logger.error(f"Fehler beim Initialisieren des RulesManager: {str(e)}")
            # Fehler beim Prüfen der Weiterleitungsregeln sollten nicht den gesamten Import-Prozess abbrechen

    
    def sanitize_path_component(self, name):
        """
        Removes or replaces problematic characters from a string to make it safe for use as a folder or file name.
        Also replaces colons (:) and forward slashes (/) with underscores.
        """
        import re
        # Replace colons and forward slashes first
        name = str(name).replace(':', '-').replace('/', '-')
        # Then replace any other problematic characters
        return re.sub(r'[^\w\-_. ]', '_', name).strip()

    def move_to_failed(self, file_path, error_msg):
        """Verschiebt eine fehlgeschlagene Datei in den Failed-Ordner
        
        Args:
            file_path (str): Pfad zur fehlgeschlagenen Datei
            error_msg (str): Fehlermeldung
        """
        try:
            if not os.path.exists(file_path):
                return
                
            # Failed-Ordner erstellen, falls nicht vorhanden
            if not os.path.exists(self.failed_folder):
                os.makedirs(self.failed_folder)
            
            # Zieldateiname mit Zeitstempel
            basename = os.path.basename(file_path)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            dest_file = os.path.join(self.failed_folder, f"{timestamp}_{basename}")
            
            # Datei verschieben
            shutil.move(file_path, dest_file)
            
            # Fehlermeldung in Datei speichern
            error_file = f"{dest_file}.error"
            with open(error_file, 'w') as f:
                f.write(f"{error_msg}\n")
                f.write(f"Originalpfad: {file_path}\n")
                f.write(f"Zeitpunkt: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            
            logger.info(f"Datei nach failed verschoben: {basename} -> {dest_file}")
            
        except Exception as e:
            logger.error(f"Fehler beim Verschieben der Datei in failed: {str(e)}")
    
    def scan_dicom_files(self, folder_path):
        """Durchsucht einen Ordner nach DICOM-Dateien und gruppiert sie nach Modalität
        
        Args:
            folder_path (str): Pfad zum Ordner
            
        Returns:
            dict: DICOM-Dateien gruppiert nach Modalität
        """
        dicom_files = {}
        
        if not os.path.exists(folder_path) or not os.path.isdir(folder_path):
            return dicom_files
        
        # Alle Dateien im Ordner durchgehen
        for root, _, files in os.walk(folder_path):
            for filename in files:
                if filename.endswith(".dcm"):
                    file_path = os.path.join(root, filename)
                    try:
                        # Zuerst nur Header lesen, um Modalität zu bestimmen
                        ds_header = pydicom.dcmread(file_path, force=True, stop_before_pixels=True)
                        modality = getattr(ds_header, "Modality", "UNKNOWN")
                        
                        # Für RTDOSE und CT Dateien müssen wir die Pixel-Daten vollständig laden
                        if modality in ["RTDOSE", "CT"]:
                            logger.info(f"Lade {modality} mit Pixel-Daten: {filename}")
                            # Vollständiges Lesen der DICOM-Datei mit Pixel-Daten
                            ds = pydicom.dcmread(file_path, force=True, stop_before_pixels=False)
                            # Dateigrößen-Info für Debugging
                            file_size = os.path.getsize(file_path)
                            logger.info(f"{modality} Dateigröße: {file_size} Bytes")
                            
                            # Prüfen, ob PixelData vorhanden ist
                            if hasattr(ds, 'PixelData'):
                                pixel_data_size = len(ds.PixelData)
                                logger.info(f"{modality} PixelData Größe: {pixel_data_size} Bytes")
                            else:
                                logger.warning(f"{modality} ohne PixelData: {os.path.basename(file_path)}")
                        else:
                            # Für andere Modalitäten können wir den Header verwenden
                            ds = ds_header
                        
                        # Nach Modalität gruppieren
                        if modality not in dicom_files:
                            dicom_files[modality] = []
                        
                        dicom_files[modality].append((file_path, ds))
                        
                    except Exception as e:
                        logger.error(f"Fehler beim Lesen von DICOM-Datei {file_path}: {str(e)}")
                        # Fehlerhafte Datei in Failed-Ordner verschieben
                        self.move_to_failed(file_path, f"Lesefehler: {str(e)}")
        
        return dicom_files
    
    def get_plans_in_folder(self):
        """Gibt alle Pläne im Überwachungsordner zurück
        
        Returns:
            list: Liste von Plan-Ordnern (relativer Pfad zum Überwachungsordner)
        """
        plans = []
        
        if not os.path.exists(self.watch_folder):
            return plans
            
        # Patient-Ordner durchsuchen
        for patient_dir in os.listdir(self.watch_folder):
            patient_path = os.path.join(self.watch_folder, patient_dir)
            
            # Nur Verzeichnisse beachten und den failed-Ordner überspringen
            if not os.path.isdir(patient_path) or patient_dir == "failed":
                continue
                
            # Plan-Ordner im Patient-Ordner durchsuchen
            for plan_dir in os.listdir(patient_path):
                plan_path = os.path.join(patient_path, plan_dir)
                
                # Nur Verzeichnisse beachten
                if not os.path.isdir(plan_path):
                    continue
                    
                # Relativen Pfad speichern (für GUI-Darstellung)
                rel_path = os.path.join(patient_dir, plan_dir)
                plans.append(rel_path)
        
        return plans

    def process_import_folder(self, status_callback=None):
        """Verarbeitet den Import-Ordner und importiert alle gefundenen DICOM-Dateien
        
        Args:
            status_callback (callable): Optionaler Callback für Statusmeldungen
            
        Returns:
            tuple: (int, int) - Anzahl der erfolgreich importierten Dateien, Anzahl der fehlgeschlagenen Dateien
        """
        # Dateizugriffsverfolgung für verzögertes Löschen initialisieren
        processed_files = set()
        
        # Alle Dateien im Import-Ordner durchgehen
        file_data = {}
        plan_files = []
        ct_files = []
        structure_files = []
        other_files = []
        
        # Detaillierte Logging für den Import-Ordner
        logger.info(f"Import-Ordner: {self.import_folder}")
        if not os.path.exists(self.import_folder):
            logger.error(f"Import-Ordner existiert nicht: {self.import_folder}")
            if status_callback:
                status_callback(f"Import-Ordner existiert nicht: {self.import_folder}")
            return False, 0, f"Import-Ordner existiert nicht: {self.import_folder}"
            
        # Alle Dateien im Import-Ordner auflisten
        all_files = []
        for root, dirs, files in os.walk(self.import_folder):
            logger.info(f"Durchsuche Verzeichnis: {root}")
            logger.info(f"Gefundene Unterverzeichnisse: {dirs}")
            logger.info(f"Gefundene Dateien: {files}")
            for f in files:
                all_files.append(os.path.join(root, f))
        
        logger.info(f"Insgesamt {len(all_files)} Dateien im Import-Ordner gefunden")
        
        # DICOM-Dateien verarbeiten
        for file_path in all_files:
            if file_path.lower().endswith(".dcm"):
                logger.info(f"Verarbeite DICOM-Datei: {file_path}")
                try:
                    # Zuerst nur Header lesen, um Modalität zu bestimmen
                    ds_header = pydicom.dcmread(file_path, force=True, stop_before_pixels=True)
                    modality = getattr(ds_header, "Modality", "UNKNOWN")
                    logger.info(f"Datei {os.path.basename(file_path)} hat Modalität: {modality}")
                    
                    # Für RTDOSE und CT Dateien müssen wir die Pixel-Daten vollständig laden
                    if modality in ["RTDOSE", "CT"]:
                        #logger.info(f"Lade {modality} mit Pixel-Daten: {os.path.basename(file_path)}")
                        # Dateigröße vor dem Lesen
                        file_size_before = os.path.getsize(file_path)
                        #logger.info(f"{modality} Dateigröße vor dem Lesen: {file_size_before} Bytes")
                        
                        # Vollständiges Lesen der DICOM-Datei mit Pixel-Daten
                        try:
                            ds = pydicom.dcmread(file_path, force=True, stop_before_pixels=False)
                            # Prüfen, ob PixelData vorhanden ist
                            if hasattr(ds, 'PixelData'):
                                pixel_data_size = len(ds.PixelData)
                                #logger.info(f"{modality} PixelData Größe: {pixel_data_size} Bytes")
                            #else:
                                #logger.warning(f"{modality} ohne PixelData: {os.path.basename(file_path)}")
                        except Exception as e:
                            logger.error(f"Fehler beim Laden der {modality} Pixel-Daten: {str(e)}")
                            # Fallback zum Header
                            ds = ds_header
                    else:
                        # Für andere Modalitäten können wir den Header verwenden
                        ds = ds_header
                    
                    # Dateiinformationen speichern
                    file_data[file_path] = {
                        'ds': ds,
                        'modality': modality,
                        'patient_id': getattr(ds, "PatientID", "unknown"),
                        'patient_name': getattr(ds, "PatientName", "unknown"),
                        'sop_instance_uid': getattr(ds, "SOPInstanceUID", "unknown")
                    }
                    
                    # Dateien nach Typ gruppieren
                    if modality == "RTPLAN":
                        plan_files.append(file_path)
                        #logger.info(f"RTPLAN gefunden: {os.path.basename(file_path)}")
                    elif modality == "CT":
                        ct_files.append(file_path)
                        #logger.info(f"CT gefunden: {os.path.basename(file_path)}")
                    elif modality == "RTSTRUCT":
                        structure_files.append(file_path)
                        #logger.info(f"RTSTRUCT gefunden: {os.path.basename(file_path)}")
                    else:
                        other_files.append(file_path)
                        #logger.info(f"Andere Modalität gefunden: {os.path.basename(file_path)} (Modalität: {modality})")
                    
                except Exception as e:
                    logger.error(f"Fehler beim Lesen von DICOM-Datei {file_path}: {str(e)}")
                    # Fehlerhafte Datei in Failed-Ordner verschieben
                    self.move_to_failed(file_path, f"Lesefehler: {str(e)}")
            else:
                logger.info(f"Überspringe Nicht-DICOM-Datei: {file_path}")
                
        # Zusammenfassung der gefundenen Dateien
        logger.info(f"Gefundene DICOM-Dateien: RTPLAN={len(plan_files)}, CT={len(ct_files)}, RTSTRUCT={len(structure_files)}, Andere={len(other_files)}")
        
        if len(plan_files) == 0 and len(ct_files) == 0 and len(structure_files) == 0 and len(other_files) == 0:
            logger.warning("Keine DICOM-Dateien im Import-Ordner gefunden!")
            if status_callback:
                status_callback("Keine DICOM-Dateien im Import-Ordner gefunden!")
            return False, 0, "Keine DICOM-Dateien im Import-Ordner gefunden!"
        
        # Logge alle gefundenen 'other_files' (z.B. RTDOSE)
        if other_files:
            logger.info('Gefundene other_files:')
            for f in other_files:
                modality = file_data[f]['modality'] if f in file_data else 'UNKNOWN'
                logger.info(f"  {os.path.basename(f)} (Modality={modality})")
        else:
            logger.info('Keine other_files gefunden.')
        # Phase 1: Pläne mit zugeordneten Dosis- und CT-Dateien verarbeiten
        plan_ref_map = {}
        frame_ref_map = {}
        
        for plan_file in plan_files:
            plan_ds = file_data[plan_file]['ds']
            plan_sop_uid = getattr(plan_ds, "SOPInstanceUID", "unknown")
            plan_ref_map[plan_sop_uid] = []
            
            # Zugehörige Dosis-Dateien finden
            # Wichtig: Wir brauchen die SOPInstanceUID des Plans für den Vergleich mit der ReferencedSOPInstanceUID der Dosis
            plan_sop_uid = getattr(plan_ds, "SOPInstanceUID", "unknown")
            logger.info(f"Plan-UID: {plan_sop_uid} (Datei: {os.path.basename(plan_file)}, PatientID: {file_data[plan_file]['patient_id']})")
            
            for dose_file in other_files:
                dose_ds = file_data[dose_file]['ds']
                dose_patient_id = file_data[dose_file]['patient_id']
                plan_patient_id = file_data[plan_file]['patient_id']
                
                # Extrahiere die ReferencedSOPInstanceUID aus der Dosis-Datei
                try:
                    dose_ref_uid = None
                    if hasattr(dose_ds, "ReferencedRTPlanSequence") and dose_ds.ReferencedRTPlanSequence and hasattr(dose_ds.ReferencedRTPlanSequence[0], "ReferencedSOPInstanceUID"):
                        dose_ref_uid = dose_ds.ReferencedRTPlanSequence[0].ReferencedSOPInstanceUID
                        logger.info(f"Dose-Datei: {os.path.basename(dose_file)} (PatientID={dose_patient_id}) ReferencedRTPlanSequence[0].ReferencedSOPInstanceUID={dose_ref_uid}")
                    else:
                        logger.info(f"Dose-Datei: {os.path.basename(dose_file)} (PatientID={dose_patient_id}) keine ReferencedRTPlanSequence/ReferencedSOPInstanceUID gefunden.")
                except Exception as e:
                    logger.warning(f"Dose-Datei: {os.path.basename(dose_file)} (PatientID={dose_patient_id}) Fehler beim Lesen von ReferencedRTPlanSequence: {e}")
                    dose_ref_uid = None
                
                if dose_ref_uid == plan_sop_uid and dose_patient_id == plan_patient_id:
                    logger.info(f"Dose-Match: {os.path.basename(dose_file)} (ReferencedSOPInstanceUID={dose_ref_uid}, PatientID={dose_patient_id}) -> Plan {plan_sop_uid}")
                    plan_ref_map[plan_sop_uid].append(dose_file)
                elif dose_ref_uid == plan_sop_uid and dose_patient_id != plan_patient_id:
                    logger.warning(f"Dose-PatientID-Mismatch: {os.path.basename(dose_file)} hat passende ReferencedSOPInstanceUID={dose_ref_uid}, aber PatientID stimmt nicht überein: {dose_patient_id} != {plan_patient_id}")
                else:
                    logger.debug(f"Dose NO-Match: {os.path.basename(dose_file)} (ReferencedSOPInstanceUID={dose_ref_uid}) passt NICHT zu Plan {plan_sop_uid}")
            
            # Frame of Reference UID für CT- und Structure-Dateien
            frame_ref_uid = getattr(plan_ds, "FrameOfReferenceUID", "unknown")
            frame_ref_map[frame_ref_uid] = []
            
            # Zugehörige CT- und Structure-Dateien finden
            for ct_file in ct_files:
                ct_ds = file_data[ct_file]['ds']
                ct_frame_ref_uid = getattr(ct_ds, "FrameOfReferenceUID", "unknown")
                if ct_frame_ref_uid == frame_ref_uid:
                    frame_ref_map[frame_ref_uid].append(ct_file)
            
            for struct_file in structure_files:
                struct_ds = file_data[struct_file]['ds']
                struct_frame_ref_uid = getattr(struct_ds, "FrameOfReferenceUID", "unknown")
                if struct_frame_ref_uid == frame_ref_uid:
                    frame_ref_map[frame_ref_uid].append(struct_file)
        
        # Phase 2: Pläne mit zugeordneten Dosis- und CT-Dateien importieren
        plan_folders_created = {}
        processed = 0
        failed = 0
        
        for plan_file_path in plan_files:
            try:
                plan_ds = file_data[plan_file_path]['ds']
                patient_id = getattr(plan_ds, "PatientID", "unknown")
                patient_name = getattr(plan_ds, "PatientName", "unknown")
                plan_name = getattr(plan_ds, "RTPlanLabel", "unknown")
                
                study_id = getattr(plan_ds, "StudyInstanceUID", "unknown")
                study_id = study_id.split('.')[-1]  # Letzten Teil der UID verwenden
                
                # Pfadkomponenten bereinigen
                safe_patient_name = self.sanitize_path_component(patient_name)
                safe_patient_id = self.sanitize_path_component(patient_id)
                safe_plan_name = self.sanitize_path_component(plan_name)
                safe_study_id = self.sanitize_path_component(study_id)
                
                # Ordnerpfade erstellen
                patient_folder = os.path.join(self.watch_folder, f"{safe_patient_name} ({safe_patient_id})")
                plan_folder = os.path.join(patient_folder, f"{safe_plan_name}_{safe_study_id}")
                
                # Ordner erstellen
                os.makedirs(plan_folder, exist_ok=True)
                plan_folders_created[plan_file_path] = plan_folder
                
                # Plan-Datei kopieren
                plan_filename = f"RTPLAN_{safe_plan_name}.dcm"
                dest_path = os.path.join(plan_folder, plan_filename)
                if os.path.exists(dest_path):
                    os.remove(dest_path)
                shutil.copy2(plan_file_path, dest_path)
                processed_files.add(plan_file_path)  # Datei als verarbeitet markieren
                processed += 1
                
                logger.info(f"Plan importiert: {safe_plan_name} -> {dest_path}")
                
                # SOPInstanceUID des aktuellen Plans aus dem DICOM-Datensatz holen
                plan_ds = file_data[plan_file_path]['ds']
                plan_sop_uid = getattr(plan_ds, "SOPInstanceUID", "unknown")
                
                # Zugehörige Dosis-Dateien kopieren
                if plan_sop_uid in plan_ref_map:
                    logger.info(f"Prüfe Dosis-Dateien für Plan {plan_sop_uid} (Patient: {patient_id})")
                    dose_files_for_plan = plan_ref_map[plan_sop_uid]
                    if not dose_files_for_plan:
                        logger.info(f"Keine passenden Dosis-Dateien für Plan {plan_sop_uid} gefunden")
                    
                    for dose_file_path in dose_files_for_plan:
                        # Nochmals prüfen, ob die Dosis wirklich zu diesem Plan und Patienten gehört
                        dose_ds = file_data[dose_file_path]['ds']
                        dose_patient_id = file_data[dose_file_path]['patient_id']
                        
                        # Referenz-UID aus der Dosis-Datei extrahieren
                        dose_ref_uid = None
                        try:
                            if hasattr(dose_ds, "ReferencedRTPlanSequence") and dose_ds.ReferencedRTPlanSequence and hasattr(dose_ds.ReferencedRTPlanSequence[0], "ReferencedSOPInstanceUID"):
                                dose_ref_uid = dose_ds.ReferencedRTPlanSequence[0].ReferencedSOPInstanceUID
                        except Exception as e:
                            logger.warning(f"Fehler beim Lesen von ReferencedRTPlanSequence aus Dosis-Datei: {str(e)}")
                        
                        # Doppelte Prüfung vor dem Kopieren
                        # Vergleiche die ReferencedSOPInstanceUID der Dosis mit der SOPInstanceUID des Plans
                        if dose_ref_uid == plan_sop_uid and dose_patient_id == patient_id:
                            logger.info(f"Dosis-Datei verifiziert: {os.path.basename(dose_file_path)} gehört zu Plan {plan_sop_uid} und Patient {patient_id}")
                            dose_filename = f"RTDOSE_{safe_plan_name}.dcm"
                            dose_dest_path = os.path.join(plan_folder, dose_filename)
                            if os.path.exists(dose_dest_path):
                                os.remove(dose_dest_path)
                            shutil.copy2(dose_file_path, dose_dest_path)
                            processed_files.add(dose_file_path)  # Datei als verarbeitet markieren
                            processed += 1
                            logger.info(f"Dosis importiert: {os.path.basename(dose_file_path)} -> {dose_dest_path}")
                        else:
                            logger.warning(f"Dosis-Datei NICHT importiert: {os.path.basename(dose_file_path)} - Verifizierung fehlgeschlagen!")
                            logger.warning(f"  Erwartet: Plan-UID={plan_sop_uid}, PatientID={patient_id}")
                            logger.warning(f"  Gefunden: ReferencedSOPInstanceUID={dose_ref_uid}, PatientID={dose_patient_id}")
                else:
                    logger.info(f"Keine Dosis-Dateien für Plan {plan_sop_uid} in der Zuordnungstabelle gefunden")
                
                # Zugehörige CT- und Structure-Dateien über Frame of Reference UID zuordnen
                # Aber nur, wenn sie auch zum selben Patienten gehören!
                frame_ref_uid = getattr(plan_ds, "FrameOfReferenceUID", "unknown")
                if frame_ref_uid and frame_ref_uid in frame_ref_map:
                    # CT-Dateien kopieren
                    ct_counter = 0
                    for related_file_path in frame_ref_map[frame_ref_uid]:
                        related_data = file_data[related_file_path]
                        related_modality = related_data['modality']
                        related_patient_id = related_data['patient_id']
                        
                        # Prüfen, ob die Datei zum selben Patienten gehört
                        if related_patient_id != patient_id:
                            logger.warning(f"Überspringe {related_modality}-Datei {os.path.basename(related_file_path)} - PatientID stimmt nicht überein: {related_patient_id} != {patient_id}")
                            continue
                        
                        if related_modality == "CT":
                            sop_instance = related_data['sop_instance_uid']
                            safe_sop_instance = self.sanitize_path_component(sop_instance)
                            ct_filename = f"CT.{safe_sop_instance}.dcm"
                            ct_dest_path = os.path.join(plan_folder, ct_filename)
                            if os.path.exists(ct_dest_path):
                                os.remove(ct_dest_path)
                            shutil.copy2(related_file_path, ct_dest_path)
                            processed_files.add(related_file_path)  # Datei als verarbeitet markieren
                            ct_counter += 1
                            processed += 1
                            
                        elif related_modality == "RTSTRUCT":
                            struct_filename = f"RTSTRUCT_{safe_plan_name}.dcm"
                            struct_dest_path = os.path.join(plan_folder, struct_filename)
                            if os.path.exists(struct_dest_path):
                                os.remove(struct_dest_path)
                            shutil.copy2(related_file_path, struct_dest_path)
                            processed_files.add(related_file_path)  # Datei als verarbeitet markieren
                            processed += 1
                            logger.info(f"Structure importiert: {os.path.basename(related_file_path)} -> {struct_dest_path}")
                    
                    if ct_counter > 0:
                        logger.info(f"{ct_counter} CT-Dateien zum Plan {safe_plan_name} importiert")
                    else:
                        logger.warning(f"Keine passenden CT-Dateien für Plan {safe_plan_name} gefunden")
                        
            except Exception as e:
                logger.error(f"Fehler beim Verarbeiten des Plans {os.path.basename(plan_file_path)}: {str(e)}")
                self.move_to_failed(plan_file_path, f"Plan-Import-Fehler: {str(e)}")
                failed += 1
        
        # Phase 3: Verwaiste Dateien verarbeiten (ohne direkte Planzuordnung)
        orphaned_files = set(other_files)
        
        # CT-Dateien ohne zugeordneten Plan
        for ct_file in ct_files:
            if ct_file not in orphaned_files and ct_file not in processed:
                orphaned_files.add(ct_file)
        
        # Structure-Dateien ohne zugeordneten Plan
        for struct_file in structure_files:
            if struct_file not in orphaned_files and struct_file not in processed:
                orphaned_files.add(struct_file)
        
        # Dose-Dateien ohne zugeordneten Plan
        
        # Dateizugriffsverfolgung für verzögertes Löschen ist schon am Anfang der Methode initialisiert
        # NICHT überschreiben, sonst werden verarbeitete Dateien nicht gelöscht!
        
        # Verwaiste Dateien nach Patienten gruppieren
        if orphaned_files:
            for file_path in orphaned_files:
                try:
                    if file_path not in file_data:
                        continue
                        
                    file_info = file_data[file_path]
                    patient_id = file_info['patient_id']
                    patient_name = file_info['patient_name']
                    modality = file_info['modality']
                    ds = file_info['ds']
                    
                    # StudyInstanceUID als Fallback für Gruppierung
                    study_id = getattr(ds, "StudyInstanceUID", "unknown")
                    study_id = study_id.split('.')[-1]
                    
                    # Fallback Plan-Namen generieren
                    fallback_plan_name = "Unzugeordnet"
                    if hasattr(ds, "SeriesDescription"):
                        fallback_plan_name = ds.SeriesDescription
                    elif hasattr(ds, "StudyDescription"):
                        fallback_plan_name = ds.StudyDescription
                    
                    # Pfadkomponenten bereinigen
                    safe_patient_name = self.sanitize_path_component(patient_name)
                    safe_patient_id = self.sanitize_path_component(patient_id)
                    safe_plan_name = self.sanitize_path_component(fallback_plan_name)
                    safe_study_id = self.sanitize_path_component(study_id)
                    
                    # Ordnerpfade erstellen
                    patient_folder = os.path.join(self.watch_folder, f"{safe_patient_name} ({safe_patient_id})")
                    orphan_folder = os.path.join(patient_folder, f"Unzugeordnet_{safe_study_id}")
                    
                    # Ordner erstellen
                    os.makedirs(orphan_folder, exist_ok=True)
                    
                    # Datei kopieren
                    if modality == "CT":
                        sop_instance = file_info['sop_instance_uid']
                        safe_sop_instance = self.sanitize_path_component(sop_instance)
                        filename = f"CT.{safe_sop_instance}.dcm"
                    else:
                        filename = f"{modality}_{safe_plan_name}.dcm"
                    
                    dest_path = os.path.join(orphan_folder, filename)
                    if os.path.exists(dest_path):
                        os.remove(dest_path)
                    shutil.copy2(file_path, dest_path)
                    processed_files.add(file_path)  # Datei als verarbeitet markieren
                    processed += 1
                    
                    logger.info(f"Unzugeordnete Datei importiert: {os.path.basename(file_path)} -> {dest_path}")
                    
                except Exception as e:
                    logger.error(f"Fehler beim Importieren der unzugeordneten Datei {os.path.basename(file_path)}: {str(e)}")
                    self.move_to_failed(file_path, f"Orphaned-Import-Fehler: {str(e)}")
                    failed += 1
                
        # Prüfen, ob der Import-Ordner nach dem Import geleert werden soll
        # Direkt aus der settings.ini lesen, um sicherzustellen, dass wir den aktuellen Wert haben
        try:
            clear_import_folder = self.settings_manager.config['General'].getboolean('clear_import_folder_after_import', False)
            logger.info(f"Import-Ordner nach Import löschen: {clear_import_folder}")
        except Exception as e:
            logger.error(f"Fehler beim Lesen der clear_import_folder_after_import-Einstellung: {str(e)}")
            clear_import_folder = True  # Im Zweifelsfall löschen
            logger.info(f"Verwende Fallback-Wert für clear_import_folder_after_import: {clear_import_folder}")
        
        # Immer löschen, wie vom Benutzer gewünscht
        clear_import_folder = True
        logger.info(f"OVERRIDE: Import-Ordner wird immer gelöscht, unabhängig von der Einstellung")
        
        if clear_import_folder:
            if status_callback:
                status_callback("Lösche alle Dateien aus dem Import-Ordner...")
            
            # Stellen sicher, dass alle DICOM-Objekte freigegeben sind
            import gc
            gc.collect()  # Garbage Collection erzwingen
            
            files_deleted = 0
            delete_errors = []
            
            # Alle Dateien im Import-Ordner finden (nicht nur die verarbeiteten)
            all_files = []
            for root, _, files in os.walk(self.import_folder):
                for file in files:
                    all_files.append(os.path.join(root, file))
            
            if status_callback:
                status_callback(f"Entferne {len(all_files)} Dateien aus dem Import-Ordner...")
            
            # Alle Dateien löschen
            for file_path in all_files:
                try:
                    if os.path.exists(file_path):
                        # Mehrere Versuche mit kurzer Pause
                        for attempt in range(3):
                            try:
                                os.remove(file_path)
                                files_deleted += 1
                                if status_callback and files_deleted % 10 == 0:
                                    status_callback(f"Gelöscht: {files_deleted}/{len(all_files)} Dateien...")
                                break
                            except PermissionError:
                                # Kurze Pause und erneuter Versuch
                                time.sleep(0.1)
                                continue
                            except Exception as e:
                                raise e
                        else:
                            # Wenn alle Versuche fehlgeschlagen sind
                            delete_errors.append((file_path, "Datei konnte nicht gelöscht werden nach mehreren Versuchen"))
                except Exception as e:
                    error_msg = f"Konnte Datei {file_path} nicht löschen: {str(e)}"
                    logger.warning(error_msg)
                    delete_errors.append((file_path, error_msg))
            
            # Auch alle Unterordner löschen
            if status_callback:
                status_callback("Lösche alle Unterordner im Import-Ordner...")
                
            # Alle Unterordner finden und sortieren (tiefste zuerst)
            all_dirs = []
            for root, dirs, _ in os.walk(self.import_folder):
                for dir_name in dirs:
                    dir_path = os.path.join(root, dir_name)
                    all_dirs.append(dir_path)
            
            # Sortiere Verzeichnisse nach Tiefe (tiefste zuerst)
            all_dirs.sort(key=lambda x: x.count(os.sep), reverse=True)
            
            # Verzeichnisse löschen
            for dir_path in all_dirs:
                try:
                    if os.path.exists(dir_path) and os.path.isdir(dir_path):
                        # Mehrere Versuche mit kurzer Pause
                        for attempt in range(3):
                            try:
                                os.rmdir(dir_path)  # Nur leere Verzeichnisse löschen
                                logger.info(f"Verzeichnis gelöscht: {dir_path}")
                                break
                            except OSError as e:
                                if "Directory not empty" in str(e):
                                    # Verzeichnis ist nicht leer, Inhalt auflisten
                                    remaining = os.listdir(dir_path)
                                    logger.warning(f"Verzeichnis nicht leer: {dir_path}, enthält: {remaining}")
                                    # Versuche, verbliebene Dateien zu löschen
                                    for item in remaining:
                                        item_path = os.path.join(dir_path, item)
                                        try:
                                            if os.path.isfile(item_path):
                                                os.remove(item_path)
                                            elif os.path.isdir(item_path):
                                                # Rekursives Löschen für Unterverzeichnisse
                                                shutil.rmtree(item_path, ignore_errors=True)
                                        except Exception as inner_e:
                                            logger.error(f"Fehler beim Löschen von {item_path}: {str(inner_e)}")
                                    # Erneut versuchen, das Verzeichnis zu löschen
                                    continue
                                else:
                                    # Andere Fehler
                                    logger.error(f"Fehler beim Löschen von Verzeichnis {dir_path}: {str(e)}")
                                    break
                            except Exception as e:
                                logger.error(f"Unerwarteter Fehler beim Löschen von Verzeichnis {dir_path}: {str(e)}")
                                break
                except Exception as e:
                    logger.error(f"Fehler beim Löschen von Verzeichnis {dir_path}: {str(e)}")
            
            # Versuche auch, den Import-Ordner selbst zu leeren mit shutil.rmtree und neu zu erstellen
            try:
                # Sichere den Pfad
                import_folder_path = self.import_folder
                
                # Lösche den gesamten Ordner mit shutil.rmtree
                if os.path.exists(import_folder_path):
                    logger.info(f"Lösche kompletten Import-Ordner: {import_folder_path}")
                    shutil.rmtree(import_folder_path, ignore_errors=True)
                
                # Erstelle den Ordner neu
                os.makedirs(import_folder_path, exist_ok=True)
                logger.info(f"Import-Ordner neu erstellt: {import_folder_path}")
                
                if status_callback:
                    status_callback("Import-Ordner wurde komplett geleert und neu erstellt.")
            except Exception as e:
                logger.error(f"Fehler beim kompletten Löschen des Import-Ordners: {str(e)}")
                if status_callback:
                    status_callback(f"Fehler beim Löschen des Import-Ordners: {str(e)}")
        else:
            # Nur erfolgreich verarbeitete Dateien löschen (altes Verhalten)
            if status_callback:
                status_callback(f"Entferne {len(processed_files)} erfolgreich verarbeitete Dateien aus dem Import-Ordner...")
            
            # Stellen sicher, dass alle DICOM-Objekte freigegeben sind
            import gc
            gc.collect()  # Garbage Collection erzwingen
            
            files_deleted = 0
            delete_errors = []
            
            for file_path in processed_files:
                try:
                    if os.path.exists(file_path):
                        # Mehrere Versuche mit kurzer Pause
                        for attempt in range(3):
                            try:
                                os.remove(file_path)
                                files_deleted += 1
                                if status_callback and files_deleted % 10 == 0:
                                    status_callback(f"Gelöscht: {files_deleted}/{len(processed_files)} Dateien...")
                                break
                            except PermissionError:
                                # Kurze Pause und erneuter Versuch
                                time.sleep(0.1)
                                continue
                            except Exception as e:
                                raise e
                        else:
                            # Wenn alle Versuche fehlgeschlagen sind
                            delete_errors.append((file_path, "Datei konnte nicht gelöscht werden nach mehreren Versuchen"))
                except Exception as e:
                    error_msg = f"Konnte Datei {file_path} nicht löschen: {str(e)}"
                    logger.warning(error_msg)
                    delete_errors.append((file_path, error_msg))
        
        # Leere Ordner im Import-Ordner entfernen
        if status_callback:
            status_callback("Räume leere Unterordner auf...")
            
        try:
            # Verwende rekursiven Ansatz von unten nach oben (topdown=False), um zuerst tiefere Ordner zu löschen
            for root, dirs, files in os.walk(self.import_folder, topdown=False):
                # Hauptimport-Ordner selbst nicht löschen, nur Unterordner
                if root == self.import_folder:
                    continue
                    
                # Wenn der aktuelle Ordner keine Dateien mehr hat, versuchen zu löschen
                if not files and not dirs:  # Wenn keine Dateien und keine Unterordner mehr
                    try:
                        os.rmdir(root)
                        logger.info(f"Leerer Ordner entfernt: {root}")
                    except Exception as e:
                        logger.warning(f"Konnte leeren Ordner {root} nicht entfernen: {str(e)}")
                        
                # Einzelne Unterordner versuchen zu löschen
                for dir_name in dirs:
                    dir_path = os.path.join(root, dir_name)
                    if os.path.exists(dir_path) and not os.listdir(dir_path):  # Wenn Ordner existiert und leer ist
                        try:
                            os.rmdir(dir_path)
                            logger.info(f"Leerer Ordner entfernt: {dir_path}")
                        except Exception as e:
                            logger.warning(f"Konnte leeren Ordner {dir_path} nicht entfernen: {str(e)}")
        except Exception as e:
            logger.warning(f"Fehler beim Aufräumen leerer Ordner: {str(e)}")
        
        # Prüfe Weiterleitungsregeln für alle importierten Pläne
        try:
            from rules_manager import RulesManager
            rules_manager = RulesManager()
            
            # Sammle alle erfolgreich importierten Plan-Ordner
            imported_plan_folders = []
            for plan_file_path in plan_files:
                try:
                    plan_ds = file_data[plan_file_path]['ds']
                    patient_id = getattr(plan_ds, "PatientID", "unknown")
                    patient_name = getattr(plan_ds, "PatientName", "unknown")
                    plan_name = getattr(plan_ds, "RTPlanLabel", "unknown")
                    study_id = getattr(plan_ds, "StudyInstanceUID", "unknown").split('.')[-1]
                    safe_patient_name = self.sanitize_path_component(patient_name)
                    safe_patient_id = self.sanitize_path_component(patient_id)
                    safe_plan_name = self.sanitize_path_component(plan_name)
                    safe_study_id = self.sanitize_path_component(study_id)
                    
                    # Pfad zum Plan-Ordner
                    patient_folder = os.path.join(self.watch_folder, f"{safe_patient_name} ({safe_patient_id})")
                    plan_folder = os.path.join(patient_folder, f"{safe_plan_name}_{safe_study_id}")
                    
                    if os.path.exists(plan_folder):
                        # Bestimme den AE-Titel der Quelle (bei Import-Ordner ist dies meist unbekannt)
                        source_ae = getattr(plan_ds, "SourceApplicationEntityTitle", "IMPORT_FOLDER")
                        if not source_ae or source_ae == "UNKNOWN":
                            # Versuche, den AE-Titel aus den Metadaten zu holen
                            if hasattr(plan_ds, "file_meta") and hasattr(plan_ds.file_meta, "SourceApplicationEntityTitle"):
                                source_ae = plan_ds.file_meta.SourceApplicationEntityTitle
                            else:
                                # Bei Import-Ordner verwenden wir einen speziellen AE-Titel
                                source_ae = "IMPORT_FOLDER"
                        
                        imported_plan_folders.append((plan_folder, plan_name, source_ae))
                except Exception as e:
                    logger.error(f"Fehler beim Sammeln von Plan-Informationen für Weiterleitungsregeln: {str(e)}")
            
            # Prüfe Weiterleitungsregeln für jeden importierten Plan
            forwarded_plans = 0
            if status_callback:
                status_callback("Prüfe Weiterleitungsregeln für importierte Pläne...")
                
            for plan_folder, plan_name, source_ae in imported_plan_folders:
                try:
                    logger.info(f"Prüfe Weiterleitungsregeln für Plan {plan_name} von {source_ae}")
                    target_nodes = rules_manager.check_forwarding_rules(source_ae, plan_name, self.settings_manager)
                    
                    if target_nodes:
                        logger.info(f"Plan {plan_name} entspricht {len(target_nodes)} Weiterleitungsregeln")
                        for node_name, node_info in target_nodes:
                            try:
                                if status_callback:
                                    status_callback(f"Leite Plan {plan_name} an {node_name} weiter...")
                                    
                                logger.info(f"Leite Plan {plan_name} an {node_name} weiter")
                                success = self.send_plan_to_node(plan_folder, node_info)
                                if success:
                                    logger.info(f"Plan {plan_name} erfolgreich an {node_name} weitergeleitet")
                                    forwarded_plans += 1
                                else:
                                    logger.error(f"Fehler beim Weiterleiten von Plan {plan_name} an {node_name}")
                            except Exception as e:
                                logger.error(f"Fehler beim Weiterleiten von Plan {plan_name} an {node_name}: {str(e)}")
                    else:
                        logger.info(f"Keine passenden Weiterleitungsregeln für Plan {plan_name} von {source_ae} gefunden")
                except Exception as e:
                    logger.error(f"Fehler beim Prüfen der Weiterleitungsregeln für Plan {plan_name}: {str(e)}")
            
            if forwarded_plans > 0 and status_callback:
                status_callback(f"{forwarded_plans} Pläne gemäß Weiterleitungsregeln gesendet")
                
        except Exception as e:
            logger.error(f"Fehler beim Initialisieren des RulesManager: {str(e)}")
            # Fehler beim Prüfen der Weiterleitungsregeln sollten nicht den gesamten Import-Prozess abbrechen
        

        # Abschließen und Ergebnis zurückgeben
        if failed > 0:
            return True, processed, f"{processed} Dateien importiert ({files_deleted} entfernt), {failed} fehlgeschlagen"
        return True, processed, f"{processed} Dateien erfolgreich importiert, {files_deleted} Dateien entfernt"
    
    def send_plan_to_node(self, plan_path, node_info, progress_callback=None, delete_after=False):
        """Sendet einen Plan an einen DICOM-Knoten
        
        Args:
            plan_path (str): Pfad zum Plan-Ordner
            node_info (dict): Informationen zum Zielknoten
            progress_callback (callable): Funktion für Fortschrittsmeldungen
            delete_after (bool): Datei nach dem Senden löschen
            
        Returns:
            bool: True bei erfolgreichem Senden, sonst False
        """
        try:
            # Alle DICOM-Dateien im Plan-Ordner sammeln
            dicom_files_by_modality = self.scan_dicom_files(plan_path)
            
            if not dicom_files_by_modality:
                logger.warning(f"Keine DICOM-Dateien im Ordner {plan_path} gefunden")
                return False
            
            # Alle Dateien in einer Liste zusammenfassen, nach Modalität sortiert
            ordered_files = []
            
            # Bekannte Modalitäten in definierter Reihenfolge
            for modality in ["CT", "RTSTRUCT", "RTPLAN", "RTDOSE"]:
                if modality in dicom_files_by_modality:
                    ordered_files.extend(dicom_files_by_modality[modality])
                    del dicom_files_by_modality[modality]
            
            # Alle anderen Modalitäten hinzufügen
            for modality, files in dicom_files_by_modality.items():
                ordered_files.extend(files)
            
            # Alle Dateien in einer einzigen Association senden
            return self.send_all_dicom_files(
                ordered_files, 
                node_info,
                folder_name=os.path.basename(plan_path),
                progress_callback=progress_callback,
                delete_after=delete_after
            )
            
        except Exception as e:
            logger.error(f"Fehler beim Senden des Plans {plan_path}: {str(e)}")
            return False
    
    def send_all_dicom_files(self, file_dataset_pairs, node_info, folder_name="", 
                           progress_callback=None, delete_after=False):
        """Sendet alle DICOM-Dateien in einer einzigen Association
        
        Args:
            file_dataset_pairs (list): Liste von (Dateipfad, Dataset)-Paaren
            node_info (dict): Informationen zum Zielknoten
            folder_name (str): Name des Quellordners für Logging
            progress_callback (callable): Funktion für Fortschrittsmeldungen
            delete_after (bool): Dateien nach erfolgreichem Senden löschen (nur wenn letzter Node)
            
        Returns:
            bool: True wenn alle Dateien erfolgreich gesendet wurden, sonst False
        """
        if not file_dataset_pairs:
            return True
            
        file_count = len(file_dataset_pairs)
        success_count = 0
        failed_files = []
        
        # Statistik nach Modalität
        modality_stats = {}
        
        try:
            # Application Entity erstellen
            ae = AE(ae_title=b"DICOM-RT-KAFFEE")

            # Kontext für alle Storage SOP Classes hinzufügen (KORREKT!)
            ae.add_requested_context(CTImageStorage)
            ae.add_requested_context(RTStructureSetStorage)
            ae.add_requested_context(RTPlanStorage)
            ae.add_requested_context(RTDoseStorage)
            ae.add_requested_context(RTIonPlanStorage)
            ae.add_requested_context(RTBeamsTreatmentRecordStorage)
            ae.add_requested_context(RTIonBeamsTreatmentRecordStorage)
            # Private RTPLAN UID (z.B. Brainlab/SIEMENS)
            ae.add_requested_context(MyPrivateRTPlanStorage, [ExplicitVRLittleEndian, ImplicitVRLittleEndian])
            
            # Eine einzige Association für alle Dateien herstellen
            target_aet = node_info.get('aet', 'ANY-SCP')
            target_ip = node_info.get('ip', '127.0.0.1')
            target_port = int(node_info.get('port', 104))
            
            logger.info(f"Verbindung zu {target_aet}@{target_ip}:{target_port} für {file_count} Dateien herstellen")
            assoc = ae.associate(target_ip, target_port, ae_title=target_aet.encode('ascii'))
            
            if assoc.is_established:
                # Alle Datasets in einer einzigen Association senden
                for i, (file_path, ds) in enumerate(file_dataset_pairs):
                    try:
                        modality = getattr(ds, 'Modality', 'UNKNOWN')
                        
                        # Statistik für diese Modalität aktualisieren
                        if modality not in modality_stats:
                            modality_stats[modality] = {'total': 0, 'success': 0}
                        modality_stats[modality]['total'] += 1
                        
                        # Fortschritt melden
                        if progress_callback:
                            progress_callback(i + 1, file_count)
                        
                        # Detaillierten Fortschritt nur für non-CT oder in Intervallen für CT ausgeben
                        if modality != "CT" or i % 10 == 0:
                            logger.info(f"Sende {modality}-Datei {i+1}/{file_count}: {os.path.basename(file_path)}")
                        
                        # Dataset senden - für RTDOSE und CT prüfen, ob PixelData vorhanden ist
                        if modality in ["RTDOSE", "CT"]:
                            if hasattr(ds, 'PixelData'):
                                pixel_data_size = len(ds.PixelData)
                                logger.info(f"Sende {modality} mit PixelData: {pixel_data_size} Bytes")
                            else:
                                logger.warning(f"{modality} ohne PixelData wird gesendet: {os.path.basename(file_path)}")
                                # Versuchen, die Datei neu zu laden, falls PixelData fehlt
                                try:
                                    logger.info(f"Versuche {modality} neu zu laden mit Pixel-Daten")
                                    ds_full = pydicom.dcmread(file_path, force=True, stop_before_pixels=False)
                                    if hasattr(ds_full, 'PixelData'):
                                        ds = ds_full  # Ersetze das Dataset mit dem vollständigen
                                        logger.info(f"Erfolgreich {modality} mit PixelData neu geladen")
                                except Exception as e:
                                    logger.error(f"Fehler beim Nachladen von {modality}: {str(e)}")
                                    # Weiter mit dem ursprünglichen Dataset
                        
                        # Dataset senden
                        status = assoc.send_c_store(ds)
                        
                        if status and status.Status == 0x0000:  # Erfolg
                            success_count += 1
                            modality_stats[modality]['success'] += 1
                            
                            # Datei nach erfolgreicher Übertragung löschen, wenn gewünscht
                            # (nur wenn es der letzte Knoten ist)
                            if delete_after:
                                if os.path.exists(file_path):  # Sicherheitscheck
                                    logger.info(f"Lösche Datei nach erfolgreichem Senden: {os.path.basename(file_path)}")
                                    os.remove(file_path)
                        else:
                            error_msg = f"Fehler beim Senden: {os.path.basename(file_path)} - Status: {status.Status if status else 'unbekannt'}"
                            logger.error(error_msg)
                            failed_files.append((file_path, error_msg))
                            
                    except Exception as e:
                        error_msg = f"Fehler beim Senden der DICOM-Datei {os.path.basename(file_path)}: {str(e)}"
                        logger.error(error_msg)
                        failed_files.append((file_path, error_msg))
                
                # Association nach Verarbeitung aller Dateien schließen
                assoc.release()
                
                # Erfolgsstatistik ausgeben
                logger.info(f"Übertragung abgeschlossen: {success_count} von {file_count} Dateien erfolgreich gesendet aus {folder_name}")
                for modality, stats in modality_stats.items():
                    logger.info(f"  - {modality}: {stats['success']} von {stats['total']} erfolgreich")
                    
            else:
                error_msg = f"Verbindung zu {target_aet} konnte nicht hergestellt werden"
                logger.error(error_msg)
                # Alle Dateien als fehlgeschlagen markieren
                failed_files = [(file_path, error_msg) for file_path, _ in file_dataset_pairs]
            
        except Exception as e:
            error_msg = f"Übertragungsfehler: {str(e)}"
            logger.error(error_msg)
            # Alle übrigen Dateien als fehlgeschlagen markieren
            failed_files = [(file_path, error_msg) for file_path, _ in file_dataset_pairs]
        
        # Fehlgeschlagene Dateien in den Failed-Ordner verschieben
        for file_path, error_msg in failed_files:
            self.move_to_failed(file_path, error_msg)
        
        # Erfolg, wenn alle Dateien gesendet wurden
        return success_count == file_count
    
    def send_plan_to_node(self, plan_path, node_info, delete_after=False):
        """Sendet einen Plan an einen DICOM-Knoten
        
        Args:
            plan_path (str): Pfad zum Plan-Ordner
            node_info (dict): Informationen zum Zielknoten
            delete_after (bool): Ob die Dateien nach dem Senden gelöscht werden sollen
            
        Returns:
            bool: True bei Erfolg, False bei Fehler
        """
        try:
            logger.info(f"Sende Plan {os.path.basename(plan_path)} an {node_info.get('name')}")
            
            # Alle DICOM-Dateien im Plan-Ordner finden
            dicom_files = []
            for root, _, files in os.walk(plan_path):
                for file in files:
                    if file.lower().endswith('.dcm'):
                        dicom_files.append(os.path.join(root, file))
            
            if not dicom_files:
                logger.error(f"Keine DICOM-Dateien im Plan-Ordner gefunden: {plan_path}")
                return False
            
            # Dateien nach Modalität sortieren (CT, RTSTRUCT, RTPLAN, RTDOSE)
            sorted_files = self._sort_files_by_modality(dicom_files)
            
            # Dateien an den Knoten senden
            success = self._send_files_to_node(sorted_files, node_info)
            
            # Dateien löschen, wenn gewünscht und erfolgreich gesendet
            if delete_after and success:
                self.delete_plan_files(plan_path)
                
            return success
        except Exception as e:
            logger.error(f"Fehler beim Senden des Plans {plan_path}: {str(e)}")
            return False
    
    def delete_plan_files(self, plan_path):
        """Löscht alle Dateien eines Plans
        
        Args:
            plan_path (str): Pfad zum Plan-Ordner
            
        Returns:
            bool: True bei Erfolg, False bei Fehler
        """
        try:
            logger.info(f"Lösche Plan-Dateien: {plan_path}")
            
            # Alle Dateien im Plan-Ordner löschen
            for root, dirs, files in os.walk(plan_path, topdown=False):
                for file in files:
                    file_path = os.path.join(root, file)
                    os.remove(file_path)
                    logger.debug(f"Datei gelöscht: {file_path}")
                
                # Leere Unterverzeichnisse löschen
                for dir in dirs:
                    dir_path = os.path.join(root, dir)
                    try:
                        os.rmdir(dir_path)
                        logger.debug(f"Verzeichnis gelöscht: {dir_path}")
                    except OSError:
                        # Verzeichnis ist nicht leer, ignorieren
                        pass
            
            # Hauptverzeichnis löschen
            try:
                os.rmdir(plan_path)
                logger.info(f"Plan-Verzeichnis gelöscht: {plan_path}")
            except OSError as e:
                logger.warning(f"Plan-Verzeichnis konnte nicht gelöscht werden: {plan_path} - {str(e)}")
            
            return True
        except Exception as e:
            logger.error(f"Fehler beim Löschen der Plan-Dateien {plan_path}: {str(e)}")
            return False
    
    def _sort_files_by_modality(self, file_list):
        """Sortiert DICOM-Dateien nach Modalität für optimale Sendreihenfolge
        
        Args:
            file_list (list): Liste von DICOM-Dateipfaden
            
        Returns:
            list: Sortierte Liste von Dateipfaden
        """
        # Modalitätsreihenfolge definieren, falls nicht vorhanden
        if not hasattr(self, 'modality_order'):
            self.modality_order = {
                'CT': 1,       # CT zuerst senden
                'RTSTRUCT': 2, # Dann Strukturen
                'RTPLAN': 3,   # Dann Pläne
                'RTDOSE': 4,   # Dann Dosen
                'RTIMAGE': 5    # Dann Bilder
            }
            
        file_modality_map = []
        
        for file_path in file_list:
            try:
                ds = pydicom.dcmread(file_path, force=True, stop_before_pixels=True)
                modality = getattr(ds, "Modality", "UNKNOWN")
                order = self.modality_order.get(modality, 99)  # Unbekannte Modalitäten am Ende
                file_modality_map.append((file_path, order))
            except Exception:
                # Bei Fehler: Datei am Ende einordnen
                file_modality_map.append((file_path, 100))
        
        # Nach Modalitätsreihenfolge sortieren
        file_modality_map.sort(key=lambda x: x[1])
        
        # Nur die Dateipfade zurückgeben
        return [item[0] for item in file_modality_map]
        
    def _send_files_to_node(self, file_list, node_info, progress_callback=None):
        """Sendet eine Liste von DICOM-Dateien an einen DICOM-Knoten
        
        Args:
            file_list (list): Liste von DICOM-Dateipfaden
            node_info (dict): Informationen zum Zielknoten
            progress_callback (callable, optional): Callback-Funktion für Fortschrittsanzeige
            
        Returns:
            bool: True bei Erfolg, False bei Fehler
        """
        if not file_list:
            logger.error("Keine Dateien zum Senden angegeben")
            return False
            
        try:
            # DICOM-Verbindungsparameter aus node_info extrahieren
            ae_title = node_info.get('aet', 'UNKNOWN')
            ip = node_info.get('ip', '127.0.0.1')
            port = int(node_info.get('port', 104))
            
            logger.info(f"Verbinde mit DICOM-Knoten {ae_title}@{ip}:{port}")
            
            # DICOM-Verbindung aufbauen
            from pynetdicom import AE, StoragePresentationContexts
            from pynetdicom.sop_class import CTImageStorage, RTDoseStorage, RTPlanStorage, RTStructureSetStorage
            
            # Application Entity mit unserem AE Title erstellen
            local_ae_title = self.settings_manager.config.get('LocalNode', 'AET', fallback='DICOM-RT-KAFFEE')
            logger.info(f"Verwende lokalen AE Title: {local_ae_title}")
            ae = AE(ae_title=local_ae_title.encode('ascii'))
            
            # Storage SOP Classes hinzufügen
            ae.requested_contexts = StoragePresentationContexts
            
            # Verbindung herstellen (ae_title ist der Remote/Called AE Title)
            assoc = ae.associate(ip, port, ae_title=ae_title)
            
            if not assoc.is_established:
                logger.error(f"Verbindung zu {ae_title}@{ip}:{port} konnte nicht hergestellt werden")
                return False
                
            # Dateien senden
            success_count = 0
            file_count = len(file_list)
            failed_ct_count = 0  # Zähler für fehlgeschlagene CT-Dateien
            
            for i, file_path in enumerate(file_list):
                try:
                    # Fortschritt melden, wenn Callback vorhanden
                    if progress_callback:
                        progress_callback(i, file_count)
                        
                    # DICOM-Datei laden - mit file_meta erhalten
                    ds = pydicom.dcmread(file_path, force=True)
                    
                    # Spezielle Logs für RTDOSE-Dateien
                    is_rtdose = hasattr(ds, 'Modality') and ds.Modality == 'RTDOSE'
                    if is_rtdose:
                        logger.info(f"=== RTDOSE HEADER DEBUG für {os.path.basename(file_path)} ===")
                        logger.info(f"Ursprüngliche file_meta vorhanden: {hasattr(ds, 'file_meta') and ds.file_meta is not None}")
                        if hasattr(ds, 'file_meta') and ds.file_meta:
                            logger.info(f"TransferSyntaxUID vorhanden: {hasattr(ds.file_meta, 'TransferSyntaxUID')}")
                            if hasattr(ds.file_meta, 'TransferSyntaxUID'):
                                logger.info(f"TransferSyntaxUID Wert: {ds.file_meta.TransferSyntaxUID}")
                            logger.info(f"MediaStorageSOPClassUID vorhanden: {hasattr(ds.file_meta, 'MediaStorageSOPClassUID')}")
                            logger.info(f"MediaStorageSOPInstanceUID vorhanden: {hasattr(ds.file_meta, 'MediaStorageSOPInstanceUID')}")
                            # Alle 0002-Tags auflisten
                            meta_elements = [elem for elem in ds.file_meta if elem.tag.group == 0x0002]
                            logger.info(f"Anzahl 0002-Tags in file_meta: {len(meta_elements)}")
                            for elem in meta_elements:
                                logger.info(f"  {elem.tag}: {elem.keyword} = {elem.value}")
                        else:
                            logger.info("Keine file_meta-Informationen gefunden")
                    
                    # Originale file_meta aus der Datei lesen, falls nicht vorhanden
                    if not hasattr(ds, 'file_meta') or not ds.file_meta or not hasattr(ds.file_meta, 'TransferSyntaxUID'):
                        if is_rtdose:
                            logger.info("Versuche file_meta aus Datei zu rekonstruieren...")
                        # Versuche, die Datei erneut zu lesen, um file_meta zu erhalten
                        try:
                            with open(file_path, 'rb') as f:
                                # Lese die ersten 132 Bytes (DICOM Preamble + DICM)
                                preamble = f.read(132)
                                if preamble[128:132] == b'DICM':
                                    if is_rtdose:
                                        logger.info("DICM-Marker gefunden, lese file_meta...")
                                    # Lese file_meta explizit
                                    ds_with_meta = pydicom.dcmread(file_path, force=True, defer_size=None)
                                    if hasattr(ds_with_meta, 'file_meta') and ds_with_meta.file_meta:
                                        ds.file_meta = ds_with_meta.file_meta
                                        if is_rtdose:
                                            logger.info(f"File meta aus Datei {os.path.basename(file_path)} wiederhergestellt")
                                            # Erneut alle 0002-Tags auflisten
                                            meta_elements = [elem for elem in ds.file_meta if elem.tag.group == 0x0002]
                                            logger.info(f"Nach Wiederherstellung - Anzahl 0002-Tags: {len(meta_elements)}")
                                            for elem in meta_elements:
                                                logger.info(f"  {elem.tag}: {elem.keyword} = {elem.value}")
                                        else:
                                            logger.debug(f"File meta aus Datei {os.path.basename(file_path)} wiederhergestellt")
                                    else:
                                        if is_rtdose:
                                            logger.info("Keine file_meta in ds_with_meta gefunden")
                                else:
                                    if is_rtdose:
                                        logger.info("Kein DICM-Marker gefunden")
                        except Exception as e:
                            if is_rtdose:
                                logger.info(f"Fehler beim Lesen der file_meta: {str(e)}")
                            else:
                                logger.debug(f"Konnte file_meta nicht aus {os.path.basename(file_path)} lesen: {str(e)}")
                            pass
                    
                    # Stelle sicher, dass file_meta vorhanden und korrekt ist
                    if not hasattr(ds, 'file_meta') or not ds.file_meta:
                        ds.file_meta = pydicom.dataset.FileMetaDataset()
                    
                    # Transfer Syntax UID setzen, falls nicht vorhanden
                    if not hasattr(ds.file_meta, 'TransferSyntaxUID') or not ds.file_meta.TransferSyntaxUID:
                        from pydicom.uid import ImplicitVRLittleEndian
                        ds.file_meta.TransferSyntaxUID = ImplicitVRLittleEndian
                        logger.debug(f"Transfer Syntax UID für {os.path.basename(file_path)} gesetzt")
                    
                    # Media Storage SOP Class UID setzen
                    if hasattr(ds, 'SOPClassUID'):
                        ds.file_meta.MediaStorageSOPClassUID = ds.SOPClassUID
                    
                    # Media Storage SOP Instance UID setzen
                    if hasattr(ds, 'SOPInstanceUID'):
                        ds.file_meta.MediaStorageSOPInstanceUID = ds.SOPInstanceUID
                    
                    # Implementation Class UID setzen
                    if not hasattr(ds.file_meta, 'ImplementationClassUID') or not ds.file_meta.ImplementationClassUID:
                        ds.file_meta.ImplementationClassUID = '1.2.276.0.7230010.3.0.3.6.4'  # pydicom UID
                    
                    # Implementation Version Name setzen
                    if not hasattr(ds.file_meta, 'ImplementationVersionName') or not ds.file_meta.ImplementationVersionName:
                        ds.file_meta.ImplementationVersionName = 'PYDICOM'
                    
                    # Prüfen und korrigieren der SOP Class UID, falls es sich um einen anonymisierten RT-Plan handelt
                    if hasattr(ds, 'SOPClassUID') and ds.SOPClassUID == '1.2.246.352.70.1.70':
                        logger.info(f"Korrigiere nicht-standardmäßige RT-Plan SOP Class UID in {os.path.basename(file_path)}")
                        ds.SOPClassUID = '1.2.840.10008.5.1.4.1.1.481.5'  # Standard RT-Plan SOP Class UID
                        ds.file_meta.MediaStorageSOPClassUID = ds.SOPClassUID
                    
                    # C-STORE Request senden
                    status = assoc.send_c_store(ds)
                    
                    if status and status.Status == 0:
                        success_count += 1
                        logger.debug(f"Datei {os.path.basename(file_path)} erfolgreich gesendet")
                    else:
                        status_code = status.Status if status else "unbekannt"
                        # Für CT-Dateien: nur ersten Fehler loggen, dann zählen
                        if hasattr(ds, 'Modality') and ds.Modality == 'CT':
                            failed_ct_count += 1
                            if failed_ct_count == 1:
                                logger.error(f"Fehler beim Senden von CT-Dateien - Status: {status_code} (weitere CT-Fehler werden nur gezählt)")
                        else:
                            logger.error(f"Fehler beim Senden: {os.path.basename(file_path)} - Status: {status_code}")
                        
                except Exception as e:
                    logger.error(f"Fehler beim Senden der DICOM-Datei {os.path.basename(file_path)}: {str(e)}")
            
            # Verbindung beenden
            assoc.release()
            
            # Zusammenfassung für CT-Fehler
            if failed_ct_count > 1:
                logger.error(f"Insgesamt {failed_ct_count} CT-Dateien konnten nicht gesendet werden")
            
            # Erfolg, wenn alle Dateien gesendet wurden
            return success_count == file_count
            
        except Exception as e:
            logger.error(f"Fehler beim Senden der Dateien an {node_info.get('name', 'unbekannt')}: {str(e)}")
            return False
