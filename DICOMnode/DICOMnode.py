import os
import re
import time
import subprocess
import ctypes
import threading
import shutil
import configparser
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from pydicom import dcmread
from pydicom.uid import (
    ExplicitVRLittleEndian, ImplicitVRLittleEndian, DeflatedExplicitVRLittleEndian, ExplicitVRBigEndian
)
from pynetdicom import AE, evt
from pynetdicom.sop_class import (
    CTImageStorage, MRImageStorage, EnhancedMRImageStorage, EnhancedMRColorImageStorage,
    RTPlanStorage, RTStructureSetStorage, RTDoseStorage, PositronEmissionTomographyImageStorage,
    SpatialRegistrationStorage, DeformableSpatialRegistrationStorage,
    Verification
)
import concurrent.futures

# Load configuration
def load_config():
    """Load configuration from config.ini file."""
    config = configparser.ConfigParser()
    config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'config.ini')
    
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Configuration file not found: {config_path}")
    
    config.read(config_path)
    return config

# Load configuration
config = load_config()

# Configuration settings from INI file
AE_TITLE = config.get('General', 'ae_title', fallback='FOLLOW')
WINDOW_TITLE = config.get('General', 'window_title', fallback='FOLLOW-Deamon')
RECEIVE_PORT = config.getint('Network', 'receive_port', fallback=1335)
SERVER_IP = config.get('Network', 'server_ip', fallback='192.168.178.55')
DESTINATION_DIR = config.get('Directories', 'destination_dir', fallback=r'D:\IncomingFollow')
INCOMING_DIR = os.path.join(DESTINATION_DIR, "incoming")
ERROR_DIR = os.path.join(DESTINATION_DIR, "errors")
EMF2SF_PATH = config.get('Tools', 'emf2sf_path', fallback=r'C:\dcm4che\bin')

# Set window title
ctypes.windll.kernel32.SetConsoleTitleW(WINDOW_TITLE)

# Load trusted AE titles from config
trusted_ae_string = config.get('Security', 'trusted_ae_titles', fallback='MRMULTI,TR_SEND,VARIAN,MYQASRS,RTPLANNING,TRUSTED_AE_2')
TRUSTED_AE_TITLES = [ae.strip() for ae in trusted_ae_string.split(',')]

# Load AE mappings from config
AE_MAPPINGS = {}
if config.has_section('AE_Mappings'):
    for ae_title, directory in config.items('AE_Mappings'):
        AE_MAPPINGS[ae_title.upper()] = directory

def is_trusted_ae(requestor_ae_title):
    """Überprüfe, ob der AE-Titel vertrauenswürdig ist."""
    return requestor_ae_title in TRUSTED_AE_TITLES

def handle_echo(event):
    """Handle incoming C-ECHO (Verification) requests."""
    requestor_ae_title = event.assoc.requestor.ae_title
    print(f"Verification request received from AE Title: {requestor_ae_title}")

    # Überprüfen, ob der AE-Titel in der Liste der vertrauenswürdigen Titel ist
    if not is_trusted_ae(requestor_ae_title):
        print(f"Untrusted AE Title: {requestor_ae_title}. Echo rejected.")
        return 0xC001  # Status code for rejection

    print(f"Trusted AE Title: {requestor_ae_title}. Echo accepted.")
    return 0x0000  # Success status for trusted AE-Titles

# Create directories if they don't exist
os.makedirs(DESTINATION_DIR, exist_ok=True)
os.makedirs(INCOMING_DIR, exist_ok=True)
os.makedirs(ERROR_DIR, exist_ok=True)

def get_incoming_dir_for_ae(ae_title):
    """Return the incoming directory based on the AE title of the requesting device."""
    # Use configured AE mappings
    ae_title_upper = ae_title.upper()
    if ae_title_upper in AE_MAPPINGS:
        mapped_dir = AE_MAPPINGS[ae_title_upper]
        # If it's a relative path, make it relative to DESTINATION_DIR
        if not os.path.isabs(mapped_dir):
            return os.path.join(DESTINATION_DIR, mapped_dir)
        return mapped_dir
    
    # Standard fallback to 'incoming' folder
    return os.path.join(DESTINATION_DIR, "incoming")



def correct_dicom_tag(name):
    """
    Bereinigt und korrigiert den Patientennamen, indem ungültige Zeichen entfernt werden, aber das ^-Zeichen bleibt erhalten.
    Gibt den bereinigten Namen nur zurück, wenn Änderungen vorgenommen wurden.
    """
    # Schritt 1: Versuche, ungültige Zeichen zu ignorieren oder zu ersetzen
    try:
        # Versuche, den String korrekt zu dekodieren (in UTF-8)
        original_name = name
        name = name.encode('utf-8', 'replace').decode('utf-8', 'replace')
    except UnicodeEncodeError:
        print(f"Warnung: Zeichenkodierung für '{name}' fehlerhaft. Bereinige Zeichen.")

    # Schritt 2: Entferne ungültige Zeichen, aber behalte ^ und andere erlaubte Zeichen
    cleaned_name = re.sub(r'[^\w\s_^-]', '', name)
    
    # Optional: Entferne doppelte oder unerwünschte Unterstriche/Bindestriche
    cleaned_name = re.sub(r'[_-]{2,}', '_', cleaned_name)  # Ersetze doppelte Unterstriche/Bindestriche
    cleaned_name = cleaned_name.strip('_')  # Entferne führende/folgende Unterstriche
    
    # Schritt 3: Nur zurückgeben, wenn der Name sich geändert hat
    if cleaned_name != original_name:
        return cleaned_name
    else:
        return None  # Keine Änderung


def sanitize_folder_name(name):
    """Sanitize folder name to remove invalid characters."""
    return re.sub(r'[<>:"/\\|?*^]', '_', name)  # Replace invalid characters with underscores

def create_subfolder(ds, base_folder, modality, date):
    """Create a subfolder based on StudyID, SeriesID, SeriesDescription, and image count."""
    try:
        # Extract necessary metadata for folder naming
        study_id = getattr(ds, 'StudyID', None)
        series_id = getattr(ds, 'SeriesNumber', None)
        series_description = getattr(ds, 'SeriesDescription', 'NoDescription')
        image_count = getattr(ds, 'NumberOfFrames', 0)

        # Check if the necessary identifiers are available
        if not study_id or not series_id:
            raise ValueError("Missing StudyID or SeriesID for folder creation.")

        # Sanitize the SeriesDescription for safe file path usage
        series_description = sanitize_folder_name(series_description)

        # Create folder name
        if image_count == 0:
            folder_name = f"{date}_{study_id}_{series_id}_{series_description}"
        else:
            folder_name = f"{date}_{study_id}_{series_id}_{series_description}_{image_count}"
        folder_path = os.path.join(base_folder, modality, folder_name)
        os.makedirs(folder_path, exist_ok=True)
        return folder_path
    except Exception as e:
        print(f"Could not create subfolder: {str(e)}. Saving in base folder.")
        return base_folder

# Konstanter Executor für parallelisierte Aufgaben
max_workers = config.getint('FolderWatcher', 'max_workers', fallback=4)
executor = concurrent.futures.ThreadPoolExecutor(max_workers=max_workers)

tags_to_process = [
    (0x0040, 0x0254)  # PerformedProcedureStepDescription
    #(0x0008, 0x1030),  # StudyDescription
    #(0x0008, 0x103E)   # SeriesDescription
]

def handle_store(event):
    """Handle incoming DICOM C-STORE requests."""
    ds = event.dataset
    ds.file_meta = event.file_meta
    try:
        # Extract patient ID and modality, including differentiation for MR types
        requestor_ae_title = event.assoc.requestor.ae_title
        print(f"Request received from AE Title: {requestor_ae_title}")

        # Dynamically set incoming directory based on AE Title
        incoming_dir = get_incoming_dir_for_ae(requestor_ae_title)
        os.makedirs(incoming_dir, exist_ok=True)

        patient_id = sanitize_folder_name(getattr(ds, 'PatientID', 'UNKNOWN_PATIENT'))
        #patient_name_element = ds.get((0x0010, 0x0010), None)
        #patient_name = sanitize_folder_name((str(patient_name_element.value) if patient_name_element else 'UNKNOWN_PATIENT'))
        # Holen des Patientennamens und Bereinigung
        patient_name_element = ds.get((0x0010, 0x0010), None)  # DICOM Tag für Patient Name
        if patient_name_element:
            corrected_name = correct_dicom_tag(str(patient_name_element.value))
            if corrected_name:  # Nur ändern, wenn der Name korrigiert wurde
                ds.PatientName = corrected_name  # Aktualisiere den Namen im Dataset
                patient_name = corrected_name  # Verwende den bereinigten Namen
                print(f"Patientenname geändert zu: {corrected_name}")
            else:
                patient_name = str(patient_name_element.value)  # Verwende den ursprünglichen Namen
        else:
            patient_name = 'UNKNOWN_PATIENT'
        
        # Bereinigung von zusätzlichen Tags in einer Schleife
        for tag in tags_to_process:
            if tag in ds:
                element = ds[tag]  # Holen des DICOM-Elements
                if isinstance(element.value, str):  # Nur Strings bereinigen
                    print(element.value)
                    corrected_value = correct_dicom_tag(element.value)
                    if corrected_value:
                        print(f"Korrigiere Tag {element.name}: '{element.value}' zu '{corrected_value}'")
                        ds[tag].value = corrected_value  # Aktualisiere den Wert im Dataset


        patient_name = sanitize_folder_name(patient_name)
        modality = getattr(ds, 'Modality', 'UNKNOWN_MODALITY')

        # Check if modality is MR, CT, or PET to decide on folder creation
        needs_subfolder = modality in ['MR', 'CT', 'PT']  # PT is used for PET images

        # Extract date with priority: InstanceCreationDate -> ContentDate -> 'UNKNOWN_DATE'
        instance_creation_date = getattr(ds, 'InstanceCreationDate', None)
        content_date = getattr(ds, 'ContentDate', None)
        series_date = getattr(ds, 'SeriesDate', None)
        series_descr = getattr(ds, 'SeriesDescription', 'NoDescription')
        sopUID = ds.SOPInstanceUID
        date_used = series_date or content_date or instance_creation_date or 'UNKNOWN_DATE'

        # Extract time with priority: InstanceCreationTime -> ContentTime -> '000000'
        instance_creation_time = getattr(ds, 'InstanceCreationTime', None)
        content_time = getattr(ds, 'ContentTime', None)
        series_time = getattr(ds, 'SeriesTime', None)
        acquisition_time = getattr(ds, 'AcquisitionTime', None)
        time_used = (series_time or acquisition_time or content_time or instance_creation_time or '000000').split('.')[0]  # Ignore milliseconds if present

        # Create a destination folder, with subfolders only for MR, CT, and PET
        if needs_subfolder:
            destination_folder = create_subfolder(ds, os.path.join(incoming_dir, patient_id+'_'+patient_name), modality, date_used)
        elif 'iba_SRS' in incoming_dir:
            if 'DOSE' in modality:
                sequence = getattr(ds, 'ReferencedRTPlanSequence',None)
                if sequence:
                    for item in sequence:
                        sopUIDd = getattr(item,'ReferencedSOPInstanceUID',None)
                        if sopUIDd:
                            sopUID = getattr(item,'ReferencedSOPInstanceUID',None)
            destination_folder = os.path.join(incoming_dir, patient_id+'_'+patient_name, sopUID)
            os.makedirs(destination_folder, exist_ok=True)
        else:
            destination_folder = os.path.join(incoming_dir, patient_id+'_'+patient_name, modality)
            os.makedirs(destination_folder, exist_ok=True)
        
        # Create a unique filename based on SOPInstanceUID, date, and time
        filename = f"{modality}_{date_used}{time_used}_{ds.SOPInstanceUID}.dcm"
        if 'iba_SRS' in incoming_dir:
            filename = f"{series_descr}_{date_used}{time_used}_{ds.SOPInstanceUID}.dcm"
            
        file_path = os.path.join(destination_folder, filename)

        # Save the DICOM file (parallelized for enhanced MR)
        executor.submit(process_and_save_dicom, ds, file_path, modality, destination_folder)

    except Exception as e:
        # Handle errors by moving the file to the error folder
        error_filename = f"error_{event.request.AffectedSOPInstanceUID}.dcm"
        error_path = os.path.join(ERROR_DIR, error_filename)
        ds.save_as(error_path, write_like_original=False)
        print(f"Error processing file: {error_path} | Error: {str(e)}")

    return 0x0000  # Success status

def process_and_save_dicom(ds, file_path, modality, destination_folder):
    """Process and save DICOM file, including enhanced MR conversion."""
    try:
        # Debug: Prüfe PixelData für RTDOSE
        if modality == "RTDOSE":
            if hasattr(ds, "PixelData"):
                print(f"[DEBUG] RTDOSE PixelData length: {len(ds.PixelData)} bytes")
            else:
                print(f"[DEBUG] RTDOSE: Kein PixelData im Dataset vorhanden!")

        # Save the DICOM file
        ds.save_as(file_path, write_like_original=False)
        print(f"Received and sorted DICOM file: {file_path}")

        # Debug: Prüfe Dateigröße nach dem Speichern
        if modality == "RTDOSE":
            try:
                file_size = os.path.getsize(file_path)
                print(f"[DEBUG] RTDOSE gespeicherte Dateigröße: {file_size} bytes")
            except Exception as e:
                print(f"[DEBUG] Fehler beim Prüfen der Dateigröße: {e}")

        # If the file is Enhanced MR, convert it to Standard MR using emf2sf and handle registrations
        if ds.SOPClassUID in [EnhancedMRImageStorage, EnhancedMRColorImageStorage]:
            try:
                # Convert Enhanced MR to Standard MR
                convert_enhanced_mr_to_standard(file_path, destination_folder)

                # Move converted Standard MR files to the appropriate StandardMR subfolder
                move_converted_files_to_standard_subfolder(file_path, ds)
                print(f"Converted Enhanced MR file and moved to Standard MR subfolder.")
                os.remove(file_path)
                print(f"Deleted original Enhanced MR file: {file_path}")

            except Exception as conv_error:
                print(f"Error converting Enhanced MR file: {file_path} | Error: {str(conv_error)}")

    except Exception as e:
        # Handle errors by moving the file to the error folder
        error_filename = f"error_{ds.SOPInstanceUID}.dcm"
        error_path = os.path.join(ERROR_DIR, error_filename)
        ds.save_as(error_path, write_like_original=False)
        print(f"Error processing file: {error_path} | Error: {str(e)}")



def convert_enhanced_mr_to_standard(input_path, output_dir):
    """Convert Enhanced MR DICOM to Standard MR using emf2sf."""
    try:
        # Define the output directory for the conversion
        output_path = os.path.join(output_dir, 'converted')
        os.makedirs(output_path, exist_ok=True)

        # Command to execute emf2sf conversion
        command = [
            os.path.join(EMF2SF_PATH, "emf2sf.bat"),
            "--out-dir", output_path,
            input_path
        ]

        # Run the conversion command
        result = subprocess.run(command, shell=True, check=True, capture_output=True, text=True)
        print(f"MR-Conversion successful: {result.stdout}")

    except subprocess.CalledProcessError as e:
        print(f"MR-Conversion failed: {e.stderr}")
        raise
    except Exception as e:
        print(f"An unexpected error occurred: {str(e)}")
        raise

def move_converted_files_to_standard_subfolder(original_file_path, ds):
    """Move converted Standard MR files to the appropriate StandardMR subfolder."""
    try:
        # Identify the correct StandardMR subfolder
        destination_folder = create_subfolder(ds, os.path.join(INCOMING_DIR, getattr(ds, 'PatientID', 'UNKNOWN_PATIENT')), "MR", getattr(ds, 'ContentDate', 'Date'))

        # Find all converted files in the 'converted' directory
        converted_dir = os.path.join(os.path.dirname(original_file_path), 'converted')
        for file_name in os.listdir(converted_dir):
            converted_file_path = os.path.join(converted_dir, file_name)
            new_file_path = os.path.join(destination_folder, file_name)

            # Move the converted file to the StandardMR subfolder
            os.replace(converted_file_path, new_file_path)
            print(f"Moved converted file to StandardMR subfolder: {new_file_path}")

        # Clean up the temporary converted directory
        os.rmdir(converted_dir)

    except Exception as e:
        print(f"Error moving converted files to StandardMR subfolder: {str(e)}")

def start_receiver(ae_title, ip, port):
    """Start the DICOM receiver service."""
    while True:
        try:
            ae = AE(ae_title=ae_title)

            # Add supported SOP classes and transfer syntaxes
            supported_sop_classes = [
                RTPlanStorage, RTStructureSetStorage, RTDoseStorage, CTImageStorage,
                MRImageStorage, EnhancedMRImageStorage, EnhancedMRColorImageStorage, PositronEmissionTomographyImageStorage,
                SpatialRegistrationStorage, DeformableSpatialRegistrationStorage,
                Verification
            ]
            transfer_syntaxes = [
                ExplicitVRLittleEndian, ImplicitVRLittleEndian, DeflatedExplicitVRLittleEndian, ExplicitVRBigEndian
            ]

            for sop_class in supported_sop_classes:
                ae.add_supported_context(sop_class, transfer_syntaxes)

            # Define event handlers for incoming DICOM files
            handlers = [(evt.EVT_C_STORE, handle_store)]

            # Start the server on the specified IP and port
            print(f"Starting DICOM receiver on IP {ip}, port {port} with AE Title '{ae_title}'...")
            ae.start_server((ip, port), evt_handlers=handlers)
        except Exception as e:
            #Print the Error and retry after 10secs
            print(f"Critical error in DICOM receiver: {str(e)}")
            print("Retrying in 10 seconds...")
            time.sleep(10)

class DICOMFolderWatcher(FileSystemEventHandler):
    """Watches a folder for DICOM files and forwards them after 1 second of inactivity.
    Processes folders one by one with files sent in modality order: CT -> RStruct -> RTPLAN -> RTDOSE.
    Failed files are moved to a 'failed' folder with logs. Files are deleted after sending.
    Empty folders (except 'failed') are deleted at the start of each processing round."""
    
    def __init__(self, watch_folder, target_aet, target_ip, target_port):
        self.watch_folder = watch_folder
        self.target_aet = target_aet
        self.target_ip = target_ip
        self.target_port = target_port
        self.pending_files = {}
        self.timer_lock = threading.Lock()
        self.ae = AE(ae_title=AE_TITLE)  # Use configured AE title
        self.processing_lock = threading.Lock()  # Lock for ensuring only one folder is processed at a time
        self.is_processing = False               # Flag to track if folder processing is ongoing
        self.last_heartbeat = time.time()        # Track last heartbeat time
        self.folder_activity = {}                # Track last activity time for each folder
        self.folder_timers = {}                  # Timers for folder processing
        
        # Load timer intervals from config
        self.process_timer_interval = config.getfloat('FolderWatcher', 'process_timer_interval', fallback=10.0)
        self.heartbeat_interval = config.getfloat('FolderWatcher', 'heartbeat_interval', fallback=120.0)
        self.inactivity_timeout = config.getfloat('FolderWatcher', 'inactivity_timeout', fallback=1.0)
        
        # Add presentation contexts for DICOM storage
        self.ae.add_requested_context(CTImageStorage)
        self.ae.add_requested_context(MRImageStorage)
        self.ae.add_requested_context(EnhancedMRImageStorage)
        self.ae.add_requested_context(EnhancedMRColorImageStorage)
        self.ae.add_requested_context(RTPlanStorage)
        self.ae.add_requested_context(RTStructureSetStorage)
        self.ae.add_requested_context(RTDoseStorage)
        self.ae.add_requested_context(PositronEmissionTomographyImageStorage)
        self.ae.add_requested_context(SpatialRegistrationStorage)
        self.ae.add_requested_context(DeformableSpatialRegistrationStorage)
        
        # Create failed folder if it doesn't exist
        self.failed_folder = os.path.join(self.watch_folder, "failed")
        os.makedirs(self.failed_folder, exist_ok=True)
        
        # Setup logging in failed folder
        self.log_file = os.path.join(self.failed_folder, "send_errors.log")
        
        print(f"DICOM Folder Watcher initialized for: {watch_folder}")
        print(f"Target: {target_aet}@{target_ip}:{target_port}")
        print(f"Failed files will be moved to: {self.failed_folder}")
        print(f"Process timer interval: {self.process_timer_interval}s")
        print(f"Heartbeat interval: {self.heartbeat_interval}s")
        print(f"Inactivity timeout: {self.inactivity_timeout}s")
        
        # Start a timer to periodically process accumulated files
        self.process_timer = threading.Timer(self.process_timer_interval, self.process_all_folders)
        self.process_timer.daemon = True
        self.process_timer.start()
        
        # Start heartbeat timer
        self.heartbeat_timer = threading.Timer(self.heartbeat_interval, self.send_heartbeat)
        self.heartbeat_timer.daemon = True
        self.heartbeat_timer.start()
        
        # Clean empty folders at startup
        self.cleanup_empty_folders()
        
        # Process any folders that already exist at startup
        self.initial_folder_processing()
        # Process any folders that were not fully processed
        self.periodic_rescan()
        self.schedule_cleanup()
    
    def schedule_cleanup(self):
        """Schedule regular cleanup of empty folders."""
        self.cleanup_empty_folders()
        threading.Timer(300.0, self.schedule_cleanup).start()  # alle 5 Minuten
    
    def periodic_rescan(self):
        """Recheck all patient subfolders for DICOMs that were missed."""
        try:
            for patient_folder in os.listdir(self.watch_folder):
                patient_path = os.path.join(self.watch_folder, patient_folder)
                if not os.path.isdir(patient_path) or patient_path == self.failed_folder:
                    continue

                # Gibt es darunter .dcm-Dateien in beliebiger Tiefe?
                found_dicom = False
                for root, _, files in os.walk(patient_path):
                    if any(f.lower().endswith(".dcm") for f in files):
                        found_dicom = True
                        break

                if found_dicom:
                    # Falls Ordner nicht bereits in Timer-Überwachung, erneut check starten
                    with self.timer_lock:
                        if patient_path not in self.folder_timers:
                            retry_timer = threading.Timer(14.0, self.check_folder_for_processing, args=[patient_path])
                            self.folder_timers[patient_path] = retry_timer
                            retry_timer.start()
                            print(f"[RESCAN] Requeued folder after delayed DICOM detection: {patient_path}")

        except Exception as e:
            print(f"Error during periodic rescan: {str(e)}")
            self.log_error("Error in periodic_rescan", e)

        # Zeitgesteuert neu starten (alle 5 Minuten)
        threading.Timer(300.0, self.periodic_rescan).start()
    
    def on_created(self, event):
        if not event.is_directory and event.src_path.lower().endswith('.dcm'):
            self.handle_dicom_file(event.src_path)
    
    def on_modified(self, event):
        if not event.is_directory and event.src_path.lower().endswith('.dcm'):
            self.handle_dicom_file(event.src_path)
    
    def handle_dicom_file(self, file_path):
        """Handle new or modified DICOM file and update folder activity."""
        # Skip files in the failed folder
        if self.failed_folder in file_path:
            return
        
        # Determine which folder this file belongs to
        parent_folder = os.path.dirname(file_path)
        
        with self.timer_lock:
            # Update folder activity timestamp
            self.folder_activity[parent_folder] = time.time()
            
            # Cancel existing folder timer if there is one
            if parent_folder in self.folder_timers and self.folder_timers[parent_folder].is_alive():
                self.folder_timers[parent_folder].cancel()
            
            # Start new folder timer with 13 seconds inactivity delay
            folder_timer = threading.Timer(13.0, self.check_folder_for_processing, args=[parent_folder])
            self.folder_timers[parent_folder] = folder_timer
            folder_timer.start()
            
            # For compatibility, still track the file itself
            if file_path in self.pending_files:
                self.pending_files[file_path].cancel()
            
            # No need to process individual files anymore
            self.pending_files[file_path] = threading.Timer(13.0, lambda: None)
            self.pending_files[file_path].start()
    
    def check_folder_for_processing(self, folder_path):
        """Check if a folder is ready for processing after inactivity period."""
        try:
            # Remove from folder timers
            with self.timer_lock:
                if folder_path in self.folder_timers:
                    del self.folder_timers[folder_path]
            
            # Check if folder exists
            if not os.path.exists(folder_path):
                return
                
            print(f"Folder {folder_path} has been inactive for 13 seconds, scheduling for processing")
            
            # Schedule folder processing
            self.schedule_folder_processing(folder_path)
            
        except Exception as e:
            print(f"Error checking folder for processing {folder_path}: {str(e)}")
            self.log_error(f"Error checking folder: {folder_path}", e)
    
    def schedule_folder_processing(self, folder_path=None):
        """Schedule processing of specific folder or all folders if not already processing."""
        with self.processing_lock:
            if not self.is_processing:
                # If we have a specific folder, process just that one
                if folder_path:
                    self.process_timer = threading.Timer(1.0, lambda: self.process_specific_folder(folder_path))
                else:
                    # Otherwise process all folders (legacy behavior)
                    self.process_timer = threading.Timer(1.0, self.process_all_folders)
                
                self.process_timer.daemon = True
                self.process_timer.start()
    
    def process_all_folders(self):
        """Process all folders in the watch directory."""
        with self.processing_lock:
            if self.is_processing:
                return
                
            self.is_processing = True
            
        try:
            #print("Starting to process all folders...")
            # Clean up empty folders first
            #self.cleanup_empty_folders()
            
            # Get all subdirectories in the watch folder
            subdirs = [d for d in os.listdir(self.watch_folder) 
                      if os.path.isdir(os.path.join(self.watch_folder, d)) and d != "failed"]
            
            if not subdirs:
                #print("No folders to process.")
                return
                
            for folder in subdirs:
                folder_path = os.path.join(self.watch_folder, folder)
                print(f"Scheduling folder for processing after inactivity check: {folder_path}")
                # Anstatt direkt zu verarbeiten, den Inaktivitätszähler starten
                self.schedule_folder_processing(folder_path)
                
        except Exception as e:
            print(f"Error in folder processing: {str(e)}")
            self.log_error("Error in folder processing", e)
        finally:
            with self.processing_lock:
                self.is_processing = False
                
            # Schedule next processing round
            self.process_timer = threading.Timer(10.0, self.process_all_folders)
            self.process_timer.daemon = True
            self.process_timer.start()
            
            # Check if we need to send a heartbeat
            self.check_heartbeat()
    
    def process_specific_folder(self, folder_path):
        """Process a specific folder after inactivity period."""
        with self.processing_lock:
            if self.is_processing:
                print(f"Already processing, folder {folder_path} will be handled in the next round")
                return
                
            self.is_processing = True
            
        try:
            # Check if folder still exists
            if not os.path.exists(folder_path):
                print(f"Folder {folder_path} no longer exists, skipping processing")
                return
                
            print(f"Processing specific folder: {folder_path}")
            self.process_folder(folder_path)
            
            # Clean up empty folders after processing
            #self.cleanup_empty_folders()
                
        except Exception as e:
            print(f"Error processing folder {folder_path}: {str(e)}")
            self.log_error(f"Error processing folder: {folder_path}", e)
        finally:
            with self.processing_lock:
                self.is_processing = False
            
            # Check if we need to send a heartbeat
            self.check_heartbeat()
    
    def process_folder(self, folder_path):
        """Process a single folder, sending files in the correct modality order."""

        modality_order = ["CT", "RTSTRUCT", "RTPLAN", "RTDOSE"]

        # Collect all DICOM files in the folder
        all_files = []
        for root, _, files in os.walk(folder_path):
            for file in files:
                if file.lower().endswith('.dcm'):
                    all_files.append(os.path.join(root, file))

        if not all_files:
            print(f"No DICOM files found in folder: {folder_path}, rechecking after inactivity...")

            # Recheck after another 14s of inactivity
            with self.timer_lock:
                if folder_path in self.folder_timers:
                    self.folder_timers[folder_path].cancel()

                retry_timer = threading.Timer(14.0, self.check_folder_for_processing, args=[folder_path])
                self.folder_timers[folder_path] = retry_timer
                retry_timer.start()

            return

        # Group files by modality
        modality_files = {mod: [] for mod in modality_order}
        other_files = []

        for file_path in all_files:
            try:
                # Versuche die DICOM-Datei zu lesen
                ds = dcmread(file_path)
                modality = getattr(ds, 'Modality', 'UNKNOWN')

                if modality == "RTSTRUCT":
                    modality_files["RTSTRUCT"].append((file_path, ds))
                elif modality == "CT":
                    modality_files["CT"].append((file_path, ds))
                elif modality == "RTPLAN":
                    modality_files["RTPLAN"].append((file_path, ds))
                elif modality == "RTDOSE":
                    modality_files["RTDOSE"].append((file_path, ds))
                else:
                    other_files.append((file_path, ds))

            except Exception as e:
                print(f"Error reading DICOM file {file_path}: {str(e)}")
                
                # Prüfe, ob es sich um eine Dosisdatei handeln könnte (basierend auf Dateinamen)
                filename_lower = os.path.basename(file_path).lower()
                if "dose" in filename_lower or "rtdose" in filename_lower:
                    print(f"Datei könnte eine Dosisdatei sein, wird direkt ohne DICOM-Parsing gesendet: {file_path}")
                    try:
                        # Kopiere die Datei in einen temporären Ordner für die direkte Übertragung
                        import shutil
                        import tempfile
                        
                        # Erstelle einen temporären Ordner, falls er nicht existiert
                        temp_dir = os.path.join(self.watch_folder, "temp_dose_files")
                        os.makedirs(temp_dir, exist_ok=True)
                        
                        # Generiere einen eindeutigen Dateinamen
                        temp_file = os.path.join(temp_dir, f"dose_{int(time.time())}_{os.path.basename(file_path)}")
                        
                        # Kopiere die Originaldatei
                        shutil.copy2(file_path, temp_file)
                        
                        # Erstelle ein minimales Dataset nur für die Gruppierung
                        ds = Dataset()
                        ds.Modality = "RTDOSE"
                        ds._is_raw_dose_file = True  # Spezielle Markierung
                        ds._raw_dose_path = temp_file  # Speichere den Pfad zur kopierten Datei
                        
                        # Füge die Datei zur RTDOSE-Liste hinzu
                        modality_files["RTDOSE"].append((file_path, ds))
                        print(f"Dosisdatei zur direkten Übertragung vorbereitet: {file_path} -> {temp_file}")
                    except Exception as dose_error:
                        print(f"Fehler beim Vorbereiten der Dosisdatei: {str(dose_error)}")
                        self.move_to_failed(file_path, f"Error preparing dose file: {str(dose_error)}")
                else:
                    # Keine Dosisdatei, verschiebe in den Failed-Ordner
                    self.move_to_failed(file_path, f"Error reading file: {str(e)}")

        
        # Erstelle eine sortierte Liste aller Dateien in der gewünschten Modalitätsreihenfolge
        ordered_files = []
        for modality in modality_order:
            if modality_files[modality]:
                file_count = len(modality_files[modality])
                if modality == "CT":
                    print(f"Hinzufügen von {file_count} CT-Files aus Folder {os.path.basename(folder_path)}")
                else:
                    print(f"Hinzufügen von {file_count} {modality} files")
                ordered_files.extend(modality_files[modality])
        
        # Füge alle übrigen Dateien am Ende hinzu
        if other_files:
            print(f"Hinzufügen von {len(other_files)} files mit anderen Modalitäten")
            ordered_files.extend(other_files)
            
        # Sende alle Dateien in einer einzigen Association
        if ordered_files:
            print(f"Sende {len(ordered_files)} Dateien aus Ordner {os.path.basename(folder_path)} in einer Association...")
            self.send_all_dicom_files(ordered_files, folder_name=os.path.basename(folder_path))

    
    def send_all_dicom_files(self, file_dataset_pairs, folder_name=""):
        """Send all DICOM files from a folder in a single association while maintaining order."""
        if not file_dataset_pairs:
            return
            
        file_count = len(file_dataset_pairs)
        success_count = 0
        failed_files = []
        
        # Track statistics by modality
        modality_stats = {}
        
        try:
            # Establish a single association for ALL files
            print(f"Establishing association with {self.target_aet} for {file_count} DICOM files from {folder_name}")
            assoc = self.ae.associate(self.target_ip, self.target_port, ae_title=self.target_aet)
            
            if assoc.is_established:
                # Send all datasets in a single association, maintaining order
                for i, (file_path, ds) in enumerate(file_dataset_pairs):
                    try:
                        modality = getattr(ds, 'Modality', 'UNKNOWN')
                        
                        # Update statistics for this modality
                        if modality not in modality_stats:
                            modality_stats[modality] = {'total': 0, 'success': 0}
                        modality_stats[modality]['total'] += 1
                        
                        # Fix SOP Class UID for RTPLAN files to ensure compatibility
                        if modality == "RTPLAN":
                            correct_sop_class_uid = '1.2.840.10008.5.1.4.1.1.481.5'
                            current_sop_class_uid = getattr(ds, 'SOPClassUID', None)
                            if current_sop_class_uid != correct_sop_class_uid:
                                print(f"Korrigiere SOP Class UID für RTPLAN: {current_sop_class_uid} -> {correct_sop_class_uid}")
                                ds.SOPClassUID = correct_sop_class_uid
                        
                        # Only print detailed progress for non-CT files or at intervals for CT
                        if modality != "CT" or i % 10 == 0:
                            print(f"Sending {modality} file {i+1}/{file_count}: {os.path.basename(file_path)}")
                        
                        # Sende das Dataset - für Dosisdateien mit fehlendem Header verwende direkten Dateizugriff
                        if hasattr(ds, '_is_raw_dose_file') and ds._is_raw_dose_file and modality == "RTDOSE":
                            raw_file_path = ds._raw_dose_path
                            print(f"Sende Dosisdatei direkt vom Dateisystem: {os.path.basename(raw_file_path)}")
                            
                            # Erstelle ein neues DICOM-Dataset mit den notwendigen Attributen
                            from pydicom.dataset import Dataset, FileMetaDataset
                            from pydicom.uid import generate_uid, ImplicitVRLittleEndian
                            
                            # Erstelle ein neues Dataset für die Dosisdatei
                            temp_ds = Dataset()
                            temp_ds.file_meta = FileMetaDataset()
                            
                            # Setze die notwendigen DICOM-Attribute
                            temp_ds.SOPClassUID = RTDoseStorage
                            temp_ds.SOPInstanceUID = generate_uid()
                            temp_ds.Modality = "RTDOSE"
                            
                            # Setze die Datei-Meta-Informationen
                            temp_ds.file_meta.TransferSyntaxUID = ImplicitVRLittleEndian
                            temp_ds.file_meta.MediaStorageSOPClassUID = RTDoseStorage
                            temp_ds.file_meta.MediaStorageSOPInstanceUID = temp_ds.SOPInstanceUID
                            
                            # Verwende die Originaldatei direkt beim Senden
                            # Der zweite Parameter gibt den Pfad zur Datei an, die gesendet werden soll
                            status = assoc.send_c_store(temp_ds, raw_file_path)
                            print(f"Dosisdatei direkt gesendet: {raw_file_path}")
                        else:
                            # Normaler Sendevorgang für alle anderen Dateien
                            status = assoc.send_c_store(ds)
                        
                        if status and status.Status == 0x0000:  # Success
                            success_count += 1
                            modality_stats[modality]['success'] += 1
                            os.remove(file_path)  # Delete after successful send
                        else:
                            error_msg = f"Failed to send: {os.path.basename(file_path)} - Status: {status.Status if status else 'unknown'}"
                            print(error_msg)
                            failed_files.append((file_path, error_msg))
                    except Exception as e:
                        error_msg = f"Error sending DICOM file {os.path.basename(file_path)}: {str(e)}"
                        print(error_msg)
                        failed_files.append((file_path, error_msg))
                        
                # Release association after all files are processed
                assoc.release()
                print(f"Transfer complete: {success_count} of {file_count} files successfully sent from {folder_name}")
                
                # Print statistics by modality
                for modality, stats in modality_stats.items():
                    print(f"  - {modality}: {stats['success']} of {stats['total']} successful")
                    
            else:
                error_msg = f"Failed to establish association with {self.target_aet}"
                print(error_msg)
                # Move all files to failed
                failed_files = [(file_path, error_msg) for file_path, _ in file_dataset_pairs]
                
        except Exception as e:
            error_msg = f"Transfer error: {str(e)}"
            print(error_msg)
            # Move all remaining files to failed
            failed_files = [(file_path, error_msg) for file_path, _ in file_dataset_pairs]
            
        # Move failed files to the failed folder
        for file_path, error_msg in failed_files:
            self.move_to_failed(file_path, error_msg)
            
        return success_count
        
    def send_dicom_batch(self, file_dataset_pairs, modality=None, folder_name=""):
        """Send multiple DICOM files in a single association (legacy method, kept for compatibility)."""
        if not file_dataset_pairs:
            return
            
        file_count = len(file_dataset_pairs)
        success_count = 0
        failed_files = []
        
        try:
            # Establish a single association for all files
            print(f"Establishing association with {self.target_aet} for batch send of {file_count} {modality} files")
            assoc = self.ae.associate(self.target_ip, self.target_port, ae_title=self.target_aet)
            
            if assoc.is_established:
                # Send all datasets in this batch using the same association
                for i, (file_path, ds) in enumerate(file_dataset_pairs):
                    try:
                        # Only print progress for CT or if it's a small batch
                        if modality == "CT" and i % 10 == 0:
                            print(f"Sending file {i+1}/{file_count} for {modality}")
                        elif modality != "CT":
                            print(f"Sending {os.path.basename(file_path)} ({i+1}/{file_count})")
                            
                        # Send the dataset
                        status = assoc.send_c_store(ds)
                        
                        if status and status.Status == 0x0000:  # Success
                            success_count += 1
                            os.remove(file_path)  # Delete after successful send
                        else:
                            error_msg = f"Failed to send: {os.path.basename(file_path)} - Status: {status.Status if status else 'unknown'}"
                            print(error_msg)
                            failed_files.append((file_path, error_msg))
                    except Exception as e:
                        error_msg = f"Error sending DICOM file {os.path.basename(file_path)}: {str(e)}"
                        print(error_msg)
                        failed_files.append((file_path, error_msg))
                        
                # Release association after all files are processed
                assoc.release()
                print(f"Batch complete: {success_count} of {file_count} {modality} files successfully sent from {folder_name}")
            else:
                error_msg = f"Failed to establish association with {self.target_aet}"
                print(error_msg)
                # Move all files to failed
                failed_files = [(file_path, error_msg) for file_path, _ in file_dataset_pairs]
                
        except Exception as e:
            error_msg = f"Batch sending error: {str(e)}"
            print(error_msg)
            # Move all remaining files to failed
            failed_files = [(file_path, error_msg) for file_path, _ in file_dataset_pairs]
            
        # Move failed files to the failed folder
        for file_path, error_msg in failed_files:
            self.move_to_failed(file_path, error_msg)
            
        return success_count
        
    def send_dicom_file(self, ds, file_path, modality=None, is_summary=False):
        """Send a single DICOM file to target node (legacy method kept for compatibility)."""
        try:
            # Only print detailed message for non-CT files or if it's a summary message
            if modality != "CT" or is_summary:
                print(f"Sending DICOM file: {os.path.basename(file_path)}")
            
            # Establish association
            assoc = self.ae.associate(self.target_ip, self.target_port, ae_title=self.target_aet)
            
            if assoc.is_established:
                # Send the dataset
                status = assoc.send_c_store(ds)
                
                if status and status.Status == 0x0000:  # Success
                    if modality != "CT" or is_summary:
                        print(f"Successfully sent: {os.path.basename(file_path)}")
                    # Delete the file after successful send
                    assoc.release()
                    os.remove(file_path)
                    return True
                else:
                    assoc.release()
                    error_msg = f"Failed to send: {os.path.basename(file_path)} - Status: {status.Status if status else 'unknown'}"
                    print(error_msg)
                    self.move_to_failed(file_path, error_msg)
                    return False
            else:
                error_msg = f"Failed to establish association with {self.target_aet}"
                print(error_msg)
                self.move_to_failed(file_path, error_msg)
                return False
                
        except Exception as e:
            error_msg = f"Error sending DICOM file: {str(e)}"
            print(error_msg)
            self.move_to_failed(file_path, error_msg)
            return False
    
    def move_to_failed(self, file_path, error_message):
        """Move a file to the failed folder and log the error."""
        try:
            # Create a unique filename to avoid overwriting existing files
            basename = os.path.basename(file_path)
            timestamp = time.strftime("%Y%m%d_%H%M%S")
            new_filename = f"{timestamp}_{basename}"
            destination = os.path.join(self.failed_folder, new_filename)
            
            # Move the file
            shutil.copy2(file_path, destination)  # Use copy2 to preserve metadata
            os.remove(file_path)  # Remove the original
            
            # Log the error
            self.log_error(f"Failed file moved to {destination}", error_message)
            print(f"Moved failed file to: {destination}")
        except Exception as e:
            print(f"Error moving file to failed folder: {str(e)}")
            
    def log_error(self, context, error):
        """Log error to the log file in the failed folder."""
        timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
        log_message = f"[{timestamp}] {context}: {str(error)}\n"
        
        try:
            with open(self.log_file, 'a') as f:
                f.write(log_message)
        except Exception as e:
            print(f"Error writing to log file: {str(e)}")
            
    def cleanup_empty_folders(self):
        """Delete empty folders in the watch directory (excluding the failed folder) if older than 3 minutes."""
        deleted = 0
        now = time.time()

        for root, dirs, files in os.walk(self.watch_folder, topdown=False):  # Bottom-up traversal
            for dir_name in dirs:
                dir_path = os.path.join(root, dir_name)

                # Skip the failed folder
                if dir_path == self.failed_folder:
                    continue

                # Check if folder is empty and older than 3 minutes
                if not os.listdir(dir_path):
                    try:
                        last_modified = os.path.getmtime(dir_path)
                        if now - last_modified > 180:  # older than 3 minutes
                            os.rmdir(dir_path)
                            deleted += 1
                            print(f"Deleted empty folder: {dir_path}")
                    except Exception as e:
                        print(f"Error deleting empty folder {dir_path}: {str(e)}")

        if deleted > 0:
            print(f"Cleaned up {deleted} empty folders")
            
    def send_heartbeat(self):
        """Send a heartbeat message with timestamp and reschedule next heartbeat."""
        current_time = time.time()
        timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
        print(f"[HEARTBEAT {timestamp}] DICOM Folder Watcher is alive and monitoring {self.watch_folder}")
        
        # Update last heartbeat time
        self.last_heartbeat = current_time
        
        # Schedule next heartbeat
        self.heartbeat_timer = threading.Timer(120.0, self.send_heartbeat)
        self.heartbeat_timer.daemon = True
        self.heartbeat_timer.start()
        
    def check_heartbeat(self):
        """Check if it's time to send a heartbeat."""
        current_time = time.time()
        if current_time - self.last_heartbeat >= 120.0:  # 2 minutes
            self.send_heartbeat()
            
    def initial_folder_processing(self):
        """Process any folders that already exist in the watch directory at startup."""
        try:
            print("Checking for existing folders in the watch directory...")
            
            # Get all subdirectories in the watch folder
            subdirs = [d for d in os.listdir(self.watch_folder) 
                      if os.path.isdir(os.path.join(self.watch_folder, d)) and d != "failed"]
            
            if not subdirs:
                print("No existing folders to process.")
                return
            
            print(f"Found {len(subdirs)} existing folders to process")
            
            # Schedule processing of each folder with a slight delay between them
            for i, folder in enumerate(subdirs):
                folder_path = os.path.join(self.watch_folder, folder)

                # Trigger "simulierte" Dateibewegung, um die Inaktivitätslogik anzustoßen
                with self.timer_lock:
                    if folder_path not in self.folder_timers:
                        folder_timer = threading.Timer(14.0, self.check_folder_for_processing, args=[folder_path])
                        self.folder_timers[folder_path] = folder_timer
                        folder_timer.start()
                        print(f"Initial folder scheduled for processing after 14s inactivity: {folder_path}")
                
        except Exception as e:
            print(f"Error during initial folder processing: {str(e)}")
            self.log_error("Error during initial folder processing", e)

def start_folder_watcher(watch_folder=None, target_aet=None, target_ip=None, target_port=None):
    """Start the folder watcher in a separate thread."""
    # Use config values if not provided
    if watch_folder is None:
        watch_folder = config.get('FolderWatcher', 'watch_folder', fallback=r'D:\Incoming2Send')
    if target_aet is None:
        target_aet = config.get('FolderWatcher', 'target_aet', fallback='DICOM-RT-KAFFEE')
    if target_ip is None:
        target_ip = config.get('FolderWatcher', 'target_ip', fallback='192.168.178.55')
    if target_port is None:
        target_port = config.getint('FolderWatcher', 'target_port', fallback=1334)
    
    if not os.path.exists(watch_folder):
        os.makedirs(watch_folder, exist_ok=True)
        print(f"Created watch folder: {watch_folder}")
    
    event_handler = DICOMFolderWatcher(watch_folder, target_aet, target_ip, target_port)
    observer = Observer()
    observer.schedule(event_handler, watch_folder, recursive=True)
    observer.start()
    
    print(f"Started watching folder: {watch_folder}")
    return observer

if __name__ == "__main__":
    import argparse
    
    # Get default values from config
    default_watch_folder = config.get('FolderWatcher', 'watch_folder', fallback=r'D:\Incoming2Send')
    
    parser = argparse.ArgumentParser(description='FOLLOW DICOM Receiver with Folder Watcher')
    parser.add_argument('--watch-folder', default=default_watch_folder, 
                       help='Folder to watch for DICOM files')
    parser.add_argument('--no-receiver', action='store_true', 
                       help='Disable DICOM receiver, only run folder watcher')
    parser.add_argument('--config', default='config.ini',
                       help='Configuration file path')
    
    args = parser.parse_args()
    
    print(f"Configuration loaded from: {os.path.join(os.path.dirname(os.path.abspath(__file__)), 'config.ini')}")
    print(f"AE Title: {AE_TITLE}")
    print(f"Server IP: {SERVER_IP}")
    print(f"Receive Port: {RECEIVE_PORT}")
    print(f"Destination Directory: {DESTINATION_DIR}")
    print(f"Watch Folder: {args.watch_folder}")
    print(f"Trusted AE Titles: {', '.join(TRUSTED_AE_TITLES)}")
    
    # Start folder watcher in background thread
    watcher_thread = threading.Thread(target=start_folder_watcher, args=(args.watch_folder,))
    watcher_thread.daemon = True
    watcher_thread.start()
    
    if not args.no_receiver:
        # Start the DICOM receiver with specified AE title, IP, and port
        start_receiver(AE_TITLE, SERVER_IP, RECEIVE_PORT)
    else:
        print("DICOM receiver disabled, only folder watcher is running")
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            print("Shutting down...")
