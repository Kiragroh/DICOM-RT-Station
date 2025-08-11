# DICOMnode - DICOM Receiver and Folder Watcher

A standalone DICOM receiver and folder watcher service that can receive DICOM files over the network and forward files from a watched folder to target DICOM nodes.

## Features

- **DICOM Network Receiver**: Receives DICOM files via DICOM C-STORE operations
- **Folder Watcher**: Monitors a folder for new DICOM files and forwards them automatically
- **Enhanced MR Conversion**: Optionally converts Enhanced MR DICOM to Standard MR using emf2sf (see setup below)
- **Configurable Security**: Trust-based AE title filtering
- **Automatic Organization**: Creates subfolders based on study/series information
- **Batch Processing**: Sends files in optimal modality order (CT → RTSTRUCT → RTPLAN → RTDOSE)
- **Error Handling**: Failed files are moved to a dedicated error folder with logging

## How It Works

### DICOM Receiver
1. Listens on configured port for incoming DICOM connections
2. Validates sender AE title against trusted list
3. Processes and saves received files to appropriate directories
4. Converts Enhanced MR to Standard MR if emf2sf is available
5. Organizes files by patient, study, and series

### Folder Watcher
1. Monitors watch folder for new DICOM files
2. Groups files by patient/study folders
3. Waits for folder inactivity (configurable timeout)
4. Sends files in optimal order: CT → RTSTRUCT → RTPLAN → RTDOSE
5. Deletes successfully sent files
6. Moves failed files to error folder with detailed logs

### Security
- Only accepts connections from trusted AE titles
- Configurable per-AE directory mapping
- Failed authentication attempts are logged and rejected

## Integration with DICOM RT Station

This DICOMnode can be used as a source for the main DICOM RT Station application:
- Configure DICOMnode to forward files to DICOM RT Station
- Use DICOM RT Station's forwarding rules to route files from this node
- Set up appropriate AE title mappings for seamless integration

## Configuration

All settings are managed through `config.ini`. Copy `config.example.ini` to `config.ini` and modify as needed.

### Configuration Sections

#### [General]
```ini
ae_title = FOLLOW              # AE title for this DICOM node
window_title = FOLLOW-Deamon   # Console window title
```

#### [Network]
```ini
receive_port = 1335           # Port for receiving DICOM connections
server_ip = 192.168.178.55    # IP address to bind the server to
```

#### [Directories]
```ini
destination_dir = D:\IncomingFollow  # Base directory for received files
```

#### [Security]
```ini
# Comma-separated list of trusted AE titles
trusted_ae_titles = MRMULTI,TR_SEND,VARIAN,MYQASRS,RTPLANNING,TRUSTED_AE_2
```

#### [AE_Mappings]
```ini
# Map specific AE titles to custom directories
MYQASRS = \\network\iba_SRS
zCTSCANNER = incoming_ctscanner
```

#### [Tools]
```ini
# Enhanced MR splitting: See https://github.com/Kiragroh/Split-MultiFrame-DICOM for setup
# Leave empty or set to non-existent path to disable Enhanced MR conversion
emf2sf_path = C:\dcm4che\bin  # Path to emf2sf tool for Enhanced MR conversion
```

#### [FolderWatcher]
```ini
watch_folder = D:\Incoming2Send      # Folder to watch for outgoing files
target_aet = DICOM-RT-KAFFEE        # Target AE title for forwarding
target_ip = 192.168.178.55          # Target IP for forwarding
target_port = 1334                  # Target port for forwarding
max_workers = 4                     # Thread pool size
heartbeat_interval = 120            # Heartbeat interval in seconds
process_timer_interval = 10         # Processing timer interval in seconds
inactivity_timeout = 1              # Folder inactivity timeout in seconds
```

#### [Logging]
```ini
verbose = true                      # Enable verbose logging
```

## Enhanced MR Setup (Optional)

The DICOMnode can optionally convert Enhanced MR DICOM files to Standard MR format using the emf2sf tool. This feature is **completely optional** and the script works perfectly without it.

### To Enable Enhanced MR Conversion:

1. **Install dcm4che toolkit** and emf2sf tool
2. **See detailed setup instructions**: https://github.com/Kiragroh/Split-MultiFrame-DICOM
3. **Configure the path** in `config.ini`:
   ```ini
   [Tools]
   emf2sf_path = C:\dcm4che\bin
   ```

### To Disable Enhanced MR Conversion:

Simply leave the path empty or set it to a non-existent directory:
```ini
[Tools]
emf2sf_path = 
```

**Note**: When Enhanced MR conversion is disabled, Enhanced MR files are processed normally without conversion. The script handles missing tools gracefully and continues processing other DICOM types.

## Usage

### Basic Usage

1. **Configure the service**: Edit `config.ini` with your settings
2. **Start the service**:
   ```bash
   python DICOMnode.py
   ```

### Command Line Options

```bash
python DICOMnode.py [options]

Options:
  --watch-folder PATH    Override watch folder from config
  --no-receiver         Disable DICOM receiver, only run folder watcher
  --config PATH         Specify configuration file path
  -h, --help           Show help message
```

### Examples

**Run with custom watch folder:**
```bash
python DICOMnode.py --watch-folder "D:\CustomWatch"
```

**Run only folder watcher (no network receiver):**
```bash
python DICOMnode.py --no-receiver
```

**Use custom config file:**
```bash
python DICOMnode.py --config "custom_config.ini"
```

## Directory Structure

```
DICOMnode/
├── DICOMnode.py          # Main application
├── config.ini            # Configuration file (create from example)
├── config.example.ini    # Example configuration
└── README.md            # This file

# Runtime directories (created automatically):
D:\IncomingFollow/       # Base destination (configurable)
├── incoming/            # Default incoming directory
├── errors/              # Failed processing files
└── [AE_specific]/       # AE-specific directories (if configured)

D:\Incoming2Send/        # Watch folder (configurable)
├── [patient_folders]/   # Patient-specific folders
└── failed/              # Failed send attempts with logs
```

## Troubleshooting

### Common Issues

**Connection refused:**
- Check that the configured port is not in use
- Verify firewall settings allow connections on the specified port
- Ensure the IP address is correctly configured

**Files not being processed:**
- Check that the watch folder exists and is accessible
- Verify target DICOM node is reachable and accepting connections
- Review logs in the failed folder for error details

**Enhanced MR conversion fails:**
- Ensure emf2sf tool is installed and path is correct in config
- Check that dcm4che toolkit is properly installed

### Log Files

- **Console output**: Real-time status and processing information
- **Error logs**: Located in `[watch_folder]/failed/send_errors.log`
- **Failed files**: Moved to `[watch_folder]/failed/` with timestamps

### Configuration Validation

The application will display current configuration on startup:
```
Configuration loaded from: D:\path\to\config.ini
AE Title: FOLLOW
Server IP: 192.168.178.55
Receive Port: 1335
Destination Directory: D:\IncomingFollow
Watch Folder: D:\Incoming2Send
Trusted AE Titles: MRMULTI, TR_SEND, VARIAN, MYQASRS, RTPLANNING, TRUSTED_AE_2
```


