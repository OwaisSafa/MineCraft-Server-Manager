import subprocess
import time
import psutil
import logging
import requests
import os
import re  
import platform
import yaml
import shutil
from datetime import datetime
from typing import Dict, Optional, Union, List
import secrets
import getpass
import traceback
import functools
import concurrent.futures
import multiprocessing
import threading
import queue
import io

# Update config and log file paths at the top
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SERVER_DIR = os.path.join(BASE_DIR, 'mcserver')
CONFIG_DIR = os.path.join(SERVER_DIR, 'config')
LOGS_DIR = os.path.join(SERVER_DIR, 'logs')

# Ensure all directories exist
os.makedirs(SERVER_DIR, exist_ok=True)
os.makedirs(CONFIG_DIR, exist_ok=True)
os.makedirs(LOGS_DIR, exist_ok=True)

CONFIG_FILE = os.path.join(CONFIG_DIR, "server_config.yaml")
BACKUP_DIR = os.path.join(SERVER_DIR, "server_backups")
EULA_FILE = os.path.join(SERVER_DIR, "eula.txt")
SERVER_JAR = os.path.join(SERVER_DIR, "server.jar")

# Setup logging
logging.basicConfig(
    level=logging.INFO, 
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(os.path.join(LOGS_DIR, "minecraft_server.log")),
    ]
)
logger = logging.getLogger(__name__)

# Default configuration
DEFAULT_CONFIG = {
    "java": {
        "version": "17",
        "path": "/usr/bin/java"
    },
    "server": {
        "distribution": "paper",
        "version": "1.20.1",
        "port": 25565,
        "memory": {
            "min_percentage": 50,
            "max_percentage": 75
        }
    },
    "playit": {
        "enabled": True
    },
    "backup": {
        "enabled": True,
        "max_backups": 5
    }
}

def load_config() -> Dict:
    """Load or create configuration file."""
    try:
        if not os.path.exists(CONFIG_FILE):
            with open(CONFIG_FILE, 'w') as f:
                yaml.dump(DEFAULT_CONFIG, f)
            logger.info(f"Created default configuration file: {CONFIG_FILE}")
        
        with open(CONFIG_FILE, 'r') as f:
            config = yaml.safe_load(f)
        
        # Merge with default config to ensure all keys exist
        config = {**DEFAULT_CONFIG, **config}
        return config
    except Exception as e:
        logger.error(f"Error loading configuration: {e}")
        logger.warning("Using default configuration")
        return DEFAULT_CONFIG

def validate_config(config: Dict) -> bool:
    """
    Comprehensive configuration validation.
    
    Args:
        config (Dict): Configuration to validate
    
    Returns:
        bool: True if configuration is valid, False otherwise
    """
    try:
        # Validate Java configuration
        if 'java' not in config or 'version' not in config['java']:
            logger.error("Missing Java version in configuration")
            return False
        
        # Validate server configuration
        if 'server' not in config:
            logger.error("Missing server configuration")
            return False
        
        server_config = config['server']
        
        # Validate distribution
        if 'distribution' in server_config:
            try:
                sanitize_input(server_config['distribution'], 'distribution')
            except ValueError as e:
                logger.error(f"Invalid distribution: {e}")
                return False
        
        # Validate version
        if 'version' in server_config:
            try:
                sanitize_input(server_config['version'], 'version')
            except ValueError as e:
                logger.error(f"Invalid version: {e}")
                return False
        
        # Validate port
        if 'port' in server_config:
            try:
                sanitize_input(str(server_config['port']), 'port')
            except ValueError as e:
                logger.error(f"Invalid port: {e}")
                return False
        
        # Validate memory configuration
        if 'memory' in server_config:
            memory_config = server_config['memory']
            try:
                min_percentage = float(memory_config.get('min_percentage', 0))
                max_percentage = float(memory_config.get('max_percentage', 100))
                
                if not (0 <= min_percentage <= 100 and 0 <= max_percentage <= 100):
                    logger.error("Memory percentages must be between 0 and 100")
                    return False
                
                if min_percentage > max_percentage:
                    logger.error("Minimum memory percentage cannot be greater than maximum")
                    return False
            except ValueError:
                logger.error("Invalid memory percentage configuration")
                return False
        
        return True
    except Exception as e:
        logger.error(f"Configuration validation failed: {e}")
        return False

def create_backup(config: Dict):
    """Create a backup of the Minecraft server files."""
    if not config['backup']['enabled']:
        return

    try:
        # Create backup directory if it doesn't exist
        os.makedirs(BACKUP_DIR, exist_ok=True)

        # Generate backup filename with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_name = f"minecraft_backup_{timestamp}"
        backup_path = os.path.join(BACKUP_DIR, backup_name)

        # Copy server files
        shutil.copytree(".", backup_path, 
            ignore=shutil.ignore_patterns(
                'server_backups', '.git', '__pycache__', '*.log'
            )
        )

        # Manage backup rotation
        backups = sorted(os.listdir(BACKUP_DIR))
        while len(backups) > config['backup']['max_backups']:
            oldest_backup = os.path.join(BACKUP_DIR, backups[0])
            shutil.rmtree(oldest_backup)
            backups.pop(0)

        logger.info(f"Server backup created: {backup_name}")
    except Exception as e:
        logger.error(f"Backup creation failed: {e}")

def restore_backup(backup_name: Optional[str] = None):
    """Restore a specific backup or the latest backup."""
    try:
        if not os.path.exists(BACKUP_DIR):
            logger.error("No backups found.")
            return False

        backups = sorted(os.listdir(BACKUP_DIR))
        if not backups:
            logger.error("No backups available.")
            return False

        # Use the specified backup or the latest
        if backup_name:
            if backup_name not in backups:
                logger.error(f"Backup {backup_name} not found.")
                return False
            selected_backup = backup_name
        else:
            selected_backup = backups[-1]

        backup_path = os.path.join(BACKUP_DIR, selected_backup)

        # Stop any running server processes
        # (You might want to add a method to stop server processes here)

        # Restore files
        for item in os.listdir(backup_path):
            source = os.path.join(backup_path, item)
            destination = os.path.join(".", item)
            
            if os.path.isdir(source):
                shutil.copytree(source, destination, dirs_exist_ok=True)
            else:
                shutil.copy2(source, destination)

        logger.info(f"Successfully restored backup: {selected_backup}")
        return True
    except Exception as e:
        logger.error(f"Backup restoration failed: {e}")
        return False

def install_java(java_version):
    """
    Install Java using default system package manager.
    
    Args:
        java_version (str): Java version to install (e.g., '17', '16', '11', '8')
    
    Raises:
        Exception: If Java installation fails
    """
    import subprocess
    
    logger.info(f"Attempting to install Java {java_version}")
    
    try:
        # Update package lists
        update_result = subprocess.run(
            ['sudo', 'apt', 'update'], 
            capture_output=True, text=True, check=True
        )
        logger.info("Package lists updated successfully.")
        
        # Map Java version to package name
        java_packages = {
            '8': 'openjdk-8-jdk',
            '11': 'openjdk-11-jdk',
            '16': 'openjdk-16-jdk',
            '17': 'openjdk-17-jdk',
            '21': 'openjdk-21-jdk'
        }
        
        if java_version not in java_packages:
            logger.warning(f"Unsupported Java version {java_version}, defaulting to Java 17")
            java_version = '17'
            
        package = java_packages[java_version]
        
        # Install specific Java version
        install_result = subprocess.run(
            ['sudo', 'apt', 'install', '-y', package], 
            capture_output=True, text=True, check=True
        )
        
        logger.info(f"Java {java_version} installed successfully.")
        
        # Set this version as the default
        subprocess.run(['sudo', 'update-alternatives', '--set', 'java', f'/usr/lib/jvm/java-{java_version}-openjdk-amd64/bin/java'],
                      capture_output=True, text=True, check=True)
        
    except subprocess.CalledProcessError as e:
        logger.error(f"Java installation failed: {e.stderr}")
        raise Exception(f"Failed to install Java: {e.stderr}")

def setup_server_directory():
    """Set up the server directory structure."""
    try:
        # Create main directories
        os.makedirs(SERVER_DIR, exist_ok=True)
        os.makedirs(os.path.join(SERVER_DIR, "logs"), exist_ok=True)
        os.makedirs(os.path.join(SERVER_DIR, "world"), exist_ok=True)
        os.makedirs(os.path.join(SERVER_DIR, "plugins"), exist_ok=True)
        os.makedirs(CONFIG_DIR, exist_ok=True)
        os.makedirs(LOGS_DIR, exist_ok=True)
        
        # Change to server directory
        os.chdir(SERVER_DIR)
        
        # Create default server.properties if it doesn't exist
        properties_path = os.path.join(SERVER_DIR, "server.properties")
        if not os.path.exists(properties_path):
            default_properties = {
                'enable-jmx-monitoring': 'false',
                'rcon.port': '25575',
                'gamemode': 'survival',
                'enable-command-block': 'false',
                'enable-query': 'false',
                'level-name': 'world',
                'motd': 'A Minecraft Server',
                'query.port': '25565',
                'pvp': 'true',
                'difficulty': 'easy',
                'network-compression-threshold': '256',
                'require-resource-pack': 'false',
                'max-tick-time': '60000',
                'use-native-transport': 'true',
                'max-players': '20',
                'online-mode': 'false',
                'enable-status': 'true',
                'allow-flight': 'false',
                'broadcast-rcon-to-ops': 'true',
                'view-distance': '10',
                'server-ip': '',
                'resource-pack-prompt': '',
                'allow-nether': 'true',
                'server-port': '25565',
                'enable-rcon': 'false',
                'sync-chunk-writes': 'true',
                'op-permission-level': '4',
                'prevent-proxy-connections': 'false',
                'hide-online-players': 'false',
                'resource-pack': '',
                'entity-broadcast-range-percentage': '100',
                'simulation-distance': '10',
                'rcon.password': '',
                'player-idle-timeout': '0',
                'force-gamemode': 'false',
                'rate-limit': '0',
                'hardcore': 'false',
                'white-list': 'false',
                'broadcast-console-to-ops': 'true',
                'spawn-npcs': 'true',
                'spawn-animals': 'true',
                'generate-structures': 'true',
                'max-build-height': '256'
            }
            with open(properties_path, 'w') as f:
                for key, value in default_properties.items():
                    f.write(f'{key}={value}\n')
            logger.info("Created default server.properties")
        
        logger.info("Server directory setup complete")
        return True
    except Exception as e:
        logger.error(f"Failed to setup server directory: {str(e)}")
        return False

def get_required_java_version(minecraft_version: str) -> str:
    """
    Get the required Java version for a specific Minecraft version.
    
    Args:
        minecraft_version (str): Minecraft version (e.g., '1.20.4')
    
    Returns:
        str: Required Java version
    """
    version_parts = minecraft_version.split('.')
    if len(version_parts) < 2:
        return '17'  # Default to Java 17 if version format is invalid
        
    major = int(version_parts[1])
    
    if major >= 17:  # Minecraft 1.17+
        return '17'
    elif major >= 16:  # Minecraft 1.16
        return '16'
    elif major >= 12:  # Minecraft 1.12 - 1.15
        return '11'
    else:  # Older versions
        return '8'

def calculate_memory_allocation() -> tuple:
    """
    Calculate optimal memory allocation for the Minecraft server based on system resources.
    
    Returns:
        tuple: (min_memory, max_memory) in GB
    """
    try:
        # Get total system memory in GB
        total_memory = psutil.virtual_memory().total / (1024 * 1024 * 1024)
        
        # Calculate memory allocation
        if total_memory <= 4:  # 4GB or less
            min_memory = max(1, total_memory * 0.5)  # 50% of total memory
            max_memory = max(1.5, total_memory * 0.75)  # 75% of total memory
        elif total_memory <= 8:  # 4GB-8GB
            min_memory = max(2, total_memory * 0.4)  # 40% of total memory
            max_memory = max(3, total_memory * 0.6)  # 60% of total memory
        elif total_memory <= 16:  # 8GB-16GB
            min_memory = 4  # Fixed 4GB minimum
            max_memory = max(6, total_memory * 0.5)  # 50% of total memory
        else:  # More than 16GB
            min_memory = 4  # Fixed 4GB minimum
            max_memory = 8  # Fixed 8GB maximum
        
        # Round to 1 decimal place
        min_memory = round(min_memory, 1)
        max_memory = round(max_memory, 1)
        
        logger.info(f"Calculated memory allocation: {min_memory}GB - {max_memory}GB")
        return min_memory, max_memory
    
    except Exception as e:
        logger.error(f"Error calculating memory allocation: {e}")
        # Return safe default values
        return 2, 4

def create_startup_script():
    """Create a startup script with optimized JVM flags."""
    # Calculate memory allocation
    min_memory, max_memory = calculate_memory_allocation()
    
    script_content = f"""#!/bin/bash
# Optimized Minecraft Server Startup Script

# Memory settings (calculated based on system resources)
MIN_RAM="{min_memory}G"
MAX_RAM="{max_memory}G"

# Optimized JVM flags
JVM_OPTS="-XX:+UseG1GC -XX:+ParallelRefProcEnabled -XX:MaxGCPauseMillis=200"
JVM_OPTS="$JVM_OPTS -XX:+UnlockExperimentalVMOptions -XX:+DisableExplicitGC"
JVM_OPTS="$JVM_OPTS -XX:+AlwaysPreTouch -XX:G1NewSizePercent=30"
JVM_OPTS="$JVM_OPTS -XX:G1MaxNewSizePercent=40 -XX:G1HeapRegionSize=8M"
JVM_OPTS="$JVM_OPTS -XX:G1ReservePercent=20 -XX:G1HeapWastePercent=5"
JVM_OPTS="$JVM_OPTS -XX:G1MixedGCCountTarget=4 -XX:InitiatingHeapOccupancyPercent=15"
JVM_OPTS="$JVM_OPTS -XX:G1MixedGCLiveThresholdPercent=90"
JVM_OPTS="$JVM_OPTS -XX:G1RSetUpdatingPauseTimePercent=5 -XX:SurvivorRatio=32"
JVM_OPTS="$JVM_OPTS -XX:+PerfDisableSharedMem -XX:MaxTenuringThreshold=1"
JVM_OPTS="$JVM_OPTS -Dusing.aikars.flags=https://mcflags.emc.gs"
JVM_OPTS="$JVM_OPTS -Daikars.new.flags=true"

# Run the server
java -Xms$MIN_RAM -Xmx$MAX_RAM $JVM_OPTS -jar server.jar nogui
"""
    
    script_path = os.path.join(SERVER_DIR, "start.sh")
    try:
        with open(script_path, 'w') as f:
            f.write(script_content)
        os.chmod(script_path, 0o755)  # Make executable
        logger.info(f"Created optimized startup script: start.sh (Memory: {min_memory}G-{max_memory}G)")
    except Exception as e:
        logger.error(f"Failed to create startup script: {e}")

def download_server_jar(distribution, version, config):
    """Download the server jar file for the specified distribution and version."""
    try:
        # Create server directory first and change to it
        os.makedirs(SERVER_DIR, exist_ok=True)
        original_dir = os.getcwd()
        os.chdir(SERVER_DIR)
        
        # Update Java version requirement based on Minecraft version
        required_java = get_required_java_version(version)
        config['java']['version'] = required_java
        logger.info(f"Required Java version for Minecraft {version}: Java {required_java}")
        
        logger.info(f"Starting download for {distribution} version {version}...")
        
        try:
            if distribution == "paper":
                # First, get the latest build number
                builds_url = f"https://api.papermc.io/v2/projects/paper/versions/{version}/builds"
                response = requests.get(builds_url)
                response.raise_for_status()
                builds_data = response.json()
                
                if not builds_data.get('builds'):
                    raise ValueError(f"No builds found for Paper version {version}")
                
                # Get the latest build number
                latest_build = builds_data['builds'][-1]['build']
                
                # Construct download URL with the latest build number
                download_url = f"https://api.papermc.io/v2/projects/paper/versions/{version}/builds/{latest_build}/downloads/paper-{version}-{latest_build}.jar"
                
                logger.info(f"Downloading Paper server build #{latest_build}")
                response = requests.get(download_url, stream=True)
                response.raise_for_status()
            else:
                distributions = {
                    "vanilla": f"https://launcher.mojang.com/v1/objects/{version}/server.jar",
                    "purpur": f"https://api.purpurmc.org/v2/purpur/{version}/latest/download",
                    "fabric": f"https://meta.fabricmc.net/v2/versions/loader/{version}/0.11.2/0.11.2/server/jar",
                    "forge": f"https://files.minecraftforge.net/maven/net/minecraftforge/forge/{version}/forge-{version}-installer.jar"
                }

                if distribution not in distributions:
                    raise ValueError(f"Unsupported distribution: {distribution}")

                response = requests.get(distributions[distribution], stream=True)
                response.raise_for_status()
            
            # Save to server directory
            total_size = int(response.headers.get('content-length', 0))
            block_size = 8192
            progress = 0
            
            with open("server.jar", "wb") as f:
                for chunk in response.iter_content(chunk_size=block_size):
                    if chunk:
                        f.write(chunk)
                        progress += len(chunk)
                        if total_size:
                            percentage = (progress / total_size) * 100
                            logger.info(f"Download progress: {percentage:.1f}%")
            
            # Create startup script with optimized settings
            create_startup_script()
            
            logger.info(f"Successfully downloaded {distribution} server version {version}")
            return True
            
        finally:
            # Always change back to original directory
            os.chdir(original_dir)
            
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to download server: {str(e)}")
        return False
    except Exception as e:
        logger.error(f"Failed to download server: {str(e)}")
        return False

def start_playit():
    """
    Start Playit.gg tunnel with direct output capture.
    
    Returns:
        subprocess.Popen: Playit.gg process
    """
    try:
        # Create logs directory if it doesn't exist
        os.makedirs(LOGS_DIR, exist_ok=True)
        
        # Define log file path
        playit_log = os.path.join(LOGS_DIR, 'playit.log')
        
        # Start Playit.gg process and redirect output to log file
        with open(playit_log, 'w') as log_file:
            process = subprocess.Popen(
                'playit',
                stdout=log_file,
                stderr=subprocess.STDOUT,
                shell=True,  # Run in shell to capture all output
                universal_newlines=True
            )
        
        # Wait a bit to see if process starts successfully
        time.sleep(5)
        
        if process.poll() is not None:
            raise Exception("Playit.gg failed to start")
        
        logger.info("Playit.gg tunnel started")
        return process
        
    except Exception as e:
        logger.error(f"Failed to start Playit.gg: {str(e)}")
        raise

def get_tunnel_address():
    """
    Get the tunnel address or claim URL from playit.log.
    
    Returns:
        str: Tunnel address or claim URL if found, None otherwise
    """
    playit_log_path = os.path.join(LOGS_DIR, "playit.log")
    max_attempts = 15
    attempt = 0
    
    try:
        while attempt < max_attempts:
            try:
                # Try to read the log file
                with open(playit_log_path, 'r') as f:
                    content = f.read()
                    logger.debug(f"Log content: {content}")  # Debug log content
                    
                    # First check for claim URL
                    claim_match = re.search(r'Visit link to setup (https://playit\.gg/claim/[a-zA-Z0-9]+)', content)
                    if claim_match:
                        claim_url = claim_match.group(1)
                        logger.info(f"Found Playit.gg claim URL: {claim_url}")
                        return f"CLAIM_URL:{claim_url}"
                    
                    # Then check for tunnel address
                    tunnel_patterns = [
                        r'tunnel address: ([^\s]+)',
                        r'Server Address: ([^\s]+)',
                        r'minecraft server: ([^\s]+)',
                        r'([a-zA-Z0-9-]+\.playit\.gg)',
                        r'([a-zA-Z0-9-]+\.joinmc\.link)'
                    ]
                    
                    for pattern in tunnel_patterns:
                        match = re.search(pattern, content, re.MULTILINE | re.IGNORECASE)
                        if match:
                            tunnel_address = match.group(1)
                            logger.info(f"Found tunnel address: {tunnel_address}")
                            return f"TUNNEL:{tunnel_address}"
            except FileNotFoundError:
                logger.debug(f"Log file not found yet (attempt {attempt})")
            except Exception as e:
                logger.error(f"Error reading log file: {e}")
            
            # Wait and retry
            time.sleep(2)
            attempt += 1
        
        # After all attempts, try to provide more debug information
        if os.path.exists(playit_log_path):
            try:
                with open(playit_log_path, 'r') as f:
                    content = f.read()
                    logger.debug("Final log content:")
                    logger.debug(content)
                    
                    if not content.strip():
                        logger.error("Log file is empty")
                    elif "secret key is not valid" in content:
                        logger.error("Playit.gg secret key is invalid. Please configure your secret key.")
                    elif "FailedToConnect" in content:
                        logger.error("Failed to connect to Playit.gg. Check your internet connection.")
                    else:
                        logger.warning("Could not find tunnel address in log.")
            except Exception as e:
                logger.error(f"Error reading final log content: {e}")
        else:
            logger.error(f"Log file never created at: {playit_log_path}")
        
        return None
        
    except Exception as e:
        logger.error(f"Error retrieving tunnel address: {e}")
        return None

def start_server(config: Dict) -> subprocess.Popen:
    """
    Start the Minecraft server with optimized configuration.
    
    Args:
        config (dict): Server configuration
    
    Returns:
        subprocess.Popen: Server process
    """
    try:
        # Calculate memory allocation
        min_memory, max_memory = calculate_memory_allocation()
        
        # Get Java command with optimized flags
        java_cmd = [
            config['java']['path'],
            f"-Xms{int(min_memory)}G",
            f"-Xmx{int(max_memory)}G"
        ]
        
        # Add JVM flags if configured
        if 'java_flags' in config and 'startup' in config['java_flags']:
            java_cmd.extend(config['java_flags']['startup'])
        
        # Add server jar
        java_cmd.extend(["-jar", "server.jar", "nogui"])
        
        # Start server process
        process = subprocess.Popen(
            java_cmd,
            cwd=SERVER_DIR,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True
        )
        
        logger.info("Minecraft server started")
        return process
        
    except Exception as e:
        logger.error(f"Failed to start server: {str(e)}")
        return None

def check_playit_installed():
    """Check if Playit.gg is installed, and install if not."""
    logger.info("Checking if Playit.gg is installed...")
    try:
        subprocess.run(["playit", "--version"], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        logger.info("Playit.gg is already installed.")
        return True
    except FileNotFoundError:
        logger.error("Playit.gg is not installed. Attempting to install it...")
        install_playit()
    except subprocess.CalledProcessError:
        logger.error("An error occurred while checking Playit.gg version.")
        install_playit()
    return True

def install_playit():
    """Install Playit.gg using the PPA method (for Ubuntu/Debian)."""
    try:
        # Only add repository and update if not already configured
        if not os.path.exists("/etc/apt/sources.list.d/playit-cloud.list"):
            # Add the Playit PPA key
            subprocess.run("curl -SsL https://playit-cloud.github.io/ppa/key.gpg | gpg --dearmor | sudo tee /etc/apt/trusted.gpg.d/playit.gpg >/dev/null", shell=True, check=True)
            
            # Add Playit repository
            subprocess.run('echo "deb [signed-by=/etc/apt/trusted.gpg.d/playit.gpg] https://playit-cloud.github.io/ppa/data ./" | sudo tee /etc/apt/sources.list.d/playit-cloud.list', shell=True, check=True)

            # Update package list only if we added new repository
            subprocess.run("sudo apt update", shell=True, check=True)
        
        # Install Playit.gg
        subprocess.run("sudo apt install -y playit", shell=True, check=True)
        logger.info("Playit.gg installed successfully.")
        
    except subprocess.CalledProcessError as e:
        logger.error(f"Error installing Playit.gg: {e}")
        exit(1)

def stop_process(process):
    """Forcefully stop a process."""
    try:
        if process and process.poll() is None:
            process.terminate()
            process.wait(timeout=5)
        else:
            logger.warning(f"No process found with PID {process.pid}")
    except psutil.NoSuchProcess:
        logger.warning(f"Process with PID {process.pid} does not exist.")
    except psutil.AccessDenied:
        logger.error(f"Access denied when trying to terminate process {process.pid}.")
    except psutil.TimeoutExpired:
        logger.error(f"Timeout expired while trying to stop process {process.pid}.")

def check_system_requirements(config):
    """
    Check if the system meets the minimum requirements for running a Minecraft server.
    Automatically attempts to install Java if not found.
    
    Args:
        config (dict): Server configuration
    
    Returns:
        bool: True if system requirements are met, False otherwise
    """
    try:
        # Check system resources
        memory = psutil.virtual_memory()
        total_memory_gb = memory.total / (1024 ** 3)
        available_memory_gb = memory.available / (1024 ** 3)
        physical_cores = psutil.cpu_count(logical=False)
        logical_cores = psutil.cpu_count(logical=True)
        
        # Log system information once
        logger.info(f"Operating System: {platform.platform()}")
        logger.info(f"Python Version: {platform.python_version()}")
        logger.info(f"Total Memory: {total_memory_gb:.2f} GB")
        logger.info(f"Available Memory: {available_memory_gb:.2f} GB")
        logger.info(f"Physical CPU Cores: {physical_cores}")
        logger.info(f"Logical CPU Cores: {logical_cores}")
        
        # Check Java installation
        java_path = shutil.which('java')
        if not java_path:
            logger.warning("Java not found. Attempting automatic installation...")
            try:
                install_java(config.get('java_version', '17'))
            except Exception as e:
                logger.error(f"Automatic Java installation failed: {e}")
                return False
        
        # Verify Java version
        java_version_output = subprocess.check_output(['java', '-version'], stderr=subprocess.STDOUT, text=True)
        java_version = java_version_output.split()[2].strip('"')
        logger.info(f"Java version detected: {java_version}")
        
        # Warning for low resources
        if physical_cores < config.get('min_cpu_cores', 2):
            logger.warning(f"Low number of CPU cores: {physical_cores}. Server performance may be impacted.")
        
        if total_memory_gb < config.get('min_memory', 4):
            logger.warning(f"Low system memory: {total_memory_gb:.2f} GB. Server performance may be impacted.")
        
        logger.info("System requirements check passed.")
        return True
        
    except Exception as e:
        logger.error(f"Error checking system requirements: {e}")
        return False

def sanitize_input(input_string: str, input_type: str = 'generic') -> str:
    """
    Sanitize and validate user input based on type.
    
    Args:
        input_string (str): Input to sanitize
        input_type (str): Type of input for specific validation
    
    Returns:
        str: Sanitized input
    
    Raises:
        ValueError: If input is invalid
    """
    if not input_string:
        raise ValueError("Input cannot be empty")
    
    # Remove leading/trailing whitespace
    input_string = input_string.strip()
    
    # Type-specific validations
    if input_type == 'version':
        # Validate version format (e.g., 1.20.1)
        if not re.match(r'^\d+\.\d+(\.\d+)?$', input_string):
            raise ValueError("Invalid version format")
    
    elif input_type == 'distribution':
        # Validate distribution
        valid_distributions = ['vanilla', 'paper', 'purpur', 'fabric', 'forge']
        if input_string.lower() not in valid_distributions:
            raise ValueError(f"Invalid distribution. Choose from: {', '.join(valid_distributions)}")
    
    elif input_type == 'memory_percentage':
        try:
            percentage = float(input_string)
            if not (0 <= percentage <= 100):
                raise ValueError("Memory percentage must be between 0 and 100")
        except ValueError:
            raise ValueError("Memory percentage must be a number")
    
    elif input_type == 'port':
        try:
            port = int(input_string)
            if not (1024 <= port <= 65535):
                raise ValueError("Port must be between 1024 and 65535")
        except ValueError:
            raise ValueError("Port must be a valid integer")
    
    # Prevent potential command injection
    if any(char in input_string for char in ['&', '|', ';', '$', '`']):
        raise ValueError("Input contains potentially dangerous characters")
    
    return input_string

def secure_config_file(config_path: str):
    """
    Set secure permissions for configuration file.
    
    Args:
        config_path (str): Path to the configuration file
    """
    try:
        # Set read/write permissions only for the owner
        os.chmod(config_path, 0o600)
        logger.info(f"Set secure permissions for {config_path}")
    except Exception as e:
        logger.error(f"Could not set secure permissions: {e}")

def generate_secure_token(length: int = 32) -> str:
    """
    Generate a cryptographically secure random token.
    
    Args:
        length (int): Length of the token
    
    Returns:
        str: Secure random token
    """
    return secrets.token_hex(length // 2)

def mask_sensitive_config(config: Dict) -> Dict:
    """
    Create a copy of config with sensitive information masked.
    
    Args:
        config (Dict): Original configuration
    
    Returns:
        Dict: Configuration with sensitive data masked
    """
    masked_config = config.copy()
    if 'java' in masked_config and 'path' in masked_config['java']:
        masked_config['java']['path'] = '***'
    return masked_config

def retry(max_attempts: int = 3, delay: int = 2, backoff: int = 2):
    """
    Decorator for retrying a function with exponential backoff.
    
    Args:
        max_attempts (int): Maximum number of retry attempts
        delay (int): Initial delay between retries
        backoff (int): Multiplier for delay between attempts
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            attempts = 0
            current_delay = delay
            
            while attempts < max_attempts:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    attempts += 1
                    if attempts == max_attempts:
                        logger.error(f"Function {func.__name__} failed after {max_attempts} attempts")
                        raise
                    
                    logger.warning(f"Attempt {attempts} failed: {e}")
                    logger.debug(traceback.format_exc())
                    
                    time.sleep(current_delay)
                    current_delay *= backoff
            
            raise RuntimeError(f"Function {func.__name__} failed after {max_attempts} attempts")
        return wrapper
    return decorator

def log_exception(func):
    """
    Decorator to log exceptions with detailed traceback.
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            logger.error(f"Exception in {func.__name__}: {e}")
            logger.error(traceback.format_exc())
            raise
    return wrapper

class OperationContext:
    """
    Context manager for tracking and logging complex operations.
    """
    def __init__(self, operation_name: str):
        self.operation_name = operation_name
        self.start_time = None
    
    def __enter__(self):
        self.start_time = time.time()
        logger.info(f"Starting operation: {self.operation_name}")
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        end_time = time.time()
        duration = end_time - self.start_time
        
        if exc_type is None:
            logger.info(f"Operation {self.operation_name} completed successfully in {duration:.2f} seconds")
        else:
            logger.error(f"Operation {self.operation_name} failed after {duration:.2f} seconds")
            logger.error(f"Exception: {exc_val}")
            return False  # Propagate the exception

def advanced_error_handler(func):
    """
    Advanced error handling decorator with comprehensive logging.
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            with OperationContext(func.__name__):
                return func(*args, **kwargs)
        except Exception as e:
            logger.critical(f"Critical error in {func.__name__}: {e}")
            logger.critical(traceback.format_exc())
            raise
    return wrapper

def parallel_execute(func, items, max_workers=None):
    """
    Execute a function in parallel for multiple items.
    
    Args:
        func (callable): Function to execute
        items (list): List of items to process
        max_workers (int, optional): Maximum number of worker threads
    
    Returns:
        list: Results of parallel execution
    """
    if max_workers is None:
        max_workers = min(len(items), multiprocessing.cpu_count() * 2)
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        results = list(executor.map(func, items))
    
    return results

def cached_method(timeout=300):
    """
    Decorator for caching method results with timeout.
    
    Args:
        timeout (int): Cache expiration time in seconds
    """
    def decorator(func):
        cache = {}
        
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            key = str(args) + str(kwargs)
            current_time = time.time()
            
            if key in cache:
                result, timestamp = cache[key]
                if current_time - timestamp < timeout:
                    return result
            
            result = func(*args, **kwargs)
            cache[key] = (result, current_time)
            
            # Limit cache size
            if len(cache) > 100:
                oldest_key = min(cache, key=lambda k: cache[k][1])
                del cache[oldest_key]
            
            return result
        return wrapper
    return decorator

class PerformanceOptimizer:
    """
    Performance optimization utility for server management.
    """
    @staticmethod
    def memory_efficient_generator(large_list, chunk_size=1000):
        """
        Create a memory-efficient generator for processing large lists.
        
        Args:
            large_list (list): Large list to process
            chunk_size (int): Size of chunks to yield
        
        Yields:
            list: Chunks of the original list
        """
        for i in range(0, len(large_list), chunk_size):
            yield large_list[i:i + chunk_size]

class ServerPerformanceMonitor:
    def __init__(self):
        self.metrics = {
            'cpu_usage': [],
            'memory_usage': [],
            'tps': [],  # Ticks per second
            'player_count': []
        }
        self.monitoring = False
        self._monitor_thread = None

    def start_monitoring(self):
        self.monitoring = True
        self._monitor_thread = threading.Thread(target=self._monitor_loop)
        self._monitor_thread.daemon = True
        self._monitor_thread.start()

    def stop_monitoring(self):
        self.monitoring = False
        if self._monitor_thread:
            self._monitor_thread.join()

    def _monitor_loop(self):
        while self.monitoring:
            self._collect_metrics()
            time.sleep(60)  # Collect metrics every minute

    def _collect_metrics(self):
        process = psutil.Process()
        self.metrics['cpu_usage'].append(process.cpu_percent())
        self.metrics['memory_usage'].append(process.memory_percent())
        # Keep only last hour of metrics
        max_entries = 60
        for key in self.metrics:
            self.metrics[key] = self.metrics[key][-max_entries:]

    def get_performance_report(self):
        return {
            'avg_cpu': sum(self.metrics['cpu_usage']) / len(self.metrics['cpu_usage']) if self.metrics['cpu_usage'] else 0,
            'avg_memory': sum(self.metrics['memory_usage']) / len(self.metrics['memory_usage']) if self.metrics['memory_usage'] else 0,
            'peak_cpu': max(self.metrics['cpu_usage']) if self.metrics['cpu_usage'] else 0,
            'peak_memory': max(self.metrics['memory_usage']) if self.metrics['memory_usage'] else 0
        }

def optimize_server_performance(config: Dict) -> Dict:
    """
    Apply comprehensive performance optimizations to server configuration.
    
    Args:
        config (dict): Server configuration
    
    Returns:
        dict: Optimized configuration
    """
    # Get system info
    total_memory = psutil.virtual_memory().total
    cpu_count = psutil.cpu_count()
    
    # Calculate optimal thread count for garbage collection
    optimal_gc_threads = min(cpu_count, 4)  # Usually 4 is the sweet spot
    
    # Calculate optimal memory allocation
    min_memory, max_memory = calculate_memory_allocation()
    
    # JVM optimization flags
    jvm_flags = [
        f"-Xms{int(min_memory)}G",
        f"-Xmx{int(max_memory)}G",
        "-XX:+UseG1GC",  # Use G1 Garbage Collector
        f"-XX:ParallelGCThreads={optimal_gc_threads}",
        "-XX:+ParallelRefProcEnabled",
        "-XX:MaxGCPauseMillis=200",
        "-XX:+UnlockExperimentalVMOptions",
        "-XX:+DisableExplicitGC",
        "-XX:+AlwaysPreTouch",
        "-XX:G1NewSizePercent=30",
        "-XX:G1MaxNewSizePercent=40",
        "-XX:G1HeapRegionSize=8M",
        "-XX:G1ReservePercent=20",
        "-XX:G1HeapWastePercent=5",
        "-XX:G1MixedGCCountTarget=4",
        "-XX:InitiatingHeapOccupancyPercent=15",
        "-XX:G1MixedGCLiveThresholdPercent=90",
        "-XX:G1RSetUpdatingPauseTimePercent=5",
        "-XX:SurvivorRatio=32",
        "-XX:+PerfDisableSharedMem",
        "-XX:MaxTenuringThreshold=1",
        "-Dusing.aikars.flags=https://mcflags.emc.gs",
        "-Daikars.new.flags=true",
    ]
    
    # Update configuration
    config['server']['memory'] = {
        'min_memory': min_memory,
        'max_memory': max_memory
    }
    config['server']['jvm_flags'] = jvm_flags
    
    # Add performance monitoring settings
    config['performance'] = {
        'monitoring_enabled': True,
        'monitoring_interval': 60,  # seconds
        'alert_cpu_threshold': 90,  # percentage
        'alert_memory_threshold': 85,  # percentage
        'auto_restart_on_low_tps': True,
        'min_tps_threshold': 15
    }
    
    return config

def optimize_server_startup(config):
    """
    Optimize server startup process.
    
    Args:
        config (dict): Server configuration
    
    Returns:
        dict: Optimized configuration
    """
    # Get CPU count
    cpu_count = os.cpu_count() or 2
    
    # Calculate optimal thread count for garbage collection
    optimal_gc_threads = min(cpu_count, 4)  # Usually 4 is the sweet spot
    
    # Calculate optimal memory allocation
    min_memory, max_memory = calculate_memory_allocation()
    
    # JVM optimization flags
    jvm_flags = [
        f"-Xms{int(min_memory)}G",
        f"-Xmx{int(max_memory)}G",
        "-XX:+UseG1GC",
        f"-XX:ParallelGCThreads={optimal_gc_threads}",
        f"-XX:ConcGCThreads={max(1, optimal_gc_threads // 2)}",
        "-XX:+DisableExplicitGC",
        "-XX:+AlwaysPreTouch",
        "-XX:+ParallelRefProcEnabled"
    ]
    
    # Update config with optimized settings
    if 'java_flags' not in config:
        config['java_flags'] = {}
    
    config['java_flags']['startup'] = jvm_flags
    config['server']['memory'] = {
        'min': min_memory,
        'max': max_memory
    }
    
    return config

def start_server_with_playit(config: Dict):
    """Start both Minecraft server and Playit.gg, and display server panel."""
    try:
        # Check if server.jar exists
        if not os.path.exists(os.path.join(SERVER_DIR, "server.jar")):
            logger.error(f"Server JAR not found: {SERVER_JAR}")
            logger.info("Please use option 1 to install the server first.")
            return

        # Change to server directory
        os.chdir(SERVER_DIR)
        
        # First check and start Playit.gg
        logger.info("Starting Playit.gg...")
        check_playit_installed()
        playit_process = start_playit()
        
        # Give Playit.gg a moment to initialize and get its address
        time.sleep(2)
        try:
            with open(os.path.join(LOGS_DIR, 'playit.log'), 'r') as f:
                playit_output = f.read()
                # Try to find the tunnel address
                import re
                tunnel_match = re.search(r'(\S+\.joinmc\.link)', playit_output)
                if tunnel_match:
                    tunnel_address = tunnel_match.group(1)
                    print(f"\nPlayit.gg Server Address: {tunnel_address}")
        except:
            pass
        
        # Clear the screen for clean MC console
        os.system('clear' if os.name == 'posix' else 'cls')
        
        # Then start Minecraft server with console output
        logger.info("Starting Minecraft server...")
        min_memory, max_memory = calculate_memory_allocation()
        java_cmd = [
            config['java']['path'],
            f"-Xms{int(min_memory)}G",
            f"-Xmx{int(max_memory)}G",
            *config.get('server', {}).get('jvm_flags', []),
            "-jar",
            SERVER_JAR,
            "nogui"
        ]
        
        minecraft_process = subprocess.Popen(
            java_cmd,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            universal_newlines=True
        )
        
        print("\n=== Minecraft Server Console ===")
        print(f"Server Address: {tunnel_address if 'tunnel_address' in locals() else 'Check playit.log for address'}")
        print("Type 'exit' to stop both servers")
        print("Enter Minecraft commands directly:\n")
        
        def read_output():
            while True:
                output = minecraft_process.stdout.readline()
                if output:
                    print(output.strip())
                if minecraft_process.poll() is not None:
                    break
        
        # Start thread to read server output
        output_thread = threading.Thread(target=read_output)
        output_thread.daemon = True
        output_thread.start()
        
        # Handle user input
        while True:
            try:
                command = input()
                if command.lower() == 'exit':
                    break
                if minecraft_process.poll() is None:
                    minecraft_process.stdin.write(command + '\n')
                    minecraft_process.stdin.flush()
            except EOFError:
                break
            except KeyboardInterrupt:
                break
        
        # Cleanup
        logger.info("Stopping servers...")
        if minecraft_process.poll() is None:
            minecraft_process.stdin.write('stop\n')
            minecraft_process.stdin.flush()
            minecraft_process.wait(timeout=10)
        stop_process(playit_process)
        print("\nServers stopped successfully!")
            
    except Exception as e:
        logger.error(f"Error starting servers: {e}")
        if 'minecraft_process' in locals():
            stop_process(minecraft_process)
        if 'playit_process' in locals():
            stop_process(playit_process)
    finally:
        # Clean up log file
        try:
            os.remove(os.path.join(LOGS_DIR, 'playit.log'))
        except:
            pass

def is_process_running(process):
    """Check if a process is running."""
    if process is None:
        return False
    return process.poll() is None

def check_server_files():
    """Check if necessary server files exist."""
    if not os.path.exists(SERVER_JAR):
        logger.error(f"Server JAR not found: {SERVER_JAR}")
        logger.info("Please use option 1 to install the server first.")
        return False
    return True

def get_tunnel_address():
    """
    Get the tunnel address or claim URL from playit.log.
    
    Returns:
        str: Tunnel address or claim URL if found, None otherwise
    """
    playit_log_path = os.path.join(LOGS_DIR, "playit.log")
    max_attempts = 15
    attempt = 0
    
    try:
        while attempt < max_attempts:
            if os.path.exists(playit_log_path):
                with open(playit_log_path, 'r') as f:
                    content = f.read()
                
                # First check for claim URL
                claim_match = re.search(r'Visit link to setup (https://playit\.gg/claim/[a-zA-Z0-9]+)', content)
                if claim_match:
                    claim_url = claim_match.group(1)
                    logger.info(f"Found Playit.gg claim URL: {claim_url}")
                    return f"CLAIM_URL:{claim_url}"
                
                # Then check for tunnel address
                tunnel_patterns = [
                    r'tunnel address: ([^\s]+)',
                    r'Server Address: ([^\s]+)',
                    r'minecraft server: ([^\s]+)',
                    r'([a-zA-Z0-9-]+\.playit\.gg)',
                    r'([a-zA-Z0-9-]+\.joinmc\.link)'
                ]
                
                for pattern in tunnel_patterns:
                    match = re.search(pattern, content, re.MULTILINE | re.IGNORECASE)
                    if match:
                        tunnel_address = match.group(1)
                        logger.info(f"Found tunnel address: {tunnel_address}")
                        return f"TUNNEL:{tunnel_address}"
            
            # Wait and retry
            time.sleep(2)
            attempt += 1
            logger.debug(f"Waiting for Playit.gg output (attempt {attempt}/{max_attempts})")
        
        # Check for specific error messages
        if os.path.exists(playit_log_path):
            with open(playit_log_path, 'r') as f:
                log_content = f.read()
                
                if "secret key is not valid" in log_content:
                    logger.error("Playit.gg secret key is invalid. Please configure your secret key.")
                elif "FailedToConnect" in log_content:
                    logger.error("Failed to connect to Playit.gg. Check your internet connection and firewall settings.")
                else:
                    logger.warning("Could not find tunnel address in Playit.gg log.")
                    logger.debug(f"Playit.gg log content: {log_content}")
        else:
            logger.error(f"Playit.gg log file not found at: {playit_log_path}")
        
        return None
    
    except Exception as e:
        logger.error(f"Error retrieving tunnel address: {e}")
        return None

def start_playit_and_show_address():
    """
    Start Playit.gg and show the tunnel address or claim URL.
    
    Returns:
        str: Tunnel address or claim URL if found, None otherwise
    """
    # Check if Playit.gg is installed
    check_playit_installed()
    
    # Start Playit.gg tunnel
    start_playit()
    
    # Get the tunnel address or claim URL
    result = get_tunnel_address()
    
    if result:
        if result.startswith("CLAIM_URL:"):
            claim_url = result.replace("CLAIM_URL:", "")
            print("\n=== Playit.gg Setup Required ===")
            print("Please visit the following URL to complete Playit.gg setup:")
            print(f"\n    {claim_url}\n")
            print("After completing the setup, restart Playit.gg to get your tunnel address.")
            return result
        elif result.startswith("TUNNEL:"):
            tunnel_address = result.replace("TUNNEL:", "")
            logger.info(f"Playit.gg tunnel address: {tunnel_address}")
            return tunnel_address
    else:
        logger.warning("Could not retrieve Playit.gg tunnel address.")
        return None

def write_eula():
    """Write eula=true to eula.txt file."""
    try:
        with open(EULA_FILE, 'w') as f:
            f.write('eula=true')
        logger.info("Successfully wrote eula=true to eula.txt")
        return True
    except Exception as e:
        logger.error(f"Failed to write eula.txt: {str(e)}")
        return False

def configure_server_properties(config: Dict):
    """
    Configure server.properties with optimized settings.
    
    Args:
        config (dict): Server configuration
    """
    properties = {
        'server-port': config['server'].get('port', 25565),
        'gamemode': 'survival',
        'difficulty': 'normal',
        'spawn-protection': 16,
        'max-tick-time': 60000,
        'view-distance': 10,
        'max-players': 20,
        'online-mode': 'false',  # Set to false to allow non-premium players
        'enable-command-block': 'true',
        'motd': '§6Welcome to Minecraft Server§r'
    }
    
    properties_path = os.path.join(SERVER_DIR, 'server.properties')
    
    try:
        with open(properties_path, 'w') as f:
            for key, value in properties.items():
                f.write(f'{key}={value}\n')
        logger.info("Successfully configured server.properties")
    except Exception as e:
        logger.error(f"Error configuring server.properties: {e}")
        raise

def main():
    """Enhanced main menu for managing the server with configuration support."""
    global playit_process
    global minecraft_process
    
    if 'playit_process' not in globals():
        playit_process = None
    if 'minecraft_process' not in globals():
        minecraft_process = None

    # Load configuration
    config = load_config()
    
    # Validate configuration
    if not validate_config(config):
        logger.error("Invalid configuration. Using default settings.")
        config = DEFAULT_CONFIG

    if not check_system_requirements(config):
        print("System requirements check failed. Please resolve the issues above.")
        return
    
    # Apply performance optimizations
    config = optimize_server_performance(config)
    config = optimize_server_startup(config)

    # Setup server directory
    setup_server_directory()

    # Create necessary directories
    os.makedirs(SERVER_DIR, exist_ok=True)
    os.makedirs(CONFIG_DIR, exist_ok=True)
    os.makedirs(LOGS_DIR, exist_ok=True)
    
    # Write EULA
    write_eula()

    while True:
        # Show current status
        print("\n=== Advanced Minecraft Server Manager ===")
        print(f"Server Status: {'Running' if is_process_running(minecraft_process) else 'Stopped'}")
        print(f"Playit.gg Status: {'Running' if is_process_running(playit_process) else 'Stopped'}")
        print("\nOptions:")
        print("1. Install and Configure Server")
        print("2. Install and Configure Playit.gg")
        print("3. Start Server")
        print("4. Start Server with Playit.gg")
        print("5. Stop Server and Playit.gg")
        print("6. Create Server Backup")
        print("7. Restore Server Backup")
        print("8. Edit Configuration")
        print("9. Exit")
        choice = input("\nEnter your choice: ").strip()

        try:
            if choice == "1":
                # Server Installation
                print("\nAvailable Distributions:")
                print("1. Vanilla\n2. Paper\n3. Purpur\n4. Fabric\n5. Forge")
                dist_choice = input("Select distribution (number): ").strip()
                distributions = ["vanilla", "paper", "purpur", "fabric", "forge"]
                
                if dist_choice.isdigit() and 1 <= int(dist_choice) <= len(distributions):
                    distribution = distributions[int(dist_choice) - 1]
                else:
                    distribution = "paper"  # Default
                
                version = sanitize_input(input(f"Enter {distribution} version: ").strip(), 'version')
                
                # Update config
                config['server']['distribution'] = distribution
                config['server']['version'] = version
                
                # Install Java
                install_java(config['java']['version'])
                
                # Download server JAR
                download_server_jar(distribution, version, config)
                
                # Configure server properties
                configure_server_properties(config)
                
                # Save updated config
                with open(CONFIG_FILE, 'w') as f:
                    yaml.dump(config, f)
                secure_config_file(CONFIG_FILE)
                logger.info("Server setup complete!")

            elif choice == "2":
                # Install and Configure Playit.gg
                if is_process_running(playit_process):
                    logger.warning("Playit.gg is already running!")
                    continue
                
                print("\nSetting up Playit.gg tunnel...")
                if start_playit_and_show_address():
                    print("\nPlayit.gg setup complete! You can now use option 4 to start both the Minecraft server and Playit.gg together.")
                else:
                    print("\nFailed to setup Playit.gg. Please check the logs for details.")

            elif choice == "3":
                # Start Server only
                if is_process_running(minecraft_process):
                    logger.warning("Server is already running!")
                    continue
                if not check_server_files():
                    continue
                minecraft_process = start_server(config)
                logger.info("Server started successfully!")
            elif choice == "4":
                # Start both Server and Playit.gg
                if is_process_running(minecraft_process) or is_process_running(playit_process):
                    logger.warning("One or more processes are already running!")
                    continue
                if not check_server_files():
                    continue
                try:
                    # Start Playit.gg and get the tunnel address
                    tunnel_address = start_playit_and_show_address()
                    
                    # If tunnel address is found, display it prominently
                    if tunnel_address:
                        print("\n" + "=" * 50)
                        print("🌐 Playit.gg Tunnel Address 🌐")
                        print("=" * 50)
                        print(f"Server Address: {tunnel_address}")
                        print("=" * 50 + "\n")
                    
                    # Start the server with Playit.gg
                    start_server_with_playit(config)
                except Exception as e:
                    logger.error(f"Error starting server: {e}")
                    print(f"Failed to start server: {e}")
                continue
            elif choice == "5":
                # Stop both
                if not is_process_running(minecraft_process) and not is_process_running(playit_process):
                    logger.warning("No processes are running!")
                    continue
                if minecraft_process:
                    stop_process(minecraft_process)
                    minecraft_process = None
                if playit_process:
                    stop_process(playit_process)
                    playit_process = None
                logger.info("All processes stopped.")
            elif choice == "6":
                # Create Backup
                create_backup(config)
            elif choice == "7":
                # Restore Backup
                if is_process_running(minecraft_process):
                    logger.warning("Please stop the server before restoring a backup!")
                    continue
                backups = sorted(os.listdir(BACKUP_DIR)) if os.path.exists(BACKUP_DIR) else []
                if backups:
                    print("\nAvailable Backups:")
                    for i, backup in enumerate(backups, 1):
                        print(f"{i}. {backup}")
                    
                    backup_choice = input("Enter backup number to restore (or press Enter for latest): ").strip()
                    
                    if backup_choice.isdigit() and 1 <= int(backup_choice) <= len(backups):
                        selected_backup = backups[int(backup_choice) - 1]
                    else:
                        selected_backup = None
                    
                    restore_backup(selected_backup)
                else:
                    logger.warning("No backups available.")

            elif choice == "8":
                # Edit Configuration
                if is_process_running(minecraft_process):
                    logger.warning("Please stop the server before editing configuration!")
                    continue
                print("\n=== Edit Configuration ===")
                print("1. Java Version")
                print("2. Server Distribution")
                print("3. Server Version")
                print("4. Memory Allocation")
                print("5. Playit Tunnel")
                print("6. Backup Settings")
                
                config_choice = input("Select configuration to edit: ").strip()
                
                if config_choice == "1":
                    config['java']['version'] = sanitize_input(input("Enter Java version: ").strip(), 'version')
                elif config_choice == "2":
                    config['server']['distribution'] = sanitize_input(input("Enter server distribution: ").strip(), 'distribution')
                elif config_choice == "3":
                    config['server']['version'] = sanitize_input(input("Enter server version: ").strip(), 'version')
                elif config_choice == "4":
                    config['server']['memory']['min_percentage'] = sanitize_input(input("Enter minimum memory percentage: ").strip(), 'memory_percentage')
                    config['server']['memory']['max_percentage'] = sanitize_input(input("Enter maximum memory percentage: ").strip(), 'memory_percentage')
                elif config_choice == "5":
                    config['playit']['enabled'] = input("Enable Playit tunnel? (y/n): ").strip().lower() == 'y'
                elif config_choice == "6":
                    config['backup']['enabled'] = input("Enable backups? (y/n): ").strip().lower() == 'y'
                    config['backup']['max_backups'] = int(input("Maximum number of backups to keep: ").strip())
                
                # Save updated config
                with open(CONFIG_FILE, 'w') as f:
                    yaml.dump(config, f)
                secure_config_file(CONFIG_FILE)
                logger.info("Configuration updated successfully.")

            elif choice == "9":
                # Exit
                if is_process_running(minecraft_process) or is_process_running(playit_process):
                    logger.warning("Please stop all processes before exiting!")
                    continue
                print("Goodbye!")
                break

            else:
                print("Invalid choice. Please try again.")

        except Exception as e:
            logger.error(f"An error occurred: {e}")
            logger.debug(traceback.format_exc())
            input("Press Enter to continue...")

if __name__ == "__main__":
    try:        main()
    except KeyboardInterrupt:
        print("\nExiting gracefully...")
        # Cleanup any running processes
        if 'minecraft_process' in globals() and minecraft_process:
            stop_process(minecraft_process)
        if 'playit_process' in globals() and playit_process:
            stop_process(playit_process)
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        logger.debug(traceback.format_exc())