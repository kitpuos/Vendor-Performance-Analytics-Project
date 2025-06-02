import os

# Base directory (project root)
NOTEBOOKS_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = os.path.abspath(os.path.join(NOTEBOOKS_DIR, os.pardir))

# Subdirectories
DATA_DIR = os.path.join(BASE_DIR, 'data')
LOGS_DIR = os.path.join(BASE_DIR, 'logs')

# Database path
DB_PATH = os.path.join(DATA_DIR, 'company-inventory.db')

# Ensure log directory exists
os.makedirs(LOGS_DIR, exist_ok = True)