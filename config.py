"""
Configuration module for GWS Drive Migration
"""
import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

class Config:
    """Configuration settings for Drive migration"""
    
    # OAuth 2.0 Scopes Required
    SCOPES = [
        'https://www.googleapis.com/auth/admin.directory.user.readonly',
        'https://www.googleapis.com/auth/admin.directory.domain.readonly',
        'https://www.googleapis.com/auth/drive',
        'https://www.googleapis.com/auth/drive.file',
        'https://www.googleapis.com/auth/drive.metadata',
        'https://www.googleapis.com/auth/drive.readonly',
    ]
    
    # Source Domain Configuration
    SOURCE_DOMAIN = os.getenv('SOURCE_DOMAIN', 'dev.shivaami.in')
    SOURCE_ADMIN_EMAIL = os.getenv('SOURCE_ADMIN_EMAIL', 'hemant@dev.shivaami.in')
    SOURCE_CREDENTIALS_FILE = os.getenv('SOURCE_CREDENTIALS_FILE', 'source_credentials.json')
    
    # Destination Domain Configuration
    DEST_DOMAIN = os.getenv('DEST_DOMAIN', 'demo.shivaami.in')
    DEST_ADMIN_EMAIL = os.getenv('DEST_ADMIN_EMAIL', 'developers@demo.shivaami.in')
    DEST_CREDENTIALS_FILE = os.getenv('DEST_CREDENTIALS_FILE', 'dest_credentials.json')
    
    # Migration Settings
    BATCH_SIZE = int(os.getenv('BATCH_SIZE', '10'))
    MAX_WORKERS = int(os.getenv('MAX_WORKERS', '5'))
    RETRY_ATTEMPTS = int(os.getenv('RETRY_ATTEMPTS', '3'))
    RETRY_DELAY = int(os.getenv('RETRY_DELAY', '5'))
    
    # File Settings
    MAX_FILE_SIZE_MB = int(os.getenv('MAX_FILE_SIZE_MB', '5120'))  # 5GB default
    EXCLUDED_MIME_TYPES = os.getenv('EXCLUDED_MIME_TYPES', '').split(',')
    
    # Logging
    LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
    LOG_FILE = os.getenv('LOG_FILE', 'migration.log')
    
    # Database/State Management
    STATE_DB_FILE = os.getenv('STATE_DB_FILE', 'migration_state.db')
    RESUME_ON_FAILURE = os.getenv('RESUME_ON_FAILURE', 'True').lower() == 'true'
    
    # Output
    REPORT_DIR = Path(os.getenv('REPORT_DIR', 'reports'))
    REPORT_DIR.mkdir(exist_ok=True)
    
    @classmethod
    def validate(cls):
        """Validate required configuration"""
        required_vars = [
            ('SOURCE_DOMAIN', cls.SOURCE_DOMAIN),
            ('SOURCE_ADMIN_EMAIL', cls.SOURCE_ADMIN_EMAIL),
            ('DEST_DOMAIN', cls.DEST_DOMAIN),
            ('DEST_ADMIN_EMAIL', cls.DEST_ADMIN_EMAIL),
        ]
        
        missing = [name for name, value in required_vars if not value or value.startswith('admin@')]
        
        if missing:
            raise ValueError(f"Missing required configuration: {', '.join(missing)}")
        
        # Check credentials files exist
        if not Path(cls.SOURCE_CREDENTIALS_FILE).exists():
            raise FileNotFoundError(f"Source credentials file not found: {cls.SOURCE_CREDENTIALS_FILE}")
        
        if not Path(cls.DEST_CREDENTIALS_FILE).exists():
            raise FileNotFoundError(f"Destination credentials file not found: {cls.DEST_CREDENTIALS_FILE}")
        
        return True