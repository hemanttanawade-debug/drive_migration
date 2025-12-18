"""
Logging configuration for migration
"""
import logging
import sys
from pathlib import Path
from datetime import datetime


def setup_logging(log_level='INFO', log_file=None, console=True):
    """
    Setup logging configuration
    
    Args:
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_file: Path to log file (optional)
        console: Whether to log to console
    """
    # Create logs directory if needed
    if log_file:
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Configure root logger
    logger = logging.getLogger()
    logger.setLevel(getattr(logging, log_level.upper()))
    
    # Remove existing handlers
    logger.handlers.clear()
    
    # Create formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Console handler
    if console:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(getattr(logging, log_level.upper()))
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
    
    # File handler
    if log_file:
        file_handler = logging.FileHandler(log_file, mode='a', encoding='utf-8')
        file_handler.setLevel(logging.DEBUG)  # Always log everything to file
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    
    # Suppress noisy libraries
    logging.getLogger('googleapiclient.discovery_cache').setLevel(logging.ERROR)
    logging.getLogger('googleapiclient.discovery').setLevel(logging.WARNING)
    logging.getLogger('google.auth').setLevel(logging.WARNING)
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    
    logger.info("="*80)
    logger.info(f"Logging initialized - Level: {log_level}")
    if log_file:
        logger.info(f"Log file: {log_file}")
    logger.info("="*80)


class MigrationLogger:
    """Enhanced logger for migration operations"""
    
    def __init__(self, name):
        self.logger = logging.getLogger(name)
        self.migration_start = None
        self.current_user = None
    
    def info(self, message):
        """Log info message"""
        self.logger.info(message)
    
    def debug(self, message):
        """Log debug message"""
        self.logger.debug(message)
    
    def warning(self, message):
        """Log warning message"""
        self.logger.warning(message)
    
    def error(self, message, exc_info=False):
        """Log error message"""
        self.logger.error(message, exc_info=exc_info)
    
    def critical(self, message):
        """Log critical message"""
        self.logger.critical(message)
    
    def start_migration(self, total_users: int):
        """Log migration start"""
        self.migration_start = datetime.now()
        self.logger.info("="*80)
        self.logger.info(f"MIGRATION STARTED - Total Users: {total_users}")
        self.logger.info(f"Start Time: {self.migration_start.isoformat()}")
        self.logger.info("="*80)
    
    def start_user(self, source_email: str, dest_email: str):
        """Log user migration start"""
        self.current_user = source_email
        self.logger.info("-"*80)
        self.logger.info(f"USER MIGRATION START")
        self.logger.info(f"Source: {source_email}")
        self.logger.info(f"Destination: {dest_email}")
        self.logger.info("-"*80)
    
    def end_user(self, source_email: str, stats: dict):
        """Log user migration end"""
        self.logger.info("-"*80)
        self.logger.info(f"USER MIGRATION END - {source_email}")
        self.logger.info(f"Total Files: {stats.get('total', 0)}")
        self.logger.info(f"Successful: {stats.get('success', 0)}")
        self.logger.info(f"Failed: {stats.get('failed', 0)}")
        self.logger.info(f"Skipped: {stats.get('skipped', 0)}")
        self.logger.info("-"*80)
        self.current_user = None
    
    def end_migration(self, summary: dict):
        """Log migration end"""
        end_time = datetime.now()
        duration = end_time - self.migration_start if self.migration_start else None
        
        self.logger.info("="*80)
        self.logger.info("MIGRATION COMPLETED")
        self.logger.info(f"End Time: {end_time.isoformat()}")
        if duration:
            self.logger.info(f"Duration: {duration}")
        self.logger.info(f"Total Users: {summary.get('total_users', 0)}")
        self.logger.info(f"Successful Users: {summary.get('completed_users', 0)}")
        self.logger.info(f"Failed Users: {summary.get('failed_users', 0)}")
        self.logger.info(f"Total Files Migrated: {summary.get('total_files_migrated', 0)}")
        self.logger.info(f"Total Files Failed: {summary.get('total_files_failed', 0)}")
        self.logger.info("="*80)
    
    def log_file_success(self, file_name: str, file_id: str):
        """Log successful file migration"""
        self.logger.debug(f"✓ File migrated: {file_name} ({file_id})")
    
    def log_file_failure(self, file_name: str, file_id: str, error: str):
        """Log failed file migration"""
        self.logger.warning(f"✗ File failed: {file_name} ({file_id}) - Error: {error}")
    
    def log_rate_limit(self, retry_after: int):
        """Log rate limiting"""
        self.logger.warning(f"Rate limit hit - Retrying after {retry_after} seconds")
    
    def log_error(self, message: str, exception: Exception = None):
        """Log error with optional exception"""
        if exception:
            self.logger.error(f"{message}: {str(exception)}", exc_info=True)
        else:
            self.logger.error(message)
    
    def log_progress(self, current: int, total: int, item_type: str = "items"):
        """Log progress"""
        percentage = (current / total * 100) if total > 0 else 0
        self.logger.info(f"Progress: {current}/{total} {item_type} ({percentage:.1f}%)")


def create_logger(name: str) -> MigrationLogger:
    """Create a migration logger instance"""
    return MigrationLogger(name)