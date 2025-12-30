"""
Configuration optimizations for large-scale migrations (20GB+)
"""

# Add these to your .env file or config.py

# ============================================================================
# LARGE SCALE MIGRATION SETTINGS
# ============================================================================

# File size handling
MAX_FILE_SIZE_MB = 5120  # 5GB - Google Drive API limit
CHUNK_SIZE_MB = 10  # Upload chunk size for resumable uploads
USE_RESUMABLE_UPLOAD = True  # Enable for files > 5MB

# Memory optimization
BATCH_SIZE = 50  # Process files in batches to reduce memory usage
STREAM_LARGE_FILES = True  # Stream files instead of loading into memory
LARGE_FILE_THRESHOLD_MB = 100  # Files above this use streaming

# API rate limiting
REQUESTS_PER_SECOND = 10  # Google Drive API allows ~1000/min per user
REQUESTS_PER_MINUTE = 500  # Stay under quota
BACKOFF_TIME = 60  # Seconds to wait when rate limited
MAX_RETRIES = 5  # Retry attempts for failed operations

# Parallel processing
MAX_WORKERS_FILES = 3  # Parallel file uploads per user (don't set too high)
MAX_WORKERS_PERMISSIONS = 5  # Parallel permission migrations
ENABLE_PARALLEL_UPLOAD = True  # Upload multiple files simultaneously

# Performance tuning
CACHE_FILE_METADATA = True  # Cache metadata to reduce API calls
PREFETCH_FILE_INFO = True  # Pre-fetch file info in batches
USE_BATCH_API = True  # Use batch API for permissions (100 ops per request)

# Monitoring
PROGRESS_UPDATE_INTERVAL = 5  # Seconds between progress updates
ENABLE_BANDWIDTH_MONITORING = True  # Track upload/download speeds
LOG_EVERY_N_FILES = 10  # Log progress every N files

# Safety limits
MAX_TOTAL_SIZE_GB = 100  # Maximum total data to migrate per run
STOP_ON_CRITICAL_ERROR = False  # Continue even if some files fail
VALIDATE_AFTER_MIGRATION = True  # Always validate

# Optimization flags
SKIP_EMPTY_FOLDERS = False  # Migrate empty folders too
DEDUPLICATE_FILES = False  # Skip files with same name/size (risky!)
COMPRESS_BEFORE_UPLOAD = False  # Compress files (may cause issues)

# ============================================================================
# QUOTA MANAGEMENT
# ============================================================================

# Google Drive API Quotas (per user per day)
# - Queries: 1,000,000,000
# - Queries per 100 seconds per user: 1,000
# - Queries per minute per user: 1,000

QUOTA_BUFFER_PERCENT = 20  # Leave 20% buffer to avoid hitting limits
ENABLE_QUOTA_MONITORING = True  # Track API usage
PAUSE_NEAR_QUOTA_LIMIT = True  # Auto-pause if approaching quota

# ============================================================================
# CALCULATED SETTINGS
# ============================================================================

def get_optimal_settings(total_size_gb: float, file_count: int):
    """
    Calculate optimal settings based on migration size
    
    Args:
        total_size_gb: Total size in GB
        file_count: Number of files to migrate
        
    Returns:
        Optimized configuration dictionary
    """
    settings = {}
    
    # Adjust batch size based on file count
    if file_count < 100:
        settings['BATCH_SIZE'] = 50
        settings['MAX_WORKERS_FILES'] = 3
    elif file_count < 1000:
        settings['BATCH_SIZE'] = 100
        settings['MAX_WORKERS_FILES'] = 5
    else:
        settings['BATCH_SIZE'] = 200
        settings['MAX_WORKERS_FILES'] = 8
    
    # Adjust for large data
    if total_size_gb > 10:
        settings['USE_RESUMABLE_UPLOAD'] = True
        settings['STREAM_LARGE_FILES'] = True
        settings['CHUNK_SIZE_MB'] = 25
    
    if total_size_gb > 50:
        settings['CHUNK_SIZE_MB'] = 50
        settings['LOG_EVERY_N_FILES'] = 50
    
    # Estimate time
    avg_speed_mbps = 10  # Conservative estimate
    estimated_hours = (total_size_gb * 1024) / (avg_speed_mbps * 3600)
    settings['estimated_hours'] = estimated_hours
    
    return settings


# ============================================================================
# MIGRATION STRATEGIES FOR DIFFERENT SCENARIOS
# ============================================================================

MIGRATION_STRATEGIES = {
    'small': {
        'max_size_gb': 5,
        'max_files': 500,
        'workers': 3,
        'batch_size': 50,
        'description': 'Quick migration for small drives'
    },
    'medium': {
        'max_size_gb': 20,
        'max_files': 2000,
        'workers': 5,
        'batch_size': 100,
        'description': 'Standard migration (20GB, 2000 files)'
    },
    'large': {
        'max_size_gb': 50,
        'max_files': 5000,
        'workers': 8,
        'batch_size': 200,
        'description': 'Large migration with optimization'
    },
    'enterprise': {
        'max_size_gb': 100,
        'max_files': 10000,
        'workers': 10,
        'batch_size': 500,
        'description': 'Enterprise-scale migration'
    }
}


def get_migration_strategy(total_size_gb: float, file_count: int) -> dict:
    """
    Determine best migration strategy
    
    Args:
        total_size_gb: Total size in GB
        file_count: Number of files
        
    Returns:
        Strategy configuration
    """
    for strategy_name, strategy in MIGRATION_STRATEGIES.items():
        if total_size_gb <= strategy['max_size_gb'] and file_count <= strategy['max_files']:
            return strategy
    
    # Return enterprise for very large migrations
    return MIGRATION_STRATEGIES['enterprise']


# ============================================================================
# MONITORING THRESHOLDS
# ============================================================================

ALERT_THRESHOLDS = {
    'error_rate': 0.05,  # Alert if >5% files fail
    'slow_upload': 1.0,  # Alert if speed < 1 MB/s
    'high_memory': 80,   # Alert if memory usage > 80%
    'quota_usage': 80,   # Alert if API quota > 80%
}


# ============================================================================
# EXAMPLE USAGE
# ============================================================================

"""
# In your .env file:

# For 20GB migration with 1000 files
MAX_FILE_SIZE_MB=5120
BATCH_SIZE=100
MAX_WORKERS_FILES=5
USE_RESUMABLE_UPLOAD=True
STREAM_LARGE_FILES=True
LARGE_FILE_THRESHOLD_MB=100

# For 30GB migration with 3000 files
MAX_FILE_SIZE_MB=5120
BATCH_SIZE=200
MAX_WORKERS_FILES=8
USE_RESUMABLE_UPLOAD=True
STREAM_LARGE_FILES=True
LARGE_FILE_THRESHOLD_MB=50
"""