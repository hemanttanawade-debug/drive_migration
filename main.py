"""
Main application entry point for Google Workspace Drive Migration
"""
import argparse
import sys
from pathlib import Path
from datetime import datetime

from config import Config
from auth import DomainAuthManager
from users import UserManager
from drive_operations import DriveOperations
from migration_engine import MigrationEngine
from state_manager import StateManager
from logging_config import setup_logging, create_logger

logger = None


def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description='Google Workspace Drive Migration Tool',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Examples:
  # Full domain migration
  python main.py --mode full --max-workers 5
  
  # Dry run (list users only)
  python main.py --mode dry-run
  
  # Migrate specific users
  python main.py --mode custom --user-mapping users.csv
  
  # Resume failed migration
  python main.py --mode resume
  
  # Generate report only
  python main.py --mode report
        '''
    )
    
    parser.add_argument(
        '--mode',
        choices=['full', 'dry-run', 'custom', 'resume', 'report', 'validate'],
        default='full',
        help='Migration mode'
    )
    
    parser.add_argument(
        '--user-mapping',
        type=str,
        help='CSV file with user mapping (source,destination)'
    )
    
    parser.add_argument(
        '--max-workers',
        type=int,
        default=Config.MAX_WORKERS,
        help=f'Number of parallel workers (default: {Config.MAX_WORKERS})'
    )
    
    parser.add_argument(
        '--batch-size',
        type=int,
        default=Config.BATCH_SIZE,
        help=f'Batch size for processing (default: {Config.BATCH_SIZE})'
    )
    
    parser.add_argument(
        '--log-level',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
        default=Config.LOG_LEVEL,
        help=f'Logging level (default: {Config.LOG_LEVEL})'
    )
    
    parser.add_argument(
        '--no-resume',
        action='store_true',
        help='Start fresh migration (ignore previous state)'
    )
    
    parser.add_argument(
        '--filter-suspended',
        action='store_true',
        default=True,
        help='Filter out suspended users (default: True)'
    )
    
    parser.add_argument(
        '--filter-archived',
        action='store_true',
        default=True,
        help='Filter out archived users (default: True)'
    )
    
    return parser.parse_args()


def validate_setup():
    """Validate configuration and setup"""
    global logger
    logger.info("Validating setup...")
    
    try:
        Config.validate()
        logger.info("✓ Configuration validated")
        return True
    except Exception as e:
        logger.error(f"✗ Configuration validation failed: {e}")
        return False


def dry_run_mode(auth_manager):
    """Dry run - list users without migration"""
    global logger
    logger.info("="*80)
    logger.info("DRY RUN MODE - Listing users only")
    logger.info("="*80)
    
    # Get services
    source_services = auth_manager.get_source_services()
    dest_services = auth_manager.get_dest_services()
    
    # Initialize user manager
    user_mgr = UserManager(
        source_services['admin'],
        dest_services['admin'],
        Config.SOURCE_DOMAIN,
        Config.DEST_DOMAIN
    )
    
    # Get users
    logger.info("Fetching source users...")
    source_users = user_mgr.get_source_users(
        filter_suspended=True,
        filter_archived=True
    )
    
    logger.info("Fetching destination users...")
    dest_users = user_mgr.get_dest_users()
    
    # Create mapping
    user_mapping = user_mgr.create_user_mapping(source_users, dest_users)
    
    # Export mapping
    mapping_file = Config.REPORT_DIR / f'user_mapping_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
    user_mgr.export_user_mapping(user_mapping, str(mapping_file))
    
    # Summary
    logger.info("="*80)
    logger.info("DRY RUN SUMMARY")
    logger.info(f"Source Users: {len(source_users)}")
    logger.info(f"Destination Users: {len(dest_users)}")
    logger.info(f"Mapped Users: {len(user_mapping)}")
    logger.info(f"Mapping File: {mapping_file}")
    logger.info("="*80)
    
    return user_mapping


def custom_migration_mode(auth_manager, mapping_file, max_workers):
    """Custom migration with user-provided mapping"""
    global logger
    logger.info("="*80)
    logger.info("CUSTOM MIGRATION MODE")
    logger.info(f"Mapping file: {mapping_file}")
    logger.info("="*80)
    
    # Get services
    source_services = auth_manager.get_source_services()
    dest_services = auth_manager.get_dest_services()
    
    # Initialize user manager
    user_mgr = UserManager(
        source_services['admin'],
        dest_services['admin'],
        Config.SOURCE_DOMAIN,
        Config.DEST_DOMAIN
    )
    
    # Load user mapping from CSV
    logger.info(f"Loading user mapping from {mapping_file}")
    user_mapping = user_mgr.import_user_mapping(mapping_file)
    
    if not user_mapping:
        logger.error("No users found in mapping file!")
        return False
    
    logger.info(f"Loaded {len(user_mapping)} user mappings")
    
    # Verify users exist
    logger.info("Verifying users exist in both domains...")
    verified_mapping = {}
    
    for source_email, dest_email in user_mapping.items():
        # Verify source user
        if not user_mgr.verify_user_exists(source_email, source_services['admin']):
            logger.warning(f"Source user not found: {source_email} - Skipping")
            continue
        
        # Verify destination user
        if not user_mgr.verify_user_exists(dest_email, dest_services['admin']):
            logger.warning(f"Destination user not found: {dest_email} - Skipping")
            continue
        
        verified_mapping[source_email] = dest_email
        logger.info(f"✓ Verified: {source_email} -> {dest_email}")
    
    if not verified_mapping:
        logger.error("No valid user mappings found after verification!")
        return False
    
    logger.info(f"Proceeding with {len(verified_mapping)} verified users")
    
    # Initialize state manager
    state_db = Config.STATE_DB_FILE
    
    with StateManager(state_db) as state_mgr:
        # Add users to state
        for src, dst in verified_mapping.items():
            state_mgr.add_user(src, dst)
        
        # Initialize drive operations
        source_drive_ops = DriveOperations(source_services['drive'])
        dest_drive_ops = DriveOperations(dest_services['drive'])
        
        # Initialize migration engine
        migration_engine = MigrationEngine(
            source_drive_ops,
            dest_drive_ops,
            Config,
            state_mgr
        )
        
        # Start migration run
        run_id = state_mgr.start_migration_run({
            'source_domain': Config.SOURCE_DOMAIN,
            'dest_domain': Config.DEST_DOMAIN,
            'total_users': len(verified_mapping),
            'max_workers': max_workers,
            'mode': 'custom',
            'mapping_file': mapping_file
        })
        
        logger.info(f"Starting custom migration run ID: {run_id}")
        
        # Execute migration
        try:
            summary = migration_engine.migrate_domain(verified_mapping, max_workers)
            
            # Generate reports
            report_file = Config.REPORT_DIR / f'custom_migration_report_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json'
            migration_engine.generate_report(summary, str(report_file))
            
            # Update state
            state_mgr.end_migration_run(run_id, 'completed', summary)
            
            logger.info("="*80)
            logger.info("CUSTOM MIGRATION COMPLETED")
            logger.info(f"Total Users: {summary['total_users']}")
            logger.info(f"Completed Users: {summary['completed_users']}")
            logger.info(f"Files Migrated: {summary['total_files_migrated']}")
            logger.info(f"Files Failed: {summary['total_files_failed']}")
            logger.info(f"Report: {report_file}")
            logger.info("="*80)
            
            return True
            
        except Exception as e:
            logger.error(f"Custom migration failed: {e}", exc_info=True)
            state_mgr.end_migration_run(run_id, 'failed', {})
            return False


def full_migration_mode(auth_manager, max_workers, args):
    """Full domain migration"""
    global logger
    
    # Get services
    source_services = auth_manager.get_source_services()
    dest_services = auth_manager.get_dest_services()
    
    # Initialize managers
    user_mgr = UserManager(
        source_services['admin'],
        dest_services['admin'],
        Config.SOURCE_DOMAIN,
        Config.DEST_DOMAIN
    )
    
    # Get users
    logger.info("Fetching users from both domains...")
    source_users = user_mgr.get_source_users(
        filter_suspended=args.filter_suspended,
        filter_archived=args.filter_archived
    )
    dest_users = user_mgr.get_dest_users()
    
    # Create mapping
    user_mapping = user_mgr.create_user_mapping(source_users, dest_users)
    
    if not user_mapping:
        logger.error("No users to migrate!")
        return False
    
    # Initialize state manager
    state_db = Config.STATE_DB_FILE if not args.no_resume else f"migration_{datetime.now().strftime('%Y%m%d_%H%M%S')}.db"
    
    with StateManager(state_db) as state_mgr:
        # Add users to state
        for src, dst in user_mapping.items():
            state_mgr.add_user(src, dst)
        
        # Initialize drive operations
        source_drive_ops = DriveOperations(source_services['drive'])
        dest_drive_ops = DriveOperations(dest_services['drive'])
        
        # Initialize migration engine
        migration_engine = MigrationEngine(
            source_drive_ops,
            dest_drive_ops,
            Config,
            state_mgr
        )
        
        # Start migration run
        run_id = state_mgr.start_migration_run({
            'source_domain': Config.SOURCE_DOMAIN,
            'dest_domain': Config.DEST_DOMAIN,
            'total_users': len(user_mapping),
            'max_workers': max_workers
        })
        
        logger.info(f"Starting migration run ID: {run_id}")
        
        # Execute migration
        try:
            summary = migration_engine.migrate_domain(user_mapping, max_workers)
            
            # Generate reports
            report_file = Config.REPORT_DIR / f'migration_report_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json'
            migration_engine.generate_report(summary, str(report_file))
            
            # Update state
            state_mgr.end_migration_run(run_id, 'completed', summary)
            
            logger.info(f"Migration completed! Report: {report_file}")
            return True
            
        except Exception as e:
            logger.error(f"Migration failed: {e}", exc_info=True)
            state_mgr.end_migration_run(run_id, 'failed', {})
            return False


def resume_migration_mode(auth_manager, max_workers):
    """Resume failed migration"""
    global logger
    logger.info("Resuming previous migration...")
    
    if not Path(Config.STATE_DB_FILE).exists():
        logger.error("No previous migration state found!")
        return False
    
    with StateManager(Config.STATE_DB_FILE) as state_mgr:
        progress = state_mgr.get_overall_progress()
        
        logger.info(f"Previous progress:")
        logger.info(f"  Completed users: {progress.get('completed_users', 0)}/{progress.get('total_users', 0)}")
        logger.info(f"  Completed files: {progress.get('completed_files', 0)}/{progress.get('total_files', 0)}")
        logger.info(f"  Failed files: {progress.get('failed_files', 0)}")
        
        # Reset failed files for retry
        reset_count = state_mgr.reset_failed_files(max_attempts=3)
        logger.info(f"Reset {reset_count} failed files for retry")
        
        # Continue with full migration
        return full_migration_mode(auth_manager, max_workers, argparse.Namespace(
            no_resume=False,
            filter_suspended=True,
            filter_archived=True
        ))


def report_mode():
    """Generate report from existing state"""
    global logger
    
    if not Path(Config.STATE_DB_FILE).exists():
        logger.error("No migration state found!")
        return False
    
    with StateManager(Config.STATE_DB_FILE) as state_mgr:
        report_file = Config.REPORT_DIR / f'state_report_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json'
        state_mgr.export_state_report(str(report_file))
        
        progress = state_mgr.get_overall_progress()
        
        logger.info("="*80)
        logger.info("MIGRATION STATE REPORT")
        logger.info(f"Total Users: {progress.get('total_users', 0)}")
        logger.info(f"Completed Users: {progress.get('completed_users', 0)}")
        logger.info(f"Failed Users: {progress.get('failed_users', 0)}")
        logger.info(f"Total Files: {progress.get('total_files', 0)}")
        logger.info(f"Completed Files: {progress.get('completed_files', 0)}")
        logger.info(f"Failed Files: {progress.get('failed_files', 0)}")
        logger.info(f"Report saved: {report_file}")
        logger.info("="*80)
        
        return True


def validate_mode(auth_manager):
    """Validate setup and test connections"""
    global logger
    logger.info("="*80)
    logger.info("VALIDATION MODE")
    logger.info("="*80)
    
    # Test authentication
    logger.info("Testing authentication...")
    auth_manager.authenticate_all()
    
    # Test API connections
    logger.info("Testing API connections...")
    if auth_manager.test_connection():
        logger.info("✓ All connections successful")
    else:
        logger.error("✗ Connection test failed")
        return False
    
    # Test permissions
    logger.info("Validating permissions...")
    source_services = auth_manager.get_source_services()
    dest_services = auth_manager.get_dest_services()
    
    try:
        # Test source admin access
        source_services['admin'].users().list(
            domain=Config.SOURCE_DOMAIN,
            maxResults=1
        ).execute()
        logger.info("✓ Source admin access OK")
        
        # Test destination admin access
        dest_services['admin'].users().list(
            domain=Config.DEST_DOMAIN,
            maxResults=1
        ).execute()
        logger.info("✓ Destination admin access OK")
        
        logger.info("="*80)
        logger.info("✓ VALIDATION SUCCESSFUL")
        logger.info("="*80)
        return True
        
    except Exception as e:
        logger.error(f"✗ Permission validation failed: {e}")
        return False


def main():
    """Main application entry point"""
    global logger
    
    # Parse arguments
    args = parse_arguments()
    
    # Setup logging
    log_file = Config.REPORT_DIR / Config.LOG_FILE
    setup_logging(args.log_level, str(log_file))
    logger = create_logger(__name__)
    
    logger.info("Google Workspace Drive Migration Tool")
    logger.info(f"Mode: {args.mode}")
    
    # Validate setup
    if not validate_setup():
        sys.exit(1)
    
    # Report mode doesn't need authentication
    if args.mode == 'report':
        success = report_mode()
        sys.exit(0 if success else 1)
    
    # Initialize authentication
    logger.info("Initializing authentication...")
    auth_manager = DomainAuthManager(
        source_config={
            'domain': Config.SOURCE_DOMAIN,
            'credentials_file': Config.SOURCE_CREDENTIALS_FILE,
            'admin_email': Config.SOURCE_ADMIN_EMAIL
        },
        dest_config={
            'domain': Config.DEST_DOMAIN,
            'credentials_file': Config.DEST_CREDENTIALS_FILE,
            'admin_email': Config.DEST_ADMIN_EMAIL
        },
        scopes=Config.SCOPES
    )
    
    try:
        auth_manager.authenticate_all()
    except Exception as e:
        logger.error(f"Authentication failed: {e}")
        sys.exit(1)
    
    # Execute based on mode
    success = False
    
    if args.mode == 'validate':
        success = validate_mode(auth_manager)
    
    elif args.mode == 'dry-run':
        user_mapping = dry_run_mode(auth_manager)
        success = user_mapping is not None
    
    elif args.mode == 'full':
        success = full_migration_mode(auth_manager, args.max_workers, args)
    
    elif args.mode == 'resume':
        success = resume_migration_mode(auth_manager, args.max_workers)
    
    elif args.mode == 'custom':
        if not args.user_mapping or not Path(args.user_mapping).exists():
            logger.error("User mapping file required for custom mode!")
            sys.exit(1)
        
        success = custom_migration_mode(auth_manager, args.user_mapping, args.max_workers)
    
    sys.exit(0 if success else 1)


if __name__ == '__main__':
    main()