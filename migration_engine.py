"""
Core migration engine for Google Workspace Drive
"""
import logging
import time
from typing import Dict, List, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from datetime import datetime
import json

logger = logging.getLogger(__name__)


class MigrationEngine:
    """Core engine for Drive migration"""
    
    def __init__(self, source_drive_ops, dest_drive_ops, config, state_manager):
        """
        Initialize migration engine
        
        Args:
            source_drive_ops: DriveOperations for source domain
            dest_drive_ops: DriveOperations for destination domain
            config: Configuration object
            state_manager: StateManager instance
        """
        self.source_ops = source_drive_ops
        self.dest_ops = dest_drive_ops
        self.config = config
        self.state = state_manager
        
        self.stats = {
            'total_files': 0,
            'successful': 0,
            'failed': 0,
            'skipped': 0,
            'total_size': 0,
            'transferred_size': 0,
            'start_time': None,
            'end_time': None
        }
    
    def migrate_user(self, source_email: str, dest_email: str) -> Dict:
        """
        Migrate all files for a single user
        
        Args:
            source_email: Source user email
            dest_email: Destination user email
            
        Returns:
            Migration result dictionary
        """
        logger.info(f"Starting migration: {source_email} -> {dest_email}")
        
        user_result = {
            'source_email': source_email,
            'dest_email': dest_email,
            'status': 'in_progress',
            'files_total': 0,
            'files_migrated': 0,
            'files_failed': 0,
            'files_skipped': 0,
            'errors': [],
            'start_time': datetime.now().isoformat()
        }
        
        try:
            # Check if already completed
            if self.state.is_user_completed(source_email):
                logger.info(f"User {source_email} already migrated, skipping...")
                user_result['status'] = 'already_completed'
                return user_result
            
            # IMPORTANT: Create Drive service delegated to the SOURCE user
            # This allows us to see files as the user sees them
            from auth import GoogleAuthManager
            from config import Config
            
            # Get source auth manager and create delegated Drive service
            source_auth = GoogleAuthManager(
                Config.SOURCE_CREDENTIALS_FILE,
                Config.SCOPES,
                delegate_email=source_email
            )
            source_auth.authenticate()
            source_drive_delegated = source_auth.get_drive_service(user_email=source_email)
            
            # Create new DriveOperations with delegated service
            from drive_operations import DriveOperations
            source_ops_delegated = DriveOperations(source_drive_delegated, user_email=source_email)
            
            # List all files for source user using delegated credentials
            source_files = source_ops_delegated.list_files(source_email)
            user_result['files_total'] = len(source_files)
            
            if not source_files:
                logger.info(f"No files found for {source_email}")
                user_result['status'] = 'completed'
                self.state.mark_user_completed(source_email)
                return user_result
            
            # Build folder structure
            folder_mapping = self._build_folder_structure(source_files, dest_email)
            
            # Migrate files
            for file_info in tqdm(source_files, desc=f"Migrating {source_email}"):
                file_id = file_info['id']
                
                # Skip if already migrated
                if self.state.is_file_completed(file_id):
                    user_result['files_skipped'] += 1
                    continue
                
                # Migrate single file using delegated operations
                success, error = self._migrate_file(
                    file_info, 
                    dest_email, 
                    folder_mapping,
                    source_ops_delegated  # Pass delegated operations
                )
                
                if success:
                    user_result['files_migrated'] += 1
                    self.state.mark_file_completed(file_id, source_email)
                else:
                    user_result['files_failed'] += 1
                    if error:
                        user_result['errors'].append({
                            'file_id': file_id,
                            'file_name': file_info.get('name'),
                            'error': str(error)
                        })
                    self.state.mark_file_failed(file_id, source_email, str(error))
                
                # Rate limiting
                time.sleep(0.1)
            
            user_result['status'] = 'completed'
            user_result['end_time'] = datetime.now().isoformat()
            self.state.mark_user_completed(source_email)
            
            logger.info(f"Completed migration for {source_email}: "
                       f"{user_result['files_migrated']}/{user_result['files_total']} files")
            
        except Exception as e:
            logger.error(f"Error migrating user {source_email}: {e}")
            user_result['status'] = 'failed'
            user_result['errors'].append({'error': str(e)})
        
        return user_result
    
    def _build_folder_structure(self, files: List[Dict], dest_email: str) -> Dict[str, str]:
        """
        Build folder structure in destination
        
        Args:
            files: List of file metadata
            dest_email: Destination user email
            
        Returns:
            Mapping of source folder ID to destination folder ID
        """
        folder_mapping = {}
        folders = [f for f in files if f['mimeType'] == 'application/vnd.google-apps.folder']
        
        # Sort folders by hierarchy (root first)
        folders_sorted = sorted(folders, key=lambda x: len(x.get('parents', [])))
        
        for folder in folders_sorted:
            folder_id = folder['id']
            folder_name = folder['name']
            parent_ids = folder.get('parents', [])
            
            # Determine parent in destination
            dest_parent_id = None
            if parent_ids and parent_ids[0] in folder_mapping:
                dest_parent_id = folder_mapping[parent_ids[0]]
            
            # Create folder in destination
            dest_folder_id = self.dest_ops.create_folder(folder_name, dest_parent_id)
            
            if dest_folder_id:
                folder_mapping[folder_id] = dest_folder_id
                logger.debug(f"Created folder: {folder_name}")
        
        logger.info(f"Created {len(folder_mapping)} folders for {dest_email}")
        return folder_mapping
    
    def _migrate_file(self, file_info: Dict, dest_email: str, 
                     folder_mapping: Dict[str, str], source_ops=None) -> Tuple[bool, Optional[str]]:
        """
        Migrate a single file
        
        Args:
            file_info: Source file metadata
            dest_email: Destination user email
            folder_mapping: Folder ID mapping
            source_ops: Source DriveOperations (optional, uses self.source_ops if not provided)
            
        Returns:
            Tuple of (success, error_message)
        """
        # Use provided source_ops or fall back to self.source_ops
        src_ops = source_ops or self.source_ops
        
        file_id = file_info['id']
        file_name = file_info['name']
        mime_type = file_info['mimeType']
        
        try:
            # Skip folders (already created)
            if mime_type == 'application/vnd.google-apps.folder':
                return True, None
            
            # Check file size
            file_size = int(file_info.get('size', 0))
            if file_size > self.config.MAX_FILE_SIZE_MB * 1024 * 1024:
                logger.warning(f"Skipping {file_name}: File too large ({file_size} bytes)")
                return False, "File too large"
            
            # Check if Google Workspace file
            is_google_doc = mime_type.startswith('application/vnd.google-apps.')
            
            # Determine destination parent
            dest_parent_id = None
            parent_ids = file_info.get('parents', [])
            if parent_ids and parent_ids[0] in folder_mapping:
                dest_parent_id = folder_mapping[parent_ids[0]]
            
            # Download or export file
            if is_google_doc:
                success, content = src_ops.export_google_doc(
                    file_id, mime_type, 'pdf'
                )
                if not success:
                    # Try to copy instead
                    new_file_id = self.dest_ops.copy_file(
                        file_id, file_name, dest_parent_id
                    )
                    if new_file_id:
                        return self.dest_ops.transfer_ownership(new_file_id, dest_email), None
                    return False, "Failed to copy Google Doc"
            else:
                success, content = src_ops.download_file(file_id, file_name)
            
            if not success or content is None:
                return False, "Download failed"
            
            # Upload to destination
            new_file_id = self.dest_ops.upload_file(
                content, file_name, mime_type, dest_parent_id
            )
            
            if not new_file_id:
                return False, "Upload failed"
            
            # Transfer ownership
            ownership_success = self.dest_ops.transfer_ownership(
                new_file_id, dest_email
            )
            
            if not ownership_success:
                logger.warning(f"File uploaded but ownership transfer failed: {file_name}")
            
            return True, None
            
        except Exception as e:
            logger.error(f"Error migrating file {file_name}: {e}")
            return False, str(e)
    
    def migrate_domain(self, user_mapping: Dict[str, str], max_workers: int = 5) -> Dict:
        """
        Migrate all users in parallel
        
        Args:
            user_mapping: Dictionary mapping source to destination emails
            max_workers: Number of parallel workers
            
        Returns:
            Overall migration result
        """
        logger.info(f"Starting domain migration for {len(user_mapping)} users")
        self.stats['start_time'] = datetime.now().isoformat()
        
        results = []
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_user = {
                executor.submit(self.migrate_user, src, dst): src 
                for src, dst in user_mapping.items()
            }
            
            for future in tqdm(as_completed(future_to_user), 
                             total=len(user_mapping),
                             desc="Overall Progress"):
                source_email = future_to_user[future]
                try:
                    result = future.result()
                    results.append(result)
                    
                    # Update stats
                    self.stats['successful'] += result['files_migrated']
                    self.stats['failed'] += result['files_failed']
                    self.stats['skipped'] += result['files_skipped']
                    
                except Exception as e:
                    logger.error(f"Migration failed for {source_email}: {e}")
                    results.append({
                        'source_email': source_email,
                        'status': 'error',
                        'error': str(e)
                    })
        
        self.stats['end_time'] = datetime.now().isoformat()
        
        # Generate summary
        summary = {
            'total_users': len(user_mapping),
            'completed_users': sum(1 for r in results if r['status'] == 'completed'),
            'failed_users': sum(1 for r in results if r['status'] == 'failed'),
            'total_files_migrated': self.stats['successful'],
            'total_files_failed': self.stats['failed'],
            'stats': self.stats,
            'user_results': results
        }
        
        logger.info(f"Domain migration completed: {summary['completed_users']}/{summary['total_users']} users")
        
        return summary
    
    def generate_report(self, summary: Dict, output_file: str):
        """
        Generate migration report
        
        Args:
            summary: Migration summary dictionary
            output_file: Output file path
        """
        with open(output_file, 'w') as f:
            json.dump(summary, f, indent=2)
        
        logger.info(f"Migration report saved to {output_file}")
        
        # Also generate CSV for easy analysis
        csv_file = output_file.replace('.json', '.csv')
        self._generate_csv_report(summary['user_results'], csv_file)
    
    def _generate_csv_report(self, user_results: List[Dict], csv_file: str):
        """Generate CSV report of migration results"""
        import csv
        
        with open(csv_file, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow([
                'Source Email', 'Destination Email', 'Status',
                'Total Files', 'Migrated', 'Failed', 'Skipped',
                'Start Time', 'End Time'
            ])
            
            for result in user_results:
                writer.writerow([
                    result.get('source_email', ''),
                    result.get('dest_email', ''),
                    result.get('status', ''),
                    result.get('files_total', 0),
                    result.get('files_migrated', 0),
                    result.get('files_failed', 0),
                    result.get('files_skipped', 0),
                    result.get('start_time', ''),
                    result.get('end_time', '')
                ])
        
        logger.info(f"CSV report saved to {csv_file}")