"""
Permissions migration module
Copies all collaborators and their access levels
"""
import logging
from typing import Dict, List, Optional
from googleapiclient.errors import HttpError
import time

logger = logging.getLogger(__name__)


class PermissionsMigrator:
    """Handles migration of file/folder permissions"""
    
    def __init__(self, source_drive, dest_drive, domain_mapping: Dict[str, str]):
        """
        Initialize permissions migrator
        
        Args:
            source_drive: Source Drive API service
            dest_drive: Destination Drive API service
            domain_mapping: Map of source domain to destination domain
        """
        self.source_drive = source_drive
        self.dest_drive = dest_drive
        self.domain_mapping = domain_mapping
    
    def migrate_permissions(self, source_file_id: str, dest_file_id: str, 
                          source_permissions: List[Dict]) -> Dict:
        """
        Migrate permissions from source to destination file
        
        Args:
            source_file_id: Source file ID
            dest_file_id: Destination file ID
            source_permissions: List of source permissions
            
        Returns:
            Migration result dictionary
        """
        result = {
            'total_permissions': len(source_permissions),
            'migrated': 0,
            'skipped': 0,
            'failed': 0,
            'details': []
        }
        
        for permission in source_permissions:
            perm_type = permission.get('type')
            role = permission.get('role')
            email = permission.get('emailAddress')
            domain = permission.get('domain')
            
            # Skip owner permission (will be set separately)
            if role == 'owner':
                result['skipped'] += 1
                result['details'].append({
                    'email': email,
                    'role': role,
                    'status': 'skipped',
                    'reason': 'Owner permission handled separately'
                })
                continue
            
            # Map email to destination domain if needed
            if email and self.domain_mapping:
                mapped_email = self._map_email_to_dest_domain(email)
            else:
                mapped_email = email
            
            # Create permission in destination
            success, error = self._create_permission(
                dest_file_id,
                perm_type,
                role,
                mapped_email,
                domain
            )
            
            if success:
                result['migrated'] += 1
                result['details'].append({
                    'email': mapped_email or domain,
                    'role': role,
                    'type': perm_type,
                    'status': 'success'
                })
                logger.debug(f"Migrated permission: {mapped_email or domain} as {role}")
            else:
                result['failed'] += 1
                result['details'].append({
                    'email': mapped_email or domain,
                    'role': role,
                    'type': perm_type,
                    'status': 'failed',
                    'error': error
                })
                logger.warning(f"Failed to migrate permission: {mapped_email or domain} - {error}")
            
            # Rate limiting
            time.sleep(0.1)
        
        return result
    
    def _create_permission(self, file_id: str, perm_type: str, role: str, 
                          email: Optional[str], domain: Optional[str]) -> tuple:
        """
        Create a permission on destination file
        
        Returns:
            Tuple of (success, error_message)
        """
        try:
            permission = {
                'type': perm_type,
                'role': role
            }
            
            if perm_type == 'user' and email:
                permission['emailAddress'] = email
                # For users that might not exist, send notification
                send_notification = False
            elif perm_type == 'group' and email:
                permission['emailAddress'] = email
                send_notification = False
            elif perm_type == 'domain' and domain:
                permission['domain'] = domain
                send_notification = False
            elif perm_type == 'anyone':
                # Anyone with link - set general access
                send_notification = False
            else:
                send_notification = False
            
            self.dest_drive.permissions().create(
                fileId=file_id,
                body=permission,
                sendNotificationEmail=send_notification,
                supportsAllDrives=True
            ).execute()
            
            return True, None
            
        except HttpError as e:
            error_msg = str(e)
            
            # Handle common errors
            if e.resp.status == 404:
                return False, "User not found in destination domain"
            elif e.resp.status == 403:
                return False, "Permission denied"
            elif e.resp.status == 400:
                # Check if it's the "notify people" error
                if 'notify people' in error_msg.lower() or 'no Google account' in error_msg:
                    return False, f"User {email} does not have a Google account in destination domain"
                return False, "Bad request - " + error_msg
            else:
                return False, error_msg
                
        except Exception as e:
            return False, str(e)
    
    def _map_email_to_dest_domain(self, email: str) -> str:
        """Map email from source domain to destination domain"""
        if not email or '@' not in email:
            return email
        
        local_part, domain = email.split('@', 1)
        
        # Check if domain should be mapped
        for source_domain, dest_domain in self.domain_mapping.items():
            if domain == source_domain:
                return f"{local_part}@{dest_domain}"
        
        # Return original if no mapping
        return email
    
    def copy_folder_permissions(self, source_folder_id: str, dest_folder_id: str) -> Dict:
        """
        Copy permissions from source folder to destination folder
        
        Args:
            source_folder_id: Source folder ID
            dest_folder_id: Destination folder ID
            
        Returns:
            Result dictionary
        """
        try:
            # Get source permissions
            response = self.source_drive.permissions().list(
                fileId=source_folder_id,
                fields='permissions(id,type,role,emailAddress,displayName,domain)',
                supportsAllDrives=True
            ).execute()
            
            source_permissions = response.get('permissions', [])
            
            # Migrate permissions
            result = self.migrate_permissions(
                source_folder_id,
                dest_folder_id,
                source_permissions
            )
            
            logger.info(f"Folder permissions migrated: {result['migrated']}/{result['total_permissions']}")
            
            return result
            
        except HttpError as e:
            logger.error(f"Error copying folder permissions: {e}")
            return {
                'total_permissions': 0,
                'migrated': 0,
                'failed': 0,
                'error': str(e)
            }
    
    def validate_permissions(self, source_file_id: str, dest_file_id: str) -> Dict:
        """
        Validate that permissions were migrated correctly
        
        Args:
            source_file_id: Source file ID
            dest_file_id: Destination file ID
            
        Returns:
            Validation result
        """
        validation = {
            'valid': False,
            'source_count': 0,
            'dest_count': 0,
            'missing': [],
            'extra': []
        }
        
        try:
            # Get source permissions
            source_response = self.source_drive.permissions().list(
                fileId=source_file_id,
                fields='permissions(type,role,emailAddress,domain)',
                supportsAllDrives=True
            ).execute()
            
            source_perms = source_response.get('permissions', [])
            validation['source_count'] = len(source_perms)
            
            # Get destination permissions
            dest_response = self.dest_drive.permissions().list(
                fileId=dest_file_id,
                fields='permissions(type,role,emailAddress,domain)',
                supportsAllDrives=True
            ).execute()
            
            dest_perms = dest_response.get('permissions', [])
            validation['dest_count'] = len(dest_perms)
            
            # Create permission signatures for comparison
            source_sigs = set()
            for p in source_perms:
                if p.get('role') != 'owner':  # Skip owner
                    sig = f"{p.get('type')}:{p.get('role')}:{p.get('emailAddress') or p.get('domain', 'anyone')}"
                    source_sigs.add(sig)
            
            dest_sigs = set()
            for p in dest_perms:
                if p.get('role') != 'owner':  # Skip owner
                    email = p.get('emailAddress')
                    # Map back to source domain for comparison
                    if email:
                        email = self._map_email_to_source_domain(email)
                    sig = f"{p.get('type')}:{p.get('role')}:{email or p.get('domain', 'anyone')}"
                    dest_sigs.add(sig)
            
            # Find differences
            validation['missing'] = list(source_sigs - dest_sigs)
            validation['extra'] = list(dest_sigs - source_sigs)
            
            validation['valid'] = len(validation['missing']) == 0
            
            return validation
            
        except Exception as e:
            logger.error(f"Error validating permissions: {e}")
            validation['error'] = str(e)
            return validation
    
    def _map_email_to_source_domain(self, email: str) -> str:
        """Map email back to source domain for comparison"""
        if not email or '@' not in email:
            return email
        
        local_part, domain = email.split('@', 1)
        
        # Reverse mapping
        for source_domain, dest_domain in self.domain_mapping.items():
            if domain == dest_domain:
                return f"{local_part}@{source_domain}"
        
        return email