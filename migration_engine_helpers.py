def _build_folder_structure_with_permissions(self, folders: List[Dict], dest_email: str, 
                                            source_drive) -> Dict[str, str]:
    """
    Build folder structure in destination with permissions
    
    Args:
        folders: List of folder metadata from source
        dest_email: Destination user email
        source_drive: Source Drive API service
        
    Returns:
        Mapping of source folder ID to destination folder ID
    """
    folder_mapping = {}
    
    # Sort folders by hierarchy (root first)
    folders_sorted = sorted(folders, key=lambda x: len(x.get('parents', [])))
    
    # Initialize permissions migrator
    from config import Config
    domain_mapping = {
        Config.SOURCE_DOMAIN: Config.DEST_DOMAIN
    }
    
    from permissions_migrator import PermissionsMigrator
    perm_migrator = PermissionsMigrator(
        source_drive,
        self.dest_ops.drive,
        domain_mapping
    )
    
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
            
            # Migrate folder permissions
            folder_permissions = folder.get('permissions', [])
            if len(folder_permissions) > 1:  # Has collaborators
                perm_result = perm_migrator.migrate_permissions(
                    folder_id,
                    dest_folder_id,
                    folder_permissions
                )
                logger.info(f"Folder '{folder_name}' permissions: {perm_result['migrated']}/{perm_result['total_permissions']} migrated")
    
    logger.info(f"Created {len(folder_mapping)} folders for {dest_email}")
    return folder_mapping


def _migrate_file_with_permissions(self, file_info: Dict, dest_email: str, 
                                  folder_mapping: Dict[str, str], source_ops, 
                                  source_drive) -> Tuple[bool, Optional[str], Optional[str], Optional[Dict]]:
    """
    Migrate a single file with permissions
    
    Args:
        file_info: Source file metadata
        dest_email: Destination user email
        folder_mapping: Folder ID mapping
        source_ops: Source DriveOperations
        source_drive: Source Drive API service
        
    Returns:
        Tuple of (success, error_message, dest_file_id, permissions_result)
    """
    file_id = file_info['id']
    file_name = file_info['name']
    mime_type = file_info['mimeType']
    
    try:
        # Skip folders (already created)
        if mime_type == 'application/vnd.google-apps.folder':
            return True, None, None, None
        
        # Check file size
        file_size = int(file_info.get('size', 0))
        if file_size > self.config.MAX_FILE_SIZE_MB * 1024 * 1024:
            logger.warning(f"Skipping {file_name}: File too large ({file_size} bytes)")
            return False, "File too large", None, None
        
        # Check if Google Workspace file
        is_google_doc = mime_type.startswith('application/vnd.google-apps.')
        
        # Determine destination parent
        dest_parent_id = None
        parent_ids = file_info.get('parents', [])
        if parent_ids and parent_ids[0] in folder_mapping:
            dest_parent_id = folder_mapping[parent_ids[0]]
        
        dest_file_id = None
        
        # Download or export file
        if is_google_doc:
            # For Google Workspace files, try to copy directly first
            logger.debug(f"Attempting to copy Google Workspace file: {file_name}")
            
            from auth import GoogleAuthManager
            from config import Config
            dest_auth = GoogleAuthManager(
                Config.DEST_CREDENTIALS_FILE,
                Config.SCOPES,
                delegate_email=dest_email
            )
            dest_auth.authenticate()
            dest_drive_delegated = dest_auth.get_drive_service(user_email=dest_email)
            
            # Copy the file directly
            try:
                file_metadata = {'name': file_name}
                if dest_parent_id:
                    file_metadata['parents'] = [dest_parent_id]
                
                copied_file = dest_drive_delegated.files().copy(
                    fileId=file_id,
                    body=file_metadata,
                    fields='id,name',
                    supportsAllDrives=True
                ).execute()
                
                dest_file_id = copied_file.get('id')
                logger.info(f"Copied Google Workspace file: {file_name} (ID: {dest_file_id})")
                
            except Exception as copy_error:
                logger.warning(f"Failed to copy {file_name}, trying export: {copy_error}")
                return False, f"Failed to copy Google Workspace file: {str(copy_error)}", None, None
        else:
            # Regular file - download and upload
            success, content = source_ops.download_file(file_id, file_name)
            if not success or content is None:
                return False, "Download failed", None, None
            
            dest_file_id = self.dest_ops.upload_file(
                content, file_name, mime_type, dest_parent_id
            )
            
            if not dest_file_id:
                return False, "Upload failed", None, None
            
            # Transfer ownership for regular files
            ownership_success = self.dest_ops.transfer_ownership(dest_file_id, dest_email)
            if not ownership_success:
                logger.warning(f"File uploaded but ownership transfer failed: {file_name}")
        
        # Migrate permissions
        perm_result = None
        file_permissions = file_info.get('permissions', [])
        
        if len(file_permissions) > 1 and dest_file_id:  # Has collaborators
            from config import Config
            domain_mapping = {
                Config.SOURCE_DOMAIN: Config.DEST_DOMAIN
            }
            
            from permissions_migrator import PermissionsMigrator
            perm_migrator = PermissionsMigrator(
                source_drive,
                self.dest_ops.drive,
                domain_mapping
            )
            
            perm_result = perm_migrator.migrate_permissions(
                file_id,
                dest_file_id,
                file_permissions
            )
            
            logger.info(f"File '{file_name}' permissions: {perm_result['migrated']}/{perm_result['total_permissions']} migrated")
        
        return True, None, dest_file_id, perm_result
        
    except Exception as e:
        logger.error(f"Error migrating file {file_name}: {e}")
        return False, str(e), None, None