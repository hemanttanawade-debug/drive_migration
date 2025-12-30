"""
File structure and permissions mapping module
Captures complete Drive structure before migration
"""
import logging
import json
from typing import Dict, List, Optional
from pathlib import Path
from datetime import datetime
from googleapiclient.errors import HttpError

logger = logging.getLogger(__name__)


class DriveStructureMapper:
    """Maps complete Drive structure including folders, files, and permissions"""
    
    def __init__(self, drive_service):
        """
        Initialize structure mapper
        
        Args:
            drive_service: Authenticated Drive API service
        """
        self.drive = drive_service
    
    def map_user_drive_structure(self, user_email: str) -> Dict:
        """
        Map complete Drive structure for a user
        
        Args:
            user_email: User email address
            
        Returns:
            Dictionary containing complete drive structure
        """
        logger.info(f"Mapping Drive structure for {user_email}")
        
        structure = {
            'user_email': user_email,
            'timestamp': datetime.now().isoformat(),
            'files': [],
            'folders': [],
            'root_files': [],
            'folder_tree': {},
            'permissions_summary': {
                'total_files': 0,
                'files_with_permissions': 0,
                'total_permissions': 0,
                'permission_types': {}
            }
        }
        
        # Get all files and folders
        all_items = self._list_all_items(user_email)
        
        # Separate files and folders
        for item in all_items:
            item_data = self._get_detailed_item_info(item['id'])
            
            if item_data:
                if item_data['mimeType'] == 'application/vnd.google-apps.folder':
                    structure['folders'].append(item_data)
                else:
                    structure['files'].append(item_data)
                
                # Track permission statistics
                permissions = item_data.get('permissions', [])
                if len(permissions) > 1:  # More than just owner
                    structure['permissions_summary']['files_with_permissions'] += 1
                
                structure['permissions_summary']['total_permissions'] += len(permissions)
                
                for perm in permissions:
                    role = perm.get('role', 'unknown')
                    structure['permissions_summary']['permission_types'][role] = \
                        structure['permissions_summary']['permission_types'].get(role, 0) + 1
        
        structure['permissions_summary']['total_files'] = len(structure['files']) + len(structure['folders'])
        
        # Build folder tree
        structure['folder_tree'] = self._build_folder_tree(structure['folders'], structure['files'])
        
        # Identify root-level files
        structure['root_files'] = [
            f for f in structure['files'] 
            if not f.get('parents') or len(f.get('parents', [])) == 0
        ]
        
        logger.info(f"Mapped {len(structure['files'])} files and {len(structure['folders'])} folders")
        logger.info(f"Files with shared permissions: {structure['permissions_summary']['files_with_permissions']}")
        
        return structure
    
    def _list_all_items(self, user_email: str) -> List[Dict]:
        """List all files and folders for a user"""
        items = []
        page_token = None
        
        query = f"'{user_email}' in owners and trashed=false"
        
        try:
            while True:
                response = self.drive.files().list(
                    q=query,
                    pageSize=1000,
                    pageToken=page_token,
                    fields='nextPageToken,files(id,name,mimeType)',
                    supportsAllDrives=True,
                    includeItemsFromAllDrives=True
                ).execute()
                
                batch = response.get('files', [])
                items.extend(batch)
                
                page_token = response.get('nextPageToken')
                if not page_token:
                    break
            
            return items
            
        except HttpError as e:
            logger.error(f"Error listing items for {user_email}: {e}")
            return []
    
    def _get_detailed_item_info(self, file_id: str) -> Optional[Dict]:
        """Get detailed information including permissions for a file/folder"""
        try:
            file_info = self.drive.files().get(
                fileId=file_id,
                fields='id,name,mimeType,size,parents,createdTime,modifiedTime,'
                       'owners,webViewLink,md5Checksum,capabilities',
                supportsAllDrives=True
            ).execute()
            
            # Get permissions separately
            permissions = self._get_permissions(file_id)
            file_info['permissions'] = permissions
            
            return file_info
            
        except HttpError as e:
            logger.error(f"Error getting details for file {file_id}: {e}")
            return None
    
    def _get_permissions(self, file_id: str) -> List[Dict]:
        """Get all permissions for a file"""
        permissions = []
        
        try:
            response = self.drive.permissions().list(
                fileId=file_id,
                fields='permissions(id,type,role,emailAddress,displayName,domain,expirationTime)',
                supportsAllDrives=True
            ).execute()
            
            permissions = response.get('permissions', [])
            
        except HttpError as e:
            logger.warning(f"Error getting permissions for {file_id}: {e}")
        
        return permissions
    
    def _build_folder_tree(self, folders: List[Dict], files: List[Dict]) -> Dict:
        """Build hierarchical folder tree"""
        tree = {
            'root': {
                'folders': [],
                'files': [],
                'path': '/'
            }
        }
        
        # Create folder index
        folder_index = {f['id']: f for f in folders}
        
        # Build tree structure
        for folder in folders:
            folder_id = folder['id']
            parents = folder.get('parents', [])
            
            if not parents:
                # Root folder
                tree['root']['folders'].append({
                    'id': folder_id,
                    'name': folder['name'],
                    'permissions_count': len(folder.get('permissions', []))
                })
            
            # Initialize this folder in tree
            if folder_id not in tree:
                tree[folder_id] = {
                    'name': folder['name'],
                    'folders': [],
                    'files': [],
                    'permissions': folder.get('permissions', []),
                    'path': self._get_folder_path(folder_id, folder_index)
                }
        
        # Add files to folders
        for file in files:
            parents = file.get('parents', [])
            
            if not parents:
                # Root file
                tree['root']['files'].append({
                    'id': file['id'],
                    'name': file['name'],
                    'mimeType': file['mimeType'],
                    'permissions_count': len(file.get('permissions', []))
                })
            else:
                for parent_id in parents:
                    if parent_id in tree:
                        tree[parent_id]['files'].append({
                            'id': file['id'],
                            'name': file['name'],
                            'mimeType': file['mimeType'],
                            'size': file.get('size', 0),
                            'permissions_count': len(file.get('permissions', []))
                        })
        
        return tree
    
    def _get_folder_path(self, folder_id: str, folder_index: Dict) -> str:
        """Get full path of a folder"""
        if folder_id not in folder_index:
            return '/'
        
        folder = folder_index[folder_id]
        parents = folder.get('parents', [])
        
        if not parents:
            return f"/{folder['name']}"
        
        parent_path = self._get_folder_path(parents[0], folder_index)
        return f"{parent_path}/{folder['name']}"
    
    def save_structure(self, structure: Dict, output_file: str):
        """Save structure to JSON file"""
        try:
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(structure, f, indent=2, ensure_ascii=False)
            
            logger.info(f"Structure saved to {output_file}")
            
            # Also save a human-readable summary
            summary_file = output_file.replace('.json', '_summary.txt')
            self._save_summary(structure, summary_file)
            
        except Exception as e:
            logger.error(f"Error saving structure: {e}")
    
    def _save_summary(self, structure: Dict, output_file: str):
        """Save human-readable summary"""
        try:
            with open(output_file, 'w', encoding='utf-8') as f:
                f.write("="*80 + "\n")
                f.write(f"Drive Structure Summary for {structure['user_email']}\n")
                f.write(f"Generated: {structure['timestamp']}\n")
                f.write("="*80 + "\n\n")
                
                f.write("Statistics:\n")
                f.write(f"  Total Files: {len(structure['files'])}\n")
                f.write(f"  Total Folders: {len(structure['folders'])}\n")
                f.write(f"  Root-level Files: {len(structure['root_files'])}\n")
                
                ps = structure['permissions_summary']
                f.write(f"\nPermissions:\n")
                f.write(f"  Files with shared permissions: {ps['files_with_permissions']}\n")
                f.write(f"  Total permissions: {ps['total_permissions']}\n")
                f.write(f"  By role:\n")
                for role, count in ps['permission_types'].items():
                    f.write(f"    {role}: {count}\n")
                
                # Folder tree
                f.write("\n" + "="*80 + "\n")
                f.write("Folder Tree:\n")
                f.write("="*80 + "\n")
                self._write_tree(f, structure['folder_tree'], 'root', 0)
            
            logger.info(f"Summary saved to {output_file}")
            
        except Exception as e:
            logger.error(f"Error saving summary: {e}")
    
    def _write_tree(self, file, tree: Dict, node_id: str, indent: int):
        """Recursively write tree structure"""
        if node_id not in tree:
            return
        
        node = tree[node_id]
        prefix = "  " * indent
        
        if node_id == 'root':
            file.write(f"{prefix}/ (Root)\n")
        else:
            file.write(f"{prefix}📁 {node['name']}\n")
            if node.get('permissions'):
                file.write(f"{prefix}   [Shared with {len(node['permissions'])} users]\n")
        
        # Write files in this folder
        for file_info in node.get('files', []):
            perm_info = f" [Shared with {file_info['permissions_count']} users]" if file_info['permissions_count'] > 1 else ""
            file.write(f"{prefix}  📄 {file_info['name']}{perm_info}\n")
        
        # Recursively write subfolders
        for folder_info in node.get('folders', []):
            self._write_tree(file, tree, folder_info['id'], indent + 1)
    
    def load_structure(self, input_file: str) -> Optional[Dict]:
        """Load structure from JSON file"""
        try:
            with open(input_file, 'r', encoding='utf-8') as f:
                structure = json.load(f)
            
            logger.info(f"Structure loaded from {input_file}")
            return structure
            
        except Exception as e:
            logger.error(f"Error loading structure: {e}")
            return None