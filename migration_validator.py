"""
Migration validation module
Compares source and destination to ensure complete migration
"""
import logging
from typing import Dict, List
import json
from datetime import datetime

logger = logging.getLogger(__name__)


class MigrationValidator:
    """Validates migration by comparing source and destination structures"""
    
    def __init__(self):
        self.validation_results = []
    
    def validate_migration(self, source_structure: Dict, dest_structure: Dict, 
                          file_mapping: Dict[str, str]) -> Dict:
        """
        Validate complete migration
        
        Args:
            source_structure: Source drive structure
            dest_structure: Destination drive structure
            file_mapping: Mapping of source file IDs to destination file IDs
            
        Returns:
            Validation report
        """
        logger.info("Starting migration validation...")
        
        validation = {
            'timestamp': datetime.now().isoformat(),
            'source_user': source_structure['user_email'],
            'dest_user': dest_structure['user_email'],
            'overall_status': 'pending',
            'file_validation': {},
            'folder_validation': {},
            'permission_validation': {},
            'statistics': {
                'source_files': len(source_structure['files']),
                'dest_files': len(dest_structure['files']),
                'source_folders': len(source_structure['folders']),
                'dest_folders': len(dest_structure['folders']),
                'files_matched': 0,
                'files_missing': 0,
                'permissions_matched': 0,
                'permissions_missing': 0
            },
            'issues': []
        }
        
        # Validate file counts
        self._validate_counts(source_structure, dest_structure, validation)
        
        # Validate individual files
        self._validate_files(source_structure, dest_structure, file_mapping, validation)
        
        # Validate folder structure
        self._validate_folders(source_structure, dest_structure, validation)
        
        # Validate permissions
        self._validate_permissions(source_structure, dest_structure, file_mapping, validation)
        
        # Determine overall status
        if validation['statistics']['files_missing'] == 0 and \
           len(validation['issues']) == 0:
            validation['overall_status'] = 'success'
        elif validation['statistics']['files_matched'] > 0:
            validation['overall_status'] = 'partial'
        else:
            validation['overall_status'] = 'failed'
        
        logger.info(f"Validation complete: {validation['overall_status']}")
        
        return validation
    
    def _validate_counts(self, source: Dict, dest: Dict, validation: Dict):
        """Validate file and folder counts"""
        stats = validation['statistics']
        
        # Check if counts match
        if stats['source_files'] != stats['dest_files']:
            validation['issues'].append({
                'type': 'count_mismatch',
                'category': 'files',
                'severity': 'warning',
                'message': f"File count mismatch: {stats['source_files']} source vs {stats['dest_files']} destination"
            })
        
        if stats['source_folders'] != stats['dest_folders']:
            validation['issues'].append({
                'type': 'count_mismatch',
                'category': 'folders',
                'severity': 'warning',
                'message': f"Folder count mismatch: {stats['source_folders']} source vs {stats['dest_folders']} destination"
            })
    
    def _validate_files(self, source: Dict, dest: Dict, file_mapping: Dict, validation: Dict):
        """Validate individual files"""
        # Create index of destination files by name
        dest_files_by_name = {f['name']: f for f in dest['files']}
        
        for source_file in source['files']:
            file_name = source_file['name']
            source_id = source_file['id']
            
            # Check if file exists in destination
            if file_name in dest_files_by_name:
                dest_file = dest_files_by_name[file_name]
                
                validation['file_validation'][file_name] = {
                    'status': 'found',
                    'source_id': source_id,
                    'dest_id': dest_file['id'],
                    'mime_type_match': source_file['mimeType'] == dest_file['mimeType'],
                    'size_match': source_file.get('size') == dest_file.get('size')
                }
                
                validation['statistics']['files_matched'] += 1
                
                # Check MIME type
                if source_file['mimeType'] != dest_file['mimeType']:
                    # Allow Google Workspace to Office format conversions
                    if not self._is_acceptable_conversion(source_file['mimeType'], dest_file['mimeType']):
                        validation['issues'].append({
                            'type': 'mime_type_mismatch',
                            'category': 'file',
                            'severity': 'warning',
                            'file': file_name,
                            'source_mime': source_file['mimeType'],
                            'dest_mime': dest_file['mimeType']
                        })
            else:
                validation['file_validation'][file_name] = {
                    'status': 'missing',
                    'source_id': source_id
                }
                
                validation['statistics']['files_missing'] += 1
                
                validation['issues'].append({
                    'type': 'file_missing',
                    'category': 'file',
                    'severity': 'error',
                    'file': file_name,
                    'source_id': source_id
                })
    
    def _validate_folders(self, source: Dict, dest: Dict, validation: Dict):
        """Validate folder structure"""
        source_folders_by_name = {f['name']: f for f in source['folders']}
        dest_folders_by_name = {f['name']: f for f in dest['folders']}
        
        for folder_name, source_folder in source_folders_by_name.items():
            if folder_name in dest_folders_by_name:
                dest_folder = dest_folders_by_name[folder_name]
                
                validation['folder_validation'][folder_name] = {
                    'status': 'found',
                    'source_id': source_folder['id'],
                    'dest_id': dest_folder['id']
                }
            else:
                validation['folder_validation'][folder_name] = {
                    'status': 'missing',
                    'source_id': source_folder['id']
                }
                
                validation['issues'].append({
                    'type': 'folder_missing',
                    'category': 'folder',
                    'severity': 'error',
                    'folder': folder_name,
                    'source_id': source_folder['id']
                })
    
    def _validate_permissions(self, source: Dict, dest: Dict, file_mapping: Dict, validation: Dict):
        """Validate permissions were migrated"""
        source_files_with_perms = [
            f for f in source['files'] 
            if len(f.get('permissions', [])) > 1  # More than just owner
        ]
        
        dest_files_by_name = {f['name']: f for f in dest['files']}
        
        for source_file in source_files_with_perms:
            file_name = source_file['name']
            source_perms = source_file.get('permissions', [])
            source_perm_count = len([p for p in source_perms if p.get('role') != 'owner'])
            
            if file_name in dest_files_by_name:
                dest_file = dest_files_by_name[file_name]
                dest_perms = dest_file.get('permissions', [])
                dest_perm_count = len([p for p in dest_perms if p.get('role') != 'owner'])
                
                validation['permission_validation'][file_name] = {
                    'source_permissions': source_perm_count,
                    'dest_permissions': dest_perm_count,
                    'matched': source_perm_count == dest_perm_count
                }
                
                if source_perm_count == dest_perm_count:
                    validation['statistics']['permissions_matched'] += 1
                else:
                    validation['statistics']['permissions_missing'] += 1
                    
                    validation['issues'].append({
                        'type': 'permission_mismatch',
                        'category': 'permissions',
                        'severity': 'warning',
                        'file': file_name,
                        'source_count': source_perm_count,
                        'dest_count': dest_perm_count
                    })
    
    def _is_acceptable_conversion(self, source_mime: str, dest_mime: str) -> bool:
        """Check if MIME type conversion is acceptable"""
        acceptable_conversions = {
            'application/vnd.google-apps.document': [
                'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
                'application/vnd.google-apps.document'
            ],
            'application/vnd.google-apps.spreadsheet': [
                'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                'application/vnd.google-apps.spreadsheet'
            ],
            'application/vnd.google-apps.presentation': [
                'application/vnd.openxmlformats-officedocument.presentationml.presentation',
                'application/vnd.google-apps.presentation'
            ]
        }
        
        if source_mime in acceptable_conversions:
            return dest_mime in acceptable_conversions[source_mime]
        
        return False
    
    def generate_validation_report(self, validation: Dict, output_file: str):
        """Generate detailed validation report"""
        try:
            # Save JSON report
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(validation, f, indent=2, ensure_ascii=False)
            
            logger.info(f"Validation report saved to {output_file}")
            
            # Generate human-readable report
            text_file = output_file.replace('.json', '.txt')
            self._generate_text_report(validation, text_file)
            
        except Exception as e:
            logger.error(f"Error generating validation report: {e}")
    
    def _generate_text_report(self, validation: Dict, output_file: str):
        """Generate human-readable validation report"""
        try:
            with open(output_file, 'w', encoding='utf-8') as f:
                f.write("="*80 + "\n")
                f.write("MIGRATION VALIDATION REPORT\n")
                f.write("="*80 + "\n\n")
                
                f.write(f"Generated: {validation['timestamp']}\n")
                f.write(f"Source User: {validation['source_user']}\n")
                f.write(f"Destination User: {validation['dest_user']}\n")
                f.write(f"Overall Status: {validation['overall_status'].upper()}\n\n")
                
                # Statistics
                stats = validation['statistics']
                f.write("STATISTICS:\n")
                f.write("-"*80 + "\n")
                f.write(f"  Source Files: {stats['source_files']}\n")
                f.write(f"  Destination Files: {stats['dest_files']}\n")
                f.write(f"  Files Matched: {stats['files_matched']}\n")
                f.write(f"  Files Missing: {stats['files_missing']}\n")
                f.write(f"  Source Folders: {stats['source_folders']}\n")
                f.write(f"  Destination Folders: {stats['dest_folders']}\n")
                f.write(f"  Permissions Matched: {stats['permissions_matched']}\n")
                f.write(f"  Permissions Missing: {stats['permissions_missing']}\n\n")
                
                # Issues
                if validation['issues']:
                    f.write("ISSUES FOUND:\n")
                    f.write("-"*80 + "\n")
                    
                    for i, issue in enumerate(validation['issues'], 1):
                        f.write(f"{i}. [{issue['severity'].upper()}] {issue['type']}\n")
                        f.write(f"   {issue['message'] if 'message' in issue else ''}\n")
                        if 'file' in issue:
                            f.write(f"   File: {issue['file']}\n")
                        if 'folder' in issue:
                            f.write(f"   Folder: {issue['folder']}\n")
                        f.write("\n")
                else:
                    f.write("✓ No issues found!\n\n")
                
                f.write("="*80 + "\n")
            
            logger.info(f"Text validation report saved to {output_file}")
            
        except Exception as e:
            logger.error(f"Error generating text report: {e}")
    
    def print_validation_summary(self, validation: Dict):
        """Print validation summary to console"""
        print("\n" + "="*80)
        print("MIGRATION VALIDATION SUMMARY")
        print("="*80)
        print(f"Overall Status: {validation['overall_status'].upper()}")
        print(f"\nFiles: {validation['statistics']['files_matched']}/{validation['statistics']['source_files']} migrated")
        print(f"Folders: {validation['statistics']['dest_folders']}/{validation['statistics']['source_folders']} created")
        
        if validation['issues']:
            print(f"\n⚠ {len(validation['issues'])} issues found")
            print("See validation report for details")
        else:
            print("\n✓ No issues found!")
        
        print("="*80 + "\n")