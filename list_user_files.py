"""
Enhanced file lister - Shows ALL files including shared files
"""
import sys
from config import Config
from auth import DomainAuthManager
from logging_config import setup_logging
from googleapiclient.errors import HttpError

# Setup logging
setup_logging('INFO', 'reports/list_files.log')

print("="*80)
print("Google Drive File Lister - Enhanced Version")
print("="*80)

# Get user email
if len(sys.argv) > 1:
    user_email = sys.argv[1]
else:
    user_email = input("Enter user email to check: ").strip()

if not user_email:
    print("Error: No user email provided")
    sys.exit(1)

print(f"\nChecking Drive contents for: {user_email}")
print("-"*80)

# Determine which domain
if Config.SOURCE_DOMAIN in user_email:
    domain = "source"
    domain_name = Config.SOURCE_DOMAIN
elif Config.DEST_DOMAIN in user_email:
    domain = "destination"
    domain_name = Config.DEST_DOMAIN
else:
    print(f"Error: User email doesn't match source ({Config.SOURCE_DOMAIN}) or dest ({Config.DEST_DOMAIN})")
    sys.exit(1)

print(f"Domain: {domain_name} ({domain})")

# Initialize authentication
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
    print("✓ Authentication successful")
except Exception as e:
    print(f"✗ Authentication failed: {e}")
    sys.exit(1)

# Get appropriate service - Delegate to the specific user
if domain == "source":
    auth = auth_manager.source_auth
else:
    auth = auth_manager.dest_auth

# Get Drive service delegated to this specific user
drive_service = auth.get_drive_service(user_email=user_email)

print("\nFetching files...")
print("-"*80)

def list_all_files(drive_service):
    """List ALL files visible to the user"""
    all_files = []
    
    # Query parameters to get ALL files the user can see
    queries = [
        {
            'name': 'My Drive (Owned Files)',
            'query': "'me' in owners and trashed=false"
        },
        {
            'name': 'Shared with Me',
            'query': "sharedWithMe=true and trashed=false"
        },
        {
            'name': 'All Accessible Files',
            'query': "trashed=false"  # Everything not in trash
        }
    ]
    
    results_by_category = {}
    
    for query_info in queries:
        print(f"\nSearching: {query_info['name']}...")
        files = []
        page_token = None
        
        try:
            while True:
                response = drive_service.files().list(
                    q=query_info['query'],
                    spaces='drive',
                    fields='nextPageToken, files(id, name, mimeType, size, owners, parents, createdTime, modifiedTime, webViewLink)',
                    pageSize=1000,
                    pageToken=page_token,
                    supportsAllDrives=True,
                    includeItemsFromAllDrives=True
                ).execute()
                
                batch = response.get('files', [])
                files.extend(batch)
                page_token = response.get('nextPageToken')
                
                if not page_token:
                    break
            
            results_by_category[query_info['name']] = files
            print(f"  Found: {len(files)} files")
            
        except HttpError as e:
            print(f"  Error: {e}")
            results_by_category[query_info['name']] = []
    
    return results_by_category

try:
    # Get all files by category
    results = list_all_files(drive_service)
    
    # Deduplicate files (same file may appear in multiple queries)
    all_files_dict = {}
    for category, files in results.items():
        for file in files:
            file_id = file['id']
            if file_id not in all_files_dict:
                all_files_dict[file_id] = file
                file['category'] = category
    
    all_files = list(all_files_dict.values())
    
    print("\n" + "="*80)
    print("SUMMARY")
    print("="*80)
    
    if not all_files:
        print(f"\n⚠ No files found for {user_email}")
        print("\nThis is unusual. Please verify:")
        print(f"1. User {user_email} exists and is active")
        print(f"2. User has access to Google Drive")
        print(f"3. Service account has proper delegation")
    else:
        print(f"\n✓ Total Files Found: {len(all_files)}\n")
        
        # Categorize by category
        for category_name in results.keys():
            category_files = [f for f in all_files if f.get('category') == category_name]
            print(f"  {category_name}: {len(category_files)}")
        
        # Count by type
        folders = [f for f in all_files if f['mimeType'] == 'application/vnd.google-apps.folder']
        gdocs = [f for f in all_files if f['mimeType'] == 'application/vnd.google-apps.document']
        gsheets = [f for f in all_files if f['mimeType'] == 'application/vnd.google-apps.spreadsheet']
        gslides = [f for f in all_files if f['mimeType'] == 'application/vnd.google-apps.presentation']
        regular = [f for f in all_files if not f['mimeType'].startswith('application/vnd.google-apps.')]
        
        print("\nBy Type:")
        print(f"  Folders: {len(folders)}")
        print(f"  Google Docs: {len(gdocs)}")
        print(f"  Google Sheets: {len(gsheets)}")
        print(f"  Google Slides: {len(gslides)}")
        print(f"  Regular files: {len(regular)}")
        
        # Calculate total size (only for regular files)
        total_size = sum(int(f.get('size', 0)) for f in all_files if 'size' in f)
        total_size_mb = total_size / (1024 * 1024)
        print(f"  Total size: {total_size_mb:.2f} MB")
        
        print("\n" + "="*80)
        print("FILE LISTING (First 100 files)")
        print("="*80)
        
        # Sort by name
        sorted_files = sorted(all_files, key=lambda x: x.get('name', '').lower())
        
        for i, file in enumerate(sorted_files[:100], 1):
            name = file.get('name', 'Unknown')[:50]
            mime_type = file.get('mimeType', 'unknown')
            file_id = file.get('id', 'unknown')
            
            # Get owner info
            owners = file.get('owners', [])
            owner_str = owners[0].get('emailAddress', 'Unknown') if owners else 'Unknown'
            is_owner = owner_str == user_email
            
            # Simplify mime type display
            if mime_type == 'application/vnd.google-apps.folder':
                type_str = '[FOLDER]'
            elif mime_type == 'application/vnd.google-apps.document':
                type_str = '[Doc]'
            elif mime_type == 'application/vnd.google-apps.spreadsheet':
                type_str = '[Sheet]'
            elif mime_type == 'application/vnd.google-apps.presentation':
                type_str = '[Slides]'
            else:
                size = int(file.get('size', 0))
                type_str = f'[File: {size/1024:.1f}KB]'
            
            ownership = "OWNED" if is_owner else f"SHARED by {owner_str[:20]}"
            
            print(f"{i:3}. {name:50} {type_str:20} {ownership}")
        
        if len(sorted_files) > 100:
            print(f"\n... and {len(sorted_files) - 100} more files")
        
        print("\n" + "="*80)
        
        # Save detailed report
        import json
        report_file = f'reports/files_{user_email.replace("@", "_at_")}_detailed.json'
        with open(report_file, 'w', encoding='utf-8') as f:
            json.dump(all_files, f, indent=2, ensure_ascii=False)
        print(f"Detailed report saved to: {report_file}")
        
        # Print specific files mentioned
        print("\n" + "="*80)
        print("SEARCHING FOR MENTIONED FILES")
        print("="*80)
        search_terms = ['Uzercalendar', 'Calendar', 'Testing', 'Untitled spreadsheet']
        
        for term in search_terms:
            matching = [f for f in all_files if term.lower() in f.get('name', '').lower()]
            if matching:
                print(f"\nFiles matching '{term}':")
                for f in matching:
                    print(f"  - {f['name']}")
                    print(f"    ID: {f['id']}")
                    print(f"    Type: {f['mimeType']}")
                    print(f"    Link: {f.get('webViewLink', 'N/A')}")
            else:
                print(f"\nNo files matching '{term}'")

except Exception as e:
    print(f"\n✗ Error listing files: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

print("\n" + "="*80)
print("Check complete!")
print("="*80)