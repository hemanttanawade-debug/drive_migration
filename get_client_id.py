"""
Extract Service Account Client ID from credentials file
"""
import json
from pathlib import Path

print("="*80)
print("SERVICE ACCOUNT CLIENT ID EXTRACTOR")
print("="*80)

def get_client_id(cred_file):
    """Extract client ID from credentials file"""
    if not Path(cred_file).exists():
        print(f"\n✗ File not found: {cred_file}")
        return None
    
    try:
        with open(cred_file, 'r') as f:
            creds = json.load(f)
        
        if creds.get('type') == 'service_account':
            client_id = creds.get('client_id')
            client_email = creds.get('client_email')
            project_id = creds.get('project_id')
            
            print(f"\n✓ {cred_file}")
            print(f"  Project ID: {project_id}")
            print(f"  Service Account Email: {client_email}")
            print(f"  CLIENT ID: {client_id}")
            
            return client_id
        else:
            print(f"\n✗ {cred_file} is not a service account")
            return None
            
    except Exception as e:
        print(f"\n✗ Error reading {cred_file}: {e}")
        return None

# Extract from both files
source_client_id = get_client_id('source_credentials.json')
dest_client_id = get_client_id('dest_credentials.json')

print("\n" + "="*80)
print("DOMAIN-WIDE DELEGATION SETUP")
print("="*80)

if source_client_id:
    print("\n📋 COPY THIS CLIENT ID (you need it for BOTH domains):")
    print("-"*80)
    print(f"{source_client_id}")
    print("-"*80)
    
    print("\n📋 COPY THESE SCOPES:")
    print("-"*80)
    print("https://www.googleapis.com/auth/admin.directory.user.readonly,https://www.googleapis.com/auth/admin.directory.domain.readonly,https://www.googleapis.com/auth/drive,https://www.googleapis.com/auth/drive.file,https://www.googleapis.com/auth/drive.metadata,https://www.googleapis.com/auth/drive.readonly")
    print("-"*80)
    
    print("\n📝 STEPS:")
    print("1. Go to: https://admin.google.com")
    print("2. Sign in as Super Admin for dev.shivaami.in")
    print("3. Navigate to: Security → API Controls → Domain-wide Delegation")
    print("4. Click: Add new")
    print(f"5. Paste Client ID: {source_client_id}")
    print("6. Paste the scopes (from above)")
    print("7. Click: AUTHORIZE")
    print("\n8. Repeat steps 1-7 for demo.shivaami.in")
    print("\n9. Wait 10 minutes")
    print("10. Run: python verify_service_account.py")
    
    print("\n" + "="*80)
    print("Full guide available in: FIX_DELEGATION.md")
    print("="*80)
else:
    print("\n✗ Could not extract Client ID")
    print("Check that source_credentials.json exists and is a valid service account file")

print()