"""
Verify Service Account setup and domain-wide delegation
"""
import json
from pathlib import Path
from google.oauth2 import service_account
from googleapiclient.discovery import build

print("="*80)
print("Service Account Verification Tool")
print("="*80)

# Load configuration
try:
    from config import Config
    print("\n✓ Configuration loaded")
    print(f"  Source Domain: {Config.SOURCE_DOMAIN}")
    print(f"  Source Admin: {Config.SOURCE_ADMIN_EMAIL}")
    print(f"  Dest Domain: {Config.DEST_DOMAIN}")
    print(f"  Dest Admin: {Config.DEST_ADMIN_EMAIL}")
except Exception as e:
    print(f"\n✗ Failed to load config: {e}")
    exit(1)

def verify_service_account(cred_file, admin_email, domain, label):
    """Verify a service account setup"""
    print(f"\n{label}")
    print("-"*80)
    
    if not Path(cred_file).exists():
        print(f"✗ Credentials file not found: {cred_file}")
        return False
    
    try:
        # Load and check credentials file
        with open(cred_file, 'r') as f:
            cred_data = json.load(f)
        
        if cred_data.get('type') != 'service_account':
            print(f"✗ Not a service account credential file")
            print(f"  Type found: {cred_data.get('type')}")
            return False
        
        print(f"✓ Service Account detected")
        print(f"  Project: {cred_data.get('project_id')}")
        print(f"  Email: {cred_data.get('client_email')}")
        print(f"  Client ID: {cred_data.get('client_id')}")
        
        # Create credentials
        scopes = [
            'https://www.googleapis.com/auth/admin.directory.user.readonly',
            'https://www.googleapis.com/auth/admin.directory.domain.readonly',
            'https://www.googleapis.com/auth/drive.readonly'
        ]
        
        credentials = service_account.Credentials.from_service_account_file(
            cred_file,
            scopes=scopes
        )
        
        # Delegate to admin
        delegated_credentials = credentials.with_subject(admin_email)
        
        print(f"\n  Testing API access (delegated to {admin_email})...")
        
        # Test Admin SDK
        try:
            admin_service = build('admin', 'directory_v1', credentials=delegated_credentials)
            response = admin_service.users().list(
                domain=domain,
                maxResults=1
            ).execute()
            
            print(f"  ✓ Admin SDK API: SUCCESS")
            users = response.get('users', [])
            if users:
                print(f"    Sample user: {users[0].get('primaryEmail')}")
        except Exception as e:
            print(f"  ✗ Admin SDK API: FAILED")
            print(f"    Error: {str(e)}")
            if "Not Authorized" in str(e) or "403" in str(e):
                print(f"    → Domain-wide delegation NOT configured")
                print(f"    → Follow SERVICE_ACCOUNT_SETUP.md instructions")
            return False
        
        # Test Drive API
        try:
            drive_service = build('drive', 'v3', credentials=delegated_credentials)
            response = drive_service.about().get(fields='user').execute()
            
            print(f"  ✓ Drive API: SUCCESS")
            print(f"    User: {response.get('user', {}).get('emailAddress')}")
        except Exception as e:
            print(f"  ✗ Drive API: FAILED")
            print(f"    Error: {str(e)}")
            return False
        
        print(f"\n✓ {label} - ALL TESTS PASSED")
        return True
        
    except Exception as e:
        print(f"✗ Error: {e}")
        import traceback
        traceback.print_exc()
        return False

# Verify source domain
source_ok = verify_service_account(
    Config.SOURCE_CREDENTIALS_FILE,
    Config.SOURCE_ADMIN_EMAIL,
    Config.SOURCE_DOMAIN,
    "SOURCE DOMAIN VERIFICATION"
)

# Verify destination domain
dest_ok = verify_service_account(
    Config.DEST_CREDENTIALS_FILE,
    Config.DEST_ADMIN_EMAIL,
    Config.DEST_DOMAIN,
    "DESTINATION DOMAIN VERIFICATION"
)

# Summary
print("\n" + "="*80)
print("VERIFICATION SUMMARY")
print("="*80)

if source_ok and dest_ok:
    print("✓ ALL CHECKS PASSED!")
    print("\nYou're ready to migrate:")
    print("  python main.py --mode dry-run")
    print("  python main.py --mode custom --user-mapping users.csv")
else:
    print("✗ ISSUES FOUND")
    print("\nRequired actions:")
    
    if not source_ok:
        print("\n1. Configure domain-wide delegation for SOURCE domain:")
        print(f"   Domain: {Config.SOURCE_DOMAIN}")
        print(f"   Admin Console: https://admin.google.com")
        print("   Follow: SERVICE_ACCOUNT_SETUP.md")
    
    if not dest_ok:
        print("\n2. Configure domain-wide delegation for DESTINATION domain:")
        print(f"   Domain: {Config.DEST_DOMAIN}")
        print(f"   Admin Console: https://admin.google.com")
        print("   Follow: SERVICE_ACCOUNT_SETUP.md")
    
    print("\nAfter configuration, run this script again to verify.")

print("="*80)