"""
Debug script to troubleshoot GWS migration setup
"""
import os
import sys
from pathlib import Path

print("="*80)
print("Google Workspace Migration - Setup Verification")
print("="*80)

# Check Python version
print(f"\n1. Python Version: {sys.version}")

# Check if .env exists
print("\n2. Environment File Check:")
env_file = Path('.env')
if env_file.exists():
    print("   ✓ .env file found")
    with open('.env', 'r') as f:
        lines = f.readlines()
        print(f"   ✓ .env has {len(lines)} lines")
else:
    print("   ✗ .env file NOT FOUND")
    print("   → Create .env file from .env.template")

# Check credentials files
print("\n3. Credentials Files Check:")
source_creds = Path('source_credentials.json')
dest_creds = Path('dest_credentials.json')

if source_creds.exists():
    print(f"   ✓ source_credentials.json found ({source_creds.stat().st_size} bytes)")
else:
    print("   ✗ source_credentials.json NOT FOUND")

if dest_creds.exists():
    print(f"   ✓ dest_credentials.json found ({dest_creds.stat().st_size} bytes)")
else:
    print("   ✗ dest_credentials.json NOT FOUND")

# Check user mapping file
print("\n4. User Mapping File Check:")
users_csv = Path('users.csv')
if users_csv.exists():
    print(f"   ✓ users.csv found")
    with open('users.csv', 'r') as f:
        lines = f.readlines()
        print(f"   ✓ users.csv has {len(lines)} lines")
        print("\n   Content preview:")
        for i, line in enumerate(lines[:5]):
            print(f"      Line {i+1}: {line.strip()}")
else:
    print("   ✗ users.csv NOT FOUND")
    print("   → Create users.csv with format:")
    print("      Source Email,Destination Email")
    print("      user1@source.com,user1@dest.com")

# Check required modules
print("\n5. Python Modules Check:")
required_modules = [
    'google.auth',
    'google_auth_oauthlib',
    'googleapiclient',
    'dotenv',
    'tqdm',
    'tenacity'
]

for module in required_modules:
    try:
        __import__(module)
        print(f"   ✓ {module}")
    except ImportError:
        print(f"   ✗ {module} - NOT INSTALLED")
        print(f"      → pip install {module}")

# Check if config.py exists and can be imported
print("\n6. Migration Scripts Check:")
scripts = [
    'config.py',
    'auth.py',
    'users.py',
    'drive_operations.py',
    'migration_engine.py',
    'state_manager.py',
    'logging_config.py',
    'main.py'
]

for script in scripts:
    if Path(script).exists():
        print(f"   ✓ {script}")
    else:
        print(f"   ✗ {script} - MISSING")

# Try to load config
print("\n7. Configuration Loading Test:")
try:
    from config import Config
    print("   ✓ Config module loaded")
    print(f"   SOURCE_DOMAIN: {Config.SOURCE_DOMAIN}")
    print(f"   DEST_DOMAIN: {Config.DEST_DOMAIN}")
    print(f"   SOURCE_ADMIN_EMAIL: {Config.SOURCE_ADMIN_EMAIL}")
    print(f"   DEST_ADMIN_EMAIL: {Config.DEST_ADMIN_EMAIL}")
    
    try:
        Config.validate()
        print("   ✓ Configuration is valid")
    except Exception as e:
        print(f"   ✗ Configuration validation failed: {e}")
        
except Exception as e:
    print(f"   ✗ Failed to load config: {e}")

# Check directories
print("\n8. Directories Check:")
for dirname in ['reports', 'logs']:
    dirpath = Path(dirname)
    if dirpath.exists():
        print(f"   ✓ {dirname}/ exists")
    else:
        print(f"   ✗ {dirname}/ does not exist (will be created)")

print("\n" + "="*80)
print("RECOMMENDATIONS:")
print("="*80)

issues = []

if not env_file.exists():
    issues.append("1. Create .env file: cp .env.template .env")
    issues.append("2. Edit .env with your domain details")

if not source_creds.exists() or not dest_creds.exists():
    issues.append("3. Download OAuth credentials from Google Cloud Console")
    issues.append("4. Rename them to source_credentials.json and dest_credentials.json")

if not users_csv.exists():
    issues.append("5. Create users.csv with your user mappings:")
    issues.append("   echo 'Source Email,Destination Email' > users.csv")
    issues.append("   echo 'user@source.com,user@dest.com' >> users.csv")

if issues:
    print("\nISSUES FOUND - Please fix:")
    for issue in issues:
        print(f"   {issue}")
else:
    print("\n✓ All checks passed! Ready to migrate.")
    print("\nNext steps:")
    print("   1. Validate: python main.py --mode validate")
    print("   2. Dry run: python main.py --mode dry-run")
    print("   3. Custom migrate: python main.py --mode custom --user-mapping users.csv")

print("\n" + "="*80)