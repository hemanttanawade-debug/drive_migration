"""
Test and validate users.csv format
"""
import csv
from pathlib import Path

csv_file = 'users.csv'

print("="*80)
print("CSV File Validator")
print("="*80)

if not Path(csv_file).exists():
    print(f"\n✗ ERROR: {csv_file} not found!")
    print("\nCreate the file with this format:")
    print("Source Email,Destination Email")
    print("ht@dev.shivaami.in,developers@demo.shivaami.in")
    exit(1)

print(f"\n✓ File found: {csv_file}")
print(f"File size: {Path(csv_file).stat().st_size} bytes\n")

# Read raw content
print("Raw content:")
print("-"*80)
with open(csv_file, 'r') as f:
    content = f.read()
    print(content)
print("-"*80)

# Parse as CSV
print("\nParsing CSV:")
print("-"*80)

try:
    with open(csv_file, 'r') as f:
        # Check if first line has header
        first_line = f.readline().strip()
        print(f"First line: {first_line}")
        
        # Check for proper header
        if 'source' in first_line.lower() and 'destination' in first_line.lower():
            print("✓ Header detected")
        else:
            print("⚠ Warning: No proper header found")
            print("  Expected: 'Source Email,Destination Email'")
        
        f.seek(0)  # Reset to beginning
        
        reader = csv.DictReader(f)
        users = list(reader)
        
        print(f"\n✓ Successfully parsed {len(users)} user mapping(s):\n")
        
        for i, user in enumerate(users, 1):
            print(f"{i}. Source: {user.get('Source Email', user.get('source', 'N/A'))}")
            print(f"   Dest:   {user.get('Destination Email', user.get('destination', 'N/A'))}")
            print()
            
except Exception as e:
    print(f"✗ Error parsing CSV: {e}")
    print("\nMake sure your CSV has this exact format:")
    print("Source Email,Destination Email")
    print("ht@dev.shivaami.in,developers@demo.shivaami.in")
    exit(1)

print("="*80)
print("✓ CSV file is valid and ready to use!")
print("="*80)
print("\nTo run migration:")
print("python main.py --mode custom --user-mapping users.csv")
print("="*80)