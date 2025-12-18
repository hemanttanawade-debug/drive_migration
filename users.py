"""
User management module for fetching and mapping users
"""
import logging
from typing import List, Dict, Optional
from googleapiclient.errors import HttpError

logger = logging.getLogger(__name__)


class UserManager:
    """Manages user operations across domains"""
    
    def __init__(self, source_admin_service, dest_admin_service, source_domain, dest_domain):
        """
        Initialize user manager
        
        Args:
            source_admin_service: Source domain Admin SDK service
            dest_admin_service: Destination domain Admin SDK service
            source_domain: Source domain name
            dest_domain: Destination domain name
        """
        self.source_admin = source_admin_service
        self.dest_admin = dest_admin_service
        self.source_domain = source_domain
        self.dest_domain = dest_domain
    
    def get_all_users(self, domain, admin_service) -> List[Dict]:
        """
        Fetch all users from a domain
        
        Args:
            domain: Domain name
            admin_service: Admin SDK service
            
        Returns:
            List of user dictionaries
        """
        users = []
        page_token = None
        
        try:
            while True:
                logger.info(f"Fetching users from {domain}...")
                
                response = admin_service.users().list(
                    domain=domain,
                    maxResults=500,
                    orderBy='email',
                    pageToken=page_token,
                    fields='users(primaryEmail,name,suspended,archived,orgUnitPath,id),nextPageToken'
                ).execute()
                
                batch_users = response.get('users', [])
                users.extend(batch_users)
                
                logger.info(f"Retrieved {len(batch_users)} users (total: {len(users)})")
                
                page_token = response.get('nextPageToken')
                if not page_token:
                    break
            
            logger.info(f"Total users retrieved from {domain}: {len(users)}")
            return users
            
        except HttpError as e:
            logger.error(f"Error fetching users from {domain}: {e}")
            raise
    
    def get_source_users(self, filter_suspended=True, filter_archived=True) -> List[Dict]:
        """
        Get all users from source domain
        
        Args:
            filter_suspended: Exclude suspended users
            filter_archived: Exclude archived users
            
        Returns:
            List of active user dictionaries
        """
        users = self.get_all_users(self.source_domain, self.source_admin)
        
        if filter_suspended or filter_archived:
            original_count = len(users)
            
            if filter_suspended:
                users = [u for u in users if not u.get('suspended', False)]
            
            if filter_archived:
                users = [u for u in users if not u.get('archived', False)]
            
            filtered_count = original_count - len(users)
            logger.info(f"Filtered out {filtered_count} users (suspended/archived)")
        
        return users
    
    def get_dest_users(self) -> List[Dict]:
        """Get all users from destination domain"""
        return self.get_all_users(self.dest_domain, self.dest_admin)
    
    def create_user_mapping(self, source_users: List[Dict], dest_users: List[Dict], 
                           mapping_strategy='email') -> Dict[str, str]:
        """
        Create mapping between source and destination users
        
        Args:
            source_users: List of source users
            dest_users: List of destination users
            mapping_strategy: Strategy for mapping ('email', 'name', 'custom')
            
        Returns:
            Dictionary mapping source email to destination email
        """
        mapping = {}
        dest_emails = {u['primaryEmail'] for u in dest_users}
        
        for source_user in source_users:
            source_email = source_user['primaryEmail']
            source_local = source_email.split('@')[0]
            
            if mapping_strategy == 'email':
                # Map based on local part of email
                dest_email = f"{source_local}@{self.dest_domain}"
                
                if dest_email in dest_emails:
                    mapping[source_email] = dest_email
                    logger.debug(f"Mapped: {source_email} -> {dest_email}")
                else:
                    logger.warning(f"No destination user found for {source_email}")
            
            elif mapping_strategy == 'custom':
                # Custom mapping logic can be implemented here
                pass
        
        logger.info(f"Created mapping for {len(mapping)} users")
        return mapping
    
    def verify_user_exists(self, email: str, admin_service) -> bool:
        """
        Verify if a user exists in a domain
        
        Args:
            email: User email address
            admin_service: Admin SDK service
            
        Returns:
            True if user exists, False otherwise
        """
        try:
            admin_service.users().get(userKey=email).execute()
            return True
        except HttpError as e:
            if e.resp.status == 404:
                return False
            raise
    
    def get_user_info(self, email: str, admin_service) -> Optional[Dict]:
        """
        Get detailed user information
        
        Args:
            email: User email address
            admin_service: Admin SDK service
            
        Returns:
            User information dictionary or None
        """
        try:
            user = admin_service.users().get(
                userKey=email,
                fields='primaryEmail,name,suspended,archived,orgUnitPath,id,creationTime'
            ).execute()
            return user
        except HttpError as e:
            logger.error(f"Error fetching user info for {email}: {e}")
            return None
    
    def export_user_mapping(self, mapping: Dict[str, str], filename: str):
        """
        Export user mapping to CSV file
        
        Args:
            mapping: User mapping dictionary
            filename: Output CSV filename
        """
        import csv
        
        with open(filename, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['Source Email', 'Destination Email'])
            
            for source, dest in sorted(mapping.items()):
                writer.writerow([source, dest])
        
        logger.info(f"User mapping exported to {filename}")
    
    def import_user_mapping(self, filename: str) -> Dict[str, str]:
        """
        Import user mapping from CSV file
        
        Args:
            filename: Input CSV filename
            
        Returns:
            User mapping dictionary
        """
        import csv
        mapping = {}
        
        with open(filename, 'r', encoding='utf-8') as f:
            # Try to detect if file has header
            first_line = f.readline().strip()
            f.seek(0)
            
            # Check for various header formats
            has_header = any(keyword in first_line.lower() for keyword in ['source', 'destination', 'from', 'to'])
            
            if has_header:
                reader = csv.DictReader(f)
                for row in reader:
                    # Try different possible column names
                    source = (row.get('Source Email') or row.get('source') or 
                             row.get('Source') or row.get('from') or row.get('From'))
                    dest = (row.get('Destination Email') or row.get('destination') or 
                           row.get('Destination') or row.get('to') or row.get('To'))
                    
                    if source and dest:
                        mapping[source.strip()] = dest.strip()
                    else:
                        logger.warning(f"Skipping invalid row: {row}")
            else:
                # No header, assume first column is source, second is destination
                reader = csv.reader(f)
                for row in reader:
                    if len(row) >= 2:
                        mapping[row[0].strip()] = row[1].strip()
        
        logger.info(f"Imported mapping for {len(mapping)} users from {filename}")
        return mapping