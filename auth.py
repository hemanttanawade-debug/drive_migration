"""
Authentication module for Google Workspace API
Supports both OAuth 2.0 and Service Account authentication
"""
import pickle
import os
import json
from pathlib import Path
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google.oauth2 import service_account
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import logging

logger = logging.getLogger(__name__)


class GoogleAuthManager:
    """Manages Google Workspace authentication and API clients"""
    
    def __init__(self, credentials_file, scopes, token_file=None, delegate_email=None):
        """
        Initialize auth manager
        
        Args:
            credentials_file: Path to OAuth2 or Service Account credentials JSON
            scopes: List of OAuth scopes
            token_file: Path to save/load token pickle (OAuth only)
            delegate_email: Email for domain-wide delegation (Service Account only)
        """
        self.credentials_file = credentials_file
        self.scopes = scopes
        self.token_file = token_file or f"{credentials_file}.token.pickle"
        self.delegate_email = delegate_email
        self.creds = None
        self.auth_type = None
        
        # Detect credential type
        self._detect_credential_type()
    
    def _detect_credential_type(self):
        """Detect if credentials are OAuth or Service Account"""
        try:
            with open(self.credentials_file, 'r') as f:
                cred_data = json.load(f)
            
            if 'type' in cred_data:
                if cred_data['type'] == 'service_account':
                    self.auth_type = 'service_account'
                    logger.info(f"Detected Service Account credentials: {self.credentials_file}")
                elif cred_data['type'] == 'authorized_user':
                    self.auth_type = 'oauth'
                    logger.info(f"Detected OAuth credentials: {self.credentials_file}")
            elif 'installed' in cred_data or 'web' in cred_data:
                self.auth_type = 'oauth'
                logger.info(f"Detected OAuth credentials: {self.credentials_file}")
            else:
                logger.warning(f"Unknown credential type in {self.credentials_file}")
                self.auth_type = 'unknown'
                
        except Exception as e:
            logger.error(f"Error detecting credential type: {e}")
            self.auth_type = 'unknown'
    
    def authenticate(self):
        """Authenticate and return credentials"""
        if self.auth_type == 'service_account':
            return self._authenticate_service_account()
        else:
            return self._authenticate_oauth()
    
    def _authenticate_service_account(self):
        """Authenticate using Service Account with domain-wide delegation"""
        try:
            logger.info("Authenticating with Service Account...")
            
            credentials = service_account.Credentials.from_service_account_file(
                self.credentials_file,
                scopes=self.scopes
            )
            
            # Apply domain-wide delegation if email provided
            if self.delegate_email:
                logger.info(f"Delegating to: {self.delegate_email}")
                credentials = credentials.with_subject(self.delegate_email)
            
            self.creds = credentials
            logger.info("✓ Service Account authentication successful")
            return self.creds
            
        except Exception as e:
            logger.error(f"Service Account authentication failed: {e}")
            raise
    
    def _authenticate_oauth(self):
        """Authenticate using OAuth 2.0"""
        if Path(self.token_file).exists():
            logger.info(f"Loading existing token from {self.token_file}")
            with open(self.token_file, 'rb') as token:
                self.creds = pickle.load(token)
        
        # Refresh or get new credentials
        if not self.creds or not self.creds.valid:
            if self.creds and self.creds.expired and self.creds.refresh_token:
                logger.info("Refreshing expired credentials")
                try:
                    self.creds.refresh(Request())
                except Exception as e:
                    logger.error(f"Error refreshing token: {e}")
                    self.creds = None
            
            if not self.creds:
                logger.info("Initiating new OAuth flow")
                flow = InstalledAppFlow.from_client_secrets_file(
                    self.credentials_file, self.scopes
                )
                self.creds = flow.run_local_server(port=0)
            
            # Save credentials
            with open(self.token_file, 'wb') as token:
                pickle.dump(self.creds, token)
                logger.info(f"Credentials saved to {self.token_file}")
        
        return self.creds
    
    def get_drive_service(self, user_email=None):
        """
        Get authenticated Drive API service
        
        Args:
            user_email: User email for delegation (Service Account only)
        """
        if not self.creds:
            self.authenticate()
        
        # For service accounts, delegate to specific user if provided
        if self.auth_type == 'service_account' and user_email:
            delegated_creds = self.creds.with_subject(user_email)
            return build('drive', 'v3', credentials=delegated_creds)
        
        return build('drive', 'v3', credentials=self.creds)
    
    def get_admin_service(self):
        """Get authenticated Admin SDK Directory service"""
        if not self.creds:
            self.authenticate()
        return build('admin', 'directory_v1', credentials=self.creds)


class DomainAuthManager:
    """Manages authentication for both source and destination domains"""
    
    def __init__(self, source_config, dest_config, scopes):
        """
        Initialize domain auth manager
        
        Args:
            source_config: Dict with source domain settings
            dest_config: Dict with destination domain settings
            scopes: List of OAuth scopes
        """
        self.source_auth = GoogleAuthManager(
            credentials_file=source_config['credentials_file'],
            scopes=scopes,
            token_file=source_config.get('token_file', 'source_token.pickle'),
            delegate_email=source_config.get('admin_email')
        )
        
        self.dest_auth = GoogleAuthManager(
            credentials_file=dest_config['credentials_file'],
            scopes=scopes,
            token_file=dest_config.get('token_file', 'dest_token.pickle'),
            delegate_email=dest_config.get('admin_email')
        )
        
        self.source_domain = source_config['domain']
        self.dest_domain = dest_config['domain']
        self.source_admin_email = source_config.get('admin_email')
        self.dest_admin_email = dest_config.get('admin_email')
    
    def authenticate_all(self):
        """Authenticate both source and destination"""
        logger.info(f"Authenticating source domain: {self.source_domain}")
        self.source_auth.authenticate()
        
        logger.info(f"Authenticating destination domain: {self.dest_domain}")
        self.dest_auth.authenticate()
        
        logger.info("Authentication complete for both domains")
    
    def get_source_services(self):
        """Get source domain API services"""
        return {
            'drive': self.source_auth.get_drive_service(),
            'admin': self.source_auth.get_admin_service()
        }
    
    def get_dest_services(self):
        """Get destination domain API services"""
        return {
            'drive': self.dest_auth.get_drive_service(),
            'admin': self.dest_auth.get_admin_service()
        }
    
    def test_connection(self):
        """Test API connections for both domains"""
        try:
            # Test source
            source_drive = self.source_auth.get_drive_service()
            source_about = source_drive.about().get(fields='user').execute()
            logger.info(f"Source connection OK: {source_about.get('user', {}).get('emailAddress')}")
            
            # Test destination
            dest_drive = self.dest_auth.get_drive_service()
            dest_about = dest_drive.about().get(fields='user').execute()
            logger.info(f"Destination connection OK: {dest_about.get('user', {}).get('emailAddress')}")
            
            return True
        except HttpError as e:
            logger.error(f"Connection test failed: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error during connection test: {e}")
            return False