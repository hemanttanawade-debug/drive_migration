"""
Resumable upload handler for large files (>5MB)
Handles chunked uploads with progress tracking and resume capability
"""
import io
import logging
from typing import Optional, Callable
from googleapiclient.http import MediaIoBaseUpload
from googleapiclient.errors import HttpError
import time

logger = logging.getLogger(__name__)


class ResumableUploadHandler:
    """Handles resumable uploads for large files"""
    
    def __init__(self, drive_service, chunk_size_mb: int = 10):
        """
        Initialize resumable upload handler
        
        Args:
            drive_service: Authenticated Drive API service
            chunk_size_mb: Chunk size in MB (default 10MB)
        """
        self.drive = drive_service
        self.chunk_size = chunk_size_mb * 1024 * 1024
    
    def upload_file_resumable(self, file_content: bytes, file_name: str,
                             mime_type: str, parent_id: Optional[str] = None,
                             progress_callback: Optional[Callable] = None) -> Optional[str]:
        """
        Upload file with resumable upload (for files > 5MB)
        
        Args:
            file_content: File content as bytes
            file_name: File name
            mime_type: MIME type
            parent_id: Parent folder ID
            progress_callback: Callback function(current, total)
            
        Returns:
            Uploaded file ID or None
        """
        file_size = len(file_content)
        logger.info(f"Starting resumable upload: {file_name} ({file_size / 1024 / 1024:.2f} MB)")
        
        try:
            # Prepare file metadata
            file_metadata = {'name': file_name}
            if parent_id:
                file_metadata['parents'] = [parent_id]
            
            # Create media upload
            media = MediaIoBaseUpload(
                io.BytesIO(file_content),
                mimetype=mime_type,
                chunksize=self.chunk_size,
                resumable=True
            )
            
            # Create file with resumable upload
            request = self.drive.files().create(
                body=file_metadata,
                media_body=media,
                fields='id,name,size',
                supportsAllDrives=True
            )
            
            # Upload in chunks
            response = None
            last_progress = 0
            
            while response is None:
                try:
                    status, response = request.next_chunk()
                    
                    if status:
                        progress = int(status.progress() * 100)
                        
                        # Call progress callback
                        if progress_callback and progress != last_progress:
                            progress_callback(status.resumable_progress, file_size)
                            last_progress = progress
                        
                        logger.debug(f"Upload progress: {file_name} - {progress}%")
                
                except HttpError as e:
                    if e.resp.status in [500, 502, 503, 504]:
                        # Resumable - retry
                        logger.warning(f"Resumable error {e.resp.status}, retrying...")
                        time.sleep(5)
                        continue
                    else:
                        raise
            
            file_id = response.get('id')
            logger.info(f"✓ Resumable upload complete: {file_name} (ID: {file_id})")
            return file_id
            
        except Exception as e:
            logger.error(f"Resumable upload failed for {file_name}: {e}")
            return None
    
    def upload_file_streaming(self, file_path: str, file_name: str,
                             mime_type: str, parent_id: Optional[str] = None,
                             progress_callback: Optional[Callable] = None) -> Optional[str]:
        """
        Upload file from disk with streaming (no memory loading)
        
        Args:
            file_path: Path to file on disk
            file_name: File name
            mime_type: MIME type
            parent_id: Parent folder ID
            progress_callback: Callback function
            
        Returns:
            Uploaded file ID or None
        """
        import os
        
        file_size = os.path.getsize(file_path)
        logger.info(f"Starting streaming upload: {file_name} ({file_size / 1024 / 1024:.2f} MB)")
        
        try:
            # Prepare file metadata
            file_metadata = {'name': file_name}
            if parent_id:
                file_metadata['parents'] = [parent_id]
            
            # Create media upload from file
            with open(file_path, 'rb') as f:
                media = MediaIoBaseUpload(
                    f,
                    mimetype=mime_type,
                    chunksize=self.chunk_size,
                    resumable=True
                )
                
                # Create file
                request = self.drive.files().create(
                    body=file_metadata,
                    media_body=media,
                    fields='id,name,size',
                    supportsAllDrives=True
                )
                
                # Upload in chunks
                response = None
                
                while response is None:
                    try:
                        status, response = request.next_chunk()
                        
                        if status and progress_callback:
                            progress_callback(status.resumable_progress, file_size)
                    
                    except HttpError as e:
                        if e.resp.status in [500, 502, 503, 504]:
                            logger.warning(f"Resumable error, retrying...")
                            time.sleep(5)
                            continue
                        else:
                            raise
            
            file_id = response.get('id')
            logger.info(f"✓ Streaming upload complete: {file_name} (ID: {file_id})")
            return file_id
            
        except Exception as e:
            logger.error(f"Streaming upload failed for {file_name}: {e}")
            return None


class DownloadOptimizer:
    """Optimizes file downloads for large files"""
    
    def __init__(self, drive_service, chunk_size_mb: int = 10):
        self.drive = drive_service
        self.chunk_size = chunk_size_mb * 1024 * 1024
    
    def download_large_file(self, file_id: str, file_name: str,
                           output_path: str = None) -> tuple:
        """
        Download large file with chunking
        
        Args:
            file_id: File ID
            file_name: File name
            output_path: Path to save file (if None, returns bytes)
            
        Returns:
            Tuple of (success, content_or_path)
        """
        from googleapiclient.http import MediaIoBaseDownload
        
        try:
            request = self.drive.files().get_media(fileId=file_id)
            
            if output_path:
                # Download to file
                with open(output_path, 'wb') as f:
                    downloader = MediaIoBaseDownload(f, request, chunksize=self.chunk_size)
                    
                    done = False
                    while not done:
                        status, done = downloader.next_chunk()
                        if status:
                            progress = int(status.progress() * 100)
                            logger.debug(f"Download {file_name}: {progress}%")
                
                logger.info(f"✓ Downloaded to file: {output_path}")
                return True, output_path
            else:
                # Download to memory
                file_buffer = io.BytesIO()
                downloader = MediaIoBaseDownload(file_buffer, request, chunksize=self.chunk_size)
                
                done = False
                while not done:
                    status, done = downloader.next_chunk()
                    if status:
                        logger.debug(f"Download {file_name}: {int(status.progress() * 100)}%")
                
                file_buffer.seek(0)
                logger.info(f"✓ Downloaded to memory: {file_name}")
                return True, file_buffer.read()
                
        except HttpError as e:
            logger.error(f"Download failed for {file_name}: {e}")
            return False, None


class BatchOperationHandler:
    """Handles batch API operations for efficiency"""
    
    def __init__(self, drive_service):
        self.drive = drive_service
    
    def batch_create_permissions(self, file_permissions: list) -> dict:
        """
        Create multiple permissions in a single batch request
        
        Args:
            file_permissions: List of (file_id, permission_dict) tuples
            
        Returns:
            Result dictionary with success/failure counts
        """
        from googleapiclient.http import BatchHttpRequest
        
        results = {
            'total': len(file_permissions),
            'success': 0,
            'failed': 0,
            'errors': []
        }
        
        def callback(request_id, response, exception):
            if exception:
                results['failed'] += 1
                results['errors'].append(str(exception))
            else:
                results['success'] += 1
        
        # Process in batches of 100 (API limit)
        batch_size = 100
        
        for i in range(0, len(file_permissions), batch_size):
            batch = file_permissions[i:i+batch_size]
            batch_request = BatchHttpRequest(callback=callback)
            
            for file_id, permission in batch:
                batch_request.add(
                    self.drive.permissions().create(
                        fileId=file_id,
                        body=permission,
                        sendNotificationEmail=False,
                        supportsAllDrives=True
                    )
                )
            
            try:
                batch_request.execute()
                logger.info(f"Batch processed: {len(batch)} permissions")
            except Exception as e:
                logger.error(f"Batch execution error: {e}")
                results['failed'] += len(batch)
        
        return results