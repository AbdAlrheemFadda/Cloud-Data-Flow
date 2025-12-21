"""
File upload service
Handles file validation, storage, and management
"""

import os
import logging
import hashlib
from datetime import datetime
from werkzeug.utils import secure_filename
import pandas as pd

logger = logging.getLogger(__name__)

class UploadService:
    """Service for handling file uploads and validation"""
    
    def __init__(self):
        self.max_size = int(os.getenv('MAX_UPLOAD_SIZE', 5368709120))
        self.storage_path = os.getenv('STORAGE_PATH', '/dbfs/data-processing-service')
    
    def save_and_validate(self, file, filename, job_type):
        """
        Save and validate uploaded file
        
        Args:
            file: Flask file object
            filename: Filename
            job_type: Type of job to run
        
        Returns:
            Dictionary with file metadata and validation results
        """
        try:
            # Check file size
            file.seek(0, os.SEEK_END)
            file_size = file.tell()
            file.seek(0)
            
            if file_size > self.max_size:
                raise ValueError(f'File size ({file_size} bytes) exceeds maximum ({self.max_size} bytes)')
            
            # Generate file ID
            file_id = self._generate_file_id(filename)
            
            # Validate file 
            validation = self.validate_file(file)
            
            if not validation['valid']:
                raise ValueError(f"File validation failed: {validation['details']}")
            
            # Save file
            file_path = os.path.join('uploads', f'{file_id}_{filename}')
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            file.save(file_path)
            
            return {
                'file_id': file_id,
                'filename': filename,
                'size': file_size,
                'format': validation['format'],
                'timestamp': datetime.utcnow().isoformat(),
                'rows': validation.get('rows'),
                'columns': validation.get('columns'),
                'data_types': validation.get('data_types'),
                'job_type': job_type,
                'path': file_path
            }
        
        except Exception as e:
            logger.error(f'Error saving and validating file: {str(e)}')
            raise
    
    def validate_file(self, file):
        """
        Validate uploaded file
        
        Args:
            file: Flask file object
        
        Returns:
            Dictionary with validation results
        """
        try:
            file.seek(0)
            filename = file.filename
            file_ext = filename.rsplit('.', 1)[1].lower() if '.' in filename else ''
            
            result = {
                'valid': True,
                'format': file_ext,
                'details': 'File is valid'
            }
            
            # Validate based on file type
            if file_ext == 'csv':
                df = pd.read_csv(file)
                result['rows'] = len(df)
                result['columns'] = len(df.columns)
                result['data_types'] = df.dtypes.to_dict()
            
            elif file_ext == 'json':
                df = pd.read_json(file)
                result['rows'] = len(df)
                result['columns'] = len(df.columns)
                result['data_types'] = df.dtypes.to_dict()
            
            elif file_ext == 'xlsx':
                df = pd.read_excel(file)
                result['rows'] = len(df)
                result['columns'] = len(df.columns)
                result['data_types'] = df.dtypes.to_dict()
            
            elif file_ext == 'txt':
                content = file.read().decode('utf-8', errors='ignore')
                lines = content.split('\n')
                result['rows'] = len([l for l in lines if l.strip()])
                result['details'] = f'Text file with {result["rows"]} lines'
            
            elif file_ext == 'parquet':
                df = pd.read_parquet(file)
                result['rows'] = len(df)
                result['columns'] = len(df.columns)
                result['data_types'] = df.dtypes.to_dict()
            
            else:
                result['rows'] = 0
                result['columns'] = 0
            
            file.seek(0)
            return result
        
        except Exception as e:
            logger.error(f'Validation error: {str(e)}')
            return {
                'valid': False,
                'format': file_ext,
                'details': f'Validation failed: {str(e)}'
            }
    
    def _generate_file_id(self, filename):
        """Generate unique file ID"""
        timestamp = datetime.utcnow().isoformat()
        hash_input = f'{filename}{timestamp}'.encode()
        file_id = hashlib.md5(hash_input).hexdigest()[:12]
        return file_id
    
    def cleanup_old_files(self, days=7):
        """
        Clean up old uploaded files
        
        Args:
            days: Delete files older than this many days
        """
        from pathlib import Path
        import time
        
        try:
            upload_dir = Path('uploads')
            cutoff_time = time.time() - (days * 86400)
            
            for file_path in upload_dir.glob('*'):
                if os.path.isfile(file_path) and os.path.getctime(file_path) < cutoff_time:
                    os.remove(file_path)
                    logger.info(f'Deleted old file: {file_path}')
        
        except Exception as e:
            logger.error(f'Error cleaning up files: {str(e)}')
