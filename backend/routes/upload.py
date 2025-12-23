"""
File upload endpoint and handling
"""

import os
import logging
from flask import Blueprint, request, jsonify, current_app
from werkzeug.utils import secure_filename
from services.upload_service import UploadService

logger = logging.getLogger(__name__)
upload_bp = Blueprint('upload', __name__, url_prefix='/api')


ALLOWED_EXTENSIONS = {'csv', 'json', 'txt', 'pdf'}

def allowed_file(filename):
    """Check if file extension is allowed"""
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

@upload_bp.route('/upload', methods=['POST'])
def upload_file():
    """
    Upload a dataset file
    Request:
        - file: The file to upload
        - job_type: Type of jobs to run (stats, ml, both)
    Response:
        - file_id: Unique identifier for uploaded file
        - filename: Original filename
        - size: File size in bytes
        - timestamp: Upload timestamp
    """
    try:

        if 'file' not in request.files:
            return jsonify({'error': 'No file provided'}), 400
        
        file = request.files['file']
        job_type = request.form.get('job_type', 'both')
        
        if file.filename == '':
            return jsonify({'error': 'No filename provided'}), 400
        
        if not allowed_file(file.filename):
            return jsonify({
                'error': 'File type not allowed',
                'allowed_types': list(ALLOWED_EXTENSIONS)
            }), 400
    
        if job_type not in ['stats', 'ml', 'both']:
            return jsonify({
                'error': 'Invalid job_type',
                'allowed_types': ['stats', 'ml', 'both']
            }), 400
        
        filename = secure_filename(file.filename)
        upload_service = UploadService()
        
        result = upload_service.save_and_validate(
            file=file,
            filename=filename,
            job_type=job_type
        )
        
        logger.info(f'File uploaded successfully: {filename}')
        return jsonify(result), 200
        #^-_-^
    except Exception as e:
        logger.error(f'Upload error: {str(e)}')
        return jsonify({'error': 'Upload failed', 'message': str(e)}), 500

@upload_bp.route('/upload/validate', methods=['POST'])
def validate_file():
    """
    Validate uploaded file without processing
    
    Request:
        - file: The file to validate
    
    Response:
        - valid: Boolean indicating if file is valid
        - details: Validation details
    """
    try:
        if 'file' not in request.files:
            return jsonify({'error': 'No file provided'}), 400
        
        file = request.files['file']
        upload_service = UploadService()
        
        validation_result = upload_service.validate_file(file)
        return jsonify(validation_result), 200
        
    except Exception as e:
        logger.error(f'Validation error: {str(e)}')
        return jsonify({'error': 'Validation failed', 'message': str(e)}), 500

@upload_bp.route('/upload/formats', methods=['GET'])
def supported_formats():
    """Get list of supported file formats"""
    return jsonify({
        'supported_formats': list(ALLOWED_EXTENSIONS),
        'max_file_size_gb': int(os.getenv('MAX_UPLOAD_SIZE', 5368709120)) / (1024**3)
    }), 200
