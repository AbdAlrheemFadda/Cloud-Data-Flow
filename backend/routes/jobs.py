"""
Job submission, monitoring, and management endpoints
"""
import logging
import uuid
from flask import Blueprint, request, jsonify
from services.job_service import JobService
from services.databricks_service import DatabricksService

logger = logging.getLogger(__name__)
jobs_bp = Blueprint('jobs', __name__, url_prefix='/api/jobs')

job_service = JobService()
databricks_service = DatabricksService()

@jobs_bp.route('/submit', methods=['POST'])
def submit_job():
    """
    Submit a processing job to Databricks
    Request:
        - file_id: ID of uploaded file
        - job_type: Type of job (stats, ml, both)
        - ml_algorithms: List of ML algorithms to run
    Response:
        - job_id: Unique job identifier
        - status: Job status (submitted)
        - file_id: Associated file ID
    """
    try:
        data = request.get_json()
        
        if not data.get('file_id'):
            return jsonify({'error': 'file_id is required'}), 400
        
        job_type = data.get('job_type', 'both')
        ml_algorithms = data.get('ml_algorithms', [])
        
        job_id = str(uuid.uuid4())
        
        result = job_service.submit_job(
            job_id=job_id,
            file_id=data['file_id'],
            job_type=job_type,
            ml_algorithms=ml_algorithms
        )
        
        logger.info(f'Job submitted: {job_id}')
        return jsonify({
            'job_id': job_id,
            'status': 'submitted',
            'file_id': data['file_id']
        }), 201
        
    except Exception as e:
        logger.error(f'Job submission error: {str(e)}')
        return jsonify({'error': 'Job submission failed', 'message': str(e)}), 500

@jobs_bp.route('/<job_id>/status', methods=['GET'])
def get_job_status(job_id):
    """
    Get status of a submitted job
    
    Response:
        - job_id: Job identifier
        - status: Current status (submitted, running, completed, failed)
        - progress: Completion percentage
        - details: Additional status details
    """
    try:
        status = job_service.get_job_status(job_id)
        return jsonify(status), 200
        
    except Exception as e:
        logger.error(f'Status retrieval error: {str(e)}')
        return jsonify({'error': 'Status retrieval failed', 'message': str(e)}), 500

@jobs_bp.route('/<job_id>/cancel', methods=['POST'])
def cancel_job(job_id):
    """
    Cancel a running job
    
    Response:
        - job_id: Job identifier
        - status: New status (cancelled)
    """
    try:
        result = job_service.cancel_job(job_id)
        logger.info(f'Job cancelled: {job_id}')
        return jsonify(result), 200
        
    except Exception as e:
        logger.error(f'Job cancellation error: {str(e)}')
        return jsonify({'error': 'Cancellation failed', 'message': str(e)}), 500

@jobs_bp.route('/list', methods=['GET'])
def list_jobs():
    """
    List all jobs (with optional filtering)
    
    Query Parameters:
        - status: Filter by status (optional)
        - limit: Maximum results (default: 50)
        - offset: Pagination offset (default: 0)
    
    Response:
        - jobs: List of job objects
        - total: Total number of jobs
    """
    try:
        status_filter = request.args.get('status')
        limit = int(request.args.get('limit', 50))
        offset = int(request.args.get('offset', 0))
        
        jobs = job_service.list_jobs(status_filter, limit, offset)
        return jsonify(jobs), 200
        
    except Exception as e:
        logger.error(f'Job listing error: {str(e)}')
        return jsonify({'error': 'Job listing failed', 'message': str(e)}), 500

@jobs_bp.route('/<job_id>/logs', methods=['GET'])
def get_job_logs(job_id):
    """
    Get logs for a job
    Response:
        - job_id: Job identifier
        - logs: Job execution logs
    """
    try:
        logs = job_service.get_job_logs(job_id)
        return jsonify({
            'job_id': job_id,
            'logs': logs
        }), 200
        
    except Exception as e:
        logger.error(f'Log retrieval error: {str(e)}')
        return jsonify({'error': 'Log retrieval failed', 'message': str(e)}), 500

@jobs_bp.route('/algorithms', methods=['GET'])
def get_available_algorithms():
    """
    Get list of available ML algorithms
    
    Response:
        - descriptive_stats: Available stats options
        - ml_algorithms: Available ML algorithms
    """
    return jsonify({
        'descriptive_stats': [
            'row_column_count',
            'data_types',
            'numerical_stats',
            'unique_values',
            'null_percentages'
        ],
        'ml_algorithms': [
            'linear_regression',
            'decision_tree_regression',
            'kmeans_clustering',
            'fpgrowth_mining',
            'time_series_aggregation'
        ]
    }), 200
