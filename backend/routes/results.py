"""
Results retrieval and download endpoints
"""

import logging
from flask import Blueprint, jsonify, send_file
from services.results_service import ResultsService

logger = logging.getLogger(__name__)
results_bp = Blueprint('results', __name__, url_prefix='/api/results')

results_service = ResultsService()

@results_bp.route('/<job_id>', methods=['GET'])
def get_results(job_id):
    """
    Get results for a completed job
    
    Response:
        - job_id: Job identifier
        - descriptive_stats: Stats results (if applicable)
        - ml_results: ML job results (if applicable)
        - completion_time: Job completion time
    """
    try:
        results = results_service.get_results(job_id)
        return jsonify(results), 200
        
    except Exception as e:
        logger.error(f'Results retrieval error: {str(e)}')
        return jsonify({'error': 'Results retrieval failed', 'message': str(e)}), 500

@results_bp.route('/<job_id>/download', methods=['GET'])
def download_results(job_id):
    """
    Download results as a file
    
    Query Parameters:
        - format: Output format (json, csv, excel, pdf) - default: json
    
    Response:
        - File download
    """
    try:
        file_format = request.args.get('format', 'json').lower()
        
        if file_format not in ['json', 'csv', 'excel', 'pdf']:
            return jsonify({'error': 'Invalid format'}), 400
        
        file_path = results_service.export_results(job_id, file_format)
        return send_file(file_path, as_attachment=True)
        
    except Exception as e:
        logger.error(f'Export error: {str(e)}')
        return jsonify({'error': 'Export failed', 'message': str(e)}), 500

@results_bp.route('/<job_id>/stats', methods=['GET'])
def get_stats_results(job_id):
    """
    Get only descriptive statistics results
    
    Response:
        - job_id: Job identifier
        - stats: Descriptive statistics
    """
    try:
        stats = results_service.get_descriptive_stats(job_id)
        return jsonify({
            'job_id': job_id,
            'stats': stats
        }), 200
        
    except Exception as e:
        logger.error(f'Stats retrieval error: {str(e)}')
        return jsonify({'error': 'Stats retrieval failed', 'message': str(e)}), 500

@results_bp.route('/<job_id>/ml', methods=['GET'])
def get_ml_results(job_id):
    """
    Get only ML job results
    
    Response:
        - job_id: Job identifier
        - ml_results: ML job results
        - model_metrics: Performance metrics
    """
    try:
        ml_results = results_service.get_ml_results(job_id)
        return jsonify({
            'job_id': job_id,
            'ml_results': ml_results
        }), 200
        
    except Exception as e:
        logger.error(f'ML results retrieval error: {str(e)}')
        return jsonify({'error': 'ML results retrieval failed', 'message': str(e)}), 500

@results_bp.route('/<job_id>/summary', methods=['GET'])
def get_results_summary(job_id):
    """
    Get a summary of results
    
    Response:
        - job_id: Job identifier
        - summary: Results summary
        - data_shape: Original data shape
        - processing_time: Total processing time
    """
    try:
        summary = results_service.get_results_summary(job_id)
        return jsonify(summary), 200
        
    except Exception as e:
        logger.error(f'Summary retrieval error: {str(e)}')
        return jsonify({'error': 'Summary retrieval failed', 'message': str(e)}), 500
