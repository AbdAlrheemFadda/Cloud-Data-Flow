"""
Health check endpoint
"""

from flask import Blueprint, jsonify

health_bp = Blueprint('health', __name__, url_prefix='/api')

@health_bp.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({
        'status': 'healthy',
        'service': 'Databricks Data Processing Service',
        'version': '1.0.0'
    }), 200

@health_bp.route('/version', methods=['GET'])
def version():
    """Get service version"""
    return jsonify({
        'version': '1.0.0',
        'name': 'Databricks Data Processing Service'
    }), 200
