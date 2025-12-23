"""
Flask backend Application for Databricks Data Processing Service
Main entry point for the web service
"""

import os
import logging
from dotenv import load_dotenv
from flask import Flask, jsonify
from flask_cors import CORS
load_dotenv()

logging.basicConfig(
    level=os.getenv('LOG_LEVEL', 'INFO'),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)

app.config['MAX_CONTENT_LENGTH'] = int(os.getenv('MAX_UPLOAD_SIZE', 5368709120))
app.config['UPLOAD_FOLDER'] = os.path.join(os.path.dirname(__file__), 'uploads')
app.config['SECRET_KEY'] = os.getenv('SECRET_KEY', 'dev-secret-key')

os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)

from routes.upload import upload_bp
from routes.jobs import jobs_bp
from routes.results import results_bp
from routes.health import health_bp

app.register_blueprint(health_bp)
app.register_blueprint(upload_bp)
app.register_blueprint(jobs_bp)
app.register_blueprint(results_bp)

# Error handlers ^-_-^
@app.errorhandler(400)
def bad_request(error):
    """Handle bad request errors"""
    return jsonify({'error': 'Bad request', 'message': str(error)}), 400

@app.errorhandler(404)
def not_found(error):
    """Handle not found errors"""
    return jsonify({'error': 'Not found', 'message': 'Resource not found'}), 404

@app.errorhandler(500)
def internal_error(error):
    """Handle internal server errors"""
    logger.error(f'Internal server error: {error}')
    return jsonify({'error': 'Internal server error', 'message': str(error)}), 500

@app.errorhandler(413)
def file_too_large(error):
    """Handle file too large errors"""
    max_size_gb = int(os.getenv('MAX_UPLOAD_SIZE', 5368709120)) / (1024**3)
    return jsonify({
        'error': 'File too large',
        'message': f'Maximum file size is {max_size_gb}GB'
    }), 413

if __name__ == '__main__':
    port = int(os.getenv('API_PORT', 5000))
    debug = os.getenv('FLASK_DEBUG', 'False').lower() == 'true'
    
    logger.info(f'Starting Flask app on port {port}')
    app.run(
        host='0.0.0.0',
        port=port,
        debug=debug
    )

