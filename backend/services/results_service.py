"""
Results service for retrieving and exporting job results
"""

import logging
import json
from datetime import datetime
import pandas as pd

logger = logging.getLogger(__name__)

class ResultsService:
    """Service for managing job results"""
    
    def __init__(self):
        self.results_store = {}  # In-memory results (use database in production)
    
    def get_results(self, job_id):
        """
        Get results for a job
        
        Args:
            job_id: Job identifier
        
        Returns:
            Job results
        """
        try:
            if job_id not in self.results_store:
                return {
                    'job_id': job_id,
                    'status': 'not_found',
                    'message': 'Results not found'
                }
            
            return self.results_store[job_id]
        
        except Exception as e:
            logger.error(f'Error getting results: {str(e)}')
            raise
    
    def get_descriptive_stats(self, job_id):
        """
        Get descriptive statistics from results
        
        Args:
            job_id: Job identifier
        
        Returns:
            Descriptive statistics
        """
        try:
            if job_id not in self.results_store:
                return {}
            
            results = self.results_store[job_id]
            return results.get('descriptive_stats', {})
        
        except Exception as e:
            logger.error(f'Error getting stats: {str(e)}')
            raise
    
    def get_ml_results(self, job_id):
        """
        Get ML job results
        
        Args:
            job_id: Job identifier
        
        Returns:
            ML results
        """
        try:
            if job_id not in self.results_store:
                return {}
            
            results = self.results_store[job_id]
            return results.get('ml_results', {})
        
        except Exception as e:
            logger.error(f'Error getting ML results: {str(e)}')
            raise
    
    def get_results_summary(self, job_id):
        """
        Get summary of results
        
        Args:
            job_id: Job identifier
        
        Returns:
            Results summary
        """
        try:
            if job_id not in self.results_store:
                return {
                    'job_id': job_id,
                    'status': 'not_found'
                }
            
            results = self.results_store[job_id]
            
            return {
                'job_id': job_id,
                'status': results.get('status'),
                'summary': {
                    'data_shape': results.get('data_shape'),
                    'processing_time': results.get('processing_time'),
                    'algorithms_run': results.get('algorithms_run', []),
                    'completion_time': results.get('completion_time')
                }
            }
        
        except Exception as e:
            logger.error(f'Error getting summary: {str(e)}')
            raise
    
    def export_results(self, job_id, file_format='json'):
        """
        Export results to file
        
        Args:
            job_id: Job identifier
            file_format: Export format (json, csv, excel, pdf)
        
        Returns:
            File path
        """
        try:
            if job_id not in self.results_store:
                raise ValueError(f'Results not found for job {job_id}')
            
            results = self.results_store[job_id]
            filename = f'results_{job_id}.{file_format}'
            filepath = f'exports/{filename}'
            
            # Create exports directory if needed
            import os
            os.makedirs('exports', exist_ok=True)
            
            if file_format == 'json':
                with open(filepath, 'w') as f:
                    json.dump(results, f, indent=2, default=str)
            
            elif file_format == 'csv':
                # Convert results to DataFrame
                df = pd.DataFrame([results])
                df.to_csv(filepath, index=False)
            
            elif file_format == 'excel':
                # Export to Excel
                if 'descriptive_stats' in results:
                    stats_df = pd.DataFrame(results['descriptive_stats'], index=[0])
                    stats_df.to_excel(filepath, index=False, sheet_name='Stats')
            
            elif file_format == 'pdf':
                # For PDF, create a summary
                try:
                    from reportlab.lib.pagesizes import letter
                    from reportlab.pdfgen import canvas
                    
                    c = canvas.Canvas(filepath, pagesize=letter)
                    y = 750
                    
                    c.drawString(50, y, f"Results Report - {job_id}")
                    y -= 20
                    c.drawString(50, y, f"Generated: {datetime.utcnow().isoformat()}")
                    y -= 20
                    
                    c.save()
                except ImportError:
                    logger.warning('reportlab not available, creating JSON instead')
                    with open(filepath.replace('.pdf', '.json'), 'w') as f:
                        json.dump(results, f, indent=2, default=str)
            
            logger.info(f'Results exported: {filepath}')
            return filepath
        
        except Exception as e:
            logger.error(f'Error exporting results: {str(e)}')
            raise
    
    def store_results(self, job_id, results):
        """
        Store job results
        
        Args:
            job_id: Job identifier
            results: Results data
        """
        try:
            self.results_store[job_id] = results
            logger.info(f'Results stored for job: {job_id}')
        
        except Exception as e:
            logger.error(f'Error storing results: {str(e)}')
            raise
