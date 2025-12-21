"""
Job service for managing Databricks job submissions and monitoring
"""

import logging
import json
from datetime import datetime
from services.databricks_service import DatabricksService

logger = logging.getLogger(__name__)

class JobService:
    """Service for managing distributed jobs"""
    
    def __init__(self):
        self.databricks = DatabricksService()
        self.jobs_store = {}  # In-memory job tracking (use database in production)
    
    def submit_job(self, job_id, file_id, job_type, ml_algorithms=None):
        """
        Submit a job to Databricks
        
        Args:
            job_id: Unique job identifier
            file_id: ID of uploaded file
            job_type: Type of job (stats, ml, both)
            ml_algorithms: List of ML algorithms to run
        
        Returns:
            Job submission result
        """
        try:
            if ml_algorithms is None:
                ml_algorithms = []
            
            # Create job configuration
            job_config = {
                'job_id': job_id,
                'file_id': file_id,
                'job_type': job_type,
                'ml_algorithms': ml_algorithms,
                'status': 'submitted',
                'submitted_at': datetime.utcnow().isoformat(),
                'progress': 0
            }
            
            # Submit to Databricks
            databricks_job_id = self.databricks.submit_job(job_config)
            
            # Store job info
            job_config['databricks_job_id'] = databricks_job_id
            self.jobs_store[job_id] = job_config
            
            logger.info(f'Job submitted to Databricks: {job_id} -> {databricks_job_id}')
            return job_config
        
        except Exception as e:
            logger.error(f'Error submitting job: {str(e)}')
            raise
    
    def get_job_status(self, job_id):
        """
        Get job status
        
        Args:
            job_id: Job identifier
        
        Returns:
            Job status information
        """
        try:
            if job_id not in self.jobs_store:
                return {
                    'job_id': job_id,
                    'status': 'not_found',
                    'message': 'Job not found'
                }
            
            job_info = self.jobs_store[job_id]
            databricks_job_id = job_info.get('databricks_job_id')
            
            # Get actual status from Databricks
            if databricks_job_id:
                status = self.databricks.get_job_status(databricks_job_id)
                job_info['status'] = status.get('status', 'unknown')
                job_info['progress'] = status.get('progress', 0)
            
            return job_info
        
        except Exception as e:
            logger.error(f'Error getting job status: {str(e)}')
            raise
    
    def cancel_job(self, job_id):
        """
        Cancel a job
        
        Args:
            job_id: Job identifier
        
        Returns:
            Cancellation result
        """
        try:
            if job_id not in self.jobs_store:
                raise ValueError(f'Job {job_id} not found')
            
            job_info = self.jobs_store[job_id]
            databricks_job_id = job_info.get('databricks_job_id')
            
            if databricks_job_id:
                self.databricks.cancel_job(databricks_job_id)
            
            job_info['status'] = 'cancelled'
            logger.info(f'Job cancelled: {job_id}')
            
            return {
                'job_id': job_id,
                'status': 'cancelled'
            }
        
        except Exception as e:
            logger.error(f'Error cancelling job: {str(e)}')
            raise
    
    def list_jobs(self, status_filter=None, limit=50, offset=0):
        """
        List all jobs
        
        Args:
            status_filter: Filter by status (optional)
            limit: Maximum results
            offset: Pagination offset
        
        Returns:
            List of jobs
        """
        try:
            jobs = list(self.jobs_store.values())
            
            if status_filter:
                jobs = [j for j in jobs if j['status'] == status_filter]
            
            total = len(jobs)
            paginated_jobs = jobs[offset:offset + limit]
            
            return {
                'jobs': paginated_jobs,
                'total': total,
                'limit': limit,
                'offset': offset
            }
        
        except Exception as e:
            logger.error(f'Error listing jobs: {str(e)}')
            raise
    
    def get_job_logs(self, job_id):
        """
        Get logs for a job
        
        Args:
            job_id: Job identifier
        
        Returns:
            Job logs
        """
        try:
            if job_id not in self.jobs_store:
                raise ValueError(f'Job {job_id} not found')
            
            job_info = self.jobs_store[job_id]
            databricks_job_id = job_info.get('databricks_job_id')
            
            if databricks_job_id:
                logs = self.databricks.get_job_logs(databricks_job_id)
                return logs
            
            return []
        
        except Exception as e:
            logger.error(f'Error getting job logs: {str(e)}')
            raise
