"""
Databricks API integration service
"""

import os
import logging
import json
import requests
from datetime import datetime

logger = logging.getLogger(__name__)

class DatabricksService:
    """Service for interacting with Databricks API"""
    
    def __init__(self):
        self.host = os.getenv('DATABRICKS_HOST')
        self.token = os.getenv('DATABRICKS_TOKEN')
        self.workspace_id = os.getenv('DATABRICKS_WORKSPACE_ID')
        
        if not all([self.host, self.token]):
            logger.warning('Databricks credentials not configured')
        
        self.base_url = f'{self.host}/api/2.1'
        self.headers = {
            'Authorization': f'Bearer {self.token}',
            'Content-Type': 'application/json'
        }
    
    def submit_job(self, job_config):
        """
        Submit a job to Databricks
        
        Args:
            job_config: Job configuration dictionary
        
        Returns:
            Databricks job ID
        """
        try:
            # Construct Databricks job configuration
            databricks_job = {
                'name': f"data-processing-{job_config['job_id']}",
                'new_cluster': {
                    'spark_version': os.getenv('CLUSTER_SPARK_VERSION', '11.3.x-scala2.12'),
                    'node_type_id': os.getenv('CLUSTER_WORKER_NODE_TYPE_ID', 'i3.xlarge'),
                    'num_workers': int(os.getenv('CLUSTER_INITIAL_WORKERS', 2)),
                    'aws_attributes': {
                        'availability': 'SPOT'
                    }
                },
                'spark_python_task': {
                    'python_file': f'dbfs:/jobs/processing_{job_config["job_type"]}.py',
                    'parameters': [
                        job_config['file_id'],
                        ','.join(job_config.get('ml_algorithms', []))
                    ]
                },
                'timeout_seconds': int(os.getenv('JOB_TIMEOUT', 3600)),
                'max_retries': int(os.getenv('JOB_MAX_RETRIES', 3))
            }
            
            # Submit to Databricks (mock if credentials not available)
            if self.token and self.host:
                response = requests.post(
                    f'{self.base_url}/jobs/create',
                    headers=self.headers,
                    json=databricks_job
                )
                response.raise_for_status()
                job_id = response.json()['job_id']
                logger.info(f'Job submitted to Databricks: {job_id}')
                return job_id
            else:
                # Mock job ID for testing
                mock_job_id = f"mock-{job_config['job_id'][:8]}"
                logger.info(f'Mock job submission: {mock_job_id}')
                return mock_job_id
        
        except Exception as e:
            logger.error(f'Error submitting job to Databricks: {str(e)}')
            raise
    
    def get_job_status(self, databricks_job_id):
        """
        Get job status from Databricks
        
        Args:
            databricks_job_id: Databricks job ID
        
        Returns:
            Job status information
        """
        try:
            if self.token and self.host:
                response = requests.get(
                    f'{self.base_url}/jobs/get?job_id={databricks_job_id}',
                    headers=self.headers
                )
                response.raise_for_status()
                job_data = response.json()
                
                return {
                    'status': job_data.get('state', 'UNKNOWN'),
                    'progress': job_data.get('progress_percentage', 0),
                    'details': job_data
                }
            else:
                # Mock status for testing
                return {
                    'status': 'RUNNING',
                    'progress': 50,
                    'details': {}
                }
        
        except Exception as e:
            logger.error(f'Error getting job status: {str(e)}')
            return {
                'status': 'ERROR',
                'progress': 0,
                'details': {'error': str(e)}
            }
    
    def cancel_job(self, databricks_job_id):
        """
        Cancel a Databricks job
        
        Args:
            databricks_job_id: Databricks job ID
        """
        try:
            if self.token and self.host:
                response = requests.post(
                    f'{self.base_url}/jobs/cancel',
                    headers=self.headers,
                    json={'job_id': databricks_job_id}
                )
                response.raise_for_status()
                logger.info(f'Job cancelled: {databricks_job_id}')
        
        except Exception as e:
            logger.error(f'Error cancelling job: {str(e)}')
            raise
    
    def get_job_logs(self, databricks_job_id):
        """
        Get logs for a Databricks job
        
        Args:
            databricks_job_id: Databricks job ID
        
        Returns:
            Job logs
        """
        try:
            if self.token and self.host:
                response = requests.get(
                    f'{self.base_url}/jobs/runs/list?job_id={databricks_job_id}',
                    headers=self.headers
                )
                response.raise_for_status()
                runs = response.json().get('runs', [])
                
                logs = []
                for run in runs:
                    run_id = run['run_id']
                    log_response = requests.get(
                        f'{self.base_url}/jobs/runs/get-output?run_id={run_id}',
                        headers=self.headers
                    )
                    if log_response.status_code == 200:
                        logs.append(log_response.json())
                
                return logs
            else:
                return [{'message': 'Mock logs'}]
        
        except Exception as e:
            logger.error(f'Error getting job logs: {str(e)}')
            return []
    
    def list_clusters(self):
        """Get list of available clusters"""
        try:
            if self.token and self.host:
                response = requests.get(
                    f'{self.base_url}/clusters/list',
                    headers=self.headers
                )
                response.raise_for_status()
                return response.json().get('clusters', [])
            else:
                return []
        
        except Exception as e:
            logger.error(f'Error listing clusters: {str(e)}')
            return []
    
    def create_cluster(self, cluster_name, num_workers=2):
        """
        Create a new cluster
        
        Args:
            cluster_name: Name for the cluster
            num_workers: Number of worker nodes
        
        Returns:
            Cluster ID
        """
        try:
            if self.token and self.host:
                cluster_config = {
                    'cluster_name': cluster_name,
                    'spark_version': os.getenv('CLUSTER_SPARK_VERSION', '11.3.x-scala2.12'),
                    'node_type_id': os.getenv('CLUSTER_WORKER_NODE_TYPE_ID', 'i3.xlarge'),
                    'num_workers': num_workers
                }
                
                response = requests.post(
                    f'{self.base_url}/clusters/create',
                    headers=self.headers,
                    json=cluster_config
                )
                response.raise_for_status()
                cluster_id = response.json()['cluster_id']
                logger.info(f'Cluster created: {cluster_id}')
                return cluster_id
        
        except Exception as e:
            logger.error(f'Error creating cluster: {str(e)}')
            raise
