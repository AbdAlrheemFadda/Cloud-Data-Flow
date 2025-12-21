"""
PySpark Machine Learning Jobs
Implements multiple ML algorithms for distributed processing
"""

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.ml.regression import LinearRegression, DecisionTreeRegressor
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.evaluation import RegressionEvaluator, ClusteringEvaluator
from pyspark.ml import Pipeline
from pyspark.mllib.fpm import FPGrowth
import time
import sys
import json

def create_spark_session():
    """Create and return Spark session"""
    return SparkSession.builder \
        .appName("MLJobsService") \
        .getOrCreate()

class LinearRegressionJob:
    """Linear Regression ML Job"""
    
    @staticmethod
    def run(df, label_col, feature_cols):
        """Run linear regression"""
        start_time = time.time()
        
        try:
            # Prepare features
            assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
            df_assembled = assembler.transform(df)
            
            # Split data
            train_data, test_data = df_assembled.randomSplit([0.8, 0.2], seed=42)
            
            # Train model
            lr = LinearRegression(labelCol=label_col, featuresCol="features")
            model = lr.fit(train_data)
            
            # Make predictions
            predictions = model.transform(test_data)
            
            # Evaluate
            evaluator = RegressionEvaluator(labelCol=label_col, predictionCol="prediction", metricName="rmse")
            rmse = evaluator.evaluate(predictions)
            
            r2_evaluator = RegressionEvaluator(labelCol=label_col, predictionCol="prediction", metricName="r2")
            r2 = r2_evaluator.evaluate(predictions)
            
            execution_time = time.time() - start_time
            
            return {
                'algorithm': 'linear_regression',
                'metrics': {
                    'rmse': float(rmse),
                    'r2': float(r2),
                    'coefficients': [float(x) for x in model.coefficients],
                    'intercept': float(model.intercept)
                },
                'execution_time': execution_time,
                'training_samples': train_data.count(),
                'test_samples': test_data.count()
            }
        except Exception as e:
            return {'algorithm': 'linear_regression', 'error': str(e)}

class DecisionTreeRegressionJob:
    """Decision Tree Regression ML Job"""
    
    @staticmethod
    def run(df, label_col, feature_cols):
        """Run decision tree regression"""
        start_time = time.time()
        
        try:
            # Prepare features
            assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
            df_assembled = assembler.transform(df)
            
            # Split data
            train_data, test_data = df_assembled.randomSplit([0.8, 0.2], seed=42)
            
            # Train model
            dt = DecisionTreeRegressor(labelCol=label_col, featuresCol="features")
            model = dt.fit(train_data)
            
            # Make predictions
            predictions = model.transform(test_data)
            
            # Evaluate
            evaluator = RegressionEvaluator(labelCol=label_col, predictionCol="prediction", metricName="rmse")
            rmse = evaluator.evaluate(predictions)
            
            r2_evaluator = RegressionEvaluator(labelCol=label_col, predictionCol="prediction", metricName="r2")
            r2 = r2_evaluator.evaluate(predictions)
            
            execution_time = time.time() - start_time
            
            return {
                'algorithm': 'decision_tree_regression',
                'metrics': {
                    'rmse': float(rmse),
                    'r2': float(r2),
                    'max_depth': model.depth,
                    'num_nodes': model.numNodes
                },
                'execution_time': execution_time,
                'training_samples': train_data.count(),
                'test_samples': test_data.count()
            }
        except Exception as e:
            return {'algorithm': 'decision_tree_regression', 'error': str(e)}

class KMeansClusteringJob:
    """K-Means Clustering ML Job"""
    
    @staticmethod
    def run(df, feature_cols, k=3):
        """Run K-Means clustering"""
        start_time = time.time()
        
        try:
            # Prepare features
            assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
            df_assembled = assembler.transform(df)
            
            # Standardize features
            scaler = StandardScaler(inputCol="features", outputCol="scaled_features")
            df_scaled = scaler.fit(df_assembled).transform(df_assembled)
            
            # Train model
            kmeans = KMeans(k=k, seed=42, featuresCol="scaled_features")
            model = kmeans.fit(df_scaled)
            
            # Get predictions
            predictions = model.transform(df_scaled)
            
            # Calculate silhouette score
            evaluator = ClusteringEvaluator(featuresCol="scaled_features", predictionCol="prediction")
            silhouette = evaluator.evaluate(predictions)
            
            # Get cluster sizes
            cluster_sizes = predictions.groupBy("prediction").count().collect()
            cluster_distribution = {int(row["prediction"]): int(row["count"]) for row in cluster_sizes}
            
            execution_time = time.time() - start_time
            
            return {
                'algorithm': 'kmeans_clustering',
                'metrics': {
                    'silhouette_score': float(silhouette),
                    'num_clusters': k,
                    'cluster_distribution': cluster_distribution,
                    'cluster_centers': [[float(x) for x in center] for center in model.clusterCenters()]
                },
                'execution_time': execution_time,
                'num_samples': df_scaled.count()
            }
        except Exception as e:
            return {'algorithm': 'kmeans_clustering', 'error': str(e)}

class TimeSeriesAggregationJob:
    """Time Series Aggregation ML Job"""
    
    @staticmethod
    def run(df, timestamp_col, value_col, freq='daily'):
        """Run time series aggregation"""
        start_time = time.time()
        
        try:
            # Validate columns exist
            if timestamp_col not in df.columns or value_col not in df.columns:
                raise ValueError(f"Columns {timestamp_col} or {value_col} not found")
            
            # Convert timestamp if needed
            if freq == 'daily':
                df_ts = df.groupBy(F.to_date(F.col(timestamp_col)).alias("date")).agg(
                    F.sum(value_col).alias("sum"),
                    F.avg(value_col).alias("avg"),
                    F.min(value_col).alias("min"),
                    F.max(value_col).alias("max"),
                    F.count(value_col).alias("count")
                ).orderBy("date")
            
            elif freq == 'weekly':
                df_ts = df.groupBy(F.weekofyear(F.col(timestamp_col)).alias("week")).agg(
                    F.sum(value_col).alias("sum"),
                    F.avg(value_col).alias("avg"),
                    F.min(value_col).alias("min"),
                    F.max(value_col).alias("max"),
                    F.count(value_col).alias("count")
                ).orderBy("week")
            
            elif freq == 'monthly':
                df_ts = df.groupBy(F.trunc(F.col(timestamp_col), "month").alias("month")).agg(
                    F.sum(value_col).alias("sum"),
                    F.avg(value_col).alias("avg"),
                    F.min(value_col).alias("min"),
                    F.max(value_col).alias("max"),
                    F.count(value_col).alias("count")
                ).orderBy("month")
            
            else:
                raise ValueError(f"Unknown frequency: {freq}")
            
            results_list = df_ts.collect()
            results_data = [row.asDict() for row in results_list]
            
            execution_time = time.time() - start_time
            
            return {
                'algorithm': 'time_series_aggregation',
                'frequency': freq,
                'metrics': {
                    'num_periods': len(results_data),
                    'results': results_data
                },
                'execution_time': execution_time
            }
        except Exception as e:
            return {'algorithm': 'time_series_aggregation', 'error': str(e)}

class FPGrowthMiningJob:
    """FP-Growth Association Rule Mining ML Job"""
    
    @staticmethod
    def run(df, transactions_col, min_support=0.3):
        """Run FP-Growth association rule mining"""
        start_time = time.time()
        
        try:
            # Convert to RDD format for FPGrowth
            transactions = df.select(transactions_col).rdd.map(lambda x: x[0].split(',') if isinstance(x[0], str) else [str(x[0])])
            
            # Run FPGrowth
            model = FPGrowth.train(transactions, minSupport=min_support)
            
            # Get frequent itemsets
            frequent_itemsets = model.freqItemsets().collect()
            itemsets_list = []
            for itemset in frequent_itemsets[:50]:  # Limit to top 50
                itemsets_list.append({
                    'items': list(itemset.items),
                    'frequency': int(itemset.freq)
                })
            
            execution_time = time.time() - start_time
            
            return {
                'algorithm': 'fpgrowth_mining',
                'metrics': {
                    'min_support': min_support,
                    'frequent_itemsets': itemsets_list,
                    'num_itemsets': len(frequent_itemsets)
                },
                'execution_time': execution_time,
                'num_transactions': transactions.count()
            }
        except Exception as e:
            return {'algorithm': 'fpgrowth_mining', 'error': str(e)}

def run_ml_jobs(file_path, algorithms_str):
    """
    Run specified ML jobs on data
    """
    spark = create_spark_session()
    
    try:
        # Load data
        if file_path.endswith('.csv'):
            df = spark.read.option("header", "true").csv(file_path)
        elif file_path.endswith('.json'):
            df = spark.read.json(file_path)
        elif file_path.endswith('.parquet'):
            df = spark.read.parquet(file_path)
        else:
            df = spark.read.text(file_path)
        
        algorithms = algorithms_str.split(',') if algorithms_str else []
        results = []
        
        for algo in algorithms:
            algo = algo.strip()
            
            if algo == 'linear_regression':
                result = LinearRegressionJob.run(df, 'label', ['feature1', 'feature2'])
                results.append(result)
            
            elif algo == 'decision_tree_regression':
                result = DecisionTreeRegressionJob.run(df, 'label', ['feature1', 'feature2'])
                results.append(result)
            
            elif algo == 'kmeans_clustering':
                result = KMeansClusteringJob.run(df, ['feature1', 'feature2'], k=3)
                results.append(result)
            
            elif algo == 'time_series_aggregation':
                result = TimeSeriesAggregationJob.run(df, 'timestamp', 'value', 'daily')
                results.append(result)
            
            elif algo == 'fpgrowth_mining':
                result = FPGrowthMiningJob.run(df, 'transactions', 0.3)
                results.append(result)
        
        return {'ml_results': results}
    
    except Exception as e:
        print(f"Error running ML jobs: {str(e)}")
        raise

if __name__ == '__main__':
    if len(sys.argv) < 3:
        print("Usage: spark-submit ml_jobs.py <file_path> <algorithms>")
        sys.exit(1)
    
    file_path = sys.argv[1]
    algorithms = sys.argv[2]
    results = run_ml_jobs(file_path, algorithms)
    
    print(json.dumps(results, indent=2, default=str))
