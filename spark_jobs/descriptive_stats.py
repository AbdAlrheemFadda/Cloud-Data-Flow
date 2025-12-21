"""
PySpark Descriptive Statistics Job
Computes comprehensive descriptive statistics on uploaded datasets
"""

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
import sys

def create_spark_session():
    """Create and return Spark session"""
    return SparkSession.builder \
        .appName("DescriptiveStatsJob") \
        .getOrCreate()

def get_basic_stats(spark, df):
    """
    Get basic statistics: row count, column count, data types
    """
    stats = {
        'rows': df.count(),
        'columns': len(df.columns),
        'column_names': df.columns,
        'data_types': {field.name: field.dataType.simpleString() for field in df.schema.fields}
    }
    return stats

def get_numerical_stats(df):
    """
    Get numerical statistics: min, max, mean, stddev for numerical columns
    """
    numerical_cols = [field.name for field in df.schema.fields 
                     if field.dataType.simpleString() in ['int', 'long', 'float', 'double']]
    
    stats = {}
    for col in numerical_cols:
        col_stats = df.select(
            F.min(col).alias('min'),
            F.max(col).alias('max'),
            F.mean(col).alias('mean'),
            F.stddev(col).alias('stddev'),
            F.percentile_approx(col, 0.25).alias('q1'),
            F.percentile_approx(col, 0.5).alias('median'),
            F.percentile_approx(col, 0.75).alias('q3')
        ).collect()[0]
        
        stats[col] = {
            'min': col_stats['min'],
            'max': col_stats['max'],
            'mean': col_stats['mean'],
            'stddev': col_stats['stddev'],
            'q1': col_stats['q1'],
            'median': col_stats['median'],
            'q3': col_stats['q3']
        }
    
    return stats

def get_categorical_stats(df):
    """
    Get categorical statistics: unique values, null counts
    """
    stats = {}
    
    for col in df.columns:
        unique_count = df.select(col).distinct().count()
        null_count = df.filter(F.col(col).isNull()).count()
        total_count = df.count()
        null_percentage = (null_count / total_count) * 100 if total_count > 0 else 0
        
        stats[col] = {
            'unique_values': unique_count,
            'null_count': null_count,
            'null_percentage': round(null_percentage, 2),
            'non_null_count': total_count - null_count
        }

        # Get top values for categorical columns
        if df.select(col).distinct().count() <= 20:
            top_values = df.groupBy(col).count().orderBy(F.desc('count')).collect()
            stats[col]['top_values'] = [(row[col], row['count']) for row in top_values[:5]]
    
    return stats #iffffffff baddddd dayyy ^-___-^

def get_missing_data_analysis(df):
    """
    Analyze missing data patterns
    """
    total_rows = df.count()
    total_cells = total_rows * len(df.columns)
    
    missing_stats = {}
    total_missing = 0
    
    for col in df.columns:
        null_count = df.filter(F.col(col).isNull()).count()
        null_percentage = (null_count / total_rows) * 100 if total_rows > 0 else 0
        missing_stats[col] = {
            'null_count': null_count,
            'null_percentage': round(null_percentage, 2)
        }
        total_missing += null_count
    
    overall_missing_percentage = (total_missing / total_cells) * 100 if total_cells > 0 else 0
    
    return {
        'overall_missing_percentage': round(overall_missing_percentage, 2),
        'column_missing': missing_stats,
        'total_cells': total_cells,
        'missing_cells': total_missing
    }

def run_descriptive_stats(file_path):
    """
    Run complete descriptive statistics job
    """
    try:
        spark = create_spark_session()
        
        # Load data
        if file_path.endswith('.csv'):
            df = spark.read.option("header", "true").csv(file_path)
        elif file_path.endswith('.json'):
            df = spark.read.json(file_path)
        elif file_path.endswith('.parquet'):
            df = spark.read.parquet(file_path)
        else:
            df = spark.read.text(file_path)
        
        # Compute all statistics
        results = {
            'basic_stats': get_basic_stats(spark, df),
            'numerical_stats': get_numerical_stats(df),
            'categorical_stats': get_categorical_stats(df),
            'missing_data_analysis': get_missing_data_analysis(df)
        }
        
        return results
    
    except Exception as e:
        print(f"Error running descriptive stats: {str(e)}")
        raise

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Usage: spark-submit descriptive_stats.py <file_path>")
        sys.exit(1)
    
    file_path = sys.argv[1]
    results = run_descriptive_stats(file_path)
    
    # Print results as JSON for ingestion
    import json
    print(json.dumps(results, indent=2, default=str))
