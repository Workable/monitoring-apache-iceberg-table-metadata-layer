import json
import boto3
from datetime import datetime, timezone
from pyiceberg.catalog.glue import GlueCatalog
import os
import time
import uuid
import pandas as pd
import numpy as np
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

glue_client = boto3.client('glue')

required_vars = ['CW_NAMESPACE', 'GLUE_SERVICE_ROLE', 'SPARK_CATALOG_S3_WAREHOUSE']
for var in required_vars:
    # Retrieve the environment variable value
    if os.getenv(var) is None:
        # If any variable is not set, raise an exception
        raise EnvironmentError(f"Required environment variable '{var}' is not set.")
    
cw_namespace = os.environ.get('CW_NAMESPACE')
glue_service_role = os.environ.get('GLUE_SERVICE_ROLE')
warehouse_path = os.environ.get('SPARK_CATALOG_S3_WAREHOUSE')

glue_session_tags = {
    "app": "monitor-iceberg"
}

CACHE_FILE_PATH = '/tmp/iceberg_monitoring_cache.json'


def get_cache_expiry_hours():
    """Get cache expiry hours from environment variable or default to 3."""
    env_value = os.getenv('CACHE_EXPIRY_HOURS')
    if env_value is not None:
        try:
            # Try to convert to float first, then to int to handle decimal values
            return int(float(env_value))
        except (ValueError, TypeError):
            logger.warning(f"Invalid CACHE_EXPIRY_HOURS value '{env_value}', using default of 3 hours")
    return 3

def send_custom_metric( metric_name, dimensions, value, unit, namespace, timestamp=None):
    """
    Send a custom metric to AWS CloudWatch.

    :param namespace: The namespace for the metric data.
    :param ts: The ts timestamp.
    :param metric_name: The name of the metric.
    :param dimensions: A list of dictionaries, each containing 'Name' and 'Value' keys for the metric dimensions.
    :param value: The value for the metric.
    :param unit: The unit of the metric.
    """
    cloudwatch = boto3.client('cloudwatch')

    # CloudWatch requires timestamps to be within the past 2 weeks (14 days)
    TWO_WEEKS_MS = 14 * 24 * 60 * 60 * 1000  # 14 days in milliseconds
    current_time_ms = int(datetime.now(timezone.utc).timestamp() * 1000)

    # If timestamp is provided and within 2 weeks, use it; otherwise use current time
    if timestamp and (current_time_ms - timestamp) <= TWO_WEEKS_MS:
        metric_data_timestamp = datetime.fromtimestamp(timestamp / 1000.0, tz=timezone.utc)
        logger.debug(f"Using provided timestamp for metric {metric_name}: {metric_data_timestamp}")
    else:
        metric_data_timestamp = datetime.now(timezone.utc)
        if timestamp:
            logger.warning(f"Provided timestamp for metric {metric_name} is too old ({datetime.fromtimestamp(timestamp / 1000.0, tz=timezone.utc)}), using current time instead")
        else:
            logger.debug(f"No timestamp provided for metric {metric_name}, using current time")

    metric_data = {
        'MetricName': metric_name,
        'Dimensions': dimensions,
        'Value': value,
        'Unit': unit,
        'Timestamp': metric_data_timestamp
    }


    try:
        cloudwatch.put_metric_data(
            Namespace=namespace,
            MetricData=[metric_data]
        )
        logger.debug(f"Metric data sent successfully: {metric_data}")
    except Exception as e:
        logger.error(f"Failed to send metric to CloudWatch. Namespace: {namespace}, MetricData: {metric_data}, Error: {str(e)}")
        raise
    
def wait_for_session(session_id,interval=1):
    while True:
        response = glue_client.get_session(
            Id=session_id
        )
        status = response["Session"]["Status"]
        if status in ['READY','FAILED','TIMEOUT','STOPPED']:
            logger.info(f"Session {session_id} status={status}")
            break
        time.sleep(interval)
        
def wait_for_statement(session_id,statement_id,interval=2):
    while True:
        response = glue_client.get_statement(
            SessionId=session_id,
            Id=statement_id,
        )
        status = response["Statement"]["State"]
        if status in ['AVAILABLE','CANCELLED','ERROR']:
            logger.info(f"Statement status={status}")
            return response
        time.sleep(interval)

    
    
def parse_spark_show_output(output):
    lines = output.strip().split('\n')
    header = lines[1]  # Column names are typically in the second line
    columns = [col.strip() for col in header.split('|') if col.strip()]  # Clean and split by '|'
    data = []
    # Start reading data from the third line and skip the last line which is a border
    for row in lines[3:-1]:
        # Remove border and split
        row_data = [cell.strip() for cell in row.split('|') if cell.strip()]
        if row_data:
            data.append(row_data)

    # Create DataFrame
    return pd.DataFrame(data, columns=columns)   

def send_files_metrics(glue_db_name, glue_table_name, snapshot,session_id):
    sql_stmt = f"SELECT CAST(AVG(record_count) as INT) as avg_record_count, MAX(record_count) as max_record_count, MIN(record_count) as min_record_count, CAST(AVG(file_size_in_bytes) as INT) as avg_file_size, MAX(file_size_in_bytes) as max_file_size, MIN(file_size_in_bytes) as min_file_size FROM glue_catalog.{glue_db_name}.{glue_table_name}.files"
    run_stmt_response = glue_client.run_statement(
        SessionId=session_id,
        Code=f"df = spark.sql(\"{sql_stmt}\");df.show(df.count(),truncate=False)"
    )
    stmt_id = run_stmt_response["Id"]
    logger.info(f"select files statement_id={stmt_id}")
    stmt_response = wait_for_statement(session_id, run_stmt_response["Id"])
    data_str = stmt_response["Statement"]["Output"]["Data"]["TextPlain"]
    logger.info(stmt_response)
    if data_str == "":
        logger.info("No files found")
        return
    df = parse_spark_show_output(data_str)
    df = df.applymap(int)
    file_metrics = {
        "avg_record_count": df.iloc[0]["avg_record_count"],
        "max_record_count": df.iloc[0]["max_record_count"],
        "min_record_count": df.iloc[0]["min_record_count"],
        "avg_file_size": df.iloc[0]['avg_file_size'],
        "max_file_size": df.iloc[0]['max_file_size'],
        "min_file_size": df.iloc[0]['min_file_size'],
    }
    logger.info("file_metrics=")
    logger.info(file_metrics)
    # loop over file_metrics, use key as metric name and value as metric value
    # loop over partition_metrics, use key as metric name and value as metric value
    for metric_name, metric_value in file_metrics.items():     
        logger.info(f"metric_name=files.{metric_name}, metric_value={metric_value.item()}")
        send_custom_metric(
            metric_name=f"files.{metric_name}",
            dimensions=[
                {'Name': 'table_name', 'Value': f"{glue_db_name}.{glue_table_name}"}
            ],
            value=metric_value.item(),
            unit='Bytes' if "size" in metric_name else "Count",
            namespace=os.getenv('CW_NAMESPACE'),
            timestamp = snapshot.timestamp_ms,
        )
    

def send_partition_metrics(glue_db_name, glue_table_name, snapshot,session_id):
    sql_stmt = f"select partition,record_count,file_count from glue_catalog.{glue_db_name}.{glue_table_name}.partitions"    
    run_stmt_response = glue_client.run_statement(
        SessionId=session_id,
        Code=f"df = spark.sql(\"{sql_stmt}\");df.show(df.count(),truncate=False)"
    )
    
    stmt_id = run_stmt_response["Id"]
    logger.info(f"send_partition_metrics() -> statement_id={stmt_id}")
    stmt_response = wait_for_statement(session_id, stmt_id)
    data_str = stmt_response["Statement"]["Output"]["Data"]["TextPlain"]

    if data_str == "":
        logger.info("No partitions found")
        return
    
    df = parse_spark_show_output(data_str)
    partition_metrics = {
        "avg_record_count": df["record_count"].astype(int).mean().astype(int),
        "max_record_count": df["record_count"].astype(int).max(),
        "min_record_count": df["record_count"].astype(int).min(),
        "deviation_record_count": df['record_count'].astype(int).std().round(2),
        "skew_record_count": df['record_count'].astype(int).skew().round(2),
        "avg_file_count": df['file_count'].astype(int).mean().astype(int),
        "max_file_count": df['file_count'].astype(int).max(),
        "min_file_count": df['file_count'].astype(int).min(),
        "deviation_file_count": df['file_count'].astype(int).std().round(2),
        "skew_file_count": df['file_count'].astype(int).skew().round(2), 
    }
    logger.info("partition_metrics=")
    logger.info(partition_metrics)
    
    # loop over aggregated partition_metrics, use key as metric name and value as metric value
    for metric_name, metric_value in partition_metrics.items():  
        logger.info(f"metric_name=partitions.{metric_name}, metric_value={metric_value.item()}")
        send_custom_metric(
            metric_name=f"partitions.{metric_name}",
            dimensions=[
                {'Name': 'table_name', 'Value': f"{glue_db_name}.{glue_table_name}"}
            ],
            value=metric_value.item(),
            unit='Count',
            namespace=os.getenv('CW_NAMESPACE'),
            timestamp = snapshot.timestamp_ms,
        )
    
    for index, row in df.iterrows():
        partition_name = row['partition']
        record_count = row['record_count']
        file_count = row['file_count']
        logger.info(f"partition_name={partition_name}, record_count={record_count}, file_count={file_count}")
        
        send_custom_metric(
            metric_name=f"partitions.record_count",
            dimensions=[
                {'Name': 'table_name', 'Value': f"{glue_db_name}.{glue_table_name}"},
                {'Name': 'partition_name', 'Value': partition_name}
            ],
            value=int(record_count),
            unit='Count',
            namespace=os.getenv('CW_NAMESPACE'),
            timestamp = snapshot.timestamp_ms,
        )
        
        send_custom_metric(
            metric_name=f"partitions.file_count",
            dimensions=[
                {'Name': 'table_name', 'Value': f"{glue_db_name}.{glue_table_name}"},
                {'Name': 'partition_name', 'Value': partition_name}
            ],
            value=int(file_count),
            unit='Count',
            namespace=os.getenv('CW_NAMESPACE'),
            timestamp = snapshot.timestamp_ms,
        )

def get_all_sessions():
    sessions = []
    next_token = None
    
    while True:
        if next_token:
            response = glue_client.list_sessions(Tags=glue_session_tags, NextToken=next_token)
        else:
            response = glue_client.list_sessions(Tags=glue_session_tags)
        
        sessions.extend(response['Sessions'])
        next_token = response.get('NextToken')
        
        if not next_token:
            break
    
    return sessions
    
def create_or_reuse_glue_session():
    session_id = None
    
    glue_sessions = get_all_sessions()
    
    for session in glue_sessions:
        if(session["Status"] == "READY"):
            session_id = session["Id"]
            logger.info(f"Found existing session_id={session_id}")
            break
    
    if(session_id is None):
        generated_uuid_string = str(uuid.uuid4())
        session_id = f"iceberg-metadata-lambda-{generated_uuid_string}"
        logger.info(f"No active Glue session found, creating new glue session with ID = {session_id}")
        glue_client.create_session(
            Id=session_id,
            Role=glue_service_role,
            Command={'Name': 'glueetl', "PythonVersion": "3"},
            Timeout=120,
            DefaultArguments={
                "--enable-glue-datacatalog": "true",
                "--enable-observability-metrics": "true",
                "--conf": f"spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog --conf spark.sql.catalog.glue_catalog.warehouse={warehouse_path} --conf spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog --conf spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
                "--datalake-formats": "iceberg"
            },
            GlueVersion="4.0",
            NumberOfWorkers=2,
            WorkerType="G.1X",
            IdleTimeout=6,
            Tags=glue_session_tags,
        )
        wait_for_session(session_id)
    return session_id


def dt_to_ts(dt_str):
    dt_obj = datetime.fromisoformat(dt_str.replace('Z', '+00:00'))
    timestamp_seconds = dt_obj.timestamp()
    return int(timestamp_seconds * 1000)


def send_snapshot_metrics(glue_db_name, glue_table_name, snapshot_id, session_id):
    logger.info("send_snapshot_metrics")
    sql_stmt = f"select committed_at,snapshot_id,operation,summary from glue_catalog.{glue_db_name}.{glue_table_name}.snapshots where snapshot_id={snapshot_id}"
    logger.debug(sql_stmt)
    run_stmt_response = glue_client.run_statement(
        SessionId=session_id,
        Code=f"df=spark.sql(\"{sql_stmt}\");json_rdd=df.toJSON();json_strings=json_rdd.collect();print(json_strings)"
    )
    stmt_id = run_stmt_response["Id"]
    logger.info(f"send_snapshot_metrics() -> statement_id={stmt_id}")
    stmt_response = wait_for_statement(session_id, stmt_id)
    json_list_str = stmt_response["Statement"]["Output"]["Data"]["TextPlain"].replace("\'", "")
    if json_list_str.strip() == '':
        logger.info("No snapshots info found")
        return
    snapshots = json.loads(json_list_str)
    logger.info("send_snapshot_metrics()->response")
    logger.info(json.dumps(snapshots, indent=4))
    snapshot = snapshots[0]

    metrics = [
        "added-data-files", "added-records", "changed-partition-count", 
        "total-records","total-data-files", "total-delete-files",
        "added-files-size", "total-files-size", "added-position-deletes"
    ]
    for metric in metrics:
        normalized_metric_name = metric.replace("-", "_")
        if snapshot["summary"].get(metric) is None:
            snapshot["summary"][metric] = 0
        metric_value = snapshot["summary"][metric]
        timestamp_ms = dt_to_ts(snapshot["committed_at"])
        logger.info(f"metric_name=snapshot.{normalized_metric_name}, value={metric_value}")
        send_custom_metric(
            metric_name=f"snapshot.{normalized_metric_name}",
            dimensions=[
                {'Name': 'table_name', 'Value': f"{glue_db_name}.{glue_table_name}"},
                {'Name': 'snapshot_id', 'Value': str(snapshot_id)}
            ],
            value=int(metric_value),
            unit='Bytes' if "size" in normalized_metric_name else "Count",
            namespace=os.getenv('CW_NAMESPACE'),
            timestamp = timestamp_ms,
        ) 

# check if glue table is of iceberg format, return boolean
def check_table_is_of_iceberg_format(database_name, table_name):
    response = glue_client.get_table(
        DatabaseName=database_name,
        Name=table_name,
    )
    try:
        return response["Table"]["Parameters"]["table_type"] == "ICEBERG"
    except KeyError:
        logger.warning("check_table_is_of_iceberg_format() -> table_type is missing")
        return False


def load_cache():
    """Load cache from JSON file in /tmp/ directory."""
    if os.path.exists(CACHE_FILE_PATH):
        try:
            with open(CACHE_FILE_PATH, 'r') as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError) as e:
            logger.warning(f"Failed to load cache file: {e}")
    return {}


def save_cache(cache):
    """Save cache to JSON file in /tmp/ directory."""
    try:
        with open(CACHE_FILE_PATH, 'w') as f:
            json.dump(cache, f)
    except IOError as e:
        logger.warning(f"Failed to save cache file: {e}")


def should_skip_execution(database_name, table_name):
    """Check if execution should be skipped based on cache."""
    cache = load_cache()
    cache_key = f"{database_name}.{table_name}"

    if cache_key in cache:
        last_execution_time = cache[cache_key]
        current_time = time.time()
        time_diff_hours = (current_time - last_execution_time) / 3600
        cache_expiry_hours = get_cache_expiry_hours()

        if time_diff_hours < cache_expiry_hours:
            logger.info(f"Skipping execution for {cache_key}. Last execution was {time_diff_hours:.2f} hours ago (expiry: {cache_expiry_hours} hours).")
            return True

    return False


def update_cache(database_name, table_name):
    """Update cache with current execution time."""
    cache = load_cache()
    cache_key = f"{database_name}.{table_name}"
    cache[cache_key] = time.time()
    save_cache(cache)


def get_iceberg_tables_from_database(database_name):
    """Get all Iceberg tables from a Glue database."""
    iceberg_tables = []
    next_token = None

    while True:
        if next_token:
            response = glue_client.get_tables(DatabaseName=database_name, NextToken=next_token)
        else:
            response = glue_client.get_tables(DatabaseName=database_name)

        for table in response['TableList']:
            if check_table_is_of_iceberg_format(database_name, table['Name']):
                iceberg_tables.append(table['Name'])

        next_token = response.get('NextToken')
        if not next_token:
            break

    return iceberg_tables


def process_table_metrics(glue_db_name, glue_table_name):
    """Process metrics for a single table."""
    # Check cache to see if we should skip execution
    if should_skip_execution(glue_db_name, glue_table_name):
        logger.info(f"Skipping metrics generation for {glue_db_name}.{glue_table_name} due to recent execution")
        return

    # Update cache with current execution time
    update_cache(glue_db_name, glue_table_name)

    catalog = GlueCatalog(glue_db_name)
    table = catalog.load_table((glue_db_name, glue_table_name))
    logger.info(f"current snapshot id={table.metadata.current_snapshot_id}")
    snapshot = table.metadata.snapshot_by_id(table.metadata.current_snapshot_id)
    logger.info("Using glue IS to produce metrics")
    session_id = create_or_reuse_glue_session()

    send_snapshot_metrics(glue_db_name, glue_table_name, table.metadata.current_snapshot_id, session_id)
    send_partition_metrics(glue_db_name, glue_table_name, snapshot, session_id)
    send_files_metrics(glue_db_name, glue_table_name, snapshot, session_id)


def lambda_handler(event, context):
    log_format = f"[{context.aws_request_id}:%(message)s"
    logging.basicConfig(format=log_format, level=logging.INFO)

    # Check if MONITORED_DBS environment variable is set
    monitored_dbs = os.getenv('MONITORED_DBS')

    if monitored_dbs:
        # Parse comma-separated list of databases
        database_names = [db.strip() for db in monitored_dbs.split(',') if db.strip()]
        logger.info(f"MONITORED_DBS is set, processing databases: {database_names}")

        for db_name in database_names:
            logger.info(f"Processing database: {db_name}")
            # Get all Iceberg tables from the database
            iceberg_tables = get_iceberg_tables_from_database(db_name)
            logger.info(f"Found {len(iceberg_tables)} Iceberg tables in database {db_name}: {iceberg_tables}")

            # Process each table
            for table_name in iceberg_tables:
                logger.info(f"Processing table: {db_name}.{table_name}")
                try:
                    process_table_metrics(db_name, table_name)
                except Exception as e:
                    logger.error(f"Error processing table {db_name}.{table_name}: {str(e)}")
                    continue
    else:
        # Original behavior: process single table from event
        logger.info("MONITORED_DBS not set, processing single table from event")
        glue_db_name = event["detail"]["databaseName"]
        glue_table_name = event["detail"]["tableName"]

        # Ensure Table is of Iceberg format.
        if not check_table_is_of_iceberg_format(glue_db_name, glue_table_name):
            logger.info("Table is not of Iceberg format, skipping metrics generation")
            return

        process_table_metrics(glue_db_name, glue_table_name)