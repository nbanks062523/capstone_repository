import time
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

# local module imports
from dsa_utils.utils import logger, config


# setup the bigquery client
PROJECT_NAME = config['project']
DATASET_NAME = config['dataset']

# starting a variable name with _ is python convention to say 
# this is a private module variable and should not be imported outside of this module
_client: bigquery.Client = None


def get_client() -> bigquery.Client:
    """
    returns a bigquery client to the current project

    Returns:
        bigquery.Client: bigquery client
    """
    # check to see if the client has not been initialized
    global _client
    if _client is None:
        # initialize the client
        _client = bigquery.Client(project=PROJECT_NAME)
        logger.info(f"successfully created bigquery client. project={PROJECT_NAME}")
    return _client


# Define table schemas

# Performance Goals and Ratings by Fiscal Year
SMARTGoals_SCHEMA = [
    bigquery.SchemaField('Goal ID','INTEGER',mode='NULLABLE'),
    bigquery.SchemaField('Goals','STRING', mode='NULLABLE'),
    bigquery.SchemaField('FY','INTEGER', mode='NULLABLE'),
    bigquery.SchemaField('S_score','INTEGER', mode='NULLABLE'),
    bigquery.SchemaField('M_score','INTEGER', mode='NULLABLE'),
    bigquery.SchemaField('T_score','INTEGER', mode='NULLABLE'),
    bigquery.SchemaField('total_quality_score','INTEGER', mode='NULLABLE'),
]

# Performance Ratings by Fiscal Year
PerfRatings_SCHEMA = [
    bigquery.SchemaField('Emp Num','INTEGER',mode='NULLABLE'),
    bigquery.SchemaField('PerfRating2022','INTEGER', mode='NULLABLE'),
    bigquery.SchemaField('PerfRating2022_Description','STRING', mode='NULLABLE'),
    bigquery.SchemaField('PerfRating2023','INTEGER', mode='NULLABLE'),
    bigquery.SchemaField('PerfRating2023_Description','STRING', mode='NULLABLE'),
]

# global dictionary to hold all table schemas, these are the 'DATA Files' from the table loaders file
TABLE_SCHEMAS = {
    'SMARTGoals_FY22_24': SMARTGoals_SCHEMA,
    'PerfRatings_FY22_23': PerfRatings_SCHEMA,
}


def create_table(table_name: str) -> None:
    """
    This section will create the bigquery tables

    Args:
        table_name (str): one of the following table names: 'SMARTGoals_FY22_24','PerfRatings_FY22_23'
    """
    # raise an error if table name is not in one of our schemas
    assert table_name in TABLE_SCHEMAS, f"Table schema not found for table name: {table_name}"

    # full table id: project.dataset.table
    client = get_client()
    table_id = f"{PROJECT_NAME}.{DATASET_NAME}.{table_name}"
    # drop existing table if it exists
    try:
        table = client.get_table(table_id)      # table exists if this line doesn't raise exception
        client.delete_table(table)
        logger.info(f"dropped existed bigquery table: {table_id}")
        # wait a couple seconds before creating the table again
        time.sleep(2.0)
    except NotFound:
        # Table doesn't exist
        pass
    
    # create the table
    schema = TABLE_SCHEMAS[table_name]
    table = bigquery.Table(table_id, schema=schema)
    table = client.create_table(table, exists_ok=False)
    logger.info(f"created bigquery table: {table_id}")