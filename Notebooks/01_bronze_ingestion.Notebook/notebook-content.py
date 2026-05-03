# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "8553a2a1-01db-4894-a717-8cba730f435f",
# META       "default_lakehouse_name": "banking_lakehouse",
# META       "default_lakehouse_workspace_id": "008f14c7-4be3-4105-9c27-dd4c3a69bb61",
# META       "known_lakehouses": [
# META         {
# META           "id": "8553a2a1-01db-4894-a717-8cba730f435f"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Parameters

BRONZE      = 'Files/bronze'
LOG_LEVEL   = 'DEBUG'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Imports
import logging
from pyspark.sql import functions as F
from datetime import datetime

# ── Suppress py4j and pyspark debug logs
logging.getLogger('py4j').setLevel(logging.WARNING)
logging.getLogger('pyspark').setLevel(logging.WARNING)

# ── Set our logger
logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)
logger.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))

TABLES = ['trans','account','client','loan','card','disp','order','district']
RUN_TS = datetime.utcnow().isoformat()

logger.info(f'Bronze ingestion started | env={ENVIRONMENT}')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Validation — confirm all bronze tables loaded by pipeline
def validate_bronze(table_name: str) -> int:
    df = spark.table(f'bronze_{table_name}')
    row_count = df.count()
    logger.info(f'[BRONZE] bronze_{table_name}: {row_count:,} rows')
    return row_count

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ─── Cell 5: Append audit metadata columns
# Pipeline Copy Activity writes raw data — notebook adds audit columns

def append_audit_metadata(table_name: str):
    """Add audit columns to bronze table"""
    df = spark.table(f'bronze_{table_name}')
    
    # Add audit metadata
    df = (df
        .withColumn('_source_file',  F.lit(f'{BRONZE}/{table_name}.csv'))
        .withColumn('_ingestion_ts', F.lit(RUN_TS))
        .withColumn('_environment',  F.lit(ENVIRONMENT))
    )
    
    # Write with optimization (no explicit repartition — let Delta handle it)
    (df.write.format('delta')
        .mode('overwrite')
        .option('overwriteSchema', 'true')
        .option('delta.autoOptimize.optimizeWrite', 'true')
        .option('delta.autoOptimize.autoCompact', 'true')
        .saveAsTable(f'bronze_{table_name}'))
    
    row_count = df.count()
    logger.info(f'[BRONZE] {table_name}: {row_count:,} rows | audit columns appended')

# ─── Execute for all tables
logger.info(f'[BRONZE] Starting audit metadata append | {len(TABLES)} tables')

for table in TABLES:
    append_audit_metadata(table)

logger.info(f'[BRONZE] Layer complete | env={ENVIRONMENT} | timestamp={RUN_TS}')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Execute validation and audit append
for table in TABLES:
    validate_bronze(table)
    append_audit_metadata(table)

logger.info(f'Bronze layer complete | env={ENVIRONMENT} | run={RUN_TS}')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
