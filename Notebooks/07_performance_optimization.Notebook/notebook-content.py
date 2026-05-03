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

ENVIRONMENT ='dev'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

LOG_LEVEL   = 'INFO'
VACUUM_RETENTION_HOURS = 168  # 7 days for audit trail

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Imports
import logging
from datetime import datetime

# Suppress py4j and pyspark debug logs
logging.getLogger('py4j').setLevel(logging.WARNING)
logging.getLogger('pyspark').setLevel(logging.WARNING)

logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)
logger.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))

logger.info(f'Performance Optimization started | env={ENVIRONMENT}')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# OPTIMIZE Silver Tables with ZORDER
# ZORDER co-locates related data for faster filtered queries
# Run after each pipeline execution

logger.info('[OPTIMIZE] Starting Silver table optimization...')

# Silver transactions — partitioned by trans_year/trans_month
# ZORDER on account_id and trans_date for account-level queries
try:
    spark.sql('OPTIMIZE silver_transactions ZORDER BY (account_id, trans_date)')
    logger.info('[OPTIMIZE] silver_transactions: OPTIMIZE and ZORDER complete')
except Exception as e:
    logger.warning(f'[OPTIMIZE] silver_transactions failed: {str(e)}')

# Silver loans — ZORDER on district_id and status for regional aggregation
try:
    spark.sql('OPTIMIZE silver_loans ZORDER BY (district_id, status)')
    logger.info('[OPTIMIZE] silver_loans: OPTIMIZE and ZORDER complete')
except Exception as e:
    logger.warning(f'[OPTIMIZE] silver_loans failed: {str(e)}')

# Silver accounts — ZORDER on account_id for SCD join performance
try:
    spark.sql('OPTIMIZE silver_accounts ZORDER BY (account_id)')
    logger.info('[OPTIMIZE] silver_accounts: OPTIMIZE and ZORDER complete')
except Exception as e:
    logger.warning(f'[OPTIMIZE] silver_accounts failed: {str(e)}')

# Silver clients — ZORDER on client_id for customer joins
try:
    spark.sql('OPTIMIZE silver_clients ZORDER BY (client_id)')
    logger.info('[OPTIMIZE] silver_clients: OPTIMIZE and ZORDER complete')
except Exception as e:
    logger.warning(f'[OPTIMIZE] silver_clients failed: {str(e)}')

# Silver districts — small table, optimize only
try:
    spark.sql('OPTIMIZE silver_districts')
    logger.info('[OPTIMIZE] silver_districts: OPTIMIZE complete')
except Exception as e:
    logger.warning(f'[OPTIMIZE] silver_districts failed: {str(e)}')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ─── Cell 4: OPTIMIZE Gold Tables (No ZORDER needed)
# Gold tables are small post-aggregation, OPTIMIZE is sufficient

logger.info('[OPTIMIZE] Starting Gold table optimization...')

gold_tables = ['gold_loan_performance', 'gold_monthly_summary', 'gold_customer_360']

for table in gold_tables:
    try:
        spark.sql(f'OPTIMIZE {table}')
        logger.info(f'[OPTIMIZE] {table}: OPTIMIZE complete')
    except Exception as e:
        logger.warning(f'[OPTIMIZE] {table} failed: {str(e)}')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# VACUUM — Remove old file versions
# Keep files for VACUUM_RETENTION_HOURS (default 7 days) for audit/time travel

logger.info(f'[VACUUM] Starting cleanup (retention: {VACUUM_RETENTION_HOURS} hours = {VACUUM_RETENTION_HOURS/24:.0f} days)...')

all_tables = [
    'silver_transactions',
    'silver_loans',
    'silver_accounts',
    'silver_clients',
    'silver_districts',
    'gold_loan_performance',
    'gold_monthly_summary',
    'gold_customer_360'
]

for table in all_tables:
    try:
        spark.sql(f'VACUUM {table} RETAIN {VACUUM_RETENTION_HOURS} HOURS')
        logger.info(f'[VACUUM] {table}: Old versions removed')
    except Exception as e:
        logger.warning(f'[VACUUM] {table} failed: {str(e)}')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Delta Time Travel

logger.info('[TIME-TRAVEL] Querying silver_transactions as of past date...')

from datetime import datetime, timedelta

past_date = (datetime.utcnow() - timedelta(days=1)).strftime('%Y-%m-%d %H:%M:%S')

logger.info(f'[TIME-TRAVEL] Querying as of: {past_date}')

try:
    df_audit = spark.read.format('delta') \
        .option('timestampAsOf', past_date) \
        .table('silver_transactions')
    
    logger.info(f'[TIME-TRAVEL] Historical record count: {df_audit.count()}')
    logger.info('[TIME-TRAVEL] Delta versioning works for audit trails!')
    
except Exception as e:
    logger.warning(f'[TIME-TRAVEL] Cannot query past date: {str(e)}')
    logger.info('[TIME-TRAVEL] Tables only retain 7 days of history (VACUUM retention)')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_audit = (
    spark.read.format('delta')
    .option('timestampAsOf', '2026-05-01 00:00:00')
    .table('silver_transactions')
)

# View full version history — shows every write, who triggered it, when
display(spark.sql('DESCRIBE HISTORY silver_transactions LIMIT 10'))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
