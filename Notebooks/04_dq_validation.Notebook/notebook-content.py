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

# PARAMETERS CELL ********************

# ─── Cell 1: Parameters

LOG_LEVEL        = 'DEBUG'
DQ_FAIL_ON_ERROR = 'false'  # Set to 'true' in prod to halt pipeline on DQ failure

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Parameters
ENVIRONMENT = 'dev'
LOG_LEVEL   = 'DEBUG'
DQ_FAIL_ON_ERROR = 'false'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Imports
import logging
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DecimalType, TimestampType

# ── Suppress py4j and pyspark debug logs
logging.getLogger('py4j').setLevel(logging.WARNING)
logging.getLogger('pyspark').setLevel(logging.WARNING)

# ── Set our logger
logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)
logger.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))

logger.info(f'Data Quality validation started | env={ENVIRONMENT}')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Run DQ checks on Silver transactions
# 6 expectations defined below - quarantine pattern isolates non-conforming rows

df = spark.table('silver_transactions')
total_rows = df.count()
logger.info(f'[DQ] Validating {total_rows:,} rows from silver_transactions')

# ── Initialize DQ results tracking
dq_results = {
    'passed': 0,
    'failed': 0,
    'failures': []
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# DQ Expectation 1 - trans_id uniqueness
# Each transaction must have unique trans_id

df_dup_check = df.groupBy('trans_id').count().filter(F.col('count') > 1)
dup_count = df_dup_check.count()

if dup_count == 0:
    logger.info('[DQ] Expectation 1: trans_id uniqueness PASSED')
    dq_results['passed'] += 1
else:
    logger.warning(f'[DQ] Expectation 1: {dup_count} duplicate trans_id values FAILED')
    dq_results['failed'] += 1
    dq_results['failures'].append(f'Expectation 1: {dup_count} duplicates')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# DQ Expectation 2 - NOT NULL on critical columns
# trans_id, account_id, trans_date, amount, trans_type_label must be NOT NULL

critical_cols = ['trans_id', 'account_id', 'trans_date', 'amount', 'trans_type_label']
null_check = df.select([F.count(F.when(F.col(col).isNull(), 1)).alias(col) for col in critical_cols])
null_results = null_check.collect()[0].asDict()

total_nulls = sum(null_results.values())
if total_nulls == 0:
    logger.info('[DQ] Expectation 2: NOT NULL on critical columns PASSED')
    dq_results['passed'] += 1
else:
    logger.warning(f'[DQ] Expectation 2: {total_nulls} NULL values found FAILED')
    dq_results['failed'] += 1
    dq_results['failures'].append(f'Expectation 2: {total_nulls} NULLs: {null_results}')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# DQ Expectation 3 - Amount > 0
# Transaction amounts must be positive

invalid_amount = df.filter(F.col('amount') <= 0).count()

if invalid_amount == 0:
    logger.info('[DQ] Expectation 3: amount > 0 PASSED')
    dq_results['passed'] += 1
else:
    logger.warning(f'[DQ] Expectation 3: {invalid_amount} rows with amount <= 0 FAILED')
    dq_results['failed'] += 1
    dq_results['failures'].append(f'Expectation 3: {invalid_amount} invalid amounts')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# DQ Expectation 4 - trans_type_label in allowed set
# Only CREDIT, DEBIT, OTHER allowed

allowed_types = ['CREDIT', 'DEBIT', 'OTHER']
invalid_type = df.filter(~F.col('trans_type_label').isin(allowed_types)).count()

if invalid_type == 0:
    logger.info('[DQ]  Expectation 4: trans_type_label in allowed set PASSED')
    dq_results['passed'] += 1
else:
    logger.warning(f'[DQ]  Expectation 4: {invalid_type} invalid trans_type_label FAILED')
    dq_results['failed'] += 1
    dq_results['failures'].append(f'Expectation 4: {invalid_type} invalid types')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# DQ Expectation 5 - trans_date in valid range
# Dates must be between 1993-01-01 and 1999-12-31 (Berka dataset period)

invalid_date = df.filter(
    (F.col('trans_date') < F.to_date(F.lit('1993-01-01'))) |
    (F.col('trans_date') > F.to_date(F.lit('1999-12-31')))
).count()

if invalid_date == 0:
    logger.info('[DQ] Expectation 5: trans_date in range [1993-01-01, 1999-12-31] PASSED')
    dq_results['passed'] += 1
else:
    logger.warning(f'[DQ] Expectation 5: {invalid_date} dates out of range FAILED')
    dq_results['failed'] += 1
    dq_results['failures'].append(f'Expectation 5: {invalid_date} out-of-range dates')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# DQ Expectation 6 - Row count change tolerance
# Silver rows should be close to Bronze rows (allowing for dedup and filtering)
# Tolerance: 0-5% reduction from Bronze

bronze_rows = spark.table('bronze_trans').count()
silver_rows = total_rows
reduction_pct = ((bronze_rows - silver_rows) / bronze_rows * 100) if bronze_rows > 0 else 0

# Allow up to 10% reduction (dedup + invalid amount filter)
if 0 <= reduction_pct <= 10:
    logger.info(f'[DQ] Expectation 6: Row count change {reduction_pct:.2f}% within tolerance PASSED')
    dq_results['passed'] += 1
else:
    logger.warning(f'[DQ] Expectation 6: Row reduction {reduction_pct:.2f}% exceeds tolerance FAILED')
    dq_results['failed'] += 1
    dq_results['failures'].append(f'Expectation 6: {reduction_pct:.2f}% row reduction (Bronze: {bronze_rows:,}, Silver: {silver_rows:,})')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Quarantine pattern - isolate non-conforming rows
# This is a simple version - collect rows that fail any check

df_quarantine = (
    df
    .withColumn('qc_flag',
        F.when(
            F.col('trans_id').isNull() |
            F.col('account_id').isNull() |
            F.col('trans_date').isNull() |
            F.col('amount').isNull() |
            F.col('trans_type_label').isNull() |
            (F.col('amount') <= 0) |
            (~F.col('trans_type_label').isin(allowed_types)) |
            (F.col('trans_date') < F.to_date(F.lit('1993-01-01'))) |
            (F.col('trans_date') > F.to_date(F.lit('1999-12-31'))),
            True
        ).otherwise(False)
    )
    .filter(F.col('qc_flag') == True)
    .drop('qc_flag')
)

quarantine_count = df_quarantine.count()

if quarantine_count > 0:
    df_quarantine.write.format('delta').mode('overwrite').saveAsTable('dq_quarantine_transactions')
    logger.warning(f'[DQ] Quarantine table created: {quarantine_count:,} non-conforming rows isolated')
else:
    logger.info('[DQ] No quarantine rows - all data conforms to expectations')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Summary and control flow

logger.info(f'[DQ] Validation complete: {dq_results["passed"]} passed, {dq_results["failed"]} failed')

if dq_results['failed'] > 0:
    logger.warning(f'[DQ] Failures: {dq_results["failures"]}')

if DQ_FAIL_ON_ERROR.lower() == 'true' and dq_results['failed'] > 0:
    logger.error('[DQ] DQ_FAIL_ON_ERROR=true and failures detected → HALTING PIPELINE')
    raise Exception(f'Data Quality validation failed with {dq_results["failed"]} failures. Gold tables not updated.')
else:
    logger.info('[DQ] DQ_FAIL_ON_ERROR=false → Proceeding to Gold layer (check failures manually)')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
