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

# Parameters
LOG_LEVEL   = 'DEBUG'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import logging
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DecimalType, TimestampType

# ── Suppress py4j and pyspark debug logs
logging.getLogger('py4j').setLevel(logging.WARNING)
logging.getLogger('pyspark').setLevel(logging.WARNING)

logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)
logger.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))
logger.info(f'Pipeline started | env={ENVIRONMENT}')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import logging
from pyspark.sql import functions as F
from pyspark.sql.window import Window

logging.basicConfig(level=getattr(logging, LOG_LEVEL, logging.INFO))
logger = logging.getLogger(__name__)
logger.info(f'Gold aggregation started | env={ENVIRONMENT}')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# GOLD TABLE 1 - gold_monthly_summary
# Answers: Q2 (monthly trend), Q3 (credit-debit ratio), Q7 (dormant accounts)

df_trans = spark.table('silver_transactions')

gold_monthly = (
    df_trans
    .withColumn('year_month', F.date_format('trans_date', 'yyyy-MM'))
    .groupBy('account_id', 'year_month')
    .agg(
        F.count('trans_id').alias('txn_count'),
        F.sum(F.when(F.col('trans_type_label') == 'CREDIT', F.col('amount'))).alias('total_credit'),
        F.sum(F.when(F.col('trans_type_label') == 'DEBIT',  F.col('amount'))).alias('total_debit'),
        F.avg('amount').alias('avg_amount'),
        F.max('trans_date').alias('last_txn_date')
    )
    .withColumn('credit_debit_ratio',
        F.round(
            F.when(F.col('total_debit') != 0, F.col('total_credit') / F.col('total_debit'))
            .otherwise(None),
            4
        )
    )
)

gold_monthly.cache()
monthly_count = gold_monthly.count()

(gold_monthly
    .write.format('delta')
    .mode('overwrite')
    .option('overwriteSchema', 'true')
    .option('delta.autoOptimize.optimizeWrite', 'true')
    .saveAsTable('gold_monthly_summary')
)

logger.info(f'gold_monthly_summary: {monthly_count:,} rows')
gold_monthly.unpersist()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# GOLD TABLE 2 - gold_loan_performance
# Answers: Q1 (default rate by region), Q5 (avg loan by region),
#          Q8 (status breakdown), Q10 (unemployment vs default)

df_loans   = spark.table('silver_loans')
df_dist    = spark.table('silver_districts')
df_account = spark.table('silver_accounts').filter(F.col('is_current') == True)

gold_loans = (
    df_loans
    .join(df_account.select('account_id', 'district_id'), 'account_id', 'left')
    .join(df_dist, 'district_id', 'left')
    .select(
        'loan_id',
        'account_id',
        'loan_date',
        'amount',
        'duration',
        'payments',
        'status',
        'status_label',
        'is_default',
        'region',
        'avg_salary',
        'unemployment_rate',
        F.round(
            F.when(F.col('avg_salary') != 0, F.col('amount') / F.col('avg_salary'))
            .otherwise(None),
            2
        ).alias('loan_to_income_ratio')
    )
)

gold_loans.cache()
loans_count = gold_loans.count()

(gold_loans
    .write.format('delta')
    .mode('overwrite')
    .option('overwriteSchema', 'true')
    .option('delta.autoOptimize.optimizeWrite', 'true')
    .saveAsTable('gold_loan_performance')
)

logger.info(f'gold_loan_performance: {loans_count:,} rows')
gold_loans.unpersist()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# GOLD TABLE 3 - gold_customer_360
# Answers: Q4 (top 10 by volume), Q6 (gender split), Q9 (age band analysis)

df_clients = spark.table('silver_clients')

# Transaction aggregate per account
txn_agg = (
    df_trans
    .groupBy('account_id')
    .agg(
        F.count('trans_id').alias('lifetime_txns'),
        F.sum('amount').alias('lifetime_volume')
    )
)

# Loan status per account (most recent)
loan_status = (
    df_loans
    .select('account_id', F.col('status_label').alias('loan_status'))
)

gold_customers = (
    df_clients
    .join(txn_agg, df_clients.client_id == txn_agg.account_id, 'left')
    .join(loan_status, df_clients.client_id == loan_status.account_id, 'left')
    .select(
        'client_id',
        'gender',
        'age_band',
        'birth_date',
        'lifetime_txns',
        'lifetime_volume',
        'loan_status'
    )
)

gold_customers.cache()
customers_count = gold_customers.count()

(gold_customers
    .write.format('delta')
    .mode('overwrite')
    .option('overwriteSchema', 'true')
    .option('delta.autoOptimize.optimizeWrite', 'true')
    .saveAsTable('gold_customer_360')
)

logger.info(f'gold_customer_360: {customers_count:,} rows')
gold_customers.unpersist()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Summary
logger.info(f'Gold layer complete | env={ENVIRONMENT}')
logger.info(f'Totals: monthly_summary={monthly_count:,}, loan_performance={loans_count:,}, customer_360={customers_count:,}')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
