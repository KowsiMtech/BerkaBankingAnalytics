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
BRONZE      = 'Files/bronze'
SILVER      = 'Files/silver'
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
from pyspark.sql.types import IntegerType, DecimalType, TimestampType, DoubleType
from datetime import datetime

# ── Suppress py4j and pyspark debug logs
logging.getLogger('py4j').setLevel(logging.WARNING)
logging.getLogger('pyspark').setLevel(logging.WARNING)

# ── Set our logger
logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)
logger.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))

logging.basicConfig(level=getattr(logging, LOG_LEVEL, logging.INFO))
logger = logging.getLogger(__name__)
logger.info(f'Silver transform started | env={ENVIRONMENT}')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# TRANSACTIONS
# Slovak mapping and skew handling

df_trans = spark.table('bronze_trans')

df_trans_silver = (
    df_trans
    # ── Type casting
    .withColumn('trans_id',         F.col('trans_id').cast(IntegerType()))
    .withColumn('account_id',       F.col('account_id').cast(IntegerType()))
    .withColumn('date_str', F.lpad(F.col('date').cast('string'), 6, '0'))
    .withColumn('trans_date', F.to_date(F.concat(F.lit('19'), F.col('date_str')), 'yyyyMMdd'))
    .drop('date_str')
    .withColumn('amount',           F.col('amount').cast(DecimalType(15, 2)))
    .withColumn('balance',          F.col('balance').cast(DecimalType(15, 2)))
    # ── Slovak to English mapping (raw columns kept)
    .withColumn('trans_type_label',
        F.when(F.col('type') == 'PRIJEM', 'CREDIT')
         .when(F.col('type') == 'VYDAJ',  'DEBIT')
         .otherwise('OTHER'))
    .withColumn('trans_category',
        F.when(F.col('k_symbol') == 'POJISTNE',    'Insurance')
         .when(F.col('k_symbol') == 'SLUZBY',      'Statement Charge')
         .when(F.col('k_symbol') == 'UROK',        'Interest Credit')
         .when(F.col('k_symbol') == 'SANKC. UROK', 'Sanction Interest')
         .when(F.col('k_symbol') == 'SIPO',        'Household Payment')
         .when(F.col('k_symbol') == 'DUCHOD',      'Pension')
         .when(F.col('k_symbol') == 'UVER',        'Loan Payment')
         .otherwise('Other'))
    .withColumn('operation_label',
        F.when(F.col('operation') == 'VYBER KARTOU',  'Card Withdrawal')
         .when(F.col('operation') == 'VKLAD',         'Cash Deposit')
         .when(F.col('operation') == 'VYBER',         'Cash Withdrawal')
         .when(F.col('operation') == 'PREVOD Z UCTU', 'Incoming Transfer')
         .when(F.col('operation') == 'PREVOD NA UCTU','Outgoing Transfer')
         .otherwise('Unknown'))
    # ── Derived date parts for partitioning
    .withColumn('trans_year',       F.year('trans_date'))
    .withColumn('trans_month',      F.month('trans_date'))
    # ── Null handling
    .fillna({'operation': 'UNKNOWN', 'k_symbol': 'UNKNOWN', 'bank': 'UNKNOWN'})
    # ── Data quality: filter invalid amounts
    .filter(F.col('amount') > 0)
    # ── Duplicate handling
    .dropDuplicates(['trans_id'])
    # ── Skew fix: repartition on account_id (high cardinality)
    .repartition(16, F.col('account_id'))
    .drop('date', 'type')
)

# Cache before write
df_trans_silver.cache()
trans_count = df_trans_silver.count()

(df_trans_silver
    .write.format('delta')
    .mode('overwrite')
    .option('overwriteSchema', 'true')
    .option('delta.autoOptimize.optimizeWrite', 'true')
    .option('delta.autoOptimize.autoCompact', 'true')
    .partitionBy('trans_year', 'trans_month')
    .saveAsTable('silver_transactions')
)

logger.info(f'silver_transactions: {trans_count:,} rows')
df_trans_silver.unpersist()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# CLIENTS — Czech birth_number decode
# Format: YYMMDDXXXXX (YY=year, MM=month or month+50 for female, DD=day, X=sequence)

df_clients = spark.table('bronze_client')

df_clients_silver = (
    df_clients
    # ── Extract month and gender from birth_number
    .withColumn('month_raw', F.col('birth_number').substr(3, 2).cast(IntegerType()))
    .withColumn('gender',
        F.when(F.col('month_raw') > 50, 'F').otherwise('M'))
    .withColumn('actual_month',
        F.when(F.col('month_raw') > 50, F.col('month_raw') - 50)
         .otherwise(F.col('month_raw')))
    # ── Construct proper birth_date
    .withColumn('birth_date',
        F.to_date(F.concat(
            F.lit('19'), F.col('birth_number').substr(1, 2), F.lit('-'),
            F.lpad(F.col('actual_month').cast('string'), 2, '0'), F.lit('-'),
            F.col('birth_number').substr(5, 2)
        ), 'yyyy-MM-dd'))
    # ── Calculate age
    .withColumn('age', F.round(F.datediff(F.current_date(), 'birth_date') / 365.25, 1))
    # ── Age band segmentation
    .withColumn('age_band',
        F.when(F.col('age') < 30, 'Under 30')
         .when(F.col('age') < 45, '30-44')
         .when(F.col('age') < 60, '45-59')
         .otherwise('60+'))
    # ── Cleanup
    .drop('birth_number', 'month_raw', 'actual_month')
    .dropDuplicates(['client_id'])
)

df_clients_silver.cache()
client_count = df_clients_silver.count()

(df_clients_silver
    .write.format('delta')
    .mode('overwrite')
    .option('overwriteSchema', 'true')
    .saveAsTable('silver_clients')
)

logger.info(f'silver_clients: {client_count:,} rows')
df_clients_silver.unpersist()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# LOANS - Slovak status mapping and default flag

df_loans = spark.table('bronze_loan')

df_loans_silver = (
    df_loans
    # Type casting
    .withColumn('loan_id',    F.col('loan_id').cast(IntegerType()))
    .withColumn('account_id', F.col('account_id').cast(IntegerType()))
    # Date fix (was producing 2094-2099)
    .withColumn('date_str', F.lpad(F.col('date').cast('string'), 6, '0'))
    .withColumn('loan_date', F.to_date(F.concat(F.lit('19'), F.col('date_str')), 'yyyyMMdd'))
    .drop('date_str')
    .withColumn('amount',   F.col('amount').cast(DecimalType(15, 2)))
    .withColumn('payments', F.col('payments').cast(DecimalType(15, 2)))
    # Slovak status to English label and default flag
    .withColumn('status_label',
        F.when(F.col('status') == 'A', 'Completed - Paid OK')
         .when(F.col('status') == 'B', 'Completed - Defaulted')
         .when(F.col('status') == 'C', 'Running - Good Standing')
         .when(F.col('status') == 'D', 'Running - In Debt')
         .otherwise('Unknown'))
    .withColumn('is_default',
        F.when(F.col('status').isin('B', 'D'), 1).otherwise(0))
    # Cleanup
    .drop('date')
    .dropDuplicates(['loan_id'])
)

df_loans_silver.cache()
loan_count = df_loans_silver.count()

(df_loans_silver
    .write.format('delta')
    .mode('overwrite')
    .option('overwriteSchema', 'true')
    .saveAsTable('silver_loans')
)

logger.info(f'silver_loans: {loan_count:,} rows')
df_loans_silver.unpersist()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ACCOUNTS — date fix and Slovak frequency mapping

df_account = spark.table('bronze_account')

df_account_silver = (
    df_account
    # ── Date fix
    .withColumn('date_str', F.lpad(F.col('date').cast('string'), 6, '0'))
    .withColumn('open_date', F.to_date(F.concat(F.lit('19'), F.col('date_str')), 'yyyyMMdd'))
    .drop('date_str')
    # ── Slovak frequency to English (raw kept)
    .withColumn('statement_frequency',
        F.when(F.col('frequency') == 'POPLATEK MESICNE',   'Monthly')
         .when(F.col('frequency') == 'POPLATEK TYDNE',     'Weekly')
         .when(F.col('frequency') == 'POPLATEK PO OBRATU', 'Per Transaction')
         .otherwise('Unknown'))
    .drop('date')
)

df_account_silver.cache()
account_count = df_account_silver.count()

(df_account_silver
    .write.format('delta')
    .mode('overwrite')
    .option('overwriteSchema', 'true')
    .saveAsTable('silver_accounts_base')
)

logger.info(f'silver_accounts_base: {account_count:,} rows')
df_account_silver.unpersist()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# DISTRICTS

df_dist = spark.table('bronze_district')

df_dist_silver = (
    df_dist
    # ── Column rename: A1=district_id, A3=region, A4=population, A11=avg_salary, A12=unemployment_rate
    .withColumnRenamed('A1', 'district_id')
    .withColumnRenamed('A3', 'region')
    .withColumnRenamed('A4', 'population')
    .withColumnRenamed('A11', 'avg_salary')
    .withColumnRenamed('A12', 'unemployment_rate')
    .withColumnRenamed('A16', 'crime_rate')
    # ── Type casting
    .withColumn('district_id',       F.col('district_id').cast(IntegerType()))
    .withColumn('avg_salary',        F.col('avg_salary').cast(DoubleType()))
    .withColumn('unemployment_rate', F.col('unemployment_rate').cast(DoubleType()))
    # ── Select only needed columns
    .select('district_id', 'region', 'population', 'avg_salary', 'unemployment_rate', 'crime_rate')
    .dropDuplicates(['district_id'])
)

df_dist_silver.cache()
dist_count = df_dist_silver.count()

(df_dist_silver
    .write.format('delta')
    .mode('overwrite')
    .option('overwriteSchema', 'true')
    .saveAsTable('silver_districts')
)

logger.info(f'silver_districts: {dist_count:,} rows')
df_dist_silver.unpersist()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Summary
logger.info(f'Silver layer complete | env={ENVIRONMENT}')
logger.info(f'Totals: trans={trans_count:,}, clients={client_count:,}, loans={loan_count:,}, accounts={account_count:,}, districts={dist_count:,}')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
