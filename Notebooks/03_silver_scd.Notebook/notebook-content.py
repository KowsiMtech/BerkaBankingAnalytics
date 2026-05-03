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

# Parameters (Toggle as Parameter Cell in Fabric UI)
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
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType, TimestampType, BooleanType
from delta.tables import DeltaTable
from datetime import datetime

# ── Suppress py4j and pyspark debug logs
logging.getLogger('py4j').setLevel(logging.WARNING)
logging.getLogger('pyspark').setLevel(logging.WARNING)

logging.basicConfig(level=getattr(logging, LOG_LEVEL, logging.INFO))
logger = logging.getLogger(__name__)
logger.info(f'SCD Type 2 processing started | env={ENVIRONMENT}')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Prepare incoming account data with SCD columns
# SCD Type 2: tracks changes to district_id and frequency over time

df_new = (
    spark.table('silver_accounts_base')
    .withColumn('account_id', F.col('account_id').cast(IntegerType()))
    .withColumn('district_id', F.col('district_id').cast(IntegerType()))
    # ── SCD columns
    .withColumn('effective_from', F.current_timestamp())
    .withColumn('effective_to',   F.lit('9999-12-31').cast(TimestampType()))
    .withColumn('is_current',     F.lit(True).cast(BooleanType()))
    # ── Skew fix: repartition before merge
    .repartition(8, F.col('account_id'))
)

new_count = df_new.count()
logger.info(f'[SCD] Incoming records: {new_count:,}')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# SCD Type 2 MERGE
# Expire changed rows
# Insert all new rows (new + unchanged)
# Dedup - keep only latest active version per account

table_exists = spark._jsparkSession.catalog().tableExists('silver_accounts')

if table_exists:
    logger.info('[SCD] silver_accounts exists - running MERGE')
    dt = DeltaTable.forName(spark, 'silver_accounts')
    
    # ── Step 1: Mark rows as expired where attributes changed
    (dt.alias('existing')
        .merge(
            df_new.alias('incoming'),
            'existing.account_id = incoming.account_id AND existing.is_current = true'
        )
        .whenMatchedUpdate(
            condition='existing.district_id != incoming.district_id OR existing.frequency != incoming.frequency',
            set={
                'is_current': F.lit(False),
                'effective_to': F.col('incoming.effective_from')
            }
        )
        .execute())
    
    logger.info('[SCD] Step 1: Changed rows expired')
    
    # ── Step 2: Insert all incoming rows (new + unchanged)
    (df_new.write.format('delta')
        .mode('append')
        .saveAsTable('silver_accounts'))
    
    logger.info('[SCD] Step 2: New rows inserted')
    
    # ── Step 3: Dedup - keep only latest active version per account
    df_accounts_dedup = (
        spark.table('silver_accounts')
        .withColumn('rn', F.row_number().over(
            Window.partitionBy('account_id')  # ← Changed from F.Window to Window
                  .orderBy(F.col('effective_from').desc())
        ))
        .filter(F.col('rn') == 1)
        .drop('rn')
    )
    
    (df_accounts_dedup.write.format('delta')
        .mode('overwrite')
        .option('overwriteSchema', 'true')
        .saveAsTable('silver_accounts'))
    
    logger.info('[SCD] Step 3: Duplicates removed')
    
else:
    logger.info('[SCD] silver_accounts does not exist - full load')
    (df_new.write.format('delta')
        .mode('overwrite')
        .saveAsTable('silver_accounts'))

final_count = spark.table('silver_accounts').count()
logger.info(f'[SCD] silver_accounts complete: {final_count:,} rows')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
