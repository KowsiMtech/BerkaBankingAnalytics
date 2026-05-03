# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

# Parameters  
# Pipeline overrides these values at runtime for each workspace

ENVIRONMENT = 'dev'
BRONZE      = 'Files/bronze'
SILVER      = 'Files/silver'
GOLD        = 'Files/gold'
LOG_LEVEL   = 'DEBUG'
DQ_FAIL_ON_ERROR = 'false'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Imports and logger
import logging
from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import datetime

logging.basicConfig(level=getattr(logging, LOG_LEVEL, logging.INFO))
logger = logging.getLogger(__name__)
logger.info(f'Pipeline started | env={ENVIRONMENT}')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
