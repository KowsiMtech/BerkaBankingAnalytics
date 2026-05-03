-- Fabric notebook source

-- METADATA ********************

-- META {
-- META   "kernel_info": {
-- META     "name": "synapse_pyspark"
-- META   },
-- META   "dependencies": {
-- META     "lakehouse": {
-- META       "default_lakehouse": "8553a2a1-01db-4894-a717-8cba730f435f",
-- META       "default_lakehouse_name": "banking_lakehouse",
-- META       "default_lakehouse_workspace_id": "008f14c7-4be3-4105-9c27-dd4c3a69bb61",
-- META       "known_lakehouses": [
-- META         {
-- META           "id": "8553a2a1-01db-4894-a717-8cba730f435f"
-- META         }
-- META       ]
-- META     },
-- META     "warehouse": {
-- META       "default_warehouse": "bb4f2821-9378-4c8d-bc86-b6d4ef72d6d9",
-- META       "known_warehouses": [
-- META         {
-- META           "id": "bb4f2821-9378-4c8d-bc86-b6d4ef72d6d9",
-- META           "type": "Lakewarehouse"
-- META         }
-- META       ]
-- META     }
-- META   }
-- META }

-- CELL ********************

SELECT 'bronze_account' AS table_name, COUNT(*) AS row_count FROM dbo.bronze_account
UNION ALL
SELECT 'bronze_card', COUNT(*) FROM dbo.bronze_card
UNION ALL
SELECT 'bronze_client', COUNT(*) FROM dbo.bronze_client
UNION ALL
SELECT 'bronze_disp', COUNT(*) FROM dbo.bronze_disp
UNION ALL
SELECT 'bronze_district', COUNT(*) FROM dbo.bronze_district
UNION ALL
SELECT 'bronze_loan', COUNT(*) FROM dbo.bronze_loan
UNION ALL
SELECT 'bronze_order', COUNT(*) FROM dbo.bronze_order
UNION ALL
SELECT 'bronze_trans', COUNT(*) FROM dbo.bronze_trans
UNION ALL
SELECT 'silver_accounts', COUNT(*) FROM dbo.silver_accounts
UNION ALL
SELECT 'silver_clients', COUNT(*) FROM dbo.silver_clients
UNION ALL
SELECT 'silver_districts', COUNT(*) FROM dbo.silver_districts
UNION ALL
SELECT 'silver_loans', COUNT(*) FROM dbo.silver_loans
UNION ALL
SELECT 'silver_transactions', COUNT(*) FROM dbo.silver_transactions;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }
