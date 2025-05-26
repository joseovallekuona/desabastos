
# import libraries
import pandas as pd
import numpy as np
from pyathena import connect
from pyathena.cursor import Cursor
from datetime import datetime, timedelta
import math
import logging
import argparse

# set connection

conn = connect("add credentials as needed"
)



# Argument parser for CLI parameters
parser = argparse.ArgumentParser(description="Create filtered dataframes.")
parser.add_argument("--start_date", type = int, help = "Start date for filtering")
parser.add_argument("--end_date", type = int, help = "End date for filtering")

# define timeframe variables for parser
args = parser.parse_args()

# start timeframe
start_date = args.start_date
end_date = args.end_date

# Configure logging
logging.basicConfig(filename=f"dataframe_creation_log_{start_date}_{end_date}.txt", 
                    level=logging.ERROR, 
                    format='%(asctime)s - %(levelname)s - %(message)s')

def create_dataframe(query, df_name):
    try:
        print(f'Executing {df_name}...')
        file_name = f"{df_name}_{start_date}_{end_date}"
        df = pd.read_sql_query(query, conn, params = {'start_date': start_date, 'end_date': end_date})
        columns_to_replace = ['nombre_supergrupo','nombre_supercategoria','marca','marca_proveedor']
        df[columns_to_replace] = df[columns_to_replace].fillna('Todos')
        df.to_csv(f"{file_name}.csv", index = False)
        print(f"{file_name} created successfully.")
        print(df.head())
        return None
    except Exception as e:
        logging.error(f"Failed to create {df_name}: {e}")
        print(f"Error creating {df_name}. Check the log file for details.")
        return None

# declare queries to be run


#  BASE QUERY WITH NO GROUPING

query_0_1 = """SELECT
    NULL AS nombre_supergrupo,
    NULL AS nombre_supercategoria,
    NULL AS marca,
    NULL AS marca_proveedor,
    date_trunc('week',date_parse(cast(id_fecha_adm as varchar), '%%Y%%m%%d')) week,
    COUNT(DISTINCT CONCAT(CAST(id_ticket AS VARCHAR), '_', CAST(id_tienda AS VARCHAR), '_', CAST(id_fecha_adm AS VARCHAR))) AS tickets,
    SUM(venta_amt) AS venta_tmt,
    COUNT(sellout.id_producto) AS articulos,
    SUM(venta_amt) / NULLIF(COUNT(DISTINCT CONCAT(CAST(id_ticket AS VARCHAR), '_', CAST(id_tienda AS VARCHAR), '_', CAST(id_fecha_adm AS VARCHAR))), 0) AS ticket_promedio
FROM 
    awsdatacatalog.oxxo_silver_database.sellout_promotions sellout
LEFT JOIN
    (select
        id_producto,
        nombre_supergrupo,
        nombre_supercategoria,
        marca,
        marca_proveedor
    from
        awsdatacatalog.oxxo_silver_database.products
    where
        origin_file = (select max(origin_file) from awsdatacatalog.oxxo_silver_database.products)
    ) products
    ON sellout.id_producto = CAST(products.id_producto AS INT)
WHERE 
    id_fecha_adm between %(start_date)s and %(end_date)s
    AND products.id_producto NOT LIKE '%%\\%%' -- Ignore specific products GROUP BY nombre_supergrupo
GROUP BY date_trunc('week',date_parse(cast(id_fecha_adm as varchar), '%%Y%%m%%d'))"""



# ONE COLUMN POSSIBLE COMBINATIONS

query_1_1 = """SELECT
    nombre_supergrupo,
    NULL AS nombre_supercategoria,
    NULL AS marca,
    NULL AS marca_proveedor,
    date_trunc('week',date_parse(cast(id_fecha_adm as varchar), '%%Y%%m%%d')) week,
    COUNT(DISTINCT CONCAT(CAST(id_ticket AS VARCHAR), '_', CAST(id_tienda AS VARCHAR), '_', CAST(id_fecha_adm AS VARCHAR))) AS tickets,
    SUM(venta_amt) AS venta_tmt,
    COUNT(sellout.id_producto) AS articulos,
    SUM(venta_amt) / NULLIF(COUNT(DISTINCT CONCAT(CAST(id_ticket AS VARCHAR), '_', CAST(id_tienda AS VARCHAR), '_', CAST(id_fecha_adm AS VARCHAR))), 0) AS ticket_promedio
FROM 
    awsdatacatalog.oxxo_silver_database.sellout_promotions sellout
LEFT JOIN
    (select
        id_producto,
        nombre_supergrupo,
        nombre_supercategoria,
        marca,
        marca_proveedor
    from
        awsdatacatalog.oxxo_silver_database.products
    where
        origin_file = (select max(origin_file) from awsdatacatalog.oxxo_silver_database.products)
    ) products
    ON sellout.id_producto = CAST(products.id_producto AS INT)
WHERE 
    id_fecha_adm between %(start_date)s and %(end_date)s
    AND products.id_producto NOT LIKE '%%\\%%' -- Ignore specific products GROUP BY nombre_supergrupo
GROUP BY nombre_supergrupo, date_trunc('week',date_parse(cast(id_fecha_adm as varchar), '%%Y%%m%%d'))"""


query_1_2 = """SELECT
    NULL AS nombre_supergrupo,
    nombre_supercategoria,
    NULL AS marca,
    NULL AS marca_proveedor,
    date_trunc('week',date_parse(cast(id_fecha_adm as varchar), '%%Y%%m%%d')) week,
    COUNT(DISTINCT CONCAT(CAST(id_ticket AS VARCHAR), '_', CAST(id_tienda AS VARCHAR), '_', CAST(id_fecha_adm AS VARCHAR))) AS tickets,
    SUM(venta_amt) AS venta_tmt,
    COUNT(sellout.id_producto) AS articulos,
    SUM(venta_amt) / NULLIF(COUNT(DISTINCT CONCAT(CAST(id_ticket AS VARCHAR), '_', CAST(id_tienda AS VARCHAR), '_', CAST(id_fecha_adm AS VARCHAR))), 0) AS ticket_promedio
FROM 
    awsdatacatalog.oxxo_silver_database.sellout_promotions sellout
LEFT JOIN
    (select
        id_producto,
        nombre_supergrupo,
        nombre_supercategoria,
        marca,
        marca_proveedor
    from
        awsdatacatalog.oxxo_silver_database.products
    where
        origin_file = (select max(origin_file) from awsdatacatalog.oxxo_silver_database.products)
    ) products
    ON sellout.id_producto = CAST(products.id_producto AS INT)
WHERE 
    id_fecha_adm between %(start_date)s and %(end_date)s
    AND products.id_producto NOT LIKE '%%\\%%' -- Ignore specific products GROUP BY nombre_supergrupo
GROUP BY nombre_supercategoria, date_trunc('week',date_parse(cast(id_fecha_adm as varchar), '%%Y%%m%%d'))"""


query_1_3 = """SELECT
    NULL AS nombre_supergrupo,
    NULL AS nombre_supercategoria,
    marca,
    NULL AS marca_proveedor,
    date_trunc('week',date_parse(cast(id_fecha_adm as varchar), '%%Y%%m%%d')) week,
    COUNT(DISTINCT CONCAT(CAST(id_ticket AS VARCHAR), '_', CAST(id_tienda AS VARCHAR), '_', CAST(id_fecha_adm AS VARCHAR))) AS tickets,
    SUM(venta_amt) AS venta_tmt,
    COUNT(sellout.id_producto) AS articulos,
    SUM(venta_amt) / NULLIF(COUNT(DISTINCT CONCAT(CAST(id_ticket AS VARCHAR), '_', CAST(id_tienda AS VARCHAR), '_', CAST(id_fecha_adm AS VARCHAR))), 0) AS ticket_promedio
FROM 
    awsdatacatalog.oxxo_silver_database.sellout_promotions sellout
LEFT JOIN
    (select
        id_producto,
        nombre_supergrupo,
        nombre_supercategoria,
        marca,
        marca_proveedor
    from
        awsdatacatalog.oxxo_silver_database.products
    where
        origin_file = (select max(origin_file) from awsdatacatalog.oxxo_silver_database.products)
    ) products
    ON sellout.id_producto = CAST(products.id_producto AS INT)
WHERE 
    id_fecha_adm between %(start_date)s and %(end_date)s
    AND products.id_producto NOT LIKE '%%\\%%' -- Ignore specific products GROUP BY nombre_supergrupo
GROUP BY marca, date_trunc('week',date_parse(cast(id_fecha_adm as varchar), '%%Y%%m%%d'))"""


query_1_4 = """SELECT
    NULL AS nombre_supergrupo,
    NULL AS nombre_supercategoria,
    NULL AS marca,
    marca_proveedor,
    date_trunc('week',date_parse(cast(id_fecha_adm as varchar), '%%Y%%m%%d')) week,
    COUNT(DISTINCT CONCAT(CAST(id_ticket AS VARCHAR), '_', CAST(id_tienda AS VARCHAR), '_', CAST(id_fecha_adm AS VARCHAR))) AS tickets,
    SUM(venta_amt) AS venta_tmt,
    COUNT(sellout.id_producto) AS articulos,
    SUM(venta_amt) / NULLIF(COUNT(DISTINCT CONCAT(CAST(id_ticket AS VARCHAR), '_', CAST(id_tienda AS VARCHAR), '_', CAST(id_fecha_adm AS VARCHAR))), 0) AS ticket_promedio
FROM 
    awsdatacatalog.oxxo_silver_database.sellout_promotions sellout
LEFT JOIN
    (select
        id_producto,
        nombre_supergrupo,
        nombre_supercategoria,
        marca,
        marca_proveedor
    from
        awsdatacatalog.oxxo_silver_database.products
    where
        origin_file = (select max(origin_file) from awsdatacatalog.oxxo_silver_database.products)
    ) products
    ON sellout.id_producto = CAST(products.id_producto AS INT)
WHERE 
    id_fecha_adm between %(start_date)s and %(end_date)s
    AND products.id_producto NOT LIKE '%%\\%%' -- Ignore specific products GROUP BY nombre_supergrupo
GROUP BY marca_proveedor, date_trunc('week',date_parse(cast(id_fecha_adm as varchar), '%%Y%%m%%d'))"""


# TWO POSSIBLE COLUMN COMBINATIONS

query_2_1 = """SELECT
    nombre_supergrupo,
    nombre_supercategoria,
    NULL AS marca,
    NULL AS marca_proveedor,
    date_trunc('week',date_parse(cast(id_fecha_adm as varchar), '%%Y%%m%%d')) week,
    COUNT(DISTINCT CONCAT(CAST(id_ticket AS VARCHAR), '_', CAST(id_tienda AS VARCHAR), '_', CAST(id_fecha_adm AS VARCHAR))) AS tickets,
    SUM(venta_amt) AS venta_tmt,
    COUNT(sellout.id_producto) AS articulos,
    SUM(venta_amt) / NULLIF(COUNT(DISTINCT CONCAT(CAST(id_ticket AS VARCHAR), '_', CAST(id_tienda AS VARCHAR), '_', CAST(id_fecha_adm AS VARCHAR))), 0) AS ticket_promedio
FROM 
    awsdatacatalog.oxxo_silver_database.sellout_promotions sellout
LEFT JOIN
    (select
        id_producto,
        nombre_supergrupo,
        nombre_supercategoria,
        marca,
        marca_proveedor
    from
        awsdatacatalog.oxxo_silver_database.products
    where
        origin_file = (select max(origin_file) from awsdatacatalog.oxxo_silver_database.products)
    ) products
    ON sellout.id_producto = CAST(products.id_producto AS INT)
WHERE 
    id_fecha_adm between %(start_date)s and %(end_date)s
    AND products.id_producto NOT LIKE '%%\\%%' -- Ignore specific products GROUP BY nombre_supergrupo
GROUP BY nombre_supergrupo, nombre_supercategoria, date_trunc('week',date_parse(cast(id_fecha_adm as varchar), '%%Y%%m%%d'))"""


query_2_2= """SELECT
    nombre_supergrupo,
    NULL AS nombre_supercategoria,
    marca,
    NULL AS marca_proveedor,
    date_trunc('week',date_parse(cast(id_fecha_adm as varchar), '%%Y%%m%%d')) week,
    COUNT(DISTINCT CONCAT(CAST(id_ticket AS VARCHAR), '_', CAST(id_tienda AS VARCHAR), '_', CAST(id_fecha_adm AS VARCHAR))) AS tickets,
    SUM(venta_amt) AS venta_tmt,
    COUNT(sellout.id_producto) AS articulos,
    SUM(venta_amt) / NULLIF(COUNT(DISTINCT CONCAT(CAST(id_ticket AS VARCHAR), '_', CAST(id_tienda AS VARCHAR), '_', CAST(id_fecha_adm AS VARCHAR))), 0) AS ticket_promedio
FROM 
    awsdatacatalog.oxxo_silver_database.sellout_promotions sellout
LEFT JOIN
    (select
        id_producto,
        nombre_supergrupo,
        nombre_supercategoria,
        marca,
        marca_proveedor
    from
        awsdatacatalog.oxxo_silver_database.products
    where
        origin_file = (select max(origin_file) from awsdatacatalog.oxxo_silver_database.products)
    ) products
    ON sellout.id_producto = CAST(products.id_producto AS INT)
WHERE 
    id_fecha_adm between %(start_date)s and %(end_date)s
    AND products.id_producto NOT LIKE '%%\\%%' -- Ignore specific products GROUP BY nombre_supergrupo
GROUP BY nombre_supergrupo, marca, date_trunc('week',date_parse(cast(id_fecha_adm as varchar), '%%Y%%m%%d'))"""


query_2_3 = """SELECT
    nombre_supergrupo,
    NULL AS nombre_supercategoria,
    NULL AS marca,
    marca_proveedor,
    date_trunc('week',date_parse(cast(id_fecha_adm as varchar), '%%Y%%m%%d')) week,
    COUNT(DISTINCT CONCAT(CAST(id_ticket AS VARCHAR), '_', CAST(id_tienda AS VARCHAR), '_', CAST(id_fecha_adm AS VARCHAR))) AS tickets,
    SUM(venta_amt) AS venta_tmt,
    COUNT(sellout.id_producto) AS articulos,
    SUM(venta_amt) / NULLIF(COUNT(DISTINCT CONCAT(CAST(id_ticket AS VARCHAR), '_', CAST(id_tienda AS VARCHAR), '_', CAST(id_fecha_adm AS VARCHAR))), 0) AS ticket_promedio
FROM 
    awsdatacatalog.oxxo_silver_database.sellout_promotions sellout
LEFT JOIN
    (select
        id_producto,
        nombre_supergrupo,
        nombre_supercategoria,
        marca,
        marca_proveedor
    from
        awsdatacatalog.oxxo_silver_database.products
    where
        origin_file = (select max(origin_file) from awsdatacatalog.oxxo_silver_database.products)
    ) products
    ON sellout.id_producto = CAST(products.id_producto AS INT)
WHERE 
    id_fecha_adm between %(start_date)s and %(end_date)s
    AND products.id_producto NOT LIKE '%%\\%%' -- Ignore specific products GROUP BY nombre_supergrupo
GROUP BY nombre_supergrupo, marca_proveedor, date_trunc('week',date_parse(cast(id_fecha_adm as varchar), '%%Y%%m%%d'))"""


query_2_4 = """SELECT
    NULL AS nombre_supergrupo,
    nombre_supercategoria,
    marca,
    NULL AS marca_proveedor,
    date_trunc('week',date_parse(cast(id_fecha_adm as varchar), '%%Y%%m%%d')) week,
    COUNT(DISTINCT CONCAT(CAST(id_ticket AS VARCHAR), '_', CAST(id_tienda AS VARCHAR), '_', CAST(id_fecha_adm AS VARCHAR))) AS tickets,
    SUM(venta_amt) AS venta_tmt,
    COUNT(sellout.id_producto) AS articulos,
    SUM(venta_amt) / NULLIF(COUNT(DISTINCT CONCAT(CAST(id_ticket AS VARCHAR), '_', CAST(id_tienda AS VARCHAR), '_', CAST(id_fecha_adm AS VARCHAR))), 0) AS ticket_promedio
FROM 
    awsdatacatalog.oxxo_silver_database.sellout_promotions sellout
LEFT JOIN
    (select
        id_producto,
        nombre_supergrupo,
        nombre_supercategoria,
        marca,
        marca_proveedor
    from
        awsdatacatalog.oxxo_silver_database.products
    where
        origin_file = (select max(origin_file) from awsdatacatalog.oxxo_silver_database.products)
    ) products
    ON sellout.id_producto = CAST(products.id_producto AS INT)
WHERE 
    id_fecha_adm between %(start_date)s and %(end_date)s
    AND products.id_producto NOT LIKE '%%\\%%' -- Ignore specific products GROUP BY nombre_supergrupo
GROUP BY nombre_supercategoria, marca, date_trunc('week',date_parse(cast(id_fecha_adm as varchar), '%%Y%%m%%d'))"""


query_2_5 = """SELECT
    NULL AS nombre_supergrupo,
    nombre_supercategoria,
    NULL AS marca,
    marca_proveedor,
    date_trunc('week',date_parse(cast(id_fecha_adm as varchar), '%%Y%%m%%d')) week,
    COUNT(DISTINCT CONCAT(CAST(id_ticket AS VARCHAR), '_', CAST(id_tienda AS VARCHAR), '_', CAST(id_fecha_adm AS VARCHAR))) AS tickets,
    SUM(venta_amt) AS venta_tmt,
    COUNT(sellout.id_producto) AS articulos,
    SUM(venta_amt) / NULLIF(COUNT(DISTINCT CONCAT(CAST(id_ticket AS VARCHAR), '_', CAST(id_tienda AS VARCHAR), '_', CAST(id_fecha_adm AS VARCHAR))), 0) AS ticket_promedio
FROM 
    awsdatacatalog.oxxo_silver_database.sellout_promotions sellout
LEFT JOIN
    (select
        id_producto,
        nombre_supergrupo,
        nombre_supercategoria,
        marca,
        marca_proveedor
    from
        awsdatacatalog.oxxo_silver_database.products
    where
        origin_file = (select max(origin_file) from awsdatacatalog.oxxo_silver_database.products)
    ) products
    ON sellout.id_producto = CAST(products.id_producto AS INT)
WHERE 
    id_fecha_adm between %(start_date)s and %(end_date)s
    AND products.id_producto NOT LIKE '%%\\%%' -- Ignore specific products GROUP BY nombre_supergrupo
GROUP BY nombre_supercategoria, marca_proveedor, date_trunc('week',date_parse(cast(id_fecha_adm as varchar), '%%Y%%m%%d'))"""


query_2_6 = """SELECT
    NULL AS nombre_supergrupo,
    NULL AS nombre_supercategoria,
    marca,
    marca_proveedor,
    date_trunc('week',date_parse(cast(id_fecha_adm as varchar), '%%Y%%m%%d')) week,
    COUNT(DISTINCT CONCAT(CAST(id_ticket AS VARCHAR), '_', CAST(id_tienda AS VARCHAR), '_', CAST(id_fecha_adm AS VARCHAR))) AS tickets,
    SUM(venta_amt) AS venta_tmt,
    COUNT(sellout.id_producto) AS articulos,
    SUM(venta_amt) / NULLIF(COUNT(DISTINCT CONCAT(CAST(id_ticket AS VARCHAR), '_', CAST(id_tienda AS VARCHAR), '_', CAST(id_fecha_adm AS VARCHAR))), 0) AS ticket_promedio
FROM 
    awsdatacatalog.oxxo_silver_database.sellout_promotions sellout
LEFT JOIN
    (select
        id_producto,
        nombre_supergrupo,
        nombre_supercategoria,
        marca,
        marca_proveedor
    from
        awsdatacatalog.oxxo_silver_database.products
    where
        origin_file = (select max(origin_file) from awsdatacatalog.oxxo_silver_database.products)
    ) products
    ON sellout.id_producto = CAST(products.id_producto AS INT)
WHERE 
    id_fecha_adm between %(start_date)s and %(end_date)s
    AND products.id_producto NOT LIKE '%%\\%%' -- Ignore specific products GROUP BY nombre_supergrupo
GROUP BY marca, marca_proveedor, date_trunc('week',date_parse(cast(id_fecha_adm as varchar), '%%Y%%m%%d'))"""


#  THREE COLUMN POSSIBLE COMBINATIONS

query_3_1 = """SELECT
    nombre_supergrupo,
    nombre_supercategoria,
    marca,
    NULL AS marca_proveedor,
    date_trunc('week',date_parse(cast(id_fecha_adm as varchar), '%%Y%%m%%d')) week,
    COUNT(DISTINCT CONCAT(CAST(id_ticket AS VARCHAR), '_', CAST(id_tienda AS VARCHAR), '_', CAST(id_fecha_adm AS VARCHAR))) AS tickets,
    SUM(venta_amt) AS venta_tmt,
    COUNT(sellout.id_producto) AS articulos,
    SUM(venta_amt) / NULLIF(COUNT(DISTINCT CONCAT(CAST(id_ticket AS VARCHAR), '_', CAST(id_tienda AS VARCHAR), '_', CAST(id_fecha_adm AS VARCHAR))), 0) AS ticket_promedio
FROM 
    awsdatacatalog.oxxo_silver_database.sellout_promotions sellout
LEFT JOIN
    (select
        id_producto,
        nombre_supergrupo,
        nombre_supercategoria,
        marca,
        marca_proveedor
    from
        awsdatacatalog.oxxo_silver_database.products
    where
        origin_file = (select max(origin_file) from awsdatacatalog.oxxo_silver_database.products)
    ) products
    ON sellout.id_producto = CAST(products.id_producto AS INT)
WHERE 
    id_fecha_adm between %(start_date)s and %(end_date)s
    AND products.id_producto NOT LIKE '%%\\%%' -- Ignore specific products GROUP BY nombre_supergrupo
GROUP BY nombre_supergrupo, nombre_supercategoria, marca, date_trunc('week',date_parse(cast(id_fecha_adm as varchar), '%%Y%%m%%d'))"""


query_3_2 = """SELECT
    nombre_supergrupo,
    nombre_supercategoria,
    NULL AS marca,
    marca_proveedor,
    date_trunc('week',date_parse(cast(id_fecha_adm as varchar), '%%Y%%m%%d')) week,
    COUNT(DISTINCT CONCAT(CAST(id_ticket AS VARCHAR), '_', CAST(id_tienda AS VARCHAR), '_', CAST(id_fecha_adm AS VARCHAR))) AS tickets,
    SUM(venta_amt) AS venta_tmt,
    COUNT(sellout.id_producto) AS articulos,
    SUM(venta_amt) / NULLIF(COUNT(DISTINCT CONCAT(CAST(id_ticket AS VARCHAR), '_', CAST(id_tienda AS VARCHAR), '_', CAST(id_fecha_adm AS VARCHAR))), 0) AS ticket_promedio
FROM 
    awsdatacatalog.oxxo_silver_database.sellout_promotions sellout
LEFT JOIN
    (select
        id_producto,
        nombre_supergrupo,
        nombre_supercategoria,
        marca,
        marca_proveedor
    from
        awsdatacatalog.oxxo_silver_database.products
    where
        origin_file = (select max(origin_file) from awsdatacatalog.oxxo_silver_database.products)
    ) products
    ON sellout.id_producto = CAST(products.id_producto AS INT)
WHERE 
    id_fecha_adm between %(start_date)s and %(end_date)s
    AND products.id_producto NOT LIKE '%%\\%%' -- Ignore specific products GROUP BY nombre_supergrupo
GROUP BY nombre_supergrupo, nombre_supercategoria, marca_proveedor, date_trunc('week',date_parse(cast(id_fecha_adm as varchar), '%%Y%%m%%d'))"""


query_3_3 = """SELECT
    nombre_supergrupo,
    NULL AS nombre_supercategoria,
    marca,
    marca_proveedor,
    date_trunc('week',date_parse(cast(id_fecha_adm as varchar), '%%Y%%m%%d')) week,
    COUNT(DISTINCT CONCAT(CAST(id_ticket AS VARCHAR), '_', CAST(id_tienda AS VARCHAR), '_', CAST(id_fecha_adm AS VARCHAR))) AS tickets,
    SUM(venta_amt) AS venta_tmt,
    COUNT(sellout.id_producto) AS articulos,
    SUM(venta_amt) / NULLIF(COUNT(DISTINCT CONCAT(CAST(id_ticket AS VARCHAR), '_', CAST(id_tienda AS VARCHAR), '_', CAST(id_fecha_adm AS VARCHAR))), 0) AS ticket_promedio
FROM 
    awsdatacatalog.oxxo_silver_database.sellout_promotions sellout
LEFT JOIN
    (select
        id_producto,
        nombre_supergrupo,
        nombre_supercategoria,
        marca,
        marca_proveedor
    from
        awsdatacatalog.oxxo_silver_database.products
    where
        origin_file = (select max(origin_file) from awsdatacatalog.oxxo_silver_database.products)
    ) products
    ON sellout.id_producto = CAST(products.id_producto AS INT)
WHERE 
    id_fecha_adm between %(start_date)s and %(end_date)s
    AND products.id_producto NOT LIKE '%%\\%%' -- Ignore specific products GROUP BY nombre_supergrupo
GROUP BY nombre_supergrupo, marca, marca_proveedor, date_trunc('week',date_parse(cast(id_fecha_adm as varchar), '%%Y%%m%%d')) """


query_3_4 = """SELECT
    NULL AS nombre_supergrupo,
    nombre_supercategoria,
    marca,
    marca_proveedor,
    date_trunc('week',date_parse(cast(id_fecha_adm as varchar), '%%Y%%m%%d')) week,
    COUNT(DISTINCT CONCAT(CAST(id_ticket AS VARCHAR), '_', CAST(id_tienda AS VARCHAR), '_', CAST(id_fecha_adm AS VARCHAR))) AS tickets,
    SUM(venta_amt) AS venta_tmt,
    COUNT(sellout.id_producto) AS articulos,
    SUM(venta_amt) / NULLIF(COUNT(DISTINCT CONCAT(CAST(id_ticket AS VARCHAR), '_', CAST(id_tienda AS VARCHAR), '_', CAST(id_fecha_adm AS VARCHAR))), 0) AS ticket_promedio
FROM 
    awsdatacatalog.oxxo_silver_database.sellout_promotions sellout
LEFT JOIN
    (select
        id_producto,
        nombre_supergrupo,
        nombre_supercategoria,
        marca,
        marca_proveedor
    from
        awsdatacatalog.oxxo_silver_database.products
    where
        origin_file = (select max(origin_file) from awsdatacatalog.oxxo_silver_database.products)
    ) products
    ON sellout.id_producto = CAST(products.id_producto AS INT)
WHERE 
    id_fecha_adm between %(start_date)s and %(end_date)s
    AND products.id_producto NOT LIKE '%%\\%%' -- Ignore specific products GROUP BY nombre_supergrupo
GROUP BY nombre_supercategoria, marca, marca_proveedor, date_trunc('week',date_parse(cast(id_fecha_adm as varchar), '%%Y%%m%%d'))"""


#  FOUR COLUMN  POSSIBLE COMBINATIONS

query_4_1 = """ SELECT
    nombre_supergrupo,
    nombre_supercategoria,
    marca,
    marca_proveedor,
    date_trunc('week',date_parse(cast(id_fecha_adm as varchar), '%%Y%%m%%d')) week,
    COUNT(DISTINCT CONCAT(CAST(id_ticket AS VARCHAR), '_', CAST(id_tienda AS VARCHAR), '_', CAST(id_fecha_adm AS VARCHAR))) AS tickets,
    SUM(venta_amt) AS venta_tmt,
    COUNT(sellout.id_producto) AS articulos,
    SUM(venta_amt) / NULLIF(COUNT(DISTINCT CONCAT(CAST(id_ticket AS VARCHAR), '_', CAST(id_tienda AS VARCHAR), '_', CAST(id_fecha_adm AS VARCHAR))), 0) AS ticket_promedio
FROM 
    awsdatacatalog.oxxo_silver_database.sellout_promotions sellout
LEFT JOIN
    (select
        id_producto,
        nombre_supergrupo,
        nombre_supercategoria,
        marca,
        marca_proveedor
    from
        awsdatacatalog.oxxo_silver_database.products
    where
        origin_file = (select max(origin_file) from awsdatacatalog.oxxo_silver_database.products)
    ) products
    ON sellout.id_producto = CAST(products.id_producto AS INT)
WHERE 
    id_fecha_adm between %(start_date)s and %(end_date)s
    AND products.id_producto NOT LIKE '%%\\%%' -- Ignore specific products GROUP BY nombre_supergrupo
GROUP BY nombre_supergrupo, nombre_supercategoria, marca, marca_proveedor, date_trunc('week',date_parse(cast(id_fecha_adm as varchar), '%%Y%%m%%d'))"""


# Base query
create_dataframe(query_0_1, df_name = 'df_0_1')

# One-column combinations
create_dataframe(query_1_1, df_name = 'df_1_1')
create_dataframe(query_1_2, df_name = 'df_1_2')
create_dataframe(query_1_3, df_name = 'df_1_3')
create_dataframe(query_1_4, df_name = 'df_1_4')

# Two-column combinations
create_dataframe(query_2_1, df_name = 'df_2_1')
create_dataframe(query_2_2, df_name = 'df_2_2')
create_dataframe(query_2_3, df_name = 'df_2_3')
create_dataframe(query_2_4, df_name = 'df_2_4')
create_dataframe(query_2_5, df_name = 'df_2_5')
create_dataframe(query_2_6, df_name = 'df_2_6')

# Three-column combinations
create_dataframe(query_3_1, df_name = 'df_3_1')
create_dataframe(query_3_2, df_name = 'df_3_2')
create_dataframe(query_3_3, df_name = 'df_3_3')
create_dataframe(query_3_4, df_name = 'df_3_4')

#  Four-column combination
create_dataframe(query_4_1, df_name = 'df_4_1')