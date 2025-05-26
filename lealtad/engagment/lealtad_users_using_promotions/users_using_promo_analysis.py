import pandas as pd
import numpy as np
from pyathena import connect
from pyathena.cursor import Cursor
from datetime import datetime, timedelta
import math
import logging
import argparse

# Set connection
conn = connect()


# Argument parser for CLI parameters
parser = argparse.ArgumentParser(description="Create filtered dataframes.")
parser.add_argument("--start_date", type=int, help="Start date for filtering")
parser.add_argument("--end_date", type=int, help="End date for filtering")

# Define timeframe variables for parser
args = parser.parse_args()

# Start timeframe
start_date = args.start_date
end_date = args.end_date

start_date_str = str(args.start_date)
end_date_str = str(args.end_date)

# Configure logging
logging.basicConfig(filename=f"churn_creation_log_{start_date}_{end_date}.txt", 
                    level=logging.ERROR, 
                    format='%(asctime)s - %(levelname)s - %(message)s')

# SQL query for active customers
query_active = """
    SELECT
         %(start_date)s "from"
        ,%(end_date)s "to" 
        ,perfil_premia
        ,etiquetas_necesidad
        ,etiqueta_redencion
        ,rango_frecuencia
        ,rango_ticket
        ,count(distinct accounts.accountid) customers_with_transactions
    FROM
        (
        SELECT
            distinct accountid
        FROM
        (
            SELECT
                *
            FROM
            (
                SELECT
                    distinct estrellas.accountid
                FROM
                    awsdatacatalog.oxxo_loyalty_raw_database.estrellas_csv_gz estrellas
                LEFT JOIN
                    (
                    SELECT
                        distinct transactionid, accountid
                    FROM 
                        awsdatacatalog.oxxo_loyalty_raw_database.estrellas_csv_gz estrellas
                    where
                        (NOT regexp_like(SUBSTRING(estrellas.transactionid, LENGTH(estrellas.transactionid) - 5, 2),'^[0-9]+$')
                        OR NOT regexp_like(SUBSTRING(estrellas.transactionid, LENGTH(estrellas.transactionid) - 3, 2),'^[0-9]+$')
                        OR NOT regexp_like(SUBSTRING(estrellas.transactionid, 11, 2),'^[0-9]+$')
                        )
                    ) exclude_estrellas
                on
                    estrellas.transactionid = exclude_estrellas.transactionid
                where
                    exclude_estrellas.transactionid is null
                    and estrellas.dia_id between %(start_date)s and %(end_date)s        
            ) 
            estrellas_transactionids
            UNION
            SELECT
                *
            FROM
            (
                SELECT
                    distinct sellos.accountid
                FROM
                    awsdatacatalog.oxxo_loyalty_raw_database.sellos_csv_gz sellos
                LEFT JOIN
                    (
                    SELECT
                        distinct transactionid, accountid
                    FROM 
                        awsdatacatalog.oxxo_loyalty_raw_database.sellos_csv_gz sellos
                    where
                        (NOT regexp_like(SUBSTRING(sellos.transactionid, LENGTH(sellos.transactionid) - 5, 2),'^[0-9]+$')
                        OR NOT regexp_like(SUBSTRING(sellos.transactionid, LENGTH(sellos.transactionid) - 3, 2),'^[0-9]+$')
                        OR NOT regexp_like(SUBSTRING(sellos.transactionid, 11, 2),'^[0-9]+$')
                        )
                    ) exclude_sellos
                on
                    sellos.transactionid = exclude_sellos.transactionid
                where
                    exclude_sellos.transactionid is null
                    and sellos.dia_id between %(start_date)s and %(end_date)s
            ) 
            sellos_transactionids
            UNION
            SELECT
                *
            FROM
            (
                SELECT
                    distinct bpos.accountid
                FROM
                    awsdatacatalog.oxxo_loyalty_raw_database.bpos_csv_gz bpos
                LEFT JOIN
                    (
                    SELECT
                        distinct transactionid, accountid
                    FROM 
                        awsdatacatalog.oxxo_loyalty_raw_database.bpos_csv_gz bpos
                    where
                        (NOT regexp_like(SUBSTRING(bpos.transactionid, LENGTH(bpos.transactionid) - 5, 2),'^[0-9]+$')
                        OR NOT regexp_like(SUBSTRING(bpos.transactionid, LENGTH(bpos.transactionid) - 3, 2),'^[0-9]+$')
                        OR NOT regexp_like(SUBSTRING(bpos.transactionid, 11, 2),'^[0-9]+$')
                        )
                    ) exclude_bpos
                on
                    bpos.transactionid = exclude_bpos.transactionid
                where
                    exclude_bpos.transactionid is null
                    and bpos.dia_id between %(start_date)s and %(end_date)s
            ) 
            bpos_transactionids
        ) promos
        UNION
        SELECT
            *
        from
        (
        SELECT
            distinct accountid
        from
            awsdatacatalog.oxxo_loyalty_raw_database.cupones_csv_gz
        where
            length(cast(dia_id_redencion as varchar)) = 8
            and dia_id_redencion between %(start_date)s and %(end_date)s
        ) cupones
    ) lealtad_transacciones
    inner join
        awsdatacatalog.oxxo_loyalty_raw_database.mc_actual_csv accounts
        on accounts.accountid = lealtad_transacciones.accountid
    group by
         perfil_premia
        ,etiquetas_necesidad
        ,etiqueta_redencion
        ,rango_frecuencia
        ,rango_ticket  
"""

# SQL query for catalog customers
query_catalog = """
    select
         perfil_premia
        ,etiquetas_necesidad
        ,etiqueta_redencion
        ,rango_frecuencia
        ,rango_ticket  
        ,count(distinct accountid) accounts_catalog
    from
        awsdatacatalog.oxxo_loyalty_raw_database.mc_actual_csv
    where
        dia_de_registro IS NOT NULL 
        and dia_de_registro <> '""'
        and cast(dia_de_registro as date) <= date_parse(%(start_date)s, '%%Y%%m%%d')
    group by
         perfil_premia
        ,etiquetas_necesidad
        ,etiqueta_redencion
        ,rango_frecuencia
        ,rango_ticket  
"""


parameters_active = {'start_date': start_date, 'end_date': end_date}
parameters_catalog = {'start_date': start_date_str}

try:
    # Run query_active customers
    df_active = pd.read_sql(query_active, conn, params=parameters_active)

    # Run query_catalog for catalogue of customers
    df_catalog = pd.read_sql(query_catalog, conn, params=parameters_catalog)

    # Concatenate and export dataframe
    # df_concat = pd.concat([df_active, df_catalog], axis=1)
    df_concat = pd.merge(df_catalog,df_active, on = ['perfil_premia','etiquetas_necesidad','etiqueta_redencion','rango_frecuencia','rango_ticket'], how = 'left')
    df_name = 'users_using_promo'
    file_name = f"{df_name}_{start_date}_{end_date}"
    df_concat.to_csv(f"{file_name}.csv", index=False)
    print(file_name + 'process_finished')

except Exception as e:
    logging.error(f"Failed to create {file_name}: {e}")
    print(f"Error creating {file_name}. Check the log file for details.")
