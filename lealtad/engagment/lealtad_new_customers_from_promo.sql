SELECT
     accounts.perfil_premia
    ,accounts.etiquetas_necesidad
    ,accounts.etiqueta_redencion
    ,accounts.rango_frecuencia
    ,accounts.rango_ticket
    ,date_trunc('week',cast(accounts.fecha_registro as date)) as fecha
    ,sum(case when date_parse(cast(first_transaction.first_date as varchar), '%Y%m%d') = cast(accounts.fecha_registro as date) then 1 else 0 end) clientes_nuevo_promocion
    FROM
    (
    SELECT
        accountid, min(dia_id) first_date
    FROM
        (
        SELECT
            distinct accountid, dia_id
        FROM
        (
            SELECT
                *
            FROM
            (
                SELECT
                    distinct estrellas.accountid, estrellas.dia_id
                FROM
                    awsdatacatalog.oxxo_loyalty_raw_database.estrellas_csv_gz estrellas
                LEFT JOIN
                    (
                    SELECT
                        distinct transactionid, accountid, dia_id
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
                    and estrellas.dia_id >= 20240101
            ) 
            estrellas_transactionids
            UNION
            SELECT
                *
            FROM
            (
                SELECT
                    distinct sellos.accountid, sellos.dia_id
                FROM
                    awsdatacatalog.oxxo_loyalty_raw_database.sellos_csv_gz sellos
                LEFT JOIN
                    (
                    SELECT
                        distinct transactionid, accountid, dia_id
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
                    and sellos.dia_id >= 20240101
            ) 
            sellos_transactionids
            UNION
            SELECT
                *
            FROM
            (
                SELECT
                    distinct bpos.accountid, bpos.dia_id
                FROM
                    awsdatacatalog.oxxo_loyalty_raw_database.bpos_csv_gz bpos
                LEFT JOIN
                    (
                    SELECT
                        distinct transactionid, accountid, dia_id
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
                    and bpos.dia_id >= 20240101
            ) 
            bpos_transactionids
        ) promos
        UNION
        SELECT
            *
        from
        (
        SELECT
            distinct accountid, dia_id_redencion
        from
            awsdatacatalog.oxxo_loyalty_raw_database.cupones_csv_gz
        where
            length(cast(dia_id_redencion as varchar)) = 8
            and dia_id_redencion >= 20240101
        ) cupones
    ) lealtad_transacciones
    group by
        accountid
    ) first_transaction
    inner join
        awsdatacatalog.oxxo_loyalty_raw_database.mc_actual_csv accounts
        on  first_transaction.accountid = accounts.accountid
        and accounts.fecha_registro != ' '
        and accounts.fecha_registro is not null
        and accounts.fecha_registro != ''
    where
        date_parse(cast(first_transaction.first_date as varchar), '%Y%m%d') = cast(accounts.fecha_registro as date)
    group by
         accounts.perfil_premia
        ,accounts.etiquetas_necesidad
        ,accounts.etiqueta_redencion
        ,accounts.rango_frecuencia
        ,accounts.rango_ticket
        ,date_trunc('week',cast(accounts.fecha_registro as date))