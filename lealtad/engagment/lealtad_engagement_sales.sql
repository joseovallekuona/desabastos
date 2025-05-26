SELECT
        	 perfil_premia
        	,etiquetas_necesidad
        	,etiqueta_redencion
            ,rango_frecuencia
            ,rango_ticket
            ,sum(venta_amt) venta_amt
            ,count(*) records   
            ,count(distinct(ticket_id)) tickets 
        FROM
        (
            SELECT
                accountid
                ,concat(cast(sellout.id_ticket as varchar),' ', cast(sellout.id_tienda as varchar),' ',cast(sellout.id_fecha_adm as varchar)) ticket_id
            FROM
            (
                SELECT
                    distinct 
                     --transactionid
                     tienda_key id_tienda
                    ,dia_id id_fecha_adm
                    ,CAST(SUBSTRING(transactionid, 11, 2) AS INT) id_caja
                    ,CAST(SUBSTRING(transactionid, LENGTH(transactionid) - 5, 2) AS INT) * 60 + 
                     CAST(SUBSTRING(transactionid, LENGTH(transactionid) - 3, 2) AS INT) id_hora
                    ,accountid
                FROM
                (
                    SELECT
                        *
                    FROM
                    (
                        SELECT
                            distinct estrellas.transactionid, estrellas.tienda_key, estrellas.dia_id, estrellas.accountid
                        FROM
                            awsdatacatalog.oxxo_loyalty_raw_database.estrellas_csv_gz estrellas
                        LEFT JOIN
                            (
                                SELECT
                                    distinct transactionid
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
                        and estrellas.dia_id >= 20250101
                    ) 
                    estrellas_transactionids
                    UNION
                    SELECT
                        *
                    FROM
                    (
                        SELECT
                            distinct sellos.transactionid, sellos.tienda_key, sellos.dia_id, sellos.accountid
                        FROM
                            awsdatacatalog.oxxo_loyalty_raw_database.sellos_csv_gz sellos
                        LEFT JOIN
                        (
                            SELECT
                                distinct transactionid
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
                        and sellos.dia_id >= 20250101
                    ) 
                    sellos_transactionids
                    UNION
                    SELECT
                        *
                    FROM
                    (
                        SELECT
                            distinct bpos.transactionid, bpos.tienda_key, bpos.dia_id, bpos.accountid
                        FROM
                            awsdatacatalog.oxxo_loyalty_raw_database.bpos_csv_gz bpos
                        LEFT JOIN
                            (
                            SELECT
                                distinct transactionid
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
                        and bpos.dia_id >= 20250101
                    ) 
                    bpos_transactionids
                ) promos
                UNION
                SELECT
                    *
                from
                (
                SELECT
                    distinct 
                     id_tienda
                    ,id_fecha_adm
                    ,id_caja
                    ,coalesce(cast(id_hora AS INT), 0) id_hora
                    ,'Cupones' accountid
                from
                    awsdatacatalog.oxxo_silver_database.sellout_promotions
                where
                    id_fecha_adm >= 20250101
                    and concat(cast(id_ticket as varchar),' ', cast(id_tienda as varchar),' ',cast(id_fecha_adm as varchar)) in 
                    (
                    SELECT
                        distinct(concat(cast(id_ticket as varchar),' ', cast(id_tienda as varchar),' ',cast(id_fecha_adm as varchar))) as ticket
                    from 
                        awsdatacatalog.oxxo_silver_database.sellout_promotions sellout
                    where
                         sellout.id_fecha_adm >= 20250101
                         and sellout.folio_retek 
                         in (SELECT distinct promotion_id FROM awsdatacatalog.oxxo_loyalty_raw_database.cupones_csv_gz)
                    )
                ) cupones
        ) lealtad_key
        inner join
            awsdatacatalog.oxxo_silver_database.sellout_promotions sellout
            on sellout.id_tienda = lealtad_key.id_tienda
            and sellout.id_fecha_adm = lealtad_key.id_fecha_adm
            and sellout.id_caja = lealtad_key.id_caja
            and (lealtad_key.id_hora - sellout.id_hora) between 0 and 10
        where
            sellout.id_fecha_adm >= 20250101
        group by
             accountid
            ,concat(cast(sellout.id_ticket as varchar),' ', cast(sellout.id_tienda as varchar),' ',cast(sellout.id_fecha_adm as varchar))
        ) ticket_id_account
        inner join
            awsdatacatalog.oxxo_loyalty_raw_database.mc_actual_csv accounts
            on ticket_id_account.accountid = accounts.accountid
        inner join 
            awsdatacatalog.oxxo_silver_database.sellout_promotions sellout
            on concat(cast(id_ticket as varchar),' ', cast(id_tienda as varchar),' ',cast(id_fecha_adm as varchar)) = ticket_id_account.ticket_id
        where
            id_fecha_adm >=20250101    
        group by
        	 perfil_premia
        	,etiquetas_necesidad
        	,etiqueta_redencion
            ,rango_frecuencia
            ,rango_ticket