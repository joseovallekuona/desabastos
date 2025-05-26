SELECT 
             precio_promedio.promotionid
            ,precio_promedio.folio_retek
            ,precio_promedio.precio_promedio_promo valor_sello
            ,top_sku.id_producto
            ,top_sku.nombre_producto
            ,top_sku.nombre_supergrupo
            ,top_sku.nombre_supercategoria
            ,top_sku.marca
            ,top_sku.marca_proveedor
            ,top_sku.precio_promedio_promo_sku precio_promedio_top_sku
            ,promo_name.promocion_name nombre_promocion
            ,precio_promedio.venta_hipotetica
            ,precio_promedio.unidades_vendidas_num
            ,precio_promedio.tickets
            from
            (
                SELECT
                     *
                    ,venta_hipotetica / uds_promo precio_promedio_promo 
                FROM
                (
                    SELECT
                         sellos.promotionid
                    FROM
                        awsdatacatalog.oxxo_loyalty_raw_database.sellos_csv_gz sellos
                    where
                        try_cast(sellos.promotionid AS integer) IS NOT NULL
                    group by
                         sellos.promotionid
                )
                sellos
                inner join
                (
                    select 
                     sellout.folio_retek
                    ,sum(sellout.venta_amt) venta_amt
                    ,sum(sellout.venta_hipotetica) venta_hipotetica
                    ,sum(sellout.venta_amt_con_imp) venta_amt_con_imp
                    ,sum(sellout.venta_regular) venta_regular
                    ,sum(sellout.venta_promo) venta_promo
                    ,sum(sellout.uds_promo) uds_promo 
                    ,sum(sellout.uds_vendidas_num) unidades_vendidas_num
                    ,sum(sellout.uds_regular) uds_regular
                    ,count(distinct(concat(cast(sellout.id_ticket as varchar),' ', cast(sellout.id_tienda as varchar),' ',cast(sellout.id_fecha_adm as varchar)))) tickets
                    from 
                        awsdatacatalog.oxxo_silver_database.sellout_promotions sellout
                    left join
                        (
                        select
                            *
                        from
                            awsdatacatalog.oxxo_silver_database.products
                        where
                            origin_file = (select max(origin_file) from awsdatacatalog.oxxo_silver_database.products)
                        ) products
                        on sellout.id_producto = cast(products.id_producto as int)
                    where 
                        sellout.id_fecha_adm >= 20240101 
                        --and sellout.uds_promo = 1 
                        --and sellout.uds_regular = 0 
                    group by 
                     sellout.folio_retek
                )  sellout
                    on cast(sellos.promotionid as int) = sellout.folio_retek
            ) precio_promedio
            left join 
            (
                SELECT
                    *
                from
                (
                SELECT
                     *
                    ,venta_hipotetica_sku / uds_promo precio_promedio_promo_sku
                    ,row_number() over (partition by promotionid order by venta_hipotetica_sku desc) row_
                FROM
                (
                    SELECT
                         sellos.promotionid
                    FROM
                        awsdatacatalog.oxxo_loyalty_raw_database.sellos_csv_gz sellos
                    where
                        try_cast(sellos.promotionid AS integer) IS NOT NULL
                    group by
                         sellos.promotionid
                )
                sellos
                inner join
                (
                    select 
                     sellout.folio_retek
                    ,products.id_producto
                    ,products.nombre_producto
                    ,products.nombre_supergrupo
                    ,products.nombre_supercategoria
                    ,products.marca
                    ,products.marca_proveedor
                    ,sum(sellout.uds_promo) uds_promo 
                    ,sum(sellout.venta_hipotetica) venta_hipotetica_sku
                    from 
                        awsdatacatalog.oxxo_silver_database.sellout_promotions sellout
                    left join
                        (
                        select
                            *
                        from
                            awsdatacatalog.oxxo_silver_database.products
                        where
                            origin_file = (select max(origin_file) from awsdatacatalog.oxxo_silver_database.products)
                        ) products
                        on sellout.id_producto = cast(products.id_producto as int)
                    where 
                        sellout.id_fecha_adm >= 20240101 
                        and sellout.uds_promo = 1 
                        and sellout.uds_regular = 0 
                    group by 
                     sellout.folio_retek
                    ,products.id_producto
                    ,products.nombre_producto
                    ,products.nombre_supergrupo
                    ,products.nombre_supercategoria
                    ,products.marca
                    ,products.marca_proveedor
                    )  sellout
                        on cast(sellos.promotionid as int) = sellout.folio_retek
                    ) top_sku
                    where
                        row_ = 1            
            ) top_sku
            on 
                precio_promedio.promotionid = top_sku.promotionid
            left join
            (
                select
                    *
                from
                    (   
                    select
                        *
                        ,row_number() over(partition by folio_retek order by venta_amt desc) row_ 
                    from
                        (
                        select 
                             sellout.folio_retek
                            ,sellout.promocion_name
                            ,sum(venta_amt) venta_amt
                        from 
                            awsdatacatalog.oxxo_silver_database.sellout_promotions sellout
                        left join
                            (
                            select
                                *
                            from
                                awsdatacatalog.oxxo_silver_database.products
                            where
                                origin_file = (select max(origin_file) from awsdatacatalog.oxxo_silver_database.products)
                            ) products
                            on sellout.id_producto = cast(products.id_producto as int)
                        where 
                            sellout.id_fecha_adm >= 20240101 
                            and sellout.uds_promo = 1 
                            and sellout.uds_regular = 0 
                            and sellout.promocion_name is not null
                            and sellout.promocion_name not in (' ','')
                        group by 
                         sellout.folio_retek
                        ,sellout.promocion_name
                        )
                    ) 
                    where row_ = 1
            )
            promo_name 
            on
                precio_promedio.promotionid = cast(promo_name.folio_retek as varchar)