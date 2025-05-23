/*lógic forecast vs solumen sellout*/

select
    fcst.*
    ,ROW_NUMBER() OVER (PARTITION BY fcst.cproductopadre, fcst.tienda, fcst.ccadena, fcst.is_future_date order by fcst.fecha asc) week_order -- last 12 weeks are forecast, more then 13 are historical forecasts, is_future_date also partitions the data for weeks that haven't not passed yet    
    ,COALESCE(fcst.moving_avg_, LAST_VALUE(fcst.moving_avg_) IGNORE NULLS OVER (PARTITION BY fcst.cproductopadre, fcst.tienda, fcst.ccadena ORDER BY fcst.fecha asc ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) AS moving_avg
from 
    (
select 
     cast(fcst.fecha as timestamp) fecha
    ,row_number() over(partition by fcst.ccadena, fcst.tienda, fcst.cproductopadre order by cast(fcst.fecha as timestamp) desc ) week_order_ 
    ,case when cast(fcst.fecha as timestamp) >= date_trunc('week',cast(current_date as date)) then 1 else 0 end as is_future_date    
    ,fcst.cproductopadre
    ,fcst.tienda
    ,fcst.ccadena
    ,fcst.unidades_venta_prediccion
    ,AVG(sellout.sellout_piezas) OVER (partition by fcst.cproductopadre, fcst.tienda, fcst.ccadena ORDER BY fecha asc ROWS BETWEEN 4 PRECEDING AND 1 PRECEDING) moving_avg_
    ,sellout.sellout_piezas unidades_venta_real
    ,sellout.sellout_money / COALESCE(NULLIF(sellout.sellout_piezas, 0), NULL) precio_semana 
    ,rango_asertividad
    ,calendar
    ,status_resurtible
    ,prod.producto
    ,prod.marcaproducto
    ,tienda.nombre_tienda    
    ,tienda.areanielsen
    ,tienda.formato    
    ,tienda.longitud
    ,tienda.latitud
    ,prices.avg_ptc
    ,equivalencia.factorconversion    
from
    ragasa_interface.public.fcst_sellout_weekly fcst
    LEFT JOIN
    (
        SELECT
             cast(date_trunc('week',cast(sellout.sale_date as timestamp)) as date) week_
            ,substring(coalesce(ap.ups,sellout.source_id),8,4) cproducto
            ,ab.export_id numerotienda
            ,cadena.ccadena
            ,sum(sellout.sales) sellout_piezas
            ,sum(sellout.money) sellout_money
        FROM
            ragasa_redshift.public.sellout_storeproductsellout sellout
        LEFT JOIN
            (
                SELECT
                    sellout_id, -- sellout_id
                    product_id, -- product_id
                    active_product_id
                FROM
                    ragasa_redshift.public.sellout_substituteproduct ss
                JOIN
                    ragasa_redshift.public.sellout_selloutsubstituteproductgroup sse
                    ON ss.substitute_product_group_id = sse.substitute_product_group_id
            )
            substitute_sub
            ON sellout.sellout_id = substitute_sub.sellout_id
            AND sellout.product_id = substitute_sub.product_id
        LEFT JOIN
            ragasa_redshift.public.analytics_product ap
            on coalesce(substitute_sub.active_product_id,sellout.product_id) = ap.id
        LEFT JOIN
            ragasa_redshift.public.analytics_batstore ab
            on sellout.store_id = ab.id -- store_id
        LEFT JOIN
            ragasa_redshift.public.sellout_sellout
            on sellout.sellout_id = sellout_sellout.id
        LEFT JOIN
            ragasa_interface.public.sellout_moderno_cadena cadena
            on sellout_sellout.channel_id = cadena.sellout_channel_id
        WHERE
            extract(year from cast(sellout.sale_date as timestamp)) in (extract(year from current_date),extract(year from current_date)-1)
        GROUP by
             cast(date_trunc('week',cast(sellout.sale_date as timestamp)) as date)
            ,substring(coalesce(ap.ups,sellout.source_id),8,4)
            ,ab.export_id
            ,cadena.ccadena
    ) sellout
        on fcst.fecha = sellout.week_
        and fcst.cproductopadre = sellout.cproducto
        and fcst.tienda = sellout.numerotienda
        and fcst.ccadena = sellout.ccadena
    left JOIN
    (
    SELECT 
        ccadena
        ,cproducto
        ,avg_ptc
    FROM 
    (
    SELECT
        *
    FROM
        (
            SELECT
                 substring(coalesce(ap.ups,sellout.source_id),8,4) cproducto
                ,cadena.ccadena
                ,sum(sellout.sales) sellout_piezas
                ,sum(sellout.money) sellout_importe
                ,sum(sellout.money) / sum(sellout.sales) avg_ptc
            FROM
                ragasa_redshift.public.sellout_storeproductsellout sellout
            LEFT JOIN
                (
                SELECT
                    sellout_id, -- sellout_id
                    product_id, -- product_id
                    active_product_id
                FROM
                    ragasa_redshift.public.sellout_substituteproduct ss
                JOIN
                    ragasa_redshift.public.sellout_selloutsubstituteproductgroup sse
                    ON ss.substitute_product_group_id = sse.substitute_product_group_id
                )
                substitute_sub
                ON sellout.sellout_id = substitute_sub.sellout_id
                AND sellout.product_id = substitute_sub.product_id
            LEFT JOIN
                ragasa_redshift.public.analytics_product ap
                on coalesce(substitute_sub.active_product_id,sellout.product_id) = ap.id
            LEFT JOIN
                ragasa_redshift.public.analytics_batstore ab
                on sellout.store_id = ab.id -- store_id
            LEFT JOIN
                ragasa_redshift.public.sellout_sellout
                on sellout.sellout_id = sellout_sellout.id
            LEFT JOIN
                ragasa_interface.public.sellout_moderno_cadena cadena
                on sellout_sellout.channel_id = cadena.sellout_channel_id
            WHERE
                extract(year from cast(sellout.sale_date as timestamp)) in (extract(year from current_date),extract(year from current_date)-1)
                and sellout.sales > 0
                and sellout.money > 0
            GROUP by
                substring(coalesce(ap.ups,sellout.source_id),8,4)
                ,cadena.ccadena
        ) sellout
    )
        prices
    )
    prices
    on
     fcst.ccadena = prices.ccadena
     and fcst.cproductopadre = prices.cproducto     
    left JOIN
        ragasa_interface.public.producto prod
        ON fcst.cproductopadre = prod.cproducto
    left JOIN 
    (
        SELECT
            ccadena
            ,numerotienda
            ,formato
            ,tienda nombre_tienda
            ,longitud
            ,latitud
            ,estado
            ,areanielsen
        FROM
        (
            SELECT
                ccadena
                ,numerotienda
                ,formato
                ,tienda
                ,longitud
                ,latitud
                ,estado
                ,areanielsen
                ,row_number() over(partition by ccadena, numerotienda order by create_date desc) row_store
            FROM
                ragasa_interface.public.sellout_moderno_tienda
        )
        WHERE
            row_store = 1
    ) tienda
        ON fcst.ccadena = tienda.ccadena
        AND fcst.tienda = tienda.numerotienda
    left JOIN
    (
        SELECT
        *
        FROM
        (
        SELECT *, row_number() over(partition by cproducto, umequivalente order by create_date desc) row_
        FROM ragasa_interface.public.equivalencias_producto
        )
        WHERE row_ = 1
    ) equivalencia
        ON fcst.cproductopadre = equivalencia.cproducto
        AND equivalencia.umequivalente = 'UC'
WHERE
    (
    sellout.sellout_piezas IS NULL
    or sellout.sellout_piezas > 0 
    )
    and extract(year from fcst.fecha ) in (extract(year from current_date) - 1, extract(year from current_date),extract(year from current_date)+1) 
) fcst