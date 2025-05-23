-- sellout logic
-- this query pulls data at chain, store, product, day level, sales are used for different measures but mostly to calculate lost sales as % of total sales
        SELECT
             cast(sellout.sale_date as timestamp) fecha_
            ,substring(coalesce(ap_active.ups,sellout.source_id),8,4) cproducto_
            ,substring(coalesce(ap_product.ups,sellout.source_id),8,4) cproducto_individual_
            ,ab.export_id numerotienda_
            ,cadena.ccadena ccadena_
            ,case when resurtible.resurtible is NULL then 'Por identificar' else resurtible.resurtible end resurtible
            ,sum(sellout.sales) sellout_piezas
            ,sum(sellout.money) sellout_importe
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
            ragasa_redshift.public.analytics_product ap_active
            on coalesce(substitute_sub.active_product_id,sellout.product_id) = ap_active.id
        LEFT JOIN
            ragasa_redshift.public.analytics_product ap_product
            on coalesce(substitute_sub.product_id,sellout.product_id) = ap_product.id
        LEFT JOIN
            ragasa_redshift.public.analytics_batstore ab
            on sellout.store_id = ab.id -- store_id
        LEFT JOIN
            ragasa_redshift.public.sellout_sellout
            on sellout.sellout_id = sellout_sellout.id
        LEFT JOIN
            ragasa_interface.public.sellout_moderno_cadena cadena
            on sellout_sellout.channel_id = cadena.sellout_channel_id
        LEFT JOIN
        (SELECT
        	date,
        	coalesce (ap2.id, ap.id) as product_id,
        	store_id,
        	sellout_id,
        	case when is_replenishable = true then 'Resurtible' else NULL end resurtible
        FROM
        	kuona_analytics.public.perfect_order_restockstatus por
        LEFT JOIN
            kuona_analytics.public.analytics_product ap 
            ON ap.id = por.product_id
        LEFT JOIN 
        	ragasa_interface.public.productopadre rp 
        	ON rp.cproducto = substring(ap.ups,8,4)
        LEFT join 
        	kuona_analytics.public.analytics_product ap2 on 'Ragasa_' || rp.cproductopadrepropuesta = ap2.ups 
        ) resurtible
            on date_trunc('month',resurtible.date) = date_trunc('month',cast(sellout.sale_date as timestamp))
            and resurtible.product_id = ap_product.id
            and resurtible.sellout_id = sellout.sellout_id
            and resurtible.store_id = ab.id
        INNER JOIN
        (
            SELECT
                DISTINCT ccadena from ragasa_interface.public.sellout_moderno_desabastos

        )
        cadenas
        on cadenas.ccadena = cadena.ccadena 
        WHERE
            sellout.sale_date >= cast(date_add('year',-2,date_trunc('year',current_date)) as varchar)
            --and cadena.ccadena in ('CHE')
        GROUP by
            cast(sellout.sale_date as timestamp)
            ,substring(coalesce(ap_active.ups,sellout.source_id),8,4)
            ,substring(coalesce(ap_product.ups,sellout.source_id),8,4) 
            ,ab.export_id
            ,cadena.ccadena
            ,case when resurtible.resurtible is NULL then 'Por identificar' else resurtible.resurtible end