
/* generate series of days for each combination and then left join to inv, with rules of inventory*/
select
    *
    ,case when out_of_stock = 0 then 0
     else row_number() over (partition by ccadena, tienda,cproducto, grupo order by fecha) - 1 
    end as dias_consecutivos_sin_inventario
from
    (
    select
        *
        ,fila - sum(out_of_stock) over (partition by ccadena, tienda,cproducto order by fecha asc
                rows between unbounded preceding and current row) 
        grupo
    from 
        (
        select 
          cast(series.date as date) fecha
         ,selloutid.channel_id 
         ,series.store_id
         ,series.product_id
         ,cadena.ccadena
         ,series.tienda
         ,series.cproducto
         ,prod.producto
         ,prod.marcaproducto  
         ,coalesce(tienda.formato,'') formato
         ,coalesce(tienda.nombre_tienda,'') nombre_tienda
         ,tienda.longitud
         ,tienda.latitud
         ,coalesce(tienda.estado,'') estado 
         ,tienda.areanielsen
         ,case 
            when inventory.units_inventory = 0 then 1
            when inventory.units_inventory IS NULL then 1
            else 0 end out_of_stock
         ,case when inventory.units_inventory IS NULL then 0 else inventory.units_inventory end units_inventory
         ,case when resurtibles.resurtible IS NULL then 'Sin definir' else resurtibles.resurtible end resurtible
         ,row_number() over (partition by cadena.ccadena, series.tienda, series.cproducto order by cast(series.date as date) asc) fila
         from
            (
            select 
                *
            from
                (
                select t.date
                from (
                		select sequence(
                				CAST(CAST(EXTRACT(year FROM current_date) - 1 AS VARCHAR) || '-01-01' AS DATE),
                				(select max(cast(cast(date as timestamp) as date)) date from ragasa_redshift.public.inventory_inventoryperiod),
                				interval '1' day
                			) dates
                	),
                	unnest(dates) as t(date)
                )series
                cross join
                    (
                SELECT
                    distinct
                     sellout_id            
                    ,iss.store_id
                    ,iss.product_id
                    ,ab.export_id tienda
                    ,SUBSTR(ap.ups,length(ap.ups)- 3) cproducto        
                FROM
                	ragasa_redshift.public.inventory_storeproductinventory iss
                	JOIN ragasa_redshift.public.inventory_inventoryperiod ii ON iss.inventory_period_id = ii.id
                	JOIN ragasa_redshift.public.analytics_product ap ON ap.id = iss.product_id
                	JOIN ragasa_redshift.public.analytics_batstore ab ON ab.id = iss.store_id
                	JOIN ragasa_redshift.public.inventory_inventorytype it ON iss.inventory_type_id = it.type
                WHERE
                	sellout_id in(65,59,71,64,52,54,55,56,58,62) --SELLOUTIDS de ragasa
                	AND cast(cast(ii.date as timestamp) as date) >= CAST(CAST(EXTRACT(year FROM current_date) - 1 AS VARCHAR) || '-01-01' AS DATE)
                	AND it.description = 'Existencia Tienda'
                    AND ii.active = True
                ) keys_
            ) series
        left join
            (
            SELECT
                 cast(cast(ii.date as timestamp) as date) date 
                ,sellout_id
                ,iss.store_id
                ,iss.product_id
                ,ab.export_id tienda
                ,SUBSTR(ap.ups,length(ap.ups)- 3) cproducto
                ,iss.units units_inventory
                ,row_number() over (partition by ii.date, sellout_id, iss.store_id, iss.product_id,ab.export_id,SUBSTR(ap.ups,length(ap.ups)- 3) order by iss.create_date desc) row_
            FROM
            	ragasa_redshift.public.inventory_storeproductinventory iss
            	JOIN ragasa_redshift.public.inventory_inventoryperiod ii ON iss.inventory_period_id = ii.id
            	JOIN ragasa_redshift.public.analytics_product ap ON ap.id = iss.product_id
            	JOIN ragasa_redshift.public.analytics_batstore ab ON ab.id = iss.store_id
            	JOIN ragasa_redshift.public.inventory_inventorytype it ON iss.inventory_type_id = it.type
            WHERE
            	sellout_id in(65,59,71,64,52,54,55,56,58,62) --SELLOUTIDS de ragasa
            	AND cast(cast(ii.date as timestamp) as date) >= CAST(CAST(EXTRACT(year FROM current_date) - 1 AS VARCHAR) || '-01-01' AS DATE)
            	AND it.description = 'Existencia Tienda'
                AND ii.active = True
            ) inventory
            on series.store_id = inventory.store_id
            and series.product_id = inventory.product_id
            and series.sellout_id = inventory.sellout_id
            and series.date = inventory.date
            and row_ = 1
        left join 
            ragasa_redshift.public.sellout_sellout selloutid
            on series.sellout_id = selloutid.id
        left join
            ragasa_interface.public.sellout_moderno_cadena cadena
            on selloutid.channel_id = cadena.sellout_channel_id
        left join 
            (SELECT
                distinct --review duplicates logic with arath
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
            ) resurtibles
                on date_trunc('month',resurtibles.date) = date_trunc('month',series.date)
                and resurtibles.product_id = series.product_id
                and resurtibles.sellout_id = series.sellout_id
                and resurtibles.store_id = series.store_id
                and resurtibles.resurtible = 'Resurtible'
        left join
            ragasa_interface.public.producto prod
            ON series.cproducto = prod.cproducto
        left join
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
            )
            tienda
            on cadena.ccadena = tienda.ccadena
            and series.tienda = tienda.numerotienda
        ) hist_inventario
    ) analysis