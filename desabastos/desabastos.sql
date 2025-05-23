SELECT
             desabasto.fecha fecha_
            ,desabasto.ccadena ccadena_
            ,desabasto.tienda tienda_
            ,desabasto.cproducto cproducto_
            ,desabasto.vts_piezas vts_piezas_
            ,desabasto.vts_importe_real vts_importe_real_
            ,desabasto.existencia_piezas
            ,desabasto.criterio_desabasto
            ,desabasto.promedio_inventario
            ,desabasto.promedio_venta
            ,desabasto.precio
            ,desabasto.venta_perdida venta_perdida_
            ,CASE
                WHEN desabasto.status_resurtible = 'No Resurtible' THEN 'No Resurtible'
                WHEN desabasto.status_resurtible = 'Resurtible' THEN 'Resurtible'
                ELSE 'Por identificar'
                END status_resurtible
            ,desabasto.tienda_nueva
            ,desabasto.margen_venta
            ,desabasto.ptr
            ,desabasto.ptc
            ,desabasto.cogs
            ,desabasto.venta_perdida_cajas
            ,desabasto.ptc_promo
            ,desabasto.ptr_promo
            ,desabasto.cogs_promo
            ,desabasto.multiples_dias_sin_inventario
            ,desabasto.ptc_regular
            ,desabasto.ptr_regular
            ,desabasto.cogs_regular
            ,desabasto.venta_promedio_2_meses
            ,desabasto.dias_consecutivos_sin_inventario
            ,desabasto.venta_en_periodo_promocional
            ,resurtible.current_resurtible current_resurtible_
            -- added fieds
            ,desabasto.cproducto_individual cproducto_individual_
            ,desabasto.inventario_individual
            ,desabasto.inventario_grupo
            ,(case 
                when productos_agrupados ='' then 1.0
                when productos_agrupados is null then 1.0
                when desabasto.inventario_individual = 0 then 1.0
                else desabasto.inventario_individual * 1.0 / desabasto.inventario_grupo
                end
            ) * cast((case when desabasto.venta_perdida = '' then '0' else desabasto.venta_perdida end) as decimal) venta_perdida_dec
            ,cast((case when desabasto.venta_perdida = '' then '0' else desabasto.venta_perdida end) as decimal) venta_perdida_grupo
            ,cast((case when desabasto.precio = '' then '0' else desabasto.precio end) as decimal) precio_dec
            ,cast((case when desabasto.ptr = '' then '0' else desabasto.ptr end) as decimal) ptr_dec
            ,date_trunc('week',desabasto.fecha) semana_desabasto
            -- added fieds
        FROM
        (
            /*unnest and trim logic*/
            SELECT 
                 *
                ,TRIM(' ' FROM SPLIT_PART(element, ':', 1)) AS cproducto_individual
                ,CAST((TRIM(BOTH ')' FROM SPLIT_PART(SPLIT_PART(element, ':', 2), 'Decimal(', 2))) AS DECIMAL) inventario_individual
                ,SUM(CAST((TRIM(BOTH ')' FROM SPLIT_PART(SPLIT_PART(element, ':', 2), 'Decimal(', 2))) AS DECIMAL)) OVER (PARTITION BY fecha,ccadena,tienda,cproducto) inventario_grupo
            FROM
                (
                SELECT
                    *
                FROM 
                (
                SELECT
                    *
                FROM  
                    ragasa_interface.public.sellout_moderno_desabastos
                --WHERE
                    --ccadena = 'CHE'
                    --AND cproducto = '0017'
                    --AND productos_agrupados like '%|%'
                )
                CROSS JOIN UNNEST
                (
                SPLIT(REPLACE(case when productos_agrupados is null then cproducto else productos_agrupados end,'''','') , '|') --| or  ,
                ) AS t(element) -- Split `productos_agrupados` into individual key-value pairs
                ) desabasto
       )
       desabasto
        INNER JOIN
       (
        SELECT
            ccadena
            ,tienda
            ,cproducto
            ,CASE
                WHEN status_resurtible = 'No Resurtible' THEN 'N'
                WHEN status_resurtible = 'Resurtible' THEN 'Y'
                END current_resurtible
            FROM
                (
                SELECT
                    ccadena
                    ,tienda
                    ,cproducto
                    ,status_resurtible
                    ,row_number() over(partition by ccadena, tienda, cproducto order by fecha desc) row_
                FROM
                    ragasa_interface.public.sellout_moderno_desabastos
                )
                desabasto
            WHERE
                row_ = 1
        )
        resurtible
            ON desabasto.ccadena = resurtible.ccadena
            AND desabasto.tienda = resurtible.tienda
            AND desabasto.cproducto = resurtible.cproducto
        WHERE
            extract(year from desabasto.fecha) >= extract(year from current_date) - 2