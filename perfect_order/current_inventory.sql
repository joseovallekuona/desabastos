
/*latest inventory interface*/

SELECT  
    *
    ,max(semana_ultima_fecha) over(partition by fecha_actual) max_fecha
	,date_trunc('week',cast(current_date as date)) semana_en_curso
	--,date_trunc('week', fecha_actual) semana_en_curso hardcoded for explanation purposes
FROM 
(
SELECT
	 date_trunc('week',ultima_fecha_inventario) semana_ultima_fecha
	,ultima_fecha_inventario
	,ccadena
	,NumeroTienda numerotienda
	,cproductopadrepropuesta cproductopadre
    ,fecha_actual
	,sum(case when tipo_inventario = 'inventario_tienda' then existencia_piezas_real else 0 end) inventario_tienda  
	,sum(case when tipo_inventario = 'inventario_transito' then existencia_piezas_real else 0 end) inventario_transito  
	,sum(case when tipo_inventario = 'inventario_cedis' then existencia_piezas_real else 0 end) inventario_cedis  
	,sum(case when tipo_inventario = 'inventario_ordenado' then existencia_piezas_real else 0 end) inventario_ordenado  
	,sum(existencia_piezas_real) inventario_tuberia
FROM
(
WITH
	latest_inventory AS (
		SELECT
			sms.ccadena,
			sms."NumeroTienda",
			pp.cproductopadrepropuesta,
			id_tipo_inventario,
			max("Fecha") AS ultima_fecha
		FROM
			ragasa_interface.public.sellout_moderno_inventarios sms
			INNER JOIN ragasa_interface.public.sellout_moderno_producto smp ON smp.id_dimproductos = sms.id_dimproductos
			INNER JOIN ragasa_interface.public.productopadre pp ON smp.cproducto = pp.cproducto
		WHERE
            sms.id_tipo_inventario = 1
            and sms.Fecha >= date_add('day',-90,cast(current_date as date))
		GROUP BY
			sms.ccadena,
			pp.cproductopadrepropuesta,
			id_tipo_inventario,
			sms."NumeroTienda"
	)
SELECT
	smi."Fecha" fecha_inventario,
	smi.ccadena,
	smi."NumeroTienda",
	pp.cproductopadrepropuesta,
	smi.id_tipo_inventario,
		case 
        when smi.id_tipo_inventario = 1 then 'inventario_tienda'
        when smi.id_tipo_inventario = 2 then 'inventario_transito'
        when smi.id_tipo_inventario = 3 then 'inventario_cedis'
        when smi.id_tipo_inventario = 4 then 'inventario_ordenado'
    end tipo_inventario,
    case when existencia_piezas_real < 0 then 0 else existencia_piezas_real end existencia_piezas_real,
	case when existencia_piezas < 0 then 0 else existencia_piezas end existencia_piezas, 
	latest_inventory.ultima_fecha ultima_fecha_inventario,
	date_add('day',1,current_date) fecha_actual
FROM
	ragasa_interface.public.sellout_moderno_inventarios smi
	INNER JOIN ragasa_interface.public.sellout_moderno_producto smp ON smp.id_dimproductos = smi.id_dimproductos
	INNER JOIN ragasa_interface.public.productopadre pp ON smp.cproducto = pp.cproducto
	JOIN latest_inventory 
	ON latest_inventory.ccadena = smi.ccadena 
	AND latest_inventory.cproductopadrepropuesta = pp.cproductopadrepropuesta
	AND latest_inventory.ultima_fecha = smi."Fecha"
	AND latest_inventory."NumeroTienda" = smi."NumeroTienda"
WHERE
	smi.Fecha >= date_add('day',-90,cast(current_date as date))
)
group by
	 date_trunc('week',ultima_fecha_inventario)
    ,ultima_fecha_inventario
	,ccadena
	,NumeroTienda
	,cproductopadrepropuesta
    ,fecha_actual
)