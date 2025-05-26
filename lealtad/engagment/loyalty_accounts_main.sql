/*accouts main*/
    
select
    rango_ticket
    ,rango_frecuencia
    ,perfil_premia
    ,etiquetas_necesidad
    ,etiqueta_redencion
    ,flag_activo
    ,CASE 
        WHEN dias_sin_transaccion BETWEEN 2 AND 5 THEN '5 días'
        WHEN dias_sin_transaccion BETWEEN 6 AND 10 THEN '10 días'
        WHEN dias_sin_transaccion BETWEEN 11 AND 15 THEN '15 días'
        WHEN dias_sin_transaccion BETWEEN 16 AND 20 THEN '20 días'
        WHEN dias_sin_transaccion BETWEEN 21 AND 25 THEN '25 días'
        WHEN dias_sin_transaccion BETWEEN 26 AND 30 THEN '30 días'
        ELSE '+30 días' 
    END AS grupo_dias
    ,CASE 
            WHEN dias_sin_transaccion BETWEEN 2 AND 5 THEN 1 
            WHEN dias_sin_transaccion BETWEEN 6 AND 10 THEN 2 
            WHEN dias_sin_transaccion BETWEEN 11 AND 15 THEN 3 
            WHEN dias_sin_transaccion BETWEEN 16 AND 20 THEN 4 
            WHEN dias_sin_transaccion BETWEEN 21 AND 25 THEN 5 
            WHEN dias_sin_transaccion BETWEEN 26 AND 30 THEN 6 
            ELSE 7 
        END AS sort_row
    ,count(*) accounts
    ,sum(activo) activo
    ,sum(inactivo) inactivo
    ,sum(cliente_nuevo) cliente_nuevo
    ,sum(cliente_digital) cliente_digital
from   
    (
    select
        *
        ,case when perfil_abandono in ('','Nuevo_0','Nuevo_1','Nuevo_2','Nuevo_3') then 1 else 0 end activo
        ,case when perfil_abandono not in ('','Nuevo_0','Nuevo_1','Nuevo_2','Nuevo_3') then 1 else 0 end inactivo
        ,case when perfil_abandono in ('Nuevo_0','Nuevo_1','Nuevo_2','Nuevo_3') then 1 else 0 end cliente_nuevo
        ,case when flag_cliente_digital = true then 1 else 0 end cliente_digital
        ,case when perfil_abandono in ('','Nuevo_0','Nuevo_1','Nuevo_2','Nuevo_3') then 'Activo' else 'Inactivo' end flag_activo
    from
        awsdatacatalog.oxxo_loyalty_raw_database.mc_actual_csv
    ) accounts
group by
    rango_ticket
    ,rango_frecuencia
    ,perfil_premia
    ,etiquetas_necesidad
    ,etiqueta_redencion
    ,flag_activo
    ,CASE 
        WHEN dias_sin_transaccion BETWEEN 2 AND 5 THEN '5 días'
        WHEN dias_sin_transaccion BETWEEN 6 AND 10 THEN '10 días'
        WHEN dias_sin_transaccion BETWEEN 11 AND 15 THEN '15 días'
        WHEN dias_sin_transaccion BETWEEN 16 AND 20 THEN '20 días'
        WHEN dias_sin_transaccion BETWEEN 21 AND 25 THEN '25 días'
        WHEN dias_sin_transaccion BETWEEN 26 AND 30 THEN '30 días'
        ELSE '+30 días' END
    ,CASE 
            WHEN dias_sin_transaccion BETWEEN 2 AND 5 THEN 1 
            WHEN dias_sin_transaccion BETWEEN 6 AND 10 THEN 2 
            WHEN dias_sin_transaccion BETWEEN 11 AND 15 THEN 3 
            WHEN dias_sin_transaccion BETWEEN 16 AND 20 THEN 4 
            WHEN dias_sin_transaccion BETWEEN 21 AND 25 THEN 5 
            WHEN dias_sin_transaccion BETWEEN 26 AND 30 THEN 6 
            ELSE 7 END