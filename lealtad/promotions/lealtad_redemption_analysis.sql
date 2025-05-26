/*logica nueva redencion*/

select
     promotionid
    ,promotionname
    ,perfil_premia
    ,rango_frecuencia
    ,redencion_tipo
    ,interacciones_tier
    ,interacciones_completas_tier
    ,progress_sello
    ,progress_sello_sort
    ,sum(sellos_no_canjeados) sellos_no_canjeados
    ,sum(interacciones_decimal) interacciones_decimal
    ,sum(interacciones_no_canjeadas_decimal) interacciones_no_canjeadas_decimal
    ,sum(interacciones) interacciones
    ,sum(interacciones_no_canjeadas) interacciones_no_canjeadas
    ,sum(interacciones_completas) interacciones_completas
    ,sum(interacciones_completas) / sum(interacciones) redencion
    ,count(distinct accountid) accounts
    ,avg(regla_sello) regla_sello
from 
(
select
    sellos.promotionid
    ,'' promotionname
    ,collect + punchcard_redemption sellos_no_canjeados
    ,collect / regla_sello interacciones_decimal
    ,((collect + punchcard_redemption) / regla_sello) interacciones_no_canjeadas_decimal
    ,ceil(collect / regla_sello) interacciones
    ,floor((collect + punchcard_redemption) / regla_sello) interacciones_no_canjeadas
    ,((collect / regla_sello) - (collect + punchcard_redemption) / regla_sello) interacciones_completas
    ,((collect / regla_sello) - (collect + punchcard_redemption) / regla_sello)  / ceil(collect / regla_sello) redencion
    ,accounts.*
    ,sellos.REGLA_SELLO
    ,case 
        when ((collect / regla_sello) - (collect + punchcard_redemption) / regla_sello)  / ceil(collect / regla_sello)  <= 0 then 'Sin redención' 
        when ((collect / regla_sello) - (collect + punchcard_redemption) / regla_sello)  / ceil(collect / regla_sello)  IS NULL then 'Sin redención'
        else 'Al menos 1 redención' end redencion_tipo
    ,case
        when ceil(collect / regla_sello) = 0 then '0'
        when ceil(collect / regla_sello) = 1 then '1'
        when ceil(collect / regla_sello) = 2 then '2'
        when ceil(collect / regla_sello) = 3 then '3'
        when ceil(collect / regla_sello) = 4 then '4'
        when ceil(collect / regla_sello) >= 5 then '5 o más' end interacciones_tier
    ,case
        when ((collect / regla_sello) - (collect + punchcard_redemption) / regla_sello)= 0 then '0'
        when ((collect / regla_sello) - (collect + punchcard_redemption) / regla_sello) = 1 then '1'
        when ((collect / regla_sello) - (collect + punchcard_redemption) / regla_sello) = 2 then '2'
        when ((collect / regla_sello) - (collect + punchcard_redemption) / regla_sello)= 3 then '3'
        when ((collect / regla_sello) - (collect + punchcard_redemption) / regla_sello)= 4 then '4'
        when ((collect / regla_sello) - (collect + punchcard_redemption) / regla_sello) >= 5 then '5 o más' end interacciones_completas_tier
    ,case 
        when collect / regla_sello IS NULL then 1
        when collect / regla_sello <= .25 then 1
        when collect / regla_sello <= .50 then 2
        when collect / regla_sello <= .75 then 3
        when collect / regla_sello <= .1 then 4
        else 5 end
        progress_sello_sort
    ,case 
        when collect / regla_sello IS NULL then '25%'
        when collect / regla_sello <= .25 then '25%'
        when collect / regla_sello <= .50 then '50%'
        when collect / regla_sello <= .75 then '75%'
        when collect / regla_sello <= .1 then '100%'
        else '+100%' end
        progress_sello
from
    (
    select 
         sellos.promotionid
        ,sellos.accountid
        ,sum(case when sellos.transactiontype = 'COLLECT' then sellos.mutation else 0 end) COLLECT
        ,sum(case when sellos.transactiontype = 'PUNCHCARD_REDEMPTION' then sellos.mutation else 0 end) PUNCHCARD_REDEMPTION
        ,sum(case when sellos.transactiontype = 'PURCHASE' then sellos.mutation else 0 end) PURCHASE
        ,sum(case when sellos.transactiontype = 'REDEEM' then sellos. mutation else 0 end) REDEEM
        ,avg(sellos.requiredbalanceforreward) REGLA_SELLO
    from 
        awsdatacatalog.oxxo_loyalty_raw_database.sellos_csv_gz sellos
    inner join
        (
            select
                 promotionid
                ,transactionid 
            from
            (
                select
                     promotionid
                    ,transactionid
                    ,sum(case when transactiontype = 'COLLECT' then mutation else 0 end)  collect
                    ,avg(requiredbalanceforreward) regla_sello
                from 
                    awsdatacatalog.oxxo_loyalty_raw_database.sellos_csv_gz
                where
                    try_cast(promotionid AS integer) IS NOT NULL
                    --and promotionid = '19316660'
                group by
                    promotionid
                    ,transactionid
            )
            where
                collect != regla_sello
        ) valid_sellos
        on
          sellos.transactionid =valid_sellos.transactionid
          and sellos.promotionid = valid_sellos.promotionid
    where
         try_cast(sellos.promotionid AS integer) IS NOT NULL
         --and accountid = '95305413-1638-4546-9db1-7994f8431c1e'
    group by
         sellos.promotionid
        ,sellos.accountid
    having
    --collect + PUNCHCARD_REDEMPTION must be >= 0 otherwise there were more redeemed punchards than collects which is not feasible
       (sum(case when sellos.transactiontype = 'COLLECT' then sellos.mutation else 0 end) + sum(case when sellos.transactiontype = 'PUNCHCARD_REDEMPTION' then sellos.mutation else 0 end)) >= 0  
    )
    sellos
    left join
    awsdatacatalog.oxxo_loyalty_raw_database.mc_actual_csv accounts
    on sellos.accountid = accounts.accountid

    --and punchcard_redemption <= collect 
)
group by
     promotionid
    ,promotionname
    ,perfil_premia
    ,rango_frecuencia
    ,redencion_tipo
    ,interacciones_tier
    ,interacciones_completas_tier
    ,progress_sello
    ,progress_sello_sort