
Las visualizaciones del módulo de engagement utilizan 4 datasets:

    * Loyalty_accounts_main - ejecuta desde un query - almacena indicadores generales: usuarios activos, usuarios perdidos, usuarios_nuevo y usuarios digitales
    * Lealtad_engagement_sales - ejecuta desde un query - obtiene el ticket promedio en promociones
    * Lealtad_new_customers_from_promotions - ejecuta desde un query - cuantifica el número de usuarios nuevos obtenidos por promociones
    * Lealtad_users_uing_promotions - se carga desde un csv que se genera con .py y sh debido al volumen de datos - calcula el núnero de usuarios utilizando promociones como % de los usuarios totales de catálogo 
    * Lealtad_main_promotions_engagement - ejecuta desde un query - identifica las promociones principales

