Overall process:

1. Se ejectua el sh run_script_loop_users_using_promo.sh que corre el archivo users_using_promo_analysis.py con diferentes fechas
2. Los csv's procesados se concactenan después utilizando el archivo users_using_promo_analysis.ipynb que integra los csv's en un sólo archivo
3. El archivo de salida users_using_promo.csv se carga a quicksight para alimentar la gráfica correspondiente


NOTAS:
* El catálogo se modifica con el tiempo y el número de usuarios usando promociones % se calcula con el catálogo de clientes que se modifica a través del tiempo