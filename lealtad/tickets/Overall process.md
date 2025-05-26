* Overall process

1. Se ejecuta el archivo sh
2. El sh ejecuta el archivo oxxo_sellout_preaggregates.py con diferentes fechas
3. Utilizando el notebook oxxo_dashboard_v2.ipynb se genera el archivo csv merged_tickets_csv
4. Este archivo se carga a quicksight como un dataset


* Notas
  - La información de oxxo no se ha actualizado porque lo no se ctualizaría hasta que sellout_promotions se actualice
  - El proceso se ejecutó inicialmente de esta forma por el constraint para generar los cubos directamente desde athena
  - Se pueden fitlrar combinaciones de filtros: categoría, supercategoría, marca
  - La desventaja principal de este proceso es el impacto en el filtrado de las columnas ya que no se pueden seleccionar "multiples valores" en el filtro, sólo 1 valor por filtro
  - Los calculos count_distinct() para evluar identificar ticket unicos son dificiles de escalar para diferentes granularidad, un query directo solucionaría esta problema, pero el tiemout de quicksight es de 2 minutos para el tiempo de conexión

