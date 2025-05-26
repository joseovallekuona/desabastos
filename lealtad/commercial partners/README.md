
Descripción general:

Este módulo contiene información relevante de socios comerciales (marca_proveedor) 
en la cual se analiza venta lealtad y venta no lealtad y con información de perfiles de cliente: perfil_premia, etiqueta_segment, etc.

Los indicadores princiapales son: ventas, tickets, ticket promedio


Notas

El móduo de Commercial partners sólo utiliza un dataset, pero por el volumen de datos
no se puede procesar desde athena, por lo que se ejecuta en ventanas de tiempo y en diferentes archivos
para después integrarse.

El proceso es el siguiente:
    1. Se ejecuta el archivo sh run_script_loop_socios comerciales para venta tipo lealtad
    2. El sh file ejecuta el proceso analysis_per_proveedor_promolealtad.py que genera los archivos .csv con cortes de fechas
    3. Las diferentes fechas se integran utilizando el archivo socios_comerciales_analysis.ipynb
    4. Una vez procesada esta información se repite el proceso utilizando el archivo analysis_per_proveedr_not_promolealtad.py
    5. Finalmente el proceso exporta un archivo csv: merged_vendor_analysis_lealtad_vs_no_lealtad

* El sh file se modifica cambiando el archivo analysis_per_proveedor_promolealtad.py por el archivo analysis_per_proveedr_not_promolealtad.py

* Una muestra del data está disponible en esta carpeta: merged_vendor_analysis_lealtad_vs_no_lealtad.csv
