Overall process

* Este codigo une las promociones de sellos con los precios promedio de los artículos participantes en los sellos.
De esta logica se obtienen indicadores como bonificación, redención, etc.

* El modelo de datos sólo es un join entre las promociones y los precios promedio de los sellos

* Los queries se almacen en spice, y para actualizar sólo es nesesario hacer refresh al dataset

* El query lealtad_planilla_transaccion tiene información de cuantas planillase se llenan por usuario, ya que el usuario puede redimir más de 1 planilla

