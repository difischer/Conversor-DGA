# Conversor-DGA
Este programa convierte el formato de tablas DGA a un formato de un dato por fila.
El script recibe, dentro de la carpeta `datos`, carpetas con los datos de estaciones en formato DGA

El script realiza las siguientes operaciones:

- Carga de datos: Utiliza la biblioteca pandas para cargar archivos de Excel. El motor de lectura se selecciona en función de la extensión del archivo.

- Limpieza de datos: Limpia los valores de mes y año, eliminando el texto 'MES:' y los espacios en blanco.

- Transformación de datos: Reestructura los datos en un nuevo formato. Los datos se dividen en columnas específicas para el día, la hora, la altura y el caudal. Cada una de estas categorías tiene tres columnas correspondientes en el DataFrame.

Por favor, asegúrese de instalar las dependencias necesarias para ejecutar este script, que incluyen `pandas` y `xldr`

    pip install pandas
    pip install xldr

Uso
Para utilizar este script, ponga dentro de la carpeta `datos` las carpetas con los datos de la estación o estaciones a convertir, luego ejecute el archivo `convertir_formato_DGA.py`

Este script ha sido diseñado para ser fácil de usar y eficiente, utilizando la biblioteca `concurrent.futures` para un procesamiento más rápido.
