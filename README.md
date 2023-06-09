
# Arquitectura de AWS para procesamiento de datos del RNDC

## Objetivo: 

Diseñar e implementar una arquitectura en AWS que permita la carga, limpieza y disponibilidad de los datos públicos encontrados en la página del RNDC (Registro Nacional Despacho de Carga) del Ministerio de Transporte de Colombia.

## Alcance:

El proyecto planteado cubre desde la extracción de las diferentes fuentes de datos en local hasta su cargue, transformación y disponiblización en los servicios de AWS (S3, Lambda, Glue y Redshift).

## Etapas:

* Extracción de datos: crear un mecanismo para obtener diferentes fuentes de datos de manera automática.

* Almacenamiento y carga: diseñar e implementar una infraestructura en AWS que permita cargary almacenar los datos de manera automática luego de su descarga.

* Limpieza y transformación: implementar un proceso de limpieza y transformación de datos para asegurar la calidad y coherencia de los mismos.

* Disponibilidad y acceso: Establecer mecanismos para que los datos estén disponibles para su consulta.

## Casos de uso:

 Una vez completado el proyecto, la arquitectura implementada permitirá a diferentes procesos y equipos acceder y utilizar los datos del RNDC para visualización, análisis y modelado. Estos datos podrán ser utilizados para generar información valiosa, informes, visualizaciones o incluso alimentar modelos de aprendizaje automático y toma de decisiones estratégicas en el ámbito del transporte en Colombia. Los análisis y/o modelos derivados del uso de estos datos puede tener como foco temas como los siguientes:

* Valores pagados.
* Tiempos logísticos promedio.
* Orígenes y destinos con mayor participación en movimiento de carga.
* Tipos de flota.
* Corredores desbalanceados (con diferencias sustanciales entre la carga de entrada y la carga de salida).

## Exploración de archivos:

### Contexto de los archivos:

Los archivos públicos que se utilizan en el proyecto son descargados a través de:

* Generar Archivo de estadísticas de las rutas y mercancías transportadas - opción mensual - xlsx.
* Generar Archivo con los Tiempos Logisticos de cada viaje - opción mensual - txt.
* Generar Archivo por Antigüedad de Vehículos y Combustible - opción anual - xlsx.

Desde este punto en adelante se hará referencia a estos archivos de manera colectiva como "fuentes de datos" y de manera individual de la siguiente manera:

* Estadísticas: estadisticas 
* Tiempos logísticos: tiempos_logisticos
* Vehículos y combustible: vehiculos_y_combustible

En la siguiente imagen se puede ver la interfaz de usuario para la descarga de los archivos.

![Reporte públicos RNDC](images/reportes_rndc.jpeg)


### Obtención de los archivos:

Los archivos se hacen disponibles una vez al mes (excepto el de vehiculos_y_combustible), sin una fecha específica de disponibilidad (usualmente en las primeras dos semanas del mes). Para obtener una cantidad de archivos suficientes con la cual cumplir la restricción de un dataset con más de un millón de registros se opta por descargar 10 periodos de tiempo desde el mes inmediatamente anterior.

Para hacer la descarga de los archivos se crea un robot en Python usando la librería Selenium que tiene como parámetro la cantidad de peridos solicitados. El script que realiza esta acción se encuentra en la ruta src/data/download_rndc_files.py.

### Exploración de los archivos:

El proceso de exploración de datos se dividió en tres etapas: conocer la estructura de los archivos, identificar posibles validaciones de datos, aplicar transformación de datos. Todo lo anterior a nivel local, en notebooks, sin modificar los archivos descargados y para cada fuente de datos.

Los notebooks con el detalle de las etapas y los resultados se encuentran en la ruta notebooks/exploration_*.ipynb. De manera general se realizan los siguientes procesos para cada fuente de datos:

* Validar que todos los archivos mantienen el mismo esquema.
* Definir campos redundantes. Por ejemplo: existe el campo "COD_CONFIG_VEHICULO" (código tipo str) y "CONFIG_VEHICULO" (descripción del código) ambos haciendo referencia al mismo atributo (configuración del vehpiculo), en este y los demás casos se opta por no tener en cuenta la descripción para temas de transformación. Nota: Como se verá más adelante estos campos pasan a ser parte de las tablas de dimensiones del modelo entidad-relación.
* Identificar tipos de datos para cada columna de los archivos.
* Validar que todos los archivos son del mismo tipo.
* Validar coherencia de los datos de los archivos. En este caso se valida si los valores contenidos en el campo de periodo corresponden al periodo incluido en el nombre del archivo.
* Transformar los datasets para:
    * Eliminar duplicados (en este caso los registro duplicados son un error debido a que los datos ya se encuentran agregados).
    * Aplicar el tipo de dato a cada columna.
    * Convertir en mayúsculas todo campos tipo str.
    * Reemplazar caracteres con tilde.
    * Cambiar nombre de columnas.
    * Agrupar datos, específicamente en el caso de tiempo logísticos donde el objetivo será tener una tabla con los tiempos promedio.

Tener en cuenta que en el caso de las transformaciones, estas no son todas las que se aplican en el proceso final.

## Modelo de datos:

### Modelo entidad-relación:

Para comprender y visualizar la estructura de los datos se crea el siguiente modelo entidad-relación con la ayuda de MySQL Workbench:

![Modelo entidad-relación](images/er_model.png)

Como se indicó anteriormente todos los campos definidos como redundates (la lista está en los notebooks) pasan a ser parte de las tablas de dimensiones del modelo. Adicionalmente se crean identificadores únicos y automincrementales para cada valor. La distribución de tablas es la siguiente:

* Tablas de dimensiones:
    * dim_municipios
    * dim_tipos_contenedor
    * dim_operaciones_transporte
    * dim_mercancias
    * dim_naturaleza_carga
    * dim_configuraciones_vehiculo
    
* Tablas de hechos:
    * tiempos_logisticos
    * estadisticas
    * vehiculos

El script de creación de este modelo se encuentra en la ruta scr/sql/er_model.txt. Es el que se usa también para la posterior creación del modelo en Redshift.

### Arquitectura

A continuación se ilustra el esquema planteado:

![Diagrama arquitectura](images/diagrama_arquitectura.png)


La arquitectura consta de varios componentes que trabajan juntos para gestionar el flujo de archivos y el procesamiento de datos:

* En primer lugar, se utiliza un script de Python para realizar la descarga de archivos desde la página del RNDC (https://rndc.mintransporte.gov.co/MenuPrincipal/tabid/204/language/es-MX/Default.aspx?returnurl=%2FDefault.aspx) a la máquina local.

* Después de ser descargados, los archivos se suben a Amazon S3, un servicio de almacenamiento escalable y seguro proporcionado por Amazon Web Services (AWS). Se utiliza otro script de Python para llevar a cabo esta tarea.

* Cada vez que se almacena un archivo en S3, se dispara un evento que activa una función de Lambda. Lambda es un servicio de computación sin servidor en AWS que permite ejecutar código en respuesta a eventos específicos. En este caso, Lambda se encarga de manejar el evento de almacenamiento del archivo en S3.

* La función Lambda validará el archivo y su extensión para asegurarse de que cumple con los requisitos esperados. Una vez validado, la función Lambda dirigirá el flujo de procesamiento al job de Glue adecuado. AWS Glue es un servicio de extracción, transformación y carga (ETL) que simplifica y automatiza el procesamiento de datos.

* El job de Glue se encarga de procesar el archivo utilizando su lógica específica. Puede realizar transformaciones, limpieza de datos y cualquier otra tarea necesaria para preparar los datos para su posterior análisis. Una vez procesados, los datos se guardarán en un modelo entidad-relación en Amazon Redshift. Redshift es un servicio de almacenamiento y análisis de datos en la nube, diseñado especialmente para cargas de trabajo de análisis de datos a gran escala.

## Estructura del repositorio:

![repositorio](images/repositorio.png)

* drivers: aquí se encuentra el archivo "chromedriver.exe" con el driver de chrome necesario para la ejecución del bot de Python. Cabe aclarar que su versión depende de la versión de Google Chrome usada en la máquina donde se vaya a ejecutar el script.
* images: aquí se guardan las imágenes que se ven en el README.md.
* notebooks: aquí se encuentran los notebooks de exploración y los jobs de Glue (su nombre comienza por "rndc-etl"). Estos últimos se encargan de hacer la lectura desde s3 del archivo que dispara el evento, lo limpia y transforma, retirando los campos referenetes al código y su descripción para cambiarlos por su respectivo id en las tablas de dimensiones. Los pasos son detallados en el respectivo notebook.
* src: aquí están las siguientes subcarpetas:
    * aws: contiene el script "s3.py" con la función para leer el nombre de los archivos subidos a S3 y la función para cargar archivos.
    * data: contiene el script "download_rndc_files.py" con las funciones para el funcionamiento del robot.
    * lambda: contiene el script "lambda_function_validation.py" que es el usado en el servicio de Lambda para validar el archivo y definir el job de Glue a ejecutar.
    * sql: contiene el archivo "er_model.txt" con el query para la creación del modelo entidad-relación en Redshift.
    Adicionalmente contiene el archivo main.py que se encarga de orquestar la descarga de los archivos y su almacenamiento en s3.
* requirements: contiene las librerías utilizadas.


## Ejecución del flujo:

Para el ejemplo se muestran las capturas de pantalla de un flujo completo para un archivo correspondiente a la fuente de datos de estadísticas.

* Ejecución del script "download_rndc_files.py" para descarga archivo donde se ve el periodo a consultar y la fuente de datos relacionada al archivo.

![robot](images/1_print_python_consulta.png)

* Descarga del archivo "EstadisticasRNDC202206.xlsx" desde la página del RNDC usando selenium y almacenamiento en ruta local.

![descarga1](images/2_descarga_archivo.png)

![descarga2](images/3_archivo_folder_local.png)

* Ejecución del script "s3.py" para subir archivo a S3 donde se ve la fuente de datos y el archivo subido.

![s3subida](images/4_print_python_subida_s3.png)

* Bucket de S3 con el archivo.

![s3bucket](images/5_bucket_s3.png)

* Activación de la función Lambda con sus respectivos logs.

![lambda1](images/6_activacion_lamba.png)

![lambda2](images/7_eventos_lambda.png)

* Monitor de Glue, con sus respectivo argumentos de entrada y logs.

![glue1](images/8_monitor_glue_job.png)

![glue2](images/9_input_glue_job.png)

![glue3](images/10_glue_job_logs.png)

* Queries ejecutados en Redshift.

![redshit](images/11_ejecucion_queries_redshift.png)

Nota: la prueba se hizo sin la carga de datos en las tablas de hechos.

# Pendientes:
* Cargar datos completos en las tablas de hechos.

# Fuentes:
* Documentación Boto3: https://boto3.amazonaws.com/v1/documentation/api/latest/index.html
* Documentación Redshift: https://docs.aws.amazon.com/redshift/latest/gsg/new-user-serverless.html
* Documentación Glue: https://docs.aws.amazon.com/glue/latest/dg/aws-glue-api-crawler-pyspark-extensions-get-resolved-options.html