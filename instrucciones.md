# Proyecto: Pipeline de Datos de E-Commerce

Para resolver este proyecto y crear el pipeline ELT, deberás completar los archivos de Python (.py) y SQL (.sql) que están distribuidos en diferentes carpetas. A continuación, te proporcionamos las instrucciones sobre cómo proceder y el orden de ejecución:

## 1. Extracción

En la fase de extracción de datos del pipeline, tendrás que completar todas las funciones que tengan la marca **TODO** dentro del módulo `src/extract.py`.

Si deseas verificar que tu código cumple con los requisitos, puedes probar ese módulo en particular ejecutando el siguiente comando:

```console
$ pytest tests/test_extract.py
```

## 2. Carga

Ahora que tienes todos los datos de diferentes fuentes, es momento de almacenarlos en un Data Warehouse. Usaremos SQLite como nuestro motor de base de datos para mantener la simplicidad, aunque en empresas más grandes, Snowflake es una de las opciones más populares para Data Warehouses.

Por favor, completa todas las funciones con la marca **TODO** dentro del módulo `src/load.py`.

## 3. Transformación

Teniendo los datos almacenados en el Data Warehouse, puedes empezar a hacer consultas y transformaciones.

Para esta tarea, ya te proporcionamos el código necesario dentro del módulo `src/transform.py`, pero tendrás que escribir las consultas SQL 😬.

Por favor, completa todos los scripts `.sql` con la marca **TODO** dentro de la carpeta `queries/`.

Puedes usar herramientas como DBeaver para escribir y probar las consultas de forma interactiva. Finalmente, puedes verificar que tus consultas cumplen con los requisitos ejecutando las pruebas proporcionadas con el siguiente comando:

```console
$ pytest tests/test_transform.py
```

Además, puedes validar cómo debe verse el resultado de la consulta revisando el archivo `.json` en `tests/query_results` que tiene el mismo nombre que el archivo `.sql` correspondiente.

## 4. Visualiza tus resultados

Finalmente, una vez que tengas todos los resultados de tus consultas, es hora de crear algunas visualizaciones para la presentación.

Para esto, crea un dashboard que de respuesta al problema de negocio:
*Estás trabajando para uno de los sitios de comercio electrónico más grandes de Latinoamérica, y el equipo de Ciencia de Datos ha recibido la solicitud de analizar datos de la compañía para comprender mejor su desempeño en ciertas métricas durante los años 2016-2018.*
*Hay dos áreas principales que desean explorar: Ingresos y Entregas.*
*Básicamente, quieren entender cuánto ingresaron por año, cuáles fueron las categorías de productos más y menos populares, y los ingresos por estado. Por otro lado, también es importante conocer qué tan bien está entregando la compañía los productos vendidos en tiempo y forma a sus usuarios. Por ejemplo, ver cuánto tiempo toma entregar un paquete dependiendo del mes, y la diferencia entre la fecha estimada de entrega y la fecha real.*

Piensa también en preguntas adicionales que puedas resolver con los datos y que den valor al problema de negocio. 

Recuerda que tu dashboard debe tener máximo 5 gráficos.

## 5. Orquestación de Datos con Apache Airflow

En esta tarea, te pedimos que reutilices el código del pipeline ELT actual para crear un DAG de Airflow que automatice todo el proceso.

Por favor, no modifiques ni cambies la estructura del proyecto actual ni el código que pueda romper las pruebas unitarias proporcionadas. En su lugar, te sugerimos que trabajes en una nueva carpeta dentro del proyecto y coloques allí el código de los DAGs. Esta tarea es abierta, por lo que puede tener más de una respuesta, solución o resultado correcto, y puede completarse de varias maneras. Si tienes tiempo y quieres desafiarte, ¡adelante!

## Estructura del Proyecto

Antes de empezar a trabajar, revisemos la estructura general del proyecto y cada uno de sus módulos:

```console
├── dataset
│   ├── olist_customers_dataset.csv
│   ├── olist_geolocation_dataset.csv
│   ├── olist_order_items_dataset.csv
│   ├── olist_order_payments_dataset.csv
│   ├── olist_order_reviews_dataset.csv
│   ├── olist_orders_dataset.csv
│   ├── olist_products_dataset.csv
│   ├── olist_sellers_dataset.csv
│   └── product_category_name_translation.csv
├── images
├── queries
├── src
├── tests
├── instrucciones.md
├── Project.ipynb
├── README.md
└── requirements.txt
```

### dataset
Contiene todos los archivos .csv con la información que usarás en el proyecto.

### queries
Contiene todas las consultas SQL que debes completar para luego generar tablas y gráficos.

### src
Contiene diferentes archivos fuente necesarios para que todo el proyecto funcione.

### tests
Carpeta con los archivos necesarios para probar el proyecto.

### Otros
- `instrucciones.md`: Información clave para entender el proyecto.
- `Project.ipynb`: Notebook que unifica las partes del proyecto y te indica los pasos a seguir.
- `README.md`: Archivo con la descripción del proyecto.
- `requirements.txt`: Lista de librerías necesarias.

Con esto, ya puedes empezar. ¡Buena suerte!

