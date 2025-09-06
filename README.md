# Proyecto Integrador IV
> Pipeline de Datos de E-Commerce

En este proyecto se espera que completes el código por tu cuenta, pero siempre contarás con el docente para responder cualquier inquietud. Por favor, lee todo el notebook antes de empezar, esto te dará una mejor idea de lo que necesitas lograr.

## El Problema de Negocio

Estás trabajando para uno de los sitios de comercio electrónico más grandes de Latinoamérica, y el equipo de Ciencia de Datos ha recibido la solicitud de analizar datos de la compañía para comprender mejor su desempeño en ciertas métricas durante los años 2016-2018.

Hay dos áreas principales que desean explorar: **Ingresos** y **Entregas**.

Básicamente, quieren entender cuánto ingresaron por año, cuáles fueron las categorías de productos más y menos populares, y los ingresos por estado. Por otro lado, también es importante conocer qué tan bien está entregando la compañía los productos vendidos en tiempo y forma a sus usuarios. Por ejemplo, ver cuánto tiempo toma entregar un paquete dependiendo del mes, y la diferencia entre la fecha estimada de entrega y la fecha real.

## Sobre los Datos

Vas a usar datos de dos fuentes:

1. La primera es un conjunto de datos público de pedidos de comercio electrónico en Brasil, provenientes de la tienda Olist, proporcionado en archivos CSV. Estos son datos comerciales reales, que han sido anonimizados. El conjunto de datos contiene información sobre 100,000 pedidos realizados entre 2016 y 2018 en varios marketplaces en Brasil. Sus características permiten ver los pedidos desde múltiples dimensiones: desde el estado del pedido, precio, pago y desempeño logístico, hasta la ubicación del cliente, atributos del producto y finalmente las reseñas escritas por los clientes. Encontrarás una imagen con el esquema de la base de datos en `images/data_schema.png`.  
   Para obtener el conjunto de datos, descárgalo desde este [enlace](https://drive.google.com/file/d/1HIy4LNNQESuXUj-u_mNJTCGCRrCeSbo-/view?usp=share_link), extrae la carpeta `dataset` del archivo `.zip` y colócala en la carpeta raíz del proyecto. Consulta el archivo `ASSIGNMENT.md`, sección **Estructura del Proyecto**, para validar que has ubicado el conjunto de datos correctamente.

2. La segunda fuente es una API pública: https://date.nager.at. La usarás para obtener información sobre los días festivos en Brasil y correlacionarlos con ciertas métricas sobre la entrega de productos.

## Aspectos Técnicos

Debido a que el equipo sabe que los datos vendrán de diferentes fuentes y formatos, y probablemente se tendrán que generar este tipo de informes de forma mensual o anual, se decidió construir un pipeline de datos (ELT) que se pueda ejecutar periódicamente para producir los resultados.

Las tecnologías involucradas son:
- Python como lenguaje de programación principal.
- Pandas para consumir datos desde archivos CSV.
- Requests para hacer consultas a la API de días festivos.
- SQLite como motor de base de datos.
- SQL como lenguaje principal para almacenar, manipular y recuperar datos en el Data Warehouse.
- Tableau, Power BI o looker Studio para las visualizaciones.
- Jupyter Notebooks para presentar el informe de manera interactiva.

## Instalación

Se proporciona un archivo `requirements.txt` con todas las bibliotecas de Python necesarias para ejecutar este proyecto. Para instalar las dependencias, ejecuta:

```console
$ pip install -r requirements.txt
```

*Nota:* Se recomienda instalar las dependencias dentro de un entorno virtual.

## Estilo de Código

Seguir una guía de estilo mantiene el código limpio y mejora la legibilidad, facilitando las contribuciones y las revisiones de código. Los formateadores automáticos de código en Python aseguran que tu código mantenga un estilo consistente sin necesidad de trabajo manual. Si te importa seguir un estilo específico, usar una herramienta automatizada es la mejor opción. Esto evita discusiones innecesarias sobre detalles menores en las revisiones de código y te ahorra mucho tiempo.

Usamos [Black](https://black.readthedocs.io/) como formateador automático de código en este proyecto. Puedes ejecutarlo con:

```console
$ black --line-length=88 .
```

¿Quieres leer más sobre el estilo de código en Python y buenas prácticas? Consulta:
- [The Hitchhiker’s Guide to Python: Code Style](https://docs.python-guide.org/writing/style/)
- [Google Python Style Guide](https://google.github.io/styleguide/pyguide.html)

## Pruebas

El proyecto incluye pruebas unitarias que puedes ejecutar para verificar que el código cumple con los requisitos mínimos de corrección. Para ejecutar las pruebas, corre el siguiente comando:

```console
$ pytest tests/
```

Si deseas aprender más sobre cómo probar código en Python, revisa:
- [Effective Python Testing With Pytest](https://realpython.com/pytest-python-testing/)
- [The Hitchhiker’s Guide to Python: Testing Your Code](https://docs.python-guide.org/writing/tests/)

