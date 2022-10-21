# Proyecto Final
  # Grupo 17

![HenryLogo](https://d31uz8lwfmyn8g.cloudfront.net/Assets/logo-henry-white-lg.png)

# Vuelos Comerciales

(https://images-wixmp-ed30a86b8c4ca887773594c2.wixmp.com/f/bc0a4715-c860-464f-88e4-3045f9106b4c/d8kgg6n-59fc17d1-aad2-47f0-b036-2a7c522dd403.png?token=eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJ1cm46YXBwOjdlMGQxODg5ODIyNjQzNzNhNWYwZDQxNWVhMGQyNmUwIiwiaXNzIjoidXJuOmFwcDo3ZTBkMTg4OTgyMjY0MzczYTVmMGQ0MTVlYTBkMjZlMCIsIm9iaiI6W1t7InBhdGgiOiJcL2ZcL2JjMGE0NzE1LWM4NjAtNDY0Zi04OGU0LTMwNDVmOTEwNmI0Y1wvZDhrZ2c2bi01OWZjMTdkMS1hYWQyLTQ3ZjAtYjAzNi0yYTdjNTIyZGQ0MDMucG5nIn1dXSwiYXVkIjpbInVybjpzZXJ2aWNlOmZpbGUuZG93bmxvYWQiXX0.G6xjMhhhbjE4SigACvtsuhQDCWDfAMHqSFvVTdQl8mk)


# <h1 align="center">**`¡Bienvenidos a bordo!`**</h1>

# Contenido:

1. Descripción general del proyecto
2. Equipo de trabajo
3. Metodología de trabajo
4. Cronograma a la fecha
5. Objetivo General
6. Solución: Data Pipeline
7. Diccionario de los datos


# Descripción general

El Departamento de Transporte de Estados Unidos (U.S. DOT) contrató nuestros servicios de Ingenieria, Analisis e Inteligencia de negocios interesados en conocer información relacionada al tráfico aéreo a nivel global, con el fin de poder monitorear y definir proyectos acordes a la situación actual, además de poder complementarlo con una visión completa de lo que ha pasado históricamente. Dentro de la información mínima que necesita saber el Departamento, está la cancelación de vuelos y los atrasos de éstos.

# Equipo de Trabajo

* [Alejandro Aguilera](https://www.linkedin.com/in/alejandroaguilerawilches/) - Data Analyst 
* [Alan Sánchez](https://github.com/MRXWallace) - Data Analyst
* [Christian Fajardo](https://www.linkedin.com/in/christian-fajardo-338929241/) - Data Engineer
* [Eduardo Carhuaricra](https://www.linkedin.com/in/carlos-eduardo-carhuaricra-jaimes-9b1422197/) - Data Analyst 


# Metodología de trabajo
/// imagen ///

# Cronograma

/// imagen ///

# Objetivo General
° Describir la variabilidad en la cancelación de vuelos y factores que intervienen en la misma.
° Describir a través de mapas la realidad, la concentración de vuelos, principales destinos y las rutas mas frecuentes teniendo como punto de origen EE.UU

# Solución : Data Architecture && Pipelines

**Diagrama flujo del dato**
//Imagen//
Estructura de Datos End-to-End 

# 1. Extract 

Los datasets entregados por el Product Owner se descargaron y fueron almacenados de manera local y temporal para posteriormente subirlos a la nube.

Creamos un Bucket en Google Cloud Storage(Data Lake) donde se almacenarán los datasets raw en la nube. Para esto, fue necesario crear una cuenta de servicio en el portal de Google Cloud Platform y crear un script donde automatice este paso y trabar en función con los demas.

# 2. Transform
Unión de varios datasets
Separación y eliminación de columnas
Cambio de nombres de columnas
Cambio de esquemas

# 3. Carga 
Conexión y Carga de datos en la base de datos de Google BigQuery (Data Warehouse) con sus respectivos esquemas y transformaciones. 
Para todo este proceso se creo una infraestructura de conexiones y herramientas de Google Cloud Platform asi como crear una instancia de Airflow con Google Composer para la orquesta y automatización de flujo de datos.

# Diccionario de los Datos







