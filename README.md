# Análisis de Publicaciones en Bluesky y su Correlación con Cotizaciones Bursátiles

Este proyecto, desarrollado en el marco de la materia **Web Mining** de la Maestría en Ciencia de Datos, tiene como objetivo analizar las publicaciones en la red social [Bluesky](https://bsky.social/) y estudiar su posible correlación con las cotizaciones de empresas en la bolsa de valores.

## Descripción del Proyecto

El proyecto se centra en la extracción y análisis de datos provenientes de Bluesky, una plataforma de redes sociales descentralizada basada en el [Protocolo AT](https://en.wikipedia.org/wiki/AT_Protocol). Utilizando técnicas de web scraping y procesamiento de datos financieros, se buscará identificar patrones y relaciones entre las publicaciones de los usuarios y las variaciones en las cotizaciones bursátiles de empresas seleccionadas.

## Objetivos

- Extraer publicaciones relevantes de Bluesky utilizando su API pública.
- Obtener datos históricos de cotizaciones bursátiles de empresas específicas.
- Analizar la correlación entre la actividad en Bluesky y las variaciones en las cotizaciones.
- Visualizar los resultados obtenidos para identificar posibles tendencias o patrones.

## Estructura del Proyecto

El proyecto se organiza en los siguientes módulos:

1. **Extracción de Datos de Bluesky**: Utiliza la API de Bluesky para recopilar publicaciones públicas relacionadas con las empresas de interés.
2. **Obtención de Datos Financieros**: Recopila datos históricos de cotizaciones bursátiles de las empresas seleccionadas.
3. **Análisis de Correlación**: Procesa y analiza los datos para identificar posibles correlaciones entre las publicaciones en Bluesky y las cotizaciones bursátiles.
4. **Visualización de Resultados**: Genera gráficos y reportes que ilustran los hallazgos del análisis.

## Requisitos

- Python 3.x
- Bibliotecas: `requests`, `pandas`, `numpy`, `matplotlib`, `seaborn`
- Acceso a la API de Bluesky
- Acceso a una fuente de datos financieros confiable

## Uso

1. **Configuración**: Clonar este repositorio y configurar las claves de acceso necesarias para las APIs de Bluesky y de datos financieros.
2. **Ejecución**: Ejecutar los scripts en el orden establecido en la estructura del proyecto.
3. **Análisis**: Revisar los resultados y visualizaciones generados para interpretar las correlaciones identificadas.

## Consideraciones Éticas y Legales

Es fundamental asegurar que la recopilación y el uso de datos cumplan con las políticas de privacidad y términos de servicio de Bluesky y de las fuentes de datos financieros utilizadas. Además, se debe garantizar la confidencialidad y el anonimato de la información de los usuarios en todo momento.


