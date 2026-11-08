# Programación MapReduce con Apache Spark (RDDs)
Introducción a Apache Spark con PySpark usando la API de **RDDs** y el modelo de programación **MapReduce**.

## Contenido

| Fichero | Descripción |
|---|---|
| `Practica 1.2 - SparkRDDs.ipynb` | Ejercicios MapReduce resueltos y ejecutados (ver tabla inferior). |
| `test_helper.py` | Clase `Test` usada por el notebook para validar los resultados. |
| `datos/` | Datos de entrada (ver sección *Datos*). |

La Práctica 1.1 era una guía de la API de RDDs (transformaciones, acciones, operaciones clave-valor, caché, broadcast, acumuladores, `mapPartitions`...) y no se incluye en el repositorio.

## Ejercicios de la Práctica 1.2

| Nº | Función | Qué hace | Operaciones Spark principales | Resultado |
|---|---|---|---|---|
| 1 | `cuentaPalabras` | Top 10 de palabras más repetidas en *El Quijote* (texto original y normalizado sin signos de puntuación) | `flatMap`, `filter`, `map`, `reduceByKey`, `takeOrdered` | ✅ |
| 2 | `histogramaRepeticiones` | Cuántas palabras aparecen X veces | doble `reduceByKey`, `sortByKey` | ✅ + gráfico |
| 3 | `histogramaLongitud` | Número de palabras por longitud (≤ 16) | `map`, `filter`, `reduceByKey`, `sortByKey` | ✅ + gráfico |
| 4 | `longitudMedia` | Longitud media de las palabras (≤ 16) | `map` a (longitud, 1) + `reduce` | ✅ 4.48426 |
| 5 | `amigosComunes` | Amigos comunes de cada par de amigos | `flatMap` con claves tupla, `reduceByKey` (intersección) | ✅ |
| 6 | `top20artistas` | Top 20 artistas por reproducciones en LastFM (muestra y dataset completo de 1,6 GB) | `map`, `reduceByKey`, `takeOrdered` | ✅ |
| 7 | `top20artistasUnicos` | Top 20 artistas por número de usuarios distintos | `map` a (artista, 1), `reduceByKey` | ✅ |
| 8 | `vecinoMasCercano` / `vecinoMasCercanoMapPartitions` | Índice del vector más cercano (distancia euclídea) | `np.fromstring`, `broadcast`, `zipWithIndex`, `map` / `mapPartitions`, `reduce` | ✅ |
| 9 | `vecinoMasCercanoLista` | Vecino más cercano para varios vectores a la vez | `broadcast` de la lista, `mapPartitions`, un solo recorrido del fichero | ✅ |
| 10 | `paginasEnlacesMutuos` | Pares de URLs que se enlazan mutuamente | `flatMap` con pares ordenados, `reduceByKey`, `filter` | ✅ |
| 11 | `histogramaEdadPrecio` | Precio medio de las casas de California por edad | `reduceByKey` (suma, contador), `mapValues`, `sortByKey` | ✅ + gráfico |
| 12 | `histogramaFrecuenciaLetra` | Frecuencia de las 27 letras del abecedario español y los 10 dígitos en *El Quijote* | `flatMap` por caracteres, `reduceByKey` | ✅ 37 caracteres + gráficos |
| 14 | `areaMedia` | Área media del tumor según su tipo (benigno / maligno) | `broadcast` de las etiquetas, `reduceByKey`, `mapValues` | ⚠️ ver nota |
| 15 | `kmeans` | Algoritmo k-Means distribuido | `broadcast` de centroides, `map` + `reduceByKey` por iteración | ✅ k = 2 y k = 3 |

### Decisiones y resultados destacados

- **Ejercicio 4:** el enunciado no pide `eliminaSignosPuntuacion`, así que las palabras se obtienen partiendo el texto original por espacios. Así se obtiene el valor esperado, 4.48426.
- **Ejercicio 7:** cada línea del dataset ya es un par (usuario, artista) único (se comprobó que con `distinct()` sale el mismo resultado). Por eso basta con contar líneas por artista, sin `distinct()` ni `groupByKey`, y se evita el *shuffle* extra (NOTA2 del enunciado).
- **Ejercicio 9:** se reutilizan `dist` y `parseaFichero` y se calculan todas las distancias en una única pasada por los datos.
- **Ejercicio 14:** el área media de los tumores malignos (≈ 978.38) es más del doble que la de los benignos (≈ 462.79).
  - *Nota sobre el `assert`:* se obtiene `978.3764150943394` y el test espera `978.3764150943397`, una diferencia de 3·10⁻¹³. `reduceByKey` suma primero dentro de cada partición, y `sc.parallelize` crea una partición por núcleo (12 en el equipo de ejecución), así que el orden de suma en coma flotante cambia los últimos decimales. Con 6, 8 o 10 particiones el mismo código reproduce el valor exacto del test.
- **Ejercicio 15:** con los 7 puntos del enunciado y los *k* primeros ejemplos como centroides iniciales:
  - `k = 2` → centroides (4.5, 2.5) y (1.67, 1.67), convergencia en 2 iteraciones.
  - `k = 3` → centroides (4.5, 2.5), (2.0, 1.5) y (1.0, 2.0), convergencia en 2 iteraciones.

## Datos

Carpeta `datos/`:

- `pg2000.txt` – *Don Quijote de la Mancha* (Proyecto Gutenberg).
- `amigos.txt`, `urls.txt` – ficheros pequeños con formato `X -> lista`.
- `vectores.txt` – 100 000 vectores de dimensión 3.
- `lastfm-dataset-360K/` – dataset de LastFM (`usersha1-artmbid-artname-plays.tsv`, ~1,6 GB, y una muestra de 3000 líneas).
- Los datasets *California housing* y *Breast cancer* se cargan desde `sklearn.datasets`.

> El fichero completo `usersha1-artmbid-artname-plays.tsv` (~1,6 GB) **no está en el repositorio** porque supera el límite de 100 MB de GitHub; solo se incluye la muestra de 3000 líneas. Para ejecutar los tests de la versión grande de los ejercicios 6 y 7, descarga el dataset de <http://ocelma.net/MusicRecommendationDataset/lastfm-360K.html> y colócalo en `datos/lastfm-dataset-360K/`.

## Requisitos y ejecución

- Anaconda con Python 3.11
- PySpark 3.5
- **Java 17** (PySpark 3.5 no funciona con Java 23 o superior: da el error `getSubject is not supported`)
- numpy, matplotlib, scikit-learn, jupyter

En Windows, si hay varias versiones de Java instaladas, añade esta celda al principio del notebook y reinicia el kernel:

```python
import os, sys
os.environ["JAVA_HOME"] = r"C:\Program Files\Java\jdk-17"
os.environ["PATH"] = os.environ["JAVA_HOME"] + r"\bin;" + os.environ["PATH"]
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
```

Mientras Spark está en marcha, la evolución de los trabajos se puede seguir en <http://localhost:4040>.
