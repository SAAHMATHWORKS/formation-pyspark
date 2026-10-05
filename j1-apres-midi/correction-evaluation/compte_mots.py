import re
import sys

from pyspark.sql import SparkSession

fichier = sys.argv[1]
spark = SparkSession.builder.appName("Comptage de mots").getOrCreate()

comptes = (
    spark.sparkContext.textFile(fichier)
    .flatMap(lambda ligne: re.findall(r"\w+", ligne.lower()))
    .map(lambda mot: (mot, 1))
    .reduceByKey(lambda a, b: a + b)
    .sortBy(lambda kv: (-kv[1], kv[0]))
)
for mot, nombre in comptes.take(5):
    print(f"{mot:12} {nombre}")

spark.stop()
