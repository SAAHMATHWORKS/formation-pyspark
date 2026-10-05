# Correction de l'évaluation : chargement et modification de données

> Formation **PySpark – Traitement des données** · Jour 2 matin · Évaluation pratique (30 min)
> [Retour au sommaire du dépôt](../../README.md) · Tutoriel du matin : [Lire dans HDFS depuis PySpark](../spark-hdfs/)

Toutes les sorties de cette page sont **réelles**. Les données sont produites par un générateur reproductible, la cellule 1 de la matinée (aussi disponible dans [`generer_donnees.py`](../spark-hdfs/generer_donnees.py)) : vous devez obtenir **exactement** les mêmes chiffres.

| Fichier | Format | Piège |
|---|---|---|
| `data/clients.csv` | CSV, séparateur `;`, dates `jj/mm/aaaa` | le séparateur, et la date que Spark ne reconnaît pas seul |
| `data/commandes.json` | JSON, un objet par ligne, avec l'objet imbriqué `livraison` | la colonne imbriquée |
| `data/avis.txt` | texte, champs séparés par `\|`, sans en-tête | le `\|` est un caractère spécial des expressions régulières |

## Sommaire

1. [Étape 1 : charger les clients (3 pts)](#étape-1--charger-les-clients-3-pts)
2. [Étape 2 : statuts et modes de livraison (4 pts)](#étape-2--statuts-et-modes-de-livraison-4-pts)
3. [Étape 3 : chiffre d'affaires par segment (4 pts)](#étape-3--chiffre-daffaires-par-segment-4-pts)
4. [Étape 4 : notes par commentaire, en Parquet (3 pts)](#étape-4--notes-par-commentaire-en-parquet-3-pts)
5. [QCM (6 pts)](#qcm-6-pts)
6. [Les erreurs fréquentes, avec leurs vrais messages](#les-erreurs-fréquentes-avec-leurs-vrais-messages)

```python
# Cellule 1
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("Evaluation J2").getOrCreate()
```

> **Un seul notebook Spark ouvert à la fois.** Avant de commencer, arrêtez le noyau du notebook de la matinée (**Kernel → Shut Down Kernel**) : chaque noyau garde sa propre JVM Spark, sa mémoire et son port 4040.

---

## Étape 1 : charger les clients (3 pts)

```python
clients = (
    spark.read.csv("data/clients.csv", header=True, sep=";")
    .withColumn("date_inscription", F.to_date("date_inscription", "dd/MM/yyyy"))
)
clients.printSchema()
clients.groupBy("segment").count().show()
```

```
root
 |-- id_client: string (nullable = true)
 |-- nom: string (nullable = true)
 |-- ville: string (nullable = true)
 |-- date_inscription: date (nullable = true)
 |-- segment: string (nullable = true)

+-----------+-----+
|    segment|count|
+-----------+-----+
|particulier|  160|
|        pro|   40|
+-----------+-----+
```

| Élément | Pourquoi |
|---|---|
| `header=True` | La première ligne donne les noms de colonnes. Sans lui : `_c0`, `_c1`… et 201 lignes. |
| `sep=";"` | Le CSV français utilise le point-virgule. **Sans lui, une seule colonne, et aucune erreur** (voir [les erreurs fréquentes](#les-erreurs-fréquentes-avec-leurs-vrais-messages)). |
| `F.to_date(…, "dd/MM/yyyy")` | Une conversion **avec un format** : jour, **mois** (`MM` ; `mm` désigne les minutes), année. Un simple `cast("date")` n'accepte que `aaaa-mm-jj`. |
| Le contrôle | 160 + 40 = **200** clients. |

**Variante acceptée : avec `inferSchema=True`.** Le résultat est identique, car les 5 colonnes de ce fichier sont du texte : `inferSchema` les laisse toutes en `string`, et ne reconnaît pas la date `20/01/2024`. Il coûte en revanche **une lecture complète du fichier en plus**. Inutile ici ; en production, on donne plutôt le schéma soi-même.

---

## Étape 2 : statuts et modes de livraison (4 pts)

```python
commandes = spark.read.json("data/commandes.json")
commandes.groupBy("statut").count().show()

(commandes.filter(F.col("statut") == "livree")
          .groupBy(F.col("livraison.mode").alias("mode"))
          .count()
          .orderBy(F.desc("count"))
          .show())
```

```
+--------+-----+
|  statut|count|
+--------+-----+
|en_cours|  107|
|  livree|  778|
| annulee|  115|
+--------+-----+

+--------+-----+
|    mode|count|
+--------+-----+
| retrait|  270|
|  relais|  265|
|domicile|  243|
+--------+-----+
```

- **Le JSON se lit sans option** : il est auto-décrit. `livraison` devient une colonne `struct`, avec les sous-colonnes `mode` et `delai_jours`.
- **`F.col("livraison.mode")`** : le point descend dans l'objet imbriqué. `.alias("mode")` donne un nom lisible à la colonne du résultat.
- **Filtrer avant de grouper** : on ne compte que les 778 commandes livrées. 270 + 265 + 243 = **778**.
- **L'ordre de la première table peut différer chez vous** : sans `orderBy`, l'ordre des lignes après un `groupBy` dépend des partitions. L'énoncé ne demande de tri que pour la seconde table.

---

## Étape 3 : chiffre d'affaires par segment (4 pts)

Le segment est dans `clients`, le montant dans `commandes` : il faut une **jointure** sur `id_client`.

**Avec l'API :**

```python
livrees = commandes.filter(F.col("statut") == "livree")
(livrees.join(clients, "id_client")
        .groupBy("segment")
        .agg(F.count("*").alias("nb_commandes"),
             F.round(F.sum("montant"), 2).alias("ca"))
        .orderBy(F.desc("ca"))
        .show())
```

**Ou en SQL :**

```python
clients.createOrReplaceTempView("clients")
commandes.createOrReplaceTempView("commandes")
spark.sql("""
    SELECT k.segment, COUNT(*) AS nb_commandes, ROUND(SUM(c.montant), 2) AS ca
    FROM commandes c JOIN clients k ON c.id_client = k.id_client
    WHERE c.statut = 'livree'
    GROUP BY k.segment
    ORDER BY ca DESC
""").show()
```

```
+-----------+------------+--------+
|    segment|nb_commandes|      ca|
+-----------+------------+--------+
|particulier|         614|83974.35|
|        pro|         164|23172.57|
+-----------+------------+--------+
```

- **Le contrôle qui compte :** 614 + 164 = **778**, le nombre de commandes livrées. Une jointure `inner` élimine **sans prévenir** les lignes sans correspondance : comptez toujours avant et après.
- `join(clients, "id_client")`, avec le **nom** de la colonne commune : le résultat n'a qu'une colonne `id_client`.
- `agg(...)` permet plusieurs calculs dans le même `groupBy` ; `F.round(…, 2)` arrondit au centime.
- **Comment Spark l'exécute :** `clients` ne fait que 8 Ko, très en dessous du seuil de 10 Mo. Spark l'envoie **entier** à chaque tâche (`BroadcastHashJoin` dans `.explain()`) au lieu de mélanger les commandes sur le réseau : pas de shuffle pour la jointure.

---

## Étape 4 : notes par commentaire, en Parquet (3 pts)

```python
champs = F.split(F.col("value"), "\\|")
avis = spark.read.text("data/avis.txt").select(
    champs.getItem(0).alias("date"),
    champs.getItem(1).alias("id_client"),
    champs.getItem(2).cast("int").alias("note"),
    champs.getItem(3).alias("commentaire"),
)

notes = (avis.groupBy("commentaire")
             .agg(F.round(F.avg("note"), 2).alias("note_moyenne"))
             .orderBy(F.desc("note_moyenne")))
notes.show(truncate=False)

notes.write.mode("overwrite").parquet("resultats/notes_commentaires")
print(spark.read.parquet("resultats/notes_commentaires").count(), "lignes relues")
```

```
+-------------------------+------------+
|commentaire              |note_moyenne|
+-------------------------+------------+
|Service client réactif   |4.51        |
|Très bon produit         |4.19        |
|Livraison rapide         |3.86        |
|Conforme à la description|3.54        |
|Emballage abîmé          |2.19        |
|Délai trop long          |1.45        |
+-------------------------+------------+

6 lignes relues
```

| Élément | Pourquoi |
|---|---|
| `spark.read.text(...)` | Une seule colonne `value`, une ligne du fichier par ligne : c'est à vous de découper. |
| `F.split(F.col("value"), "\\\|")` | Le motif est une **expression régulière**, où `\|` veut dire « ou » : il faut l'échapper. |
| `getItem(0)` … `getItem(3)` | Les éléments du tableau, **à partir de 0**. |
| `.cast("int")` | La note arrive en texte (`"5"`) ; il faut un entier pour la moyenne. |
| `truncate=False` | Affiche les commentaires en entier. |
| `.write.mode("overwrite")` | Sans lui, la deuxième exécution échoue avec `PATH_ALREADY_EXISTS`. |
| `.parquet("resultats/notes_commentaires")` | Crée un **dossier** : un fichier `part-…snappy.parquet` par tâche, plus `_SUCCESS`. |
| `spark.read.parquet(...)` | Relu **sans option** : les types sont enregistrés dans le fichier. |

La date peut rester une `string` : elle ne sert pas au calcul.

---

## QCM (6 pts)

| Question | Réponse | Pourquoi |
|---|---|---|
| Parmi les commandes livrées, quel mode de livraison est le plus utilisé ? | **retrait** | 270 commandes livrées, devant relais (265) et domicile (243). |
| Quel est le chiffre d'affaires des commandes livrées aux clients pro ? | **23 172,57 €** | 164 commandes. Les particuliers totalisent 83 974,35 € pour 614 commandes. |
| Quel commentaire obtient la meilleure note moyenne ? | **Service client réactif** | 4,51 de moyenne, devant Très bon produit (4,19). |

---

## Les erreurs fréquentes, avec leurs vrais messages

Ces erreurs ont été rencontrées pendant la préparation de la matinée ; les sorties sont réelles.

### Oublier `sep=";"` : une seule colonne, sans aucune erreur

```python
spark.read.csv("data/clients.csv", header=True).printSchema()
```

```
root
 |-- id_client;nom;ville;date_inscription;segment: string (nullable = true)
```

Spark cherche des virgules, n'en trouve pas, et range toute la ligne dans une seule colonne. Un `count()` donnerait bien 200 : rien ne prévient. **Regardez toujours le schéma.**

### Écrire `"|"` au lieu de `"\\|"` : `CAST_INVALID_INPUT`

Non échappé, `|` est un motif vide qui découpe **entre chaque caractère**, sans rien retirer :

```
[2, 0, 2, 6, -, 0, 4, -, 2, 6, |, C, 1, 5, 8, |, 5, |, T, r, è, s,  , b, o, n,  , p, r, o, d, u, i, t, ]
```

Dès qu'une colonne est convertie, la cellule échoue :

```
DateTimeException: [CAST_INVALID_INPUT] The value '2' of the type "STRING" cannot be cast to "DATE" because it is malformed.
Correct the value as per the syntax, or change its target type. Use `try_cast` to tolerate malformed input and return NULL instead. SQLSTATE: 22018
```

Depuis Spark 4, le mode **ANSI** est activé par défaut : une conversion impossible **arrête** le calcul au lieu de produire des `null` en silence. Pour tolérer exprès des valeurs invalides : `F.try_to_date(...)`, `try_cast`.

### `NameError: name 'commandes' is not defined`

```
NameError: name 'commandes' is not defined
```

Le noyau a redémarré depuis la cellule qui créait `commandes` : un redémarrage efface **toutes** les variables. Le numéro `In[…]` à gauche des cellules indique ce qui a tourné, et dans quel ordre. Réexécutez les cellules nécessaires, dans l'ordre.

### Relancer une écriture : `PATH_ALREADY_EXISTS`

```
AnalysisException: [PATH_ALREADY_EXISTS] Path file:/home/jovyan/work/resultats/notes_commentaires already exists.
Set mode as "overwrite" to overwrite the existing path. SQLSTATE: 42K04
```

Le mode d'écriture par défaut refuse d'écraser un résultat existant. Ajoutez `.mode("overwrite")`.

### `Connection refused` dès la `SparkSession`

Le noyau garde une session dont la JVM a disparu, par exemple après un redémarrage du conteneur `spark-lab`. **Kernel → Restart Kernel**, puis réexécutez la cellule 1.

### Tableau récapitulatif

| Symptôme | Cause | Remède |
|---|---|---|
| Une seule colonne au nom à rallonge | `sep=";"` oublié | `spark.read.csv(…, sep=";")` |
| Colonnes `_c0`, `_c1`… et 201 lignes | `header=True` oublié | `spark.read.csv(…, header=True)` |
| `date_inscription: string` | Pas de conversion | `F.to_date(…, "dd/MM/yyyy")` |
| Dates fausses ou erreur de conversion | `dd/mm/yyyy` : `mm` = minutes | `MM` pour les mois |
| `AnalysisException … livraison.mode` ou colonne introuvable | Mauvais nom de sous-colonne | `F.col("livraison.mode")` ; vérifiez avec `printSchema()` |
| Un total inférieur à 778 après la jointure | Jointure sur une mauvaise colonne, ou filtre oublié | Comptez avant et après la jointure |
| `CAST_INVALID_INPUT … '2'` | `"\|"` au lieu de `"\\\|"` | Échapper le `\|` |
| `PATH_ALREADY_EXISTS` | Deuxième écriture sans mode | `.mode("overwrite")` |
| `NameError` | Cellule précédente non réexécutée après un redémarrage | Réexécuter dans l'ordre |
