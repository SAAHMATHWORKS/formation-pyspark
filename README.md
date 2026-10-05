# Formation PySpark – Traitement des données

Ressources de la formation **PySpark** (3 jours) animée par **Thibaut SAAH** : les environnements des travaux pratiques, des tutoriels à refaire chez soi, et les corrections des évaluations.

```bash
git clone https://github.com/SAAHMATHWORKS/formation-pyspark.git
cd formation-pyspark
```

## Contenu

| Demi-journée | Dossier | Ce que vous y trouvez |
|---|---|---|
| **Jour 1 matin** · Big Data, Hadoop, HDFS, MapReduce | [`j1-matin/hadoop-cluster`](j1-matin/hadoop-cluster/) | Le mini-cluster Hadoop du TP : `docker-compose.yaml`, `config` et le mode d'emploi |
| **Jour 1 après-midi** · Spark, RDD, DataFrames | [`j1-apres-midi/spark-cluster`](j1-apres-midi/spark-cluster/) | **Tutoriel complet :** monter un cluster Spark Standalone avec Docker, étape par étape, avec schémas et captures, puis le même script lancé avec `spark-submit` en local et sur le cluster |
| | [`j1-apres-midi/spark-fabric`](j1-apres-midi/spark-fabric/) | **Tutoriel complet :** Spark dans Azure avec Microsoft Fabric, gratuitement pendant 60 jours : compte Azure, essai Fabric, notebook et lakehouse |
| Jour 2 matin · DataFrames et Spark SQL | *à venir* | |
| Jour 2 après-midi · Machine Learning avec spark.ml | *à venir* | |
| Jour 3 matin · Streaming et Spark SQL avancé | *à venir* | |
| Jour 3 après-midi · Graphes avec GraphFrames | *à venir* | |

## Corrections des évaluations

Les corrections sont publiées dans ce dépôt **à la fin de chaque demi-journée**, une fois l'évaluation terminée.

## Prérequis

- **Docker** et **Docker Compose** : `docker --version` et `docker compose version` doivent afficher un numéro de version.
- L'image de l'après-midi : `docker pull quay.io/jupyter/pyspark-notebook` (environ 2 Go à télécharger).
- L'image du matin : `docker pull apache/hadoop:3`.
- Sous Windows : un terminal **WSL** ou **Git Bash**, pour copier les commandes telles quelles.

## Bonnes pratiques

- Le **jeton** de Jupyter est un mot de passe : ne le collez ni dans un chat ni sur un écran partagé. `docker restart spark-lab` en génère un nouveau.
- Les dossiers de données générés par les TP (`data/`, `resultats/`, `spark-warehouse/`…) sont ignorés par Git (voir [`.gitignore`](.gitignore)).
