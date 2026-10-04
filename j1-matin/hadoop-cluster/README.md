# Le mini-cluster Hadoop

> Formation **PySpark – Traitement des données** · Jour 1 matin · Notion « HDFS »
> [Retour au sommaire du dépôt](../../README.md)

Un cluster Hadoop complet sur votre poste, avec Docker : **1 NameNode, 3 DataNodes, 1 ResourceManager et 1 NodeManager**, à partir de l'image officielle `apache/hadoop:3`.

| Fichier | Contenu |
|---|---|
| [`docker-compose.yaml`](docker-compose.yaml) | Les 4 services du cluster |
| [`config`](config) | La configuration de Hadoop : chaque ligne devient une propriété d'un fichier XML (`HDFS-SITE.XML_dfs.replication=3` écrit `dfs.replication` dans `hdfs-site.xml`) |

## Démarrer

Le nom du dossier, `hadoop-cluster`, compte : Docker Compose s'en sert comme préfixe pour nommer les conteneurs (`hadoop-cluster-namenode-1`…).

```bash
$ cd formation-pyspark/j1-matin/hadoop-cluster
$ docker compose config --quiet && echo "Fichiers valides"
$ docker compose up -d --scale datanode=3
$ docker compose ps
```

Attendu : **6 conteneurs** `Up`. Après une trentaine de secondes :

| Adresse | Ce qu'on y voit |
|---|---|
| `http://localhost:9870` | L'interface du **NameNode**. Onglet **Datanodes** : 3 DataNodes « In service ». |
| `http://localhost:8088` | L'interface du **ResourceManager** de YARN. Menu **Nodes** : le NodeManager. |

## Vérifier HDFS et lancer un job MapReduce

Ouvrez un terminal **dans** le NameNode : les commandes suivantes s'y exécutent.

```bash
$ docker compose exec namenode bash
```

```bash
hdfs dfsadmin -report | head -22
hdfs dfsadmin -safemode get
```

Attendu : `Live datanodes (3):`, puis `Safe mode is OFF`. Si le safe mode est encore `ON`, le cluster vient de démarrer : attendez avec `hdfs dfsadmin -safemode wait`.

Un job d'exemple, qui estime π avec 4 tâches map, à suivre sur `http://localhost:8088` :

```bash
yarn jar /opt/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-examples-*.jar pi 4 1000
```

> Écrivez les chemins en entier (`/opt/hadoop/…`) : dans le conteneur, la variable `$HADOOP_HOME` est vide. Pour revenir à votre poste : `exit`.

## Arrêter

| Commande | Effet |
|---|---|
| `docker compose stop` | **Arrête** les conteneurs sans les supprimer : `docker compose start` relance le même cluster, avec ses fichiers HDFS. |
| `docker compose down` | **Supprime** les conteneurs et le réseau : le prochain `up` repart d'un HDFS vide. |

## Dépannage

| Symptôme | Solution |
|---|---|
| `no configuration file provided` | Vous n'êtes pas dans le dossier `hadoop-cluster`. |
| `yaml: line …` | Une indentation a été abîmée : recopiez le fichier. |
| `env file … not found` | Le fichier `config` manque ou porte un autre nom. |
| Un conteneur s'arrête tout de suite | `docker compose logs namenode` (ou le nom du service concerné). |
| Les DataNodes s'arrêtent avec `Incompatible clusterIDs` | Le NameNode a été reformaté. Vérifiez la ligne `ENSURE_NAMENODE_DIR: "/tmp/hadoop-hadoop/dfs/name"`, puis `docker compose down` et `docker compose up -d --scale datanode=3`. |
| `JAR does not exist` | Écrivez le chemin complet `/opt/hadoop/share/hadoop/mapreduce/…`. |
| Port 9870 ou 8088 déjà pris | Changez le numéro de gauche dans `ports` (par exemple `9871:9870`). |

> **Réglage pédagogique :** `dfs.namenode.heartbeat.recheck-interval=15000` fait déclarer mort un DataNode silencieux en 1 minute environ au lieu de 10 min 30 s, pour observer une panne en direct. **On ne ferait jamais ça en production.**
