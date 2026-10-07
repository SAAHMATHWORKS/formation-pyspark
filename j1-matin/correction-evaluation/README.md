# Correction de l'évaluation : votre cluster Hadoop

> Formation **PySpark – Traitement des données** · Jour 1 matin · Évaluation pratique (30 min)
> [Retour au sommaire du dépôt](../../README.md) · Le cluster : [`hadoop-cluster`](../hadoop-cluster/) · Pour aller plus loin : [la réplication sur 5 DataNodes](../hdfs-replication/)

**Le scénario.** Votre équipe veut tester Hadoop avant d'en commander un vrai. Vous prouvez que votre cluster de démonstration fonctionne : il stocke, il résiste à une panne, et il calcule.

Les sorties de cette page viennent d'un vrai cluster de formation (1 NameNode, 3 DataNodes, 1 ResourceManager, 1 NodeManager). Les **adresses IP**, les **identifiants de blocs** et les **numéros d'application** changent chez vous. Les **tailles**, les **nombres de blocs** et les **comptages**, eux, doivent être **identiques**.

## Sommaire

0. [Avant de commencer : le cluster](#0-avant-de-commencer--le-cluster)
1. [Étape 1 : vérifier l'état du cluster (3 pts)](#étape-1--vérifier-létat-du-cluster-3-pts)
2. [Étape 2 : stocker des fichiers et observer les blocs (4 pts)](#étape-2--stocker-des-fichiers-et-observer-les-blocs-4-pts)
3. [Étape 3 : arrêter un DataNode (3 pts)](#étape-3--arrêter-un-datanode-3-pts)
4. [Étape 4 : compter les mots avec MapReduce (4 pts)](#étape-4--compter-les-mots-avec-mapreduce-4-pts)
5. [QCM (6 pts)](#qcm-6-pts)
6. [Les erreurs fréquentes](#les-erreurs-fréquentes)
7. [Arrêter le cluster](#arrêter-le-cluster)

## L'image à garder en tête

| Composant | Rôle | L'image |
|---|---|---|
| **NameNode** | Tient le registre : quels fichiers, quels blocs, sur quelles machines. **Ne stocke aucune donnée.** | Le bibliothécaire en chef et son registre |
| **DataNode** (×3) | Garde les blocs sur son disque | Les bâtiments où l'on range les chapitres |
| **ResourceManager** | Attribue CPU et mémoire aux applications | Le chef de chantier |
| **NodeManager** | Exécute les tâches dans des conteneurs | L'ouvrier |

---

## 0. Avant de commencer : le cluster

Si le cluster n'existe pas encore, suivez le mode d'emploi de [`hadoop-cluster`](../hadoop-cluster/). En résumé :

```bash
mkdir -p ~/hadoop-cluster && cd ~/hadoop-cluster
# déposer docker-compose.yaml et config (voir ../hadoop-cluster), puis :
docker compose config --quiet && echo "Fichiers valides"
docker compose up -d --scale datanode=3
```

S'il existe mais qu'il est arrêté :

```bash
cd ~/hadoop-cluster && docker compose up -d --scale datanode=3
```

**Le nom du dossier compte** : Docker Compose s'en sert comme préfixe des conteneurs (`hadoop-cluster-datanode-2`, utilisé à l'étape 3).

**Attendre que le cluster soit vraiment prêt**, car `Up` ne suffit pas :

```bash
until docker compose exec namenode hdfs dfsadmin -safemode wait 2>/dev/null; do sleep 3; done
```

Cette boucle réessaie toutes les 3 secondes tant que le NameNode refuse la connexion, puis attend la fin du **safe mode**. Le détail est expliqué à l'étape 1.

---

## Étape 1 : vérifier l'état du cluster (3 pts)

### Les commandes

```bash
docker compose exec namenode bash      # ouvre un terminal dans le conteneur du NameNode
```

Puis, dans le NameNode :

```bash
hdfs dfsadmin -report | head -22
hdfs dfsadmin -safemode get
```

| Commande | Ce qu'elle fait |
|---|---|
| `docker compose exec namenode bash` | Ouvre un terminal **dans** le conteneur du NameNode, où le client `hdfs` est installé. |
| `hdfs dfsadmin` | L'outil d'**administration** de HDFS. À ne pas confondre avec `hdfs dfs`, qui manipule les fichiers. |
| `-report` | L'état du cluster vu par le NameNode : capacité, espace utilisé, une fiche par DataNode. |
| `head -22` | Garde les 22 premières lignes : le résumé, puis la fiche du premier DataNode. |
| `-safemode get` | L'état du safe mode : `ON` ou `OFF`. |

### Le résultat attendu

```
Configured Capacity: …
Present Capacity: …
DFS Remaining: …
DFS Used: …
…
-------------------------------------------------
Live datanodes (3):

Name: 172.27.0.2:9866 (hadoop-cluster-datanode-2.hadoop-cluster_default)
Hostname: …
Decommission Status : Normal
…
Safe mode is OFF
```

**Ce qu'on vérifie :** `Live datanodes (3)` et `Safe mode is OFF`.

- **`9866`** est le port où chaque DataNode reçoit et envoie les **blocs**. Les données passent par là, jamais par le NameNode.
- Les capacités dépendent du disque de votre machine : les trois DataNodes sont des conteneurs **sur le même disque**.

### Le safe mode, expliqué

Le NameNode garde deux sortes d'informations :

| Information | Où elle est gardée |
|---|---|
| L'arborescence : noms de fichiers, dossiers, liste des blocs de chaque fichier | **Sur disque** (le `fsimage` et le journal `edits`), rechargée au démarrage |
| **Quel DataNode possède quel bloc** | **Nulle part sur disque.** Le NameNode l'apprend à chaque démarrage, par le **rapport de blocs** de chaque DataNode |

Au démarrage, il attend donc que les DataNodes aient signalé 99,9 % des blocs, puis une trentaine de secondes. Pendant ce temps, HDFS est en **lecture seule** :

| En safe mode, on peut… | On ne peut pas… |
|---|---|
| `hdfs dfs -ls`, `-cat`, `fsck`, `dfsadmin -report` | `hdfs dfs -mkdir`, `-put`, `-rm`, renommer |

- **« Lecture seule » concerne les fichiers HDFS, pas la configuration.** La configuration est lue au démarrage, depuis le fichier `config`.
- **Le safe mode est temporaire.** Une écriture trop tôt échoue avec `Name node is in safe mode` : il suffit d'attendre.
- **Le piège :** sur un HDFS **vide**, le NameNode sort du safe mode **tout de suite, même sans aucun DataNode**. « Safe mode OFF » ne veut pas dire « cluster en bonne santé ». Vérifiez toujours `Live datanodes (3)`.

> **L'image.** Le matin, la bibliothèque rouvre. Le registre dit quels livres existent, mais pas dans quels bâtiments sont les chapitres. Le bibliothécaire attend que chaque bâtiment l'appelle avant d'autoriser à ajouter, déplacer ou jeter des livres. On peut consulter le registre, mais rien modifier.

### La vérification dans le navigateur

`http://localhost:9870`, onglet **Datanodes** : 3 lignes « In service ».

---

## Étape 2 : stocker des fichiers et observer les blocs (4 pts)

### Les commandes

```bash
cd /tmp
hdfs dfs -mkdir -p /user/hadoop/eval

cat > corpus.txt <<'EOF'
Spark est rapide et Spark est distribué
Hadoop stocke les données et Hadoop distribue les calculs
les données sont découpées en blocs et les blocs sont répliqués
YARN attribue les ressources et Spark utilise YARN
EOF
hdfs dfs -put corpus.txt /user/hadoop/eval/

seq 1 500000 > nombres.txt
hdfs dfs -D dfs.blocksize=1048576 -put nombres.txt /user/hadoop/eval/

hdfs dfs -ls -h /user/hadoop/eval
hdfs fsck /user/hadoop/eval/nombres.txt -files -blocks -locations
```

| Commande | Ce qu'elle fait |
|---|---|
| `cd /tmp` | Se place dans un dossier **local** du conteneur, où l'on a le droit d'écrire. |
| `hdfs dfs -mkdir -p …` | Crée un dossier **dans HDFS**. `-p` crée aussi les parents. |
| `cat > corpus.txt <<'EOF' … EOF` | Écrit un petit texte **local** de 4 lignes (220 octets). Les guillemets autour de `'EOF'` empêchent le shell d'interpréter le contenu. |
| `hdfs dfs -put corpus.txt …` | Copie le fichier **local** vers **HDFS**. |
| `seq 1 500000 > nombres.txt` | Un fichier local avec un nombre par ligne : **3 388 895 octets**. |
| `-D dfs.blocksize=1048576` | Pour **cette commande seulement**, des blocs de **1 Mo** au lieu de 128 Mo, pour voir plusieurs blocs sur un petit fichier. |
| `hdfs dfs -ls -h` | Liste le dossier HDFS. `-h` affiche les tailles lisiblement. |
| `hdfs fsck … -files -blocks -locations` | Le fichier, ses blocs, et **où** est chaque copie. |

### Deux systèmes de fichiers

| | Système de fichiers **local** du conteneur | **HDFS** |
|---|---|---|
| Commandes | `ls`, `cat`, `seq > fichier` | `hdfs dfs -ls`, `-put`, `-cat` |
| Où vivent les données | Sur le disque **de ce seul conteneur** | Découpées en blocs, **sur les DataNodes** |
| Exemple | `/tmp/nombres.txt` | `/user/hadoop/eval/nombres.txt` |

`-put` est le **pont** entre les deux.

### Le résultat de `-ls -h`

```
Found 2 items
-rw-r--r--   3 hadoop supergroup        220 … /user/hadoop/eval/corpus.txt
-rw-r--r--   3 hadoop supergroup      3.2 M … /user/hadoop/eval/nombres.txt
```

Le **3** de la 2ᵉ colonne est le **facteur de réplication** de chaque fichier.

### Le résultat de `fsck` (sortie réelle, mêmes tailles chez vous)

```
/user/hadoop/eval/nombres.txt 3388895 bytes, replicated: replication=3, 4 block(s):  OK
0. BP-…:blk_1073741825_1001 len=1048576 Live_repl=3  [DatanodeInfoWithStorage[172.27.0.7:9866,…], DatanodeInfoWithStorage[172.27.0.6:9866,…], DatanodeInfoWithStorage[172.27.0.2:9866,…]]
1. BP-…:blk_1073741826_1002 len=1048576 Live_repl=3  [… .7, .6, .2 …]
2. BP-…:blk_1073741827_1003 len=1048576 Live_repl=3  [… .2, .6, .7 …]
3. BP-…:blk_1073741828_1004 len=243167  Live_repl=3  [… .2, .7, .6 …]

Status: HEALTHY
 Number of data-nodes:  3
 Number of racks:       1
 Total size:    3388895 B
 Total blocks (validated):      4 (avg. block size 847223 B)
 Under-replicated blocks:       0 (0.0 %)
 Default replication factor:    3
 Average block replication:     3.0
```

### Le calcul

```
3 388 895 octets ÷ 1 048 576 octets = 3,23
→ 3 blocs pleins de 1 048 576 octets
→ 1 dernier bloc de 3 388 895 − 3 × 1 048 576 = 243 167 octets
→ 4 blocs
```

- **Le dernier bloc n'occupe que sa taille réelle** (243 167 octets), pas 1 Mo.
- **Chaque bloc a 3 copies**, sur les 3 DataNodes : `Live_repl=3`. Cela fait **12 copies** au total (4 × 3).
- **L'ordre des DataNodes change d'un bloc à l'autre** : le premier de la liste est le premier maillon du **pipeline d'écriture**, que le NameNode fait varier pour répartir la charge.

> **« On est entré dans le NameNode pour écrire : les données sont donc dans le NameNode ? »** Non. Le conteneur du NameNode n'est qu'un endroit pratique où le **client** `hdfs` est installé. `-put` demande au NameNode **où** écrire, puis envoie les blocs **directement aux DataNodes**. La preuve : l'adresse du NameNode n'apparaît dans **aucune** liste de blocs du `fsck`.

---

## Étape 3 : arrêter un DataNode (3 pts)

### Les commandes

Dans un **second terminal**, sur votre machine, pas dans le conteneur :

```bash
cd ~/hadoop-cluster
docker compose ps datanode
docker stop hadoop-cluster-datanode-2
```

**Attendez environ 1 minute** (voir plus bas pourquoi), puis dans le NameNode :

```bash
hdfs dfsadmin -report | grep -E "Live datanodes|Dead datanodes"
hdfs fsck /user/hadoop/eval | grep -iE "under-replicated|status"
hdfs dfs -cat /user/hadoop/eval/corpus.txt
```

| Commande | Ce qu'elle fait |
|---|---|
| `docker compose ps datanode` | Liste les 3 conteneurs DataNode et leur nom exact. |
| `docker stop …datanode-2` | Arrête un DataNode, comme si la machine tombait en panne. |
| `grep -E "A\|B"` | Garde les lignes qui contiennent A **ou** B. `-E` autorise le `\|`. |
| `fsck /user/hadoop/eval` | Vérifie **tout le dossier** : les 2 fichiers. |
| `grep -iE "…"` | `-i` ignore les majuscules et minuscules. |
| `hdfs dfs -cat` | Lit un fichier : la preuve qu'on peut encore lire. |

### Le résultat attendu

```
Live datanodes (2):
Dead datanodes (1):
 Under-replicated blocks:       5 (100.0 %)
Status: HEALTHY
Spark est rapide et Spark est distribué
Hadoop stocke les données et Hadoop distribue les calculs
les données sont découpées en blocs et les blocs sont répliqués
YARN attribue les ressources et Spark utilise YARN
```

Si vous voyez encore `Live datanodes (3)`, la commande est partie trop tôt : attendez et relancez.

### Pourquoi « 5 (100 %) » ?

Le dossier `/user/hadoop/eval` contient **5 blocs** : 1 pour `corpus.txt` (220 octets) et 4 pour `nombres.txt`. Avec 3 DataNodes et 3 copies, **chaque DataNode a une copie de chaque bloc**. En perdant le datanode-2, **chaque bloc perd une copie** :

```
                  datanode-1   datanode-2 ✗   datanode-3    copies
corpus.txt        ■            ✗              ■             2/3 ⚠
nombres · bloc 0  ■            ✗              ■             2/3 ⚠
nombres · bloc 1  ■            ✗              ■             2/3 ⚠
nombres · bloc 2  ■            ✗              ■             2/3 ⚠
nombres · bloc 3  ■            ✗              ■             2/3 ⚠
```

- **`Under-replicated blocks` compte les blocs** qui ont perdu au moins une copie, chacun **une seule fois**. Ce n'est ni un nombre de fichiers, ni un nombre de DataNodes.
- **`HEALTHY` : aucune donnée n'est perdue.** Chaque bloc a encore 2 copies, et `-cat` fonctionne.
- **HDFS ne peut pas réparer ici.** La 3ᵉ copie doit aller sur un DataNode **qui n'en a pas déjà une**, et il n'en reste aucun. Sur un cluster plus grand, le NameNode recopierait tout seul : voir [la réplication sur 5 DataNodes](../hdfs-replication/).
- **Le cas grave serait `Missing blocks`** : un bloc dont **toutes** les copies ont disparu, avec un statut `CORRUPT`.

### Combien de temps avant que le NameNode déclare un DataNode mort ?

Chaque DataNode envoie un **battement de cœur** (*heartbeat*) au NameNode **toutes les 3 secondes**. S'il se tait, le NameNode ne le déclare pas mort tout de suite : il attend un délai calculé à partir de deux réglages.

```
délai avant « mort » = 2 × dfs.namenode.heartbeat.recheck-interval  +  10 × dfs.heartbeat.interval
```

| Réglage | Valeur par défaut | Dans notre fichier `config` |
|---|---|---|
| `dfs.heartbeat.interval` (fréquence des battements) | 3 s | 3 s (inchangé) |
| `dfs.namenode.heartbeat.recheck-interval` (fréquence des vérifications) | 300 000 ms = 5 min | **15 000 ms = 15 s** |
| **Délai avant « mort »** | 2 × 5 min + 10 × 3 s = **10 min 30 s** | 2 × 15 s + 10 × 3 s = **1 min** |

Notre réglage vient de cette ligne du fichier `config` :

```ini
HDFS-SITE.XML_dfs.namenode.heartbeat.recheck-interval=15000
```

**Pourquoi 10 min 30 s en production ?** Un serveur peut se taire quelques instants sans être en panne : un redémarrage rapide, un réseau saturé, une pause de la JVM. Le déclarer mort trop vite déclencherait des recopies massives et inutiles de ses blocs. On a raccourci ce délai **pour voir la panne pendant la formation**. **On ne le ferait jamais en production.**

### Voir le DataNode mort dans le navigateur

Ouvrez **`http://localhost:9870`**, l'interface du NameNode.

| Onglet | Ce qu'on y voit après la panne |
|---|---|
| **Overview** | Les lignes **`Live Nodes`** (2) et **`Dead Nodes`** (1). Cliquer sur le chiffre mène à la liste. |
| **Datanodes** | Le tableau des DataNodes. Pendant la première minute, `datanode-2` est encore « In service », et sa colonne **Last contact** grimpe (3 s, 10 s, 40 s…). Une fois le délai dépassé, il passe en **Dead** : on voit **lequel** est tombé. |

Et en ligne de commande, la liste des seuls DataNodes morts, avec leur nom :

```bash
hdfs dfsadmin -report -dead
```

```
Dead datanodes (1):

Name: 172.27.0.2:9866 (hadoop-cluster-datanode-2.hadoop-cluster_default)
…
Last contact: …
```

---

## Étape 4 : compter les mots avec MapReduce (4 pts)

### Relancer d'abord le DataNode

Dans le second terminal :

```bash
docker start hadoop-cluster-datanode-2
```

Le datanode-2 retrouve **son propre disque**, avec ses blocs intacts. Il envoie son rapport de blocs au NameNode, et tout redevient `Live_repl=3` **sans qu'aucune donnée soit recopiée**. Une panne courte ne coûte rien.

### Les commandes

Dans le NameNode :

```bash
yarn jar /opt/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-examples-*.jar \
    wordcount /user/hadoop/eval/corpus.txt /user/hadoop/eval/wordcount
hdfs dfs -ls /user/hadoop/eval/wordcount
hdfs dfs -cat /user/hadoop/eval/wordcount/part-r-00000 | sort -k2 -nr | head -5
```

| Morceau | Ce qu'il fait |
|---|---|
| `yarn jar` | Soumet un programme Java à **YARN**. C'est l'ancêtre de `spark-submit`. |
| `…examples-*.jar` | Les programmes d'exemple livrés avec Hadoop. Le `*` est remplacé par le **shell** : il faut être dans `bash`. |
| `wordcount` | Le programme choisi : compter les mots. |
| `/user/hadoop/eval/corpus.txt` | L'**entrée**, dans HDFS. |
| `/user/hadoop/eval/wordcount` | Le **dossier de sortie**, dans HDFS. Il **ne doit pas exister** avant le job. |
| `sort -k2 -nr` | Trie sur la 2ᵉ colonne (le nombre), numériquement (`-n`), du plus grand au plus petit (`-r`). |
| `head -5` | Garde les 5 premières lignes. |

### Ce qui se passe sur YARN

1. Le client contacte le **ResourceManager** (port 8032).
2. Le ResourceManager lance l'**ApplicationMaster**, le chef de chantier de **ce** job.
3. L'ApplicationMaster demande un conteneur pour la tâche **map** (1 fichier de 1 bloc, donc 1 map), puis un pour le **reduce**.
4. **Map** : chaque ligne donne des paires `(mot, 1)`. **Shuffle** : les paires du même mot sont regroupées. **Reduce** : on additionne.
5. Le résultat est écrit **dans HDFS**.

Suivez l'application sur **`http://localhost:8088`** : elle passe de `RUNNING` à `FINISHED`, puis `SUCCEEDED`.

### Le résultat attendu

```
Found 2 items
-rw-r--r--   3 hadoop supergroup          0 … /user/hadoop/eval/wordcount/_SUCCESS
-rw-r--r--   3 hadoop supergroup        … … /user/hadoop/eval/wordcount/part-r-00000
```

```
les     5
et      4
Spark   3
…       2
…       2
```

- **« les » est le mot le plus fréquent (5)**, devant « et » (4) et « Spark » (3).
- Les deux dernières lignes sont deux des mots qui apparaissent 2 fois : `Hadoop`, `YARN`, `blocs`, `données`, `est` ou `sont`. Lesquels sortent dépend de l'ordre de tri des ex æquo.
- **Le comptage Java coupe sur les espaces et respecte la casse** : « Spark » et « spark » seraient deux mots différents.
- **`_SUCCESS`** est un fichier **vide**, que MapReduce crée quand le job s'est terminé sans erreur. Les traitements suivants le testent avant de lire le résultat.
- **`part-r-00000`** : `r` pour **reduce**, `00000` pour le premier reducer. Avec 3 reducers, on aurait `part-r-00000` à `part-r-00002`.

### Le comptage complet (20 mots différents)

| Mot | Nombre | | Mot | Nombre |
|---|---|---|---|---|
| les | 5 | | attribue | 1 |
| et | 4 | | calculs | 1 |
| Spark | 3 | | distribue | 1 |
| Hadoop | 2 | | distribué | 1 |
| YARN | 2 | | découpées | 1 |
| blocs | 2 | | en | 1 |
| données | 2 | | rapide | 1 |
| est | 2 | | ressources | 1 |
| sont | 2 | | répliqués | 1 |
| | | | stocke | 1 |
| | | | utilise | 1 |

> **Pourquoi c'est lent ?** Une trentaine de secondes pour 4 lignes. MapReduce démarre une JVM par conteneur et écrit ses résultats intermédiaires **sur disque** entre map et reduce. C'est exactement ce que Spark va accélérer en gardant les données **en mémoire** (J1 après-midi).

---

## QCM (6 pts)

| Question | Réponse | Pourquoi |
|---|---|---|
| Combien de blocs HDFS pour `nombres.txt` avec une taille de bloc de 1 Mo ? | **4** | 3 388 895 ÷ 1 048 576 = 3,23, donc 3 blocs pleins et 1 bloc partiel. 12 est le nombre total de **copies** (4 × 3). |
| Après l'arrêt d'un DataNode, que signale `fsck` ? | **Des blocs sous-répliqués, mais un statut HEALTHY** | Chaque bloc a encore 2 copies sur 3 : aucune donnée perdue. |
| Quel est le mot le plus fréquent de `corpus.txt` ? | **les** | 5 occurrences, devant « et » (4) et « Spark » (3). |

---

## Les erreurs fréquentes

| Message | Cause | Solution |
|---|---|---|
| `no configuration file provided: not found` | La commande `docker compose` est lancée hors de `~/hadoop-cluster` | `cd ~/hadoop-cluster` |
| `Connection refused` juste après `up` | Le NameNode démarre encore | La boucle `until … -safemode wait` |
| `Name node is in safe mode` | Écriture tentée pendant le safe mode | Attendre : `hdfs dfsadmin -safemode wait` |
| `Configured Capacity: 0`, pas de `Live datanodes` | Les DataNodes ne sont pas connectés | `exit`, puis `docker compose ps` et `docker compose logs --tail 15 datanode` |
| `put: … File exists` | Le fichier est déjà dans HDFS | `hdfs dfs -rm /user/hadoop/eval/nombres.txt`, puis recommencer |
| Encore `Live datanodes (3)` après le `stop` | Le délai de 1 minute n'est pas écoulé | Attendre et relancer |
| `Error: No such container: hadoop-cluster-datanode-2` | Le dossier ne s'appelle pas `hadoop-cluster` | `docker compose ps` pour lire le vrai nom |
| `Output directory … already exists` | Le dossier `wordcount` existe déjà | `hdfs dfs -rm -r /user/hadoop/eval/wordcount`, puis relancer |
| `JAR does not exist or is not a normal file` | Chemin du `.jar` mal recopié, ou commande lancée sans `bash` (le `*` n'est pas remplacé) | Être dans `docker compose exec namenode bash` ; le chemin commence par `/opt/hadoop/share/` |
| `Incompatible clusterIDs` dans les logs d'un DataNode | Le NameNode a été reformaté | `docker compose down`, puis `docker compose up -d --scale datanode=3` |
| Le copier-coller ne marche pas dans le terminal | Sous Linux, `Ctrl+V` ne colle pas | `Ctrl+Shift+V` (et `Ctrl+Shift+C` pour copier) |

## Arrêter le cluster

```bash
exit                         # quitter le conteneur du NameNode
docker compose stop          # arrêter les 6 conteneurs, en gardant les fichiers HDFS
```

Pour le relancer : `docker compose up -d --scale datanode=3`. Pour tout supprimer, fichiers HDFS compris : `docker compose down`.
