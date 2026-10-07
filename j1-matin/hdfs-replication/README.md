# La réplication HDFS sur 5 DataNodes : une panne, et la réparation automatique

> Formation **PySpark – Traitement des données** · Jour 1 matin · Pour aller plus loin
> [Retour au sommaire du dépôt](../../README.md) · Le cluster : [`hadoop-cluster`](../hadoop-cluster/) · La correction : [`correction-evaluation`](../correction-evaluation/)

Pendant l'évaluation, notre mini-cluster n'avait que **3 DataNodes** : après une panne, HDFS ne pouvait **pas** se réparer. Cette page montre ce qui se passe sur un cluster un peu plus grand, **5 DataNodes**, où la réparation est automatique. Les placements de blocs sont un **exemple** : sur un vrai cluster, c'est le NameNode qui les choisit.

**Le cas étudié :** 1 fichier découpé en **3 blocs**, `dfs.replication=3`, sur un cluster de **5 DataNodes**.

Chaque bloc existe donc en **3 copies**, sur **3 DataNodes différents**. Avec 5 machines, **chaque DataNode ne possède qu'une partie des blocs**, contrairement à notre mini-cluster à 3 DataNodes où chacun avait tout.

---

## 1. Au départ : tout va bien

Le NameNode a choisi 3 DataNodes pour chaque bloc, en répartissant la charge.

```
              DN1     DN2     DN3     DN4     DN5      copies
bloc 0         ■       ■       ■       ·       ·        3/3 ✓
bloc 1         ·       ■       ·       ■       ■        3/3 ✓
bloc 2         ■       ·       ■       ·       ■        3/3 ✓
              ───     ───     ───     ───     ───
blocs gardés   2       2       2       1       2       (9 copies au total = 3 blocs × 3)
```

`■` = le DataNode possède une copie du bloc. `·` = il n'en a pas.

```bash
hdfs fsck / | grep -i "under-replicated"
#  Under-replicated blocks:   0 (0.0 %)
```

---

## 2. Panne : DN3 tombe

```bash
docker stop hadoop-cluster-datanode-3
# environ 1 minute plus tard, le NameNode le déclare mort (plus de battement de cœur)
```

```
              DN1     DN2     DN3 ✗   DN4     DN5      copies
bloc 0         ■       ■       ✗       ·       ·        2/3 ⚠
bloc 1         ·       ■       ·       ■       ■        3/3 ✓   ← DN3 n'en avait pas : rien ne change
bloc 2         ■       ·       ✗       ·       ■        2/3 ⚠
```

```bash
hdfs dfsadmin -report | grep -E "Live datanodes|Dead datanodes"
# Live datanodes (4):
# Dead datanodes (1):

hdfs fsck / | grep -i "under-replicated"
#  Under-replicated blocks:   2 (66.7 %)
```

### Pourquoi 2, et pas 3 ?

- **`Under-replicated blocks` compte les blocs qui ont perdu au moins une copie**, chacun **une seule fois**.
- DN3 avait une copie des **blocs 0 et 2**, mais **pas du bloc 1**. Seuls 2 blocs sur 3 sont touchés, soit 66,7 %.
- C'est la différence avec notre mini-cluster à 3 DataNodes, où chaque machine avait **tout** : là-bas, la panne touchait 100 % des blocs.

**Aucune donnée perdue :** chaque bloc a encore au moins 2 copies, et on lit le fichier normalement.

---

## 3. La réparation automatique

Sur ce cluster, le NameNode **peut** réparer : il reste des DataNodes vivants qui n'ont **pas encore** de copie des blocs touchés.

| Bloc | Copies restantes | DataNodes possibles pour la 3ᵉ copie | Choix du NameNode |
|---|---|---|---|
| bloc 0 | DN1, DN2 | DN4 ou DN5 (pas DN1 ni DN2, qui l'ont déjà) | **DN4**, copié depuis DN1 |
| bloc 2 | DN1, DN5 | DN2 ou DN4 | **DN2**, copié depuis DN5 |

Le NameNode **ne transporte aucune donnée** lui-même. Il **ordonne** à un DataNode qui a le bloc de l'envoyer à un autre, de DataNode à DataNode.

```
              DN1     DN2     DN3 ✗   DN4     DN5      copies
bloc 0         ■       ■       ✗       ■ ←new  ·        3/3 ✓
bloc 1         ·       ■       ·       ■       ■        3/3 ✓
bloc 2         ■       ■ ←new  ✗       ·       ■        3/3 ✓
              ───     ───             ───     ───
blocs gardés   2       3               2       2       (9 copies, de nouveau 3 × 3)
```

```bash
hdfs fsck / | grep -i "under-replicated"
#  Under-replicated blocks:   0 (0.0 %)
```

**Le cluster s'est réparé tout seul**, en quelques secondes ou quelques minutes selon la taille des blocs, sans intervention humaine.

---

## 4. Bonus : DN3 revient

```bash
docker start hadoop-cluster-datanode-3
```

DN3 envoie son rapport de blocs : « j'ai les blocs 0 et 2 ». Mais ils ont déjà été recopiés ailleurs.

```
              DN1     DN2     DN3     DN4     DN5      copies
bloc 0         ■       ■       ■       ■       ·        4/3  ↑ trop
bloc 1         ·       ■       ·       ■       ■        3/3 ✓
bloc 2         ■       ■       ■       ·       ■        4/3  ↑ trop
```

```bash
hdfs fsck / | grep -i "over-replicated"
#  Over-replicated blocks:   2 (66.7 %)
```

Le NameNode **supprime alors une copie en trop** de chaque bloc, pour revenir à exactement 3. Il choisit en général sur le DataNode le plus rempli. On revient à `Over-replicated blocks: 0`.

---

## Combien de temps avant que le NameNode remarque la panne ?

Le NameNode ne « voit » pas les machines tomber. Il **écoute** : chaque DataNode lui envoie un **battement de cœur** (*heartbeat*) **toutes les 3 secondes**. Quand un DataNode se tait, le NameNode attend un délai avant de le déclarer mort :

```
délai avant « mort » = 2 × dfs.namenode.heartbeat.recheck-interval  +  10 × dfs.heartbeat.interval
```

| Réglage | Rôle | Défaut | Notre fichier `config` |
|---|---|---|---|
| `dfs.heartbeat.interval` | Fréquence des battements de cœur envoyés par chaque DataNode | 3 s | 3 s (inchangé) |
| `dfs.namenode.heartbeat.recheck-interval` | Fréquence à laquelle le NameNode vérifie qui s'est tu | 300 000 ms (5 min) | **15 000 ms (15 s)** |
| **Délai avant « mort »** | | **2 × 5 min + 10 × 3 s = 10 min 30 s** | **2 × 15 s + 10 × 3 s = 1 min** |

La ligne de notre [`config`](../hadoop-cluster/config) qui raccourcit ce délai :

```ini
HDFS-SITE.XML_dfs.namenode.heartbeat.recheck-interval=15000
```

### La panne de DN3, seconde par seconde (avec notre réglage)

```
 t = 0 s      docker stop …datanode-3          DN3 se tait
              ♥ ♥ ♥ ♥ ♥ … ✗                    (les 4 autres DataNodes battent toujours, toutes les 3 s)

 t = 3 à 59 s DN3 est encore « vivant » pour le NameNode
              → dfsadmin -report : Live datanodes (5)
              → interface web, onglet Datanodes : « In service », colonne Last contact = 3 s, 10 s, 40 s…

 t ≈ 60 s     délai dépassé : DN3 est déclaré MORT
              → dfsadmin -report : Live datanodes (4), Dead datanodes (1)
              → onglet Datanodes : DN3 passe en « Dead »
              → les blocs 0 et 2 passent en « under-replicated »

 t ≈ 60 s +   le NameNode ordonne les recopies (il vérifie sa file de réparation toutes les 3 s)
 quelques s   → DN1 envoie le bloc 0 à DN4, DN5 envoie le bloc 2 à DN2
              → Under-replicated blocks : 0
```

**Avec les réglages par défaut**, la même scène dure **10 min 30 s** avant la déclaration de mort.

### Pourquoi attendre si longtemps en production ?

Un serveur peut se taire quelques instants **sans être en panne** : un redémarrage rapide, un réseau saturé, une pause de la JVM pendant le ramasse-miettes. Le déclarer mort trop tôt déclencherait des **recopies massives et inutiles**, qui satureraient le réseau du cluster. 10 min 30 s est un compromis : assez long pour ignorer les incidents passagers, assez court pour réparer avant une deuxième panne.

**On a raccourci ce délai pour voir la panne pendant la formation. On ne le ferait jamais en production.**

### Voir le DataNode mort

| Où | Ce qu'on voit |
|---|---|
| `http://localhost:9870`, onglet **Overview** | `Live Nodes` et `Dead Nodes` avec leur nombre |
| `http://localhost:9870`, onglet **Datanodes** | Le DataNode tombé passe de « In service » à **Dead** : on voit **lequel** |
| `hdfs dfsadmin -report -dead` | La liste des seuls DataNodes morts, avec leur nom et leur dernier contact |

---

## Comparaison avec notre mini-cluster de l'évaluation

| | 3 DataNodes (notre démo) | 5 DataNodes (ce schéma) |
|---|---|---|
| Copies par DataNode | **Tous** les blocs | Une **partie** des blocs |
| Blocs touchés quand un DataNode tombe | 100 % | Seulement ceux qu'il avait (ici 66,7 %) |
| Réparation automatique | **Impossible** : aucun DataNode libre pour la 3ᵉ copie | **Oui** : le NameNode recopie vers un DataNode qui n'en a pas |
| Retour du DataNode | Le compteur repasse à 0, rien n'est recopié | Des copies en trop, que le NameNode supprime |

## La règle à retenir

> Avec une réplication de **3**, il faut **au moins 4 DataNodes** pour que HDFS puisse se réparer seul après une panne. Plus le cluster est grand, plus une panne touche une **petite part** des blocs, et plus la réparation est **rapide**, car beaucoup de machines recopient en parallèle.

## Questions possibles des stagiaires

- **« Combien de pannes peut-on supporter ? »** Avec 3 copies, on peut perdre **2 DataNodes en même temps** sans perdre de donnée, à condition que la réparation ait le temps de se faire entre deux pannes. La perte de 3 DataNodes qui ont les 3 copies d'un même bloc donne `Missing blocks`.
- **« Qui choisit où vont les copies ? »** Le NameNode. Sur un vrai cluster, il applique aussi la règle des racks : 2 copies sur un rack, 1 sur un autre.
- **« Pourquoi ne pas mettre 10 copies ? »** Parce que chaque copie coûte du disque : 1 Go stocké en 3 copies occupe 3 Go. Le chiffre de 3 est le compromis classique.
