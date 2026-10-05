import csv
import json
import os

os.makedirs("data", exist_ok=True)

graine = 2027
def hasard(n):
    """Entier entre 0 et n-1, pseudo-aléatoire mais reproductible."""
    global graine
    graine = (graine * 1103515245 + 12345) % 2**31
    return (graine // 65536) % n

villes = ["Paris", "Lyon", "Lille", "Nantes", "Bordeaux"]
noms = ["Martin", "Bernard", "Dubois", "Thomas", "Robert", "Petit", "Durand", "Leroy", "Moreau", "Simon"]

# 1. clients.csv : 200 clients, séparateur « ; », dates au format français
with open("data/clients.csv", "w", newline="", encoding="utf-8") as f:
    w = csv.writer(f, delimiter=";")
    w.writerow(["id_client", "nom", "ville", "date_inscription", "segment"])
    for i in range(1, 201):
        date = f"{1 + hasard(28):02d}/{1 + hasard(12):02d}/{2023 + hasard(3)}"
        segment = "pro" if hasard(4) == 0 else "particulier"
        w.writerow([f"C{i:03d}", noms[hasard(10)], villes[hasard(5)], date, segment])

# 2. commandes.json : 1 000 commandes, un objet JSON par ligne, avec un objet imbriqué
statuts = ["livree"] * 8 + ["annulee", "en_cours"]
modes = ["domicile", "relais", "retrait"]
with open("data/commandes.json", "w", encoding="utf-8") as f:
    for i in range(1, 1001):
        commande = {
            "id_commande": i,
            "id_client": f"C{1 + hasard(200):03d}",
            "date": f"2026-{1 + hasard(6):02d}-{1 + hasard(28):02d}",
            "montant": (500 + hasard(29500)) / 100,
            "statut": statuts[hasard(10)],
            "livraison": {"mode": modes[hasard(3)], "delai_jours": 1 + hasard(6)},
        }
        f.write(json.dumps(commande) + "\n")

# 3. avis.txt : 300 avis, champs séparés par « | », sans en-tête
commentaires = ["Très bon produit", "Livraison rapide", "Emballage abîmé",
                "Conforme à la description", "Service client réactif", "Délai trop long"]
with open("data/avis.txt", "w", encoding="utf-8") as f:
    for i in range(300):
        c = hasard(6)
        note = [5, 4, 2, 4, 5, 1][c] if hasard(3) else 1 + hasard(5)
        f.write(f"2026-{1 + hasard(6):02d}-{1 + hasard(28):02d}|C{1 + hasard(200):03d}|{note}|{commentaires[c]}\n")

print("Fichiers prêts : data/clients.csv (200), data/commandes.json (1000), data/avis.txt (300)")
