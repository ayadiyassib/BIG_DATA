# Devoir 1 : Analyse d'un Réseau Bancaire avec Spark GraphX

**Université Abdelmalek Essaâdi** — Faculté des Sciences et Techniques de Tanger  
**Master IAID :** Intelligence Artificielle et Ingénierie des Données (Semestre 1)  
**Module :** Big Data Frameworks  
**Auteur :** Yassine Ayadi  

---

## 1. Description du Projet

Ce projet consiste à analyser un réseau de transactions financières internationales à l'aide de l'API **Apache Spark GraphX** (langage Scala).

### Le réseau modélisé comprend :
* **13 comptes bancaires (villes)** répartis sur 4 pays :
  * **Maroc (6) :** Atlas, Rif, Souss + un réseau local isolé (Tanger, Oujda, Fès)
  * **France (3) :** Lyon, Brest, Nice
  * **Suisse (2) :** Zug, Bâle
  * **Panama (2) :** Colon, Balboa
* **21 virements bancaires orientés** avec leurs montants en k€.

---

## 2. Fichiers du Projet

Vous pouvez accéder directement aux fichiers du dépôt en cliquant sur ces liens :

* 📄 [**`Devoir1GraphX.scala`**](./Devoir1GraphX.scala) : Contient tout le code source Scala Spark GraphX commenté.
* 📄 [**`resultats.txt`**](./resultats.txt) : Trace complète et brute de l'exécution dans le terminal Spark.

---

## 3. Réponses aux Questions & Résultats

### Question 1 : Construction du Graphe
* **Méthode :** Création du graphe typé `Graph[(String, String), Double]` à partir d'un RDD de sommets `(ID, (Nom, Pays))` et d'un RDD d'arêtes `Edge(Src, Dst, Montant)`.
* **Résultat obtenu :**
  * Nombre de sommets : **13**
  * Nombre d'arêtes : **21**

---

### Question 2 : Villes les plus connectées
* **Méthode :** Calcul du degré total (arêtes entrantes + sortantes) avec `bankGraph.degrees`.
* **Résultat :**
  1. **Lyon : 5 connexions**
  2. **Zug : 5 connexions**
  3. **Bâle : 4 connexions**
  4. **Brest : 4 connexions**
  5. **Nice : 4 connexions**
  6. Atlas, Rif, Souss, Balboa : 3 connexions chacun
  7. Colon, Tanger, Oujda, Fès : 2 connexions chacun

> **Conclusion :** Lyon et Zug sont les deux plaques tournantes (hubs) principales du réseau.

---

### Question 3 : Solde net d'argent par ville
Le solde net correspond à la différence : `Solde = Montant Reçu − Montant Envoyé`.

| Ville | Pays | Reçu (k€) | Envoyé (k€) | Solde Net (k€) | Rôle dans le réseau |
|:---|:---|:---:|:---:|:---:|:---|
| **Bâle** | Suisse | 120.0 | 50.0 | **+70.0** | Bénéficiaire net |
| **Balboa** | Panama | 140.0 | 70.0 | **+70.0** | Bénéficiaire net (Offshore) |
| **Zug** | Suisse | 175.0 | 110.0 | **+65.0** | Bénéficiaire net |
| **Lyon** | France | 74.0 | 72.0 | **+2.0** | Compte relais équilibré |
| **Oujda** | Maroc | 8.0 | 6.0 | **+2.0** | Réseau local |
| **Fès** | Maroc | 6.0 | 5.0 | **+1.0** | Réseau local |
| **Rif** | Maroc | 25.0 | 25.0 | **0.0** | Parfaitement équilibré |
| **Tanger** | Maroc | 5.0 | 8.0 | **-3.0** | Réseau local |
| **Colon** | Panama | 80.0 | 90.0 | **-10.0** | Transit offshore |
| **Souss** | Maroc | 20.0 | 45.0 | **-25.0** | Émetteur net |
| **Nice** | France | 42.0 | 73.0 | **-31.0** | Émetteur net |
| **Atlas** | Maroc | 0.0 | 70.0 | **-70.0** | Source principale de fonds |
| **Brest** | France | 18.0 | 89.0 | **-71.0** | Émetteur majeur vers la Suisse |

*(Remarque : La somme totale des soldes nets est égale à 0 k€, ce qui valide la conservation exacte des montants).*

---

### Question 4 : Cycles du réseau (L'argent tourne en boucle)
Trois circuits fermés sont identifiés dans le réseau :

1. **Cycle Marocain (local isolé) :**
   * Tanger (11) → Oujda (12) [8 k€] → Fès (13) [6 k€] → Tanger (11) [5 k€]
2. **Cycle Français (interne) :**
   * Lyon (4) → Nice (6) [12 k€] → Brest (5) [18 k€] → Lyon (4) [9 k€]
3. **Boucles Suisse – Panama (Cluster de 4 comptes) :**
   * Zug (7) → Colon (9) [80 k€] → Balboa (10) [90 k€] → Zug (7) [70 k€]
   * Zug (7) → Bâle (8) [30 k€] → Balboa (10) [50 k€] → Zug (7) [70 k€]

---

### Question 5 : Flux financiers entre pays
Flux agrégés calculés en filtrant les transactions transfrontalières :

| Flux Inter-Pays | Montant Total (k€) |
|:---|:---:|
| **France → Suisse** | **195** |
| **Suisse → Panama** | **130** |
| **Maroc → France** | **95** |
| **Panama → Suisse** | **70** |

---

### Question 6 : Virements internes par pays
Montants totaux des transactions réalisées au sein d'un même pays :

* **Maroc :** **64 k€**
* **France :** **39 k€**
* **Suisse :** **30 k€**
* **Panama :** **90 k€**

---

## 4. Comment Exécuter le Projet

Le projet s'exécute avec Apache Spark sur le conteneur Docker :

```bash
# 1. Entrer dans le conteneur Spark Master
docker exec -it spark-master bash

# 2. Lancer la console interactive Spark
spark-shell

# 3. Passer en mode collage
scala> :paste

# 4. Coller le contenu de Devoir1GraphX.scala puis valider avec Ctrl + D