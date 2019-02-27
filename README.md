# BDR
Base de données répartie

Le fichier readme.pdf résume nos explications.
Le fichier pagerank.scala est le fichier de code du pagerank, qu'il suffit d'importer dans un projet intelliJ pour le faire fonctionner.
Le fichier dataframe.scala est le fichier qui utilise les résultats du crawler pour donner la liste de sorts réduites, à ouvrir dans intelliJ.

Le crawler se lance depuis la racine du projet en utilisant la commande 'scrapy crawl spell -o spell.json'
le fichier gérant tout le crawling est './BDR/spiders/spell.py'
