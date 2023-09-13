
# PokerHistoryLoader

## Description
`PokerHistoryLoader` est un outil conçu pour télécharger des fichiers d'historique de poker à partir d'un répertoire local vers un bucket S3. Il offre des options pour un téléchargement quotidien ou plus fréquent.

## Fonctionnalités
- Téléchargement des fichiers d'historique de poker vers un bucket S3.
- Option pour télécharger tous les fichiers depuis le début de l'année en cours.
- Option pour télécharger uniquement les fichiers du jour.
- Vérifie si un fichier existe déjà dans le bucket pour éviter les doublons.

## Utilisation
1. Assurez-vous d'avoir les dépendances requises installées en utilisant :
```
pip install -r requirements.txt
```

2. Créez un fichier `.env` à la racine du projet avec les configurations nécessaires. Voir le template ci-dessous.

3. Pour télécharger tous les fichiers depuis le début de l'année, exécutez :
```
python daily_upload.py
```

4. Pour télécharger uniquement les fichiers du jour, exécutez :
```
python frequent_upload.py
```

## Configuration
- Les clés d'accès à S3 et d'autres configurations sont chargées à partir du fichier `.env`. Assurez-vous de le définir correctement avec le template suivant :
```
DO_REGION=VotreRégion
DO_ENDPOINT=VotreEndpoint
AWS_ACCESS_KEY_ID=VotreAccessKeyID
AWS_SECRET_ACCESS_KEY=VotreSecretAccessKey
```

- Le répertoire par défaut pour chercher les fichiers est basé sur le répertoire de l'utilisateur courant. Vous pouvez le changer si nécessaire dans le code.

## Planification avec Crontab
Si vous souhaitez exécuter les scripts en tant que tâches planifiées avec `crontab`, voici comment vous pouvez le faire :

Pour `daily_upload.py` :
```
0 0 * * * /chemin/vers/python /chemin/vers/le/projet/daily_upload.py
```

Pour `frequent_upload.py` :
```
0 * * * * /chemin/vers/python /chemin/vers/le/projet/frequent_upload.py
```

Assurez-vous de remplacer `/chemin/vers/python` par le chemin complet vers votre interpréteur Python et `/chemin/vers/le/projet/` par le chemin complet vers le dossier du projet.

## Contribution
Si vous souhaitez contribuer au projet, n'hésitez pas à faire des pull requests ou à ouvrir des issues.

