4. Output vers différents formats

Actuellement tu charges vers SQLite
Comment exporter aussi vers JSON ? CSV formaté ? Excel ?
Méthode .save_as(format) ?

5. Pipeline réutilisable / Configurable

Créer des "recettes" de pipeline qu'on peut sauvegarder
Configuration via fichier TOML ou YAML
Builder pattern pour construire des pipelines complexes

6. Déduplication

Supprimer les doublons basés sur certains champs
.deduplicate_by(|user| &user.username)
Comment gérer ça efficacement en parallèle ?

7. Enrichissement de données

Ajouter des données depuis une autre source
Ex: lookup dans une autre table/fichier
.enrich_with(other_source, join_key)

8. CLI interactif

Utiliser clap pour des arguments en ligne de commande
pipeline-etl --input users.csv --filter 'username.starts_with("j")' --output db.sqlite

9. Métriques de performance

Mesurer le temps d'exécution de chaque étape
Combien de lignes/seconde ?
Utiliser std::time::Instant

10. Testing

Tests unitaires pour chaque transformation
Mock data pour tester le pipeline
Property-based testing avec proptest