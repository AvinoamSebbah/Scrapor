# Synchronisation manuelle des images produit

Le champ `products.has_image` est la source de vérité DB :

- `TRUE` : une image existe dans DigitalOcean Spaces pour ce produit
- `FALSE` : l'absence a été prouvée après vérification
- `NULL` : état encore inconnu ou erreur technique pendant la vérification

## Commande locale

```bash
python sync_product_images.py
```

## Fichiers ajoutés

- `sync_product_images.py` : le workflow manuel
- `scripts/add_products_has_image.sql` : migration one-shot dédiée
- `.github/workflows/W7_sync_product_images.yml` : lancement manuel depuis GitHub Actions

Ordre du workflow :

1. préflight obligatoire `Pricez → Cloudinary bridge → Spaces`
2. préflight obligatoire `OpenFoodFacts → Cloudinary bridge → Spaces`
3. scan du préfixe `products/` dans DigitalOcean Spaces
4. marquage `TRUE` des produits déjà présents
5. pour les autres produits :
   - tentative Pricez
   - sinon tentative OpenFoodFacts
   - si une image est trouvée, import via Cloudinary bridge puis upload vers Spaces
   - si aucune source ne possède l'image, marquage `FALSE`

Le script ne stocke pas d'URL signée : le chemin durable reste `products/{barcode}.jpg`.

## Flags utiles

| Flag | Effet |
|---|---|
| `--reset-all-to-null` | remet toute la colonne `products.has_image` à `NULL` avant le run |
| `--skip-spaces-check` | saute le scan initial de Spaces ; les produits non prouvés restent `NULL`, jamais `FALSE` |
| `--recheck-all` | retraite tous les produits, pas seulement ceux actuellement à `NULL` |
| `--limit N` | limite le nombre de produits traités |
| `--dry-run` | exécute le flux sans écrire dans la DB |
| `--preflight-only` | teste uniquement le bridge et l'upload Space, puis s'arrête |

## Variables requises

- `POSTGRESQL_URL` ou `DATABASE_URL`
- `CLOUDINARY_CLOUD_NAME`
- `CLOUDINARY_API_KEY`
- `CLOUDINARY_API_SECRET`
- `DO_SPACES_ACCESS_KEY`
- `DO_SPACES_SECRET_KEY`
- `DO_SPACES_BUCKET`
- `DO_SPACES_REGION`

Optionnelle :

- `OPENFOODFACTS_USER_AGENT`

## Secrets GitHub à configurer pour `W7`

Le workflow référence uniquement des secrets GitHub, jamais des valeurs écrites dans le code :

- `POSTGRESQL_URL`
- `CLOUDINARY_CLOUD_NAME`
- `CLOUDINARY_API_KEY`
- `CLOUDINARY_API_SECRET`
- `DO_SPACES_ACCESS_KEY`
- `DO_SPACES_SECRET_KEY`
- `DO_SPACES_BUCKET`
- `DO_SPACES_REGION`
- les secrets SSH déjà utilisés par tes autres workflows (`DO_HOST`, `DO_USERNAME`, `DO_PORT`, `DO_SSH_KEY`)

## Migration initiale

Si tu veux appliquer uniquement ce changement de schéma, sans lancer tout `update_schema.py` :

```sql
\i scripts/add_products_has_image.sql
```

Cette migration remet volontairement tout le monde à `NULL` pour le premier rollout.

## Pourquoi `FALSE` n'est pas écrit à la légère

Le script écrit `FALSE` seulement si :

1. le scan Spaces a été réellement exécuté,
2. Pricez a explicitement répondu qu'il n'y avait pas d'image,
3. OpenFoodFacts n'a fourni aucune image sélectionnée.

Si le bridge, l'API, le réseau ou l'upload Space échoue, le produit reste `NULL`.
