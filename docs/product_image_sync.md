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
- `.github/workflows/W7_sync_product_images.yml` : lancement manuel ou automatique une fois par jour à 03:00 `Asia/Jerusalem` depuis GitHub Actions

Le workflow `W7` utilise un groupe `concurrency` dédié avec `cancel-in-progress: false` :
si un nouveau déclenchement arrive alors que le précédent n'est pas terminé, GitHub Actions
le garde en attente au lieu de lancer deux synchronisations en parallèle.

Comme GitHub Actions planifie les crons en UTC, `W7` déclare les créneaux `00:00 UTC`
et `01:00 UTC`, puis une garde exécute seulement celui qui correspond à 03:00
en heure locale `Asia/Jerusalem` (été ou hiver).

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
| `--recheck-non-ean-false` | répare les anciens `FALSE` non-EAN écrits avant que Pricez soit tenté pour eux |
| `--limit N` | limite le nombre de produits traités |
| `--dry-run` | exécute le flux sans écrire dans la DB |
| `--preflight-only` | teste uniquement le bridge et l'upload Space, puis s'arrête |
| `--progress-every N` | affiche un résumé compact toutes les `N` lignes (défaut : `100`) |

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
3. OpenFoodFacts n'a fourni aucune image sélectionnée **ou** a répondu `429`.

Si Pricez échoue techniquement, le produit reste `NULL`.
Les autres erreurs OpenFoodFacts, ainsi que les erreurs de bridge/réseau/upload, gardent aussi le produit à `NULL`.

## Logs utiles pendant un run

Le script écrit maintenant explicitement :

- `✅ ... importé depuis pricez → has_image=TRUE`
- `✅ ... importé depuis openfoodfacts → has_image=TRUE`
- `❌ ... has_image=FALSE (...)`
- `? ... has_image=NULL (...)`
- `↪️ ... OpenFoodFacts ignoré (code non-EAN), Pricez a bien été tenté`

Et toutes les `100` lignes par défaut, il affiche un résumé :

```text
📊 progression 1000/265979 | pricez=... | openfoodfacts=... | false=... | null=... | off_incompatible=... | errors=...
```

Pricez est tenté pour tous les `item_code` non vides.
OpenFoodFacts est tenté seulement pour les codes compatibles EAN (`8` à `14` chiffres), car son API est indexée par barcode.

Si un ancien run a déjà écrit des `FALSE` sur des codes non-EAN avant cette correction,
utiliser `--recheck-non-ean-false` permet de les retraiter sans remettre toute la colonne à `NULL`.
