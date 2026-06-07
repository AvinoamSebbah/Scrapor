# refresh_top_promos.py

## Variables d'environnement

| Variable | Valeur par défaut | Description |
|---|---|---|
| `POSTGRESQL_URL` / `DATABASE_URL` / `SUPABASE_DATABASE_URL` | - | Obligatoire. URL PostgreSQL |

## Flags CLI

| Flag | Description |
|---|---|
| `--window-hours N` | Compatibilité CLI : toute valeur est forcée à `0` car le cache est all-time |
| `--top-n N` | Nombre de promotions finales souhaitées par groupe de cache |
| `--skip-audit` | Saute l'audit pré-refresh |
| `--skip-image-check` | Compatibilité : ignoré |
| `--include-no-image` | Compatibilité : ignoré |
| `--images-only` | Compatibilité : ignoré |

## Comportement actuel

```bash
python refresh_top_promos.py --top-n 300 --skip-audit
```

Le script appelle `refresh_top_promotions_cache(0, top_n)`.
La fonction SQL utilise `products.has_image` comme source de vérité et filtre directement avec `p.has_image IS TRUE`.

Il n'y a plus :

- de fetch image via l'API backend
- d'appel Pricez/OpenFoodFacts
- de sur-génération `top_n x CANDIDATE_FACTOR`
- d'update post-refresh de `top_promotions_cache.has_image`

Le lock `55555` est pris dans le script manuel et aussi dans la fonction SQL, pour sérialiser les refreshs lancés depuis W3/W5 ou depuis un run manuel.
