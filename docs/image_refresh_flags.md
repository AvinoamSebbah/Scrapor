# refresh_top_promos.py — Flags & variables d'environnement

## Variables d'environnement

| Variable | Valeur par défaut | Description |
|---|---|---|
| `DATABASE_URL` | — | **Obligatoire.** URL PostgreSQL |
| `BACKEND_API_URL` | `https://api.agali.live` | URL de l'API backend pour la vérification image. Mettre `http://localhost:3000` en dev. |

## Flags CLI

| Flag | Description |
|---|---|
| `--window-hours N` | Fenêtre temporelle en heures (défaut : 168 = 7 jours) |
| `--top-n N` | Nombre de promos finales souhaitées par scope/ville/chaîne (défaut : 200) |
| `--skip-audit` | Saute l'audit pré-refresh (plus rapide, recommandé en prod nocturne) |
| `--skip-image-check` | Saute totalement la vérification image (dev/debug rapide) |
| `--include-no-image` | **Voir ci-dessous** |

## Comportement par défaut (production)

```
python refresh_top_promos.py --window-hours 168 --top-n 200 --skip-audit
```

1. SQL génère `top_n × CANDIDATE_FACTOR` (= 800) candidats
2. Appels batch à `/api/products/images/batch` (50 items/lot) → le backend vérifie dans cet ordre :
   - Cloudinary (cache existant → gratuit)
   - Pricez → si trouvé, upload dans Cloudinary
   - OpenFoodFacts → si trouvé, upload dans Cloudinary
3. `has_image` mis à jour dans la DB
4. **Lignes sans image supprimées**
5. Re-rank des lignes restantes → top 200 avec images

Le lendemain matin, la page promos affiche 100 % de produits avec images.

## Flag `--include-no-image`

```
python refresh_top_promos.py --window-hours 24 --top-n 200 --include-no-image
```

Active ce flag si tu veux **garder les promos sans image** dans le cache au lieu de les supprimer.
Elles seront classées **après** les promos avec image (`has_image DESC NULLS LAST, smart_score DESC`).

Cas d'usage :
- Tester que la page gère bien les placeholders
- Garder une grande remise même si l'image est introuvable

## Constantes modifiables dans le script

Situées en haut de `refresh_top_promos.py` :

| Constante | Valeur | Description |
|---|---|---|
| `CANDIDATE_FACTOR` | `4` | Multiplicateur de sur-génération SQL (ex: top_n=200 → 800 candidats SQL) |
| `IMAGE_BATCH_SIZE` | `50` | Taille des lots envoyés à l'API backend (limite backend = 50) |
| `IMAGE_REQUEST_TIMEOUT` | `60` | Timeout HTTP par lot (secondes) |

## Exemple de run nocturne complet (les 2 fenêtres)

```bash
export DATABASE_URL="postgresql://..."
export BACKEND_API_URL="https://api.agali.live"

python refresh_top_promos.py --window-hours 24  --top-n 200 --skip-audit
python refresh_top_promos.py --window-hours 168 --top-n 200 --skip-audit
```
