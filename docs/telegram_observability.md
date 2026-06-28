# Agali Telegram Observability Setup

## 1. Creer les 3 bots Telegram

1. Ouvre Telegram et cherche `@BotFather`.
2. Envoie `/newbot`.
3. Cree ces 3 bots:
   - `Agali Scrapor` -> Bot 1
   - `Agali Health` -> Bot 2
   - `Agali Users` -> Bot 3
4. Pour chaque bot, BotFather donne un token du type `123456:ABC...`.
5. Garde ces tokens pour GitHub Secrets:
   - Bot 1 -> `TELEGRAM_BOT1_TOKEN`
   - Bot 2 -> `TELEGRAM_BOT2_TOKEN`
   - Bot 3 -> `TELEGRAM_BOT3_TOKEN`

## 2. Creer les chats Telegram

Option recommandee: 3 groupes Telegram separes.

1. Cree un groupe `Agali Scrapor`.
2. Ajoute le Bot 1 dans ce groupe.
3. Envoie un message dans le groupe, par exemple `test`.
4. Ouvre dans ton navigateur:
   `https://api.telegram.org/bot<TELEGRAM_BOT1_TOKEN>/getUpdates`
5. Cherche `chat":{"id":...}`.
6. Copie cet ID dans `TELEGRAM_BOT1_CHAT_ID`.
7. Repete pour Bot 2 et Bot 3:
   - Bot 2 -> `TELEGRAM_BOT2_CHAT_ID`
   - Bot 3 -> `TELEGRAM_BOT3_CHAT_ID`

Si `getUpdates` ne montre rien, retire/remets le bot dans le groupe puis renvoie un message.

## 3. Secrets GitHub pour le repo Scrapor

Va dans GitHub:
`AvinoamSebbah/Scrapor` -> `Settings` -> `Secrets and variables` -> `Actions` -> `New repository secret`.

### Telegram

| Secret | Ou le prendre |
| --- | --- |
| `TELEGRAM_BOT1_TOKEN` | BotFather, token du bot `Agali Scrapor` |
| `TELEGRAM_BOT1_CHAT_ID` | `getUpdates` du bot 1 |
| `TELEGRAM_BOT2_TOKEN` | BotFather, token du bot `Agali Health` |
| `TELEGRAM_BOT2_CHAT_ID` | `getUpdates` du bot 2 |
| `TELEGRAM_BOT3_TOKEN` | BotFather, token du bot `Agali Users` |
| `TELEGRAM_BOT3_CHAT_ID` | `getUpdates` du bot 3 |

### Database et API

| Secret | Ou le prendre |
| --- | --- |
| `POSTGRESQL_URL` | La meme URL DB deja utilisee par les workflows Scrapor |
| `API_SECRET_KEY` | La cle API backend, meme valeur que dans le `.env` de production backend |

### GitHub monitoring

| Secret | Ou le prendre |
| --- | --- |
| `GH_MONITOR_TOKEN` | GitHub -> Settings -> Developer settings -> Personal access tokens |

Token GitHub recommande:
- Fine-grained token.
- Repository access: `Scrapor`, `Servor`, `Agali`.
- Permissions: Actions read-only, Metadata read-only.

### DigitalOcean backend server

| Secret | Ou le prendre |
| --- | --- |
| `DO_HOST` | IP serveur backend, actuellement `46.101.239.41` |
| `DO_USERNAME` | Utilisateur SSH, probablement `root` |
| `DO_PORT` | Port SSH, souvent `22` |
| `DO_SSH_KEY` | Cle privee SSH deja utilisee par le deploy backend |

### Kamatera scraper server

| Secret | Ou le prendre |
| --- | --- |
| `KAMATERA_HOST` | IP Kamatera: `185.229.226.79` |
| `KAMATERA_USER` | Utilisateur SSH, probablement `root` |
| `KAMATERA_SSH_KEY` | Nouvelle cle privee SSH pour Kamatera |

Important: change le mot de passe Kamatera qui a ete partage dans le chat. Ensuite utilise une cle SSH.

### PostHog

| Secret | Ou le prendre |
| --- | --- |
| `POSTHOG_PERSONAL_API_KEY` | PostHog -> Account settings -> Personal API keys |
| `POSTHOG_PROJECT_ID` | PostHog -> Project settings, ID du projet ou environment |
| `POSTHOG_HOST` | `https://eu.posthog.com` ou `https://us.posthog.com` |

Cote frontend, garde aussi:
- `VITE_POSTHOG_KEY`
- `VITE_POSTHOG_HOST`

## 4. Secrets GitHub pour le repo Servor

Va dans:
`AvinoamSebbah/Servor` -> `Settings` -> `Secrets and variables` -> `Actions`.

Ajoute pour l'alerte deploy backend:

| Secret | Ou le prendre |
| --- | --- |
| `TELEGRAM_BOT2_TOKEN` | BotFather, token Bot 2 |
| `TELEGRAM_BOT2_CHAT_ID` | Chat ID du groupe Health |

Dans `DO_ENV_FILE`, ajoute aussi les variables backend si tu veux capturer PostHog cote serveur:

```env
POSTHOG_KEY=phc_xxx
POSTHOG_HOST=https://eu.i.posthog.com
```

`POSTHOG_KEY` est la Project API Key PostHog, la meme que `VITE_POSTHOG_KEY`.

## 5. Installer Kamatera

Sur ta machine locale, cree une cle SSH si besoin:

```bash
ssh-keygen -t ed25519 -f ~/.ssh/agali_kamatera -C "agali-kamatera"
```

Copie la cle publique sur Kamatera:

```bash
ssh root@185.229.226.79
mkdir -p ~/.ssh
nano ~/.ssh/authorized_keys
chmod 700 ~/.ssh
chmod 600 ~/.ssh/authorized_keys
```

Puis sur Kamatera:

```bash
sudo mkdir -p /opt/agali-scrapor
sudo git clone https://github.com/AvinoamSebbah/Scrapor.git /opt/agali-scrapor
cd /opt/agali-scrapor
python3 -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
cp scripts/kamatera.env.example kamatera.env
nano kamatera.env
chmod +x scripts/run_kamatera_scrape.sh
```

Cron exemple:

```cron
15 */3 * * * cd /opt/agali-scrapor && . .venv/bin/activate && ./scripts/run_kamatera_scrape.sh /opt/agali-scrapor/kamatera.env
```

Le wrapper ecrit:
`/opt/agali-scrapor/run_summary.json`

Bot 2 lit ce fichier par SSH pour detecter un scraper bloque ou en erreur.

## 6. Tests

### Test 1: preflight sans envoyer Telegram

Dans GitHub `Scrapor`:

1. Va dans `Actions`.
2. Lance `W12 · Observability Preflight`.
3. Mets:
   - `dry_run=true`
   - `send_test_messages=false`
4. Le workflow doit lister les secrets manquants et les checks OK/KO.

### Test 2: envoyer les messages de test

Quand le preflight est vert:

1. Relance `W12 · Observability Preflight`.
2. Mets:
   - `dry_run=false`
   - `send_test_messages=true`
3. Tu dois recevoir:
   - un message dans Bot 1;
   - un message dans Bot 2;
   - un message dans Bot 3;
   - un rapport preflight dans Bot 2.

### Test 3: rapports reels

Lance manuellement:
- `W9 · Daily Scrapor Telegram Report`
- `W10 · Agali Health Monitor`
- `W11 · Agali Usage Telegram Report`

Si un secret manque, le script doit l'indiquer sans exposer sa valeur.

## 7. Regle de securite

Ne colle plus de mot de passe serveur dans le chat ou dans GitHub Actions.
Utilise uniquement:
- GitHub Secrets;
- cle SSH;
- fichier `.env` prive sur le serveur.
