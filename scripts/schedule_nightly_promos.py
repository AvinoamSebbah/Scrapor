#!/usr/bin/env python3
"""
schedule_nightly_promos.py
──────────────────────────
Scheduler autonome qui déclenche nightly_promos_refresh.py chaque nuit à 2h00.

Usage (Docker ou serveur) :
    python scripts/schedule_nightly_promos.py

Variables d'environnement :
    NIGHTLY_PROMOS_HOUR    (défaut: 2)    → heure de déclenchement (0-23)
    NIGHTLY_PROMOS_MINUTE  (défaut: 0)    → minute de déclenchement (0-59)
    POSTGRESQL_URL / DATABASE_URL         → connexion DB (passée au script fils)

Le script tourne en boucle infinie et log chaque exécution.
En cas d'erreur, il attend 5 minutes avant de réessayer.
"""

import os
import subprocess
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

# ─── Configuration ────────────────────────────────────────────────────────────

TRIGGER_HOUR   = int(os.getenv("NIGHTLY_PROMOS_HOUR",   "2"))
TRIGGER_MINUTE = int(os.getenv("NIGHTLY_PROMOS_MINUTE", "0"))

# Chemin du script à exécuter (relatif à ce fichier)
SCRIPT_DIR  = Path(__file__).resolve().parent.parent
REFRESH_SCRIPT = SCRIPT_DIR / "nightly_promos_refresh.py"

RETRY_DELAY_SECS = 5 * 60   # 5 min si échec
POLL_INTERVAL    = 30        # vérification toutes les 30 secondes


# ─── Logging ──────────────────────────────────────────────────────────────────


def log(msg: str):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


# ─── Calcul de la prochaine exécution ────────────────────────────────────────


def next_run_at() -> datetime:
    """Retourne le prochain datetime cible (toujours dans le futur)."""
    now = datetime.now()
    candidate = now.replace(hour=TRIGGER_HOUR, minute=TRIGGER_MINUTE, second=0, microsecond=0)
    if candidate <= now:
        candidate += timedelta(days=1)
    return candidate


# ─── Exécution du refresh ─────────────────────────────────────────────────────


def run_refresh() -> bool:
    """Lance nightly_promos_refresh.py et retourne True si succès."""
    log(f"🚀  Démarrage de {REFRESH_SCRIPT.name} …")
    try:
        env = os.environ.copy()
        result = subprocess.run(
            [sys.executable, str(REFRESH_SCRIPT)],
            env=env,
            capture_output=False,   # stdout/stderr visible dans les logs
            timeout=3600,           # 1 heure max
        )
        if result.returncode == 0:
            log("✅  Refresh terminé avec succès.")
            return True
        else:
            log(f"❌  Le script a terminé avec code {result.returncode}.")
            return False
    except subprocess.TimeoutExpired:
        log("⏱️   Timeout dépassé (1h). Refresh interrompu.")
        return False
    except Exception as e:
        log(f"❌  Erreur inattendue : {e}")
        return False


# ─── Boucle principale ────────────────────────────────────────────────────────


def main():
    if not REFRESH_SCRIPT.exists():
        log(f"❌  Script introuvable : {REFRESH_SCRIPT}")
        sys.exit(1)

    log(f"⏱️   Scheduler démarré. Déclenchement chaque nuit à {TRIGGER_HOUR:02d}:{TRIGGER_MINUTE:02d}.")
    log(f"   Script cible : {REFRESH_SCRIPT}")

    last_run_date = None

    while True:
        target = next_run_at()
        now = datetime.now()
        today = now.date()

        # On vérifie si l'heure de déclenchement est atteinte ET qu'on n'a pas déjà tourné aujourd'hui
        if now >= target - timedelta(seconds=POLL_INTERVAL) and last_run_date != today:
            log(f"🕑  Heure de déclenchement atteinte ({target.strftime('%H:%M')}).")
            success = run_refresh()
            if success:
                last_run_date = today
            else:
                log(f"⚠️   Réessai dans {RETRY_DELAY_SECS // 60} minutes.")
                time.sleep(RETRY_DELAY_SECS)
            continue

        # Attente jusqu'au prochain sondage
        wait_secs = min(POLL_INTERVAL, max(1, int((target - now).total_seconds())))
        if wait_secs > 60:
            log(f"   Prochaine exécution : {target.strftime('%Y-%m-%d %H:%M')} "
                f"(dans {int((target - now).total_seconds() / 60)} min).")
            time.sleep(60)
        else:
            time.sleep(wait_secs)


if __name__ == "__main__":
    main()
