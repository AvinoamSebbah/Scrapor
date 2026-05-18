-- Migration: Augmenter la longueur du code shopping_list de 5 → 20 chars
-- Raison sécurité: CHAR(5) avec 32^5 = 33M combinaisons est bruteforçable.
-- CHAR(20) donne 32^20 ≈ 10^30 combinaisons.
-- Run this script against the production database BEFORE deploying the backend change.

ALTER TABLE shopping_lists ALTER COLUMN code TYPE CHAR(20);

-- Mettre à jour l'index existant (il sera automatiquement mis à jour par l'ALTER)
-- Vérification
DO $$
BEGIN
  IF (
    SELECT character_maximum_length
    FROM information_schema.columns
    WHERE table_name = 'shopping_lists' AND column_name = 'code'
  ) = 20 THEN
    RAISE NOTICE 'Migration réussie: code est maintenant CHAR(20)';
  ELSE
    RAISE EXCEPTION 'Migration échouée: code nest pas CHAR(20)';
  END IF;
END;
$$;
