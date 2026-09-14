# Recherche magasins — Osher Ad et Rami Levy

Date de vérification : 14 septembre 2026.

## Résultat exploitable

La base Agali contient 24 lignes Osher Ad, 99 lignes Rami Levy sous la chaîne principale et 34 lignes « Rami Levy dans le quartier ». Les fichiers de prix regroupent aussi des sous-marques et des doublons physiques sous la chaîne principale ; le nombre de lignes Agali ne doit donc pas être présenté comme le nombre officiel de succursales.

Après rapprochement conservateur des sources :

| Groupe | Lignes DB | GPS vérifié | Téléphone | Horaires | Site officiel |
|---|---:|---:|---:|---:|---:|
| Osher Ad | 24 | 15 | 24 | 24 | 24 |
| Rami Levy principal et sous-marques | 99 | 53 | 58 | 88 | 90 |
| Rami Levy dans le quartier | 34 | 22 | 0 | 29 | 30 |

Avant cette passe, Osher Ad n'avait que 14 téléphones, 13 horaires et 14 sites ; Rami Levy dans le quartier n'avait aucun horaire, téléphone ou site. Aucune adresse ni ville n'a été modifiée pendant l'enrichissement.

## Ce que publient réellement les enseignes

Osher Ad publie une page officielle de succursales avec adresse, horaires et téléphone. La page avertit explicitement que les horaires peuvent évoluer et recommande de vérifier auprès du magasin.[^1] La page officielle du réseau indique par ailleurs que le 21e magasin a ouvert en 2024 ; la page de succursales indexée aujourd'hui en expose 24, ce qui confirme qu'un simple chiffre marketing ancien n'est pas une source de catalogue suffisante.[^2]

Rami Levy expose une page officielle et l'API JSON publique utilisée par cette page. La réponse observée contient 97 fiches : 97 ont un texte d'horaires, 62 un téléphone, et aucune ne publie de latitude/longitude. La page officielle confirme directement les horaires et téléphones de nombreuses succursales.[^3] Sa page d'accessibilité apporte une seconde preuve officielle pour les adresses et téléphones de la chaîne principale.[^4]

Le document 2025 de l'Association des industriels recense 59 succursales Rami Levy principales et 22 « dans le quartier », puis des sous-marques séparées. Il explique pourquoi les fichiers réglementaires et la DB Agali contiennent davantage de lignes et confirme plusieurs correspondances d'adresse utilisées pour les doublons physiques.[^5]

## Méthode de rapprochement mise en place

- Osher Ad : identifiant stable du magasin dans le fichier de prix + table officielle auditée des 24 succursales. L'adresse DB reste intacte, même lorsque l'adresse officielle indique un déménagement ou un numéro différent.
- Rami Levy principal : adresse stricte avec numéro et nom compatible, ou alias audité entre l'identifiant du fichier de prix et le libellé officiel actuel.
- Rami Levy dans le quartier : correspondance explicite de 30 identifiants du fichier de prix vers 30 libellés exacts de l'API officielle. Aucun rapprochement « même ville donc même magasin ».
- Doublons API : acceptés uniquement si tous les champs publiables (téléphone, horaires, URL et éventuel point) sont identiques.
- Horaires invalides : rejetés. L'API publie actuellement `22:300` pour Ben Yehuda 23 ; cette fiche reçoit le site officiel, mais pas cet horaire corrompu.
- GPS : l'API Rami Levy ne fournit aucun point. Elle ne peut donc jamais transformer seule une ligne en `verified`. La carte reste cachée tant qu'un POI exact n'est pas prouvé par une source cartographique distincte.

## Trous restants et raison

Quinze lignes Rami Levy ne possèdent pas encore de fiche officielle non ambiguë :

- chaîne principale : Karmi Gat, entrepôt Internet, nouveau Big Beer-Sheva, Admiralty avec conflit de numéro (`73` en DB contre `3` sur la fiche officielle), Netanya Diamond, Modiin New et cinq magasins Beit HaPeri ;
- chaîne « dans le quartier » : Allenby, Herzfeld, Petite Suisse Beer-Sheva et Petite Suisse Herzliya.

Les téléphones des magasins « dans le quartier » sont vides dans l'API et sur la page officielle. Un numéro trouvé sur Wolt ou un annuaire ne doit pas être transformé automatiquement en numéro officiel. Par exemple, Wolt expose un numéro pour Kfar Saba, mais ce type de donnée sert seulement de piste de révision manuelle, pas de preuve primaire.[^6]

Pour les coordonnées Osher Ad, Waze publie des POI de marque avec adresse, téléphone et horaires — par exemple Netanya, Tom Lantos 60.[^7] C'est une bonne seconde source, mais pas une justification pour avaler en masse toutes les coordonnées communautaires. Une coordonnée ne doit devenir `verified` que si le nom de chaîne, l'adresse complète et le numéro concordent, et si les éventuels points concurrents ne se contredisent pas.

Six points Osher Ad supplémentaires ont passé cette règle : Kiryat Bialik, Kanot, Hadera, Ashkelon Bat Hadar, Netanya et Haifa. Les neuf autres lignes déjà vérifiées ont été conservées. Les neuf magasins encore sans point certifié restent masqués sur la carte ; notamment Migdal HaEmek, Beitar Illit et Kiryat Yam présentent un numéro d'adresse différent entre le fichier de prix et la publication actuelle, ce qui interdit une certification automatique.

## Mise à jour gratuite et durable

Le workflow hebdomadaire du dépôt public interroge les catalogues officiels sans clé payante. Il met à jour les horaires Rami Levy depuis la source vivante, conserve le snapshot Osher Ad daté sans lui fabriquer une fraîcheur, puis tente OpenStreetMap et Overture pour les seuls GPS manquants. Une panne d'un fournisseur est isolée : elle ne bloque plus les autres enseignes et n'efface jamais les données existantes.

Cette méthode est gratuite côté API mais ne garantit pas qu'une enseigne publie ses changements de jours fériés à temps. L'interface doit donc toujours afficher la date de collecte et une mention « horaires susceptibles de changer » pour les données anciennes. La propre page Osher Ad formule cette réserve.[^1]

## Informations intéressantes à afficher ensuite

- services vérifiés : livraison, parking, accessibilité, retrait ; uniquement lorsque la source les renseigne réellement ;
- dernière vérification des horaires et badge « information ancienne » ;
- accessibilité détaillée Rami Levy (parking PMR, entrée, comptoir, sanitaires, boucle audio), disponible sur la page officielle dédiée ;[^4]
- promotions filtrées par `chain_id`, `store_id` et ville, déjà reliables aux fichiers de prix ;
- distance depuis l'utilisateur calculée localement avec les coordonnées vérifiées ;
- signalement communautaire « ces horaires sont faux », conservé comme alerte et jamais comme mise à jour automatique.

## Sources

[^1]: Osher Ad, « Archive des succursales » : https://osherad.co.il/branches/
[^2]: Osher Ad, site officiel : https://osherad.co.il/
[^3]: Rami Levy, « Succursales » : https://www.rami-levy.co.il/he/stores et API publique utilisée par la page : https://www.rami-levy.co.il/api/stores
[^4]: Rami Levy, « Accessibilité dans les succursales » : https://www.rami-levy.co.il/he/stores-accessibility
[^5]: Association des industriels d'Israël, liste 2025 des succursales Rami Levy : https://industry.org.il/files/north/passover2025/Passover2025rami2.pdf
[^6]: Wolt, Rami Levy in the Neighborhood Kfar Saba : https://wolt.com/he/isr/hasharon/venue/rami-levy-in-the-neighborhood-kfar-saba/
[^7]: Waze, Osher Ad Netanya : https://www.waze.com/he/live-map/directions/il/%D7%9E%D7%97%D7%95%D7%96-%D7%94%D7%9E%D7%A8%D7%9B%D7%96/%D7%A0%D7%AA%D7%A0%D7%99%D7%94/%D7%90%D7%95%D7%A9%D7%A8-%D7%A2%D7%93?to=place.ChIJiVV3FzM_HRURs4KW5K2kJQQ
