# Plan — Voie de requête typée : tri par nom + scellage non-oubliable

## Objectif

Rendre sûre et ergonomique la construction **en code** d'une
`ListDescriptorQuery` (voie typée, hors parsing HTTP), pour les lectures
internes dérivées dont la requête est connue statiquement. Cible motivante :
une route de scroll infini (« les 24 prochaines images d'une galerie après un
id ») où `gallery_id` vient du path (trusted), `limit`/tri sont fixes, et seul
le curseur varie par requête.

État actuel : la voie typée fonctionne mais expose deux pièges silencieux que
chaque consommateur recopie.

## Constat (vérifié dans le code)

- `ListDescriptorQuery<Descriptor>` (`list/spec/ListDescriptorQuery.h:21`) porte
  `group_key` et `cache_key` comme `std::string` nus, à remplir par l'appelant.
- `query()` (`repository/ListMixin.h:292`) lit `q.cache_key` aveuglément
  (`getByKey(query.cache_key)`, `:774`). Aucune garde : `cache_key == ""` →
  lookup sur clé vide → **toutes les requêtes non scellées collisionnent sur la
  même entrée de cache**, sans erreur.
- Le tri se règle via `SortSpec<size_t>{index, dir}` (`ListDescriptorQuery.h:18`,
  alias `DescriptorSortSpec = list::SortSpec<size_t>`). L'index dépend de l'ordre
  du tuple `Descriptor::sorts` → réordonner les `Sort<>` fait silencieusement
  trier sur la mauvaise colonne (l'index reste valide).
- En face, l'accès par nom **existe déjà** pour les filtres :
  `Filters::get<FixedString Name>()` (`spec/GeneratedFilters.h:95`) via
  `find_filter_index` (`:55`), vérifié compile-time avec `static_assert`.
- La machinerie compile-time pour le tri existe (`sort_at<D,Is>::name`,
  `ListDescriptor.h:76`) mais n'est exposée qu'en **runtime** :
  `parseSortField<Descriptor>(string_view) → optional<size_t>`
  (`spec/GeneratedTraits.h:265`).
- La grille de limites (`normalizeLimit` `GeneratedTraits.h:331`, `isLimitAllowed`)
  vit **uniquement** dans les parsers HTTP (`HttpQueryParser.h:163` tolérant,
  `:291` strict). La voie typée ne l'applique pas : `q.limit = 24` est accepté
  tel quel et encodé dans `cache_key`.
- `sealQuery<Repo>(q)` mentionné dans la proposition d'issue **n'existe pas** —
  c'est du pseudo-code pour les deux lignes `q.group_key = groupKey<Desc>(...);
  q.cache_key = cacheKey<Desc>(...)` (cf. `HttpQueryParser.h:327-328`,
  `tests/fixtures/TestQueryHelpers.h:31-32`).

## Décisions de design

### 1. `sortBy<"field", Dir>()` consteval — tri par nom (priorité haute, risque nul)

Miroir exact de `find_filter_index`. Helper `consteval` nom → `SortSpec<size_t>`,
résolu et vérifié à la compilation sur `sort_at<Descriptor, Is>::name`.

- Forme : `sortBy<"created_at", SortDirection::Desc>()`, plus sucres
  `sortAsc<"...">()` / `sortDesc<"...">()`.
- `static_assert("sort field not found")` si le nom n'existe pas dans le tuple
  `sorts`, comme `find_filter_index` pour les filtres.
- Retourne `list::SortSpec<size_t>` (l'index résolu + la direction), affecté à
  `q.sort`. Aucune dépendance runtime, aucune chaîne.
- Supprime l'index magique et transforme tout réordonnancement des `Sort<>` en
  erreur de compilation au lieu d'un tri silencieusement faux.

### 2. Type-state : scellage prouvé à la compilation (priorité haute — correctness)

Contexte : lib non shippée, un seul consommateur (produit interne). Aucune
contrainte de compat ascendante → on adopte l'invariant le plus fort plutôt que
le moins intrusif. Le scellage devient **impossible à oublier par construction**,
vérifié par le système de types, pas par la discipline ni par une garde runtime.

**Découpe en deux types :**

- `ListQueryParams<Descriptor>` — forme **mutable** : `filters`, `sort`, `limit`,
  `cursor`, `offset`. **Ne porte plus `group_key`/`cache_key`.** C'est ce qu'on
  remplit (à la main ou via le builder, décision 5).
- `ListQuery<Descriptor>` — forme **scellée, immutable** : composée des params +
  `group_key` + `cache_key`, **produite uniquement** par `seal()` /
  `Builder::build()`. C'est le **seul** type que `query()` accepte.

```cpp
static io::Immediate<ListResult> query(const ListQuery<Descriptor>& q);
// ListQueryParams n'a pas de conversion vers ListQuery autre que seal() →
// query(params) ne compile pas. Le trou est fermé par le type, pas au runtime.
```

`seal()` calcule les deux clés dans le constructeur de `ListQuery` à partir des
params **finaux**, puis le type est immutable. Conséquences :

- **`cache_key`/`group_key` ne peuvent jamais être obsolètes** : aucune mutation
  possible après scellage. Le bug « clé qui ne reflète pas la requête » devient
  inexprimable.
- **Coût chemin chaud = zéro.** Plus de branche `empty()`, plus de lazy-copy, plus
  de `const&` qu'on ne peut pas muter. Les clés sont là, toujours, calculées
  exactement une fois. Strictement meilleur que le lazy-seal sur le hot path.
- **`query()` non scellée ne compile pas** — erreur à la compilation, pas
  résultat faux silencieux.
- `parseListQueryStrict` / `parseListQuery` retournent désormais
  `ListQuery<Desc>` (resp. `expected<ListQuery<Desc>, Err>`) : ils calculent déjà
  les clés en fin de parcours (`HttpQueryParser.h:327-328`) → ils produisent
  naturellement la forme scellée. Migration mécanique du type de retour.

**Refonte requise (assumée) :**

- `ListDescriptorQuery` actuel → scindé en `ListQueryParams` + `ListQuery`. Le
  nom `Repo::ListQuery` (`ListMixin.h:278`) reste pour le type scellé (ce qu'on
  passe à `query()`) ; ajouter `Repo::ListQueryParams` pour la forme mutable.
- `cachedListQuery`, helpers Redis/L2 (`ListMixin.h:774`, `:782`, `:860`) prennent
  `const ListQuery&` — inchangés, les clés y sont garanties présentes.
- Tous les sites de test qui font `q.cache_key = …; q.group_key = …` migrent vers
  `auto q = seal(params)` ou le builder. `TestQueryHelpers.h` retourne du scellé.

**Alternative écartée — lazy-seal runtime.** Calculer les clés dans `query()` si
`cache_key` est vide (copie sur branche froide). Ne casse aucune API, mais laisse
l'invariant au runtime (une branche, une copie sur le cas fautif) et garde un
type qui peut porter des clés incohérentes. Justifié seulement sous contrainte de
compat — qui n'existe pas ici. On prend le type-state.

### 3. Pas de désactivation de la grille de limites

La grille n'est appliquée que sur le chemin HTTP (entrée non fiable). La voie
typée accepte déjà une limite arbitraire exacte par construction — c'est le
mécanisme « sans grille », gated par la frontière de confiance.

- **Aucun flag « disable grid ».** Ce serait retirer un invariant de sécurité
  sur entrée non fiable (cardinalité `cache_key` non bornée → pollution de cache
  + DoS), pas une optimisation. Contraire au critère lib généraliste : on
  intègre ce qui gagne sans régresser le cas sûr.
- Personnalisation HTTP existante = `allowedLimits` par descripteur
  (`HasAllowedLimits`, `GeneratedTraits.h:321`). Grille `{24}` ou `{12,24,48}` se
  déclare, ne se débranche pas.
- **Contrat à documenter** : sur la voie typée, le caller possède la cardinalité
  des `cache_key`. `.limit(n)` avec `n` dérivé d'entrée utilisateur réintroduit
  le risque sans garde-fou — interdit par contrat, pas empêché par code.

### 4. Curseur = champ du builder, scellé par `build()`

`cache_key` agrège `group_key + limit + cursor + offset`
(`ListQueryParams`). En scroll infini, chaque page est un curseur distinct → une
`cache_key` distincte, donc le scellage doit voir le curseur final.

Avec le type-state (décision 2), **la réserve de design disparaît** : le curseur
est un setter du builder/params comme les autres, et `build()`/`seal()` est le
seul point qui calcule les clés. Impossible de sceller « trop tôt » — il n'y a
qu'un seul moment de scellage, terminal par construction.

- `.after(Cursor)` / `.afterId(id)` posent le curseur ; `.build()` scelle.
- Pas de scellage positionnel, pas de clé recalculée : le type mutable n'a pas de
  clés du tout.

### 5. Builder fluide = chemin de construction principal (le point de scellage)

Avec le type-state, le builder n'est plus du sucre optionnel : c'est la voie
naturelle pour atteindre `seal()`/`build()` et obtenir un `ListQuery`. Il porte
l'invariant ergonomiquement.

```cpp
auto q = Repo::list()
    .filter<"gallery_id">(gid)     // par nom, vérifié compile-time
    .sortDesc<"created_at">()      // décision 1
    .limit(24)                     // limite exacte, voie trusted (décision 3)
    .after(cursor)                 // curseur de page (décision 4)
    .build();                      // → ListQuery scellé, seul type accepté par query()
co_await Repo::query(q);
```

- `.build()` est le seul producteur de `ListQuery` côté builder ; `seal(params)`
  reste disponible pour la construction sans fluent (remplir un
  `ListQueryParams` puis sceller).
- Le builder accumule un `ListQueryParams` ; `build()` le scelle. Pas de type
  intermédiaire exposé au-delà de ces deux.
- `.filter<"name">(v)` délègue à `params.filters.get<"name">()` — gère IN/NIN
  (`optional<vector>`) et scalaire via le type de slot exact (point #1).

### 6. Curseur : token opaque minté serveur, typé par descripteur

Le découpage compile-time / runtime suit la **frontière de confiance**, pas une
règle « tout en type ». Le curseur est une valeur runtime qui a fait l'aller-retour
par le client → sa validation est runtime, par design, exactement comme la grille
de limites (décision 3). Son contenu (position keyset : valeur du champ de tri +
tie-break PK) n'est pas un type — il ne peut donc pas faire l'objet d'un contrat
compile-time.

- **`.after(Cursor<Descriptor>)` opaque, minté serveur.** Le serveur émet
  `next_cursor` en fin de page (`CachedListResult::next_cursor`,
  `list/ListQuery.h:177`) ; le client le réémet tel quel. Cohérence curseur/tri
  garantie par **provenance**, pas par type. Décodage = validation runtime
  (`Cursor::decode`, `list/ListQuery.h:63`) — token tronqué/forgé rejeté.
- **Seule sécurité compile-time atteignable et utile : typer le curseur par
  descripteur** (`Cursor<Descriptor>`). Attrape à la compilation le passage d'un
  curseur d'un repo à `query()` d'un autre (confusion inter-descripteur = vraie
  distinction de type). La cohérence intra-descripteur (curseur vs tri courant)
  reste runtime — le tri varie par requête, le token vient du fil.
- **Pas de `.afterId(id)` générique.** Piège sémantique : « après l'id 42 » n'a de
  sens keyset que si le tri EST le PK ; sinon il faut la valeur de tri de la ligne
  42 (lookup) → on retombe sur le curseur. Le cas tri==PK est déjà couvert par
  `.after()` (le curseur encode alors l'id). Le sucre `.afterId` correct exigerait
  un builder type-state portant le champ de tri en NTTP + `requires` que ce champ
  soit le PK — idiomatique mais gold-plating, **déféré**.

## Points d'attention

1. **`.filter<"name">(v)` et les ops IN/NIN.** Le slot d'un filtre IN est
   `optional<vector<element>>`, un scalaire est `optional<element>`. Le setter
   par nom doit affecter le type exact retourné par `get<Name>()` — trivial si on
   délègue à `get<Name>()`, à ne pas réimplémenter.
2. **Le tri reste mono-clé.** `sort` est `optional<SortSpec<size_t>>` singulier ;
   le tie-break PK est injecté plus bas (niveau keyset). `sortBy<>` et un
   éventuel builder restent mono-tri — ne pas élargir au multi-clé sans besoin
   avéré.
3. **`allowedLimits` hors périmètre de la voie typée.** `validateLimit` /
   `isLimitAllowed` restent cantonnés au parsing HTTP. Les ajouter à la voie
   typée casserait précisément ce qui la rend adaptée aux lectures internes à
   limite connue (24 ∉ grille publique `{10,25,50,100}`).
4. **Accès par nom non exercé aujourd'hui.** Les tests indexent tout par position
   (`get<0>()`, `get<1>()`), filtres compris, alors que `get<"name">()` existe.
   Migrer au moins les fixtures (`TestQueryHelpers.h`) vers l'accès par nom en
   même temps que `sortBy<>`, pour que la voie nominale soit la voie naturelle.
5. **Curseur tranché — voir décision 6.** `.after(Cursor<Descriptor>)` opaque
   minté serveur. Pas de `.afterId()` générique.
6. **Point ouvert — nommage de la fabrique.** `Repo::list()` (fabrique) vs
   `Repo::query(q)` (exécution) ne collisionnent pas sémantiquement, mais valider
   la lisibilité du couple avant de figer l'API publique.

## Tests unitaires

Fichiers cibles existants : `tests/relais/test_decl_list_cache.cpp` (logique de
clé, L1), `test_decl_list_redis.cpp` (L2/invalidation), `test_decl_list_full.cpp`
(bout-en-bout), `test_list_limits.cpp` (limites), `tests/fixtures/TestQueryHelpers.h`
(constructeurs partagés).

### `sortBy<>` / `sortAsc` / `sortDesc` (unit, pas d'I/O)

1. **Résolution nom → index.** `sortBy<"view_count", Desc>().field ==
   parseSortField<Desc>("view_count").value()`. Lie le helper consteval au
   résolveur runtime existant pour qu'ils ne puissent pas diverger.
2. **Direction.** `sortAsc<"f">().direction == Asc`,
   `sortDesc<"f">().direction == Desc`, et `sortBy<"f", Dir>()` honore `Dir`.
3. **Équivalence de clé (régression critique).** Une `ListDescriptorQuery` réglée
   via `sortBy<"f">` produit `group_key` **et** `cache_key` identiques à la même
   requête réglée via `SortSpec<size_t>{index, dir}` brut. `group_key` pilote
   l'invalidation Redis : toute dérive corromprait l'invalidation, pas seulement
   le tri. C'est le test qui prouve que le sucre est fidèle.
4. **Nom inconnu (compile-time).** `sortBy<"inexistant">()` doit `static_assert`.
   Non testable au runtime en Catch2 ; couvrir le versant négatif via
   `parseSortField<Desc>("inexistant") == nullopt` (même table de noms), et
   documenter le `static_assert` comme garantie compile-time (test de
   non-compilation optionnel, hors CTest).

### Type-state du scellage (garanties compile-time + unit)

5. **`query(params)` ne compile pas.** Garantie compile-time : passer un
   `ListQueryParams` (non scellé) à `query()` est une erreur de compilation. Non
   testable au runtime ; couvrir via un trait `requires`
   (`static_assert(!requires(P p) { Repo::query(p); })`) ou un test de
   non-compilation documenté hors CTest. C'est ce test qui matérialise la
   fermeture du bug `cache_key == ""` — la collision n'est plus exprimable.
6. **`seal()` produit la bonne clé (unit).** Le `cache_key`/`group_key` du
   `ListQuery` issu de `seal(params)` égale `cacheKey<Desc>()`/`groupKey<Desc>()`
   calculés à part sur les mêmes params. Prouve que le constructeur scelleur est
   fidèle.
7. **Builder ≡ seal manuel ≡ params bruts (unit).** `Repo::list().filter<...>()
   .sortDesc<...>().limit(n).build()` produit un `ListQuery` égal (`operator==`)
   à `seal(params)` rempli à la main avec les mêmes valeurs. Garde le builder et
   la voie `seal()` alignés.
8. **Immutabilité (compile-time).** `ListQuery` n'expose aucun setter de clé ;
   `group_key`/`cache_key` ne sont pas assignables hors `seal()`. Vérifié par
   l'absence d'API mutable (revue) + `static_assert` sur l'absence de membres
   mutables si pertinent.

### Curseur / scroll infini (décisions 4 + 6)

9. **Le curseur change la clé.** Mêmes filtres/tri/limite, deux curseurs
   différents → `cache_key` différents (identité de page). Unit.
10. **Round-trip opaque keyset.** Le `next_cursor` émis par la page N, réémis via
    `.after()`, fait reprendre la page N+1 exactement après la fin de N : pas de
    chevauchement, pas de trou. Reproduit la route galerie (filtre `gallery_id`
    fixe, tri fixe, `limit` 24, curseur variable). Integration.
10b. **Token invalide rejeté (runtime).** Curseur tronqué/forgé → `decode` échoue
    proprement (rejet, pas d'UB ni de page incohérente). Frontière de confiance.
10c. **Curseur inter-descripteur ne compile pas (compile-time).** Passer un
    `Cursor<DescA>` à un builder/`query()` de `DescB` est une erreur de
    compilation. Couvert via `requires` ou non-compilation documentée.

### Accès par nom aux filtres (unit)

11. **`get<"name">()` aliase `get<index>()`.** Sanity de la migration des
    fixtures : écrire via le nom et lire via l'index (et inversement) touche le
    même slot. Couvre IN/NIN (`optional<vector>`) **et** scalaire
    (`optional<element>`) — types de slot différents (point d'attention #1).

### Limites (régression de la décision 3)

12. **Voie typée ignore la grille.** `q.limit = 24` (≠ grille publique
    `{10,25,50,100}`) est conservé tel quel et encodé dans `cache_key` ; aucune
    normalisation. Confirme que le contournement de `parseListQueryStrict` reste
    valide. Étendre `test_list_limits.cpp`.

## Découpage recommandé

Lib non shippée, un seul consommateur : on fait la refonte cassante en une fois,
maintenant, plutôt que d'empiler des compromis de compat.

1. **`sortBy<>` / `sortAsc` / `sortDesc` consteval** (décision 1) — autonome, ~20
   lignes, ferme l'index magique. Peut atterrir en premier, indépendamment du
   reste.
2. **Split de types `ListQueryParams` / `ListQuery` + `seal()`** (décision 2) —
   fondateur et cassant : retire `group_key`/`cache_key` du type mutable, fait de
   `ListQuery` scellé le seul argument de `query()`/`queryJson()`/`queryBinary()`,
   migre les retours de `parseListQuery*`. Tout le reste en dépend.
3. **Builder fluide `Repo::list()…build()`** (décisions 4 + 5) — chemin de
   construction principal, point de scellage. Intègre `sortBy<>` (1) et le curseur.
4. **Migration des fixtures et tests** vers `seal()`/builder et l'accès par nom
   (point #4) — supprime les `q.cache_key = …` manuels, bascule
   `TestQueryHelpers.h` sur la voie scellée.

## Hors périmètre

- Validation `allowedLimits` sur la voie typée (#3).
- Flag de désactivation de la grille de limites (décision 3).
- Tri multi-clés (#2).
