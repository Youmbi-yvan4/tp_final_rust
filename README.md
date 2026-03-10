Nom : Ngongang Youmbi Yvan

# TP Final -- MiniRedis

**Durée : 3h30** | Rust asynchrone | Serveur TCP/JSON

---

## Objectif

Implémenter un serveur **key-value store** asynchrone en Rust, capable de gérer
plusieurs clients TCP simultanés. Le serveur stocke des paires clé/valeur en
mémoire et répond à des commandes envoyées au format JSON.

Un **client de test** (binaire fourni) évaluera automatiquement votre serveur.

### Compétences évaluées

- Programmation asynchrone avec `tokio` (`async/await`, `spawn`, `interval`)
- Gestion d'un état partagé (`Arc<Mutex<_>>` ou `Arc<RwLock<_>>`)
- Sérialisation/désérialisation avec `serde`
- Gestion d'erreurs robuste
- Écriture de tests asynchrones

---

## Protocole

### Transport

- **TCP** sur le port **7878** (`127.0.0.1:7878`)
- Chaque message (requête ou réponse) est **un objet JSON sur une seule ligne**,
  terminé par `\n`
- Le serveur lit une requête, envoie une réponse, puis attend la requête suivante
- Chaque client est traité dans sa propre tâche Tokio (connexions concurrentes)

### Format des requêtes (client → serveur)

Chaque requête est un objet JSON avec un champ `"cmd"` obligatoire :

```json
{"cmd": "PING"}
{"cmd": "GET", "key": "ma_cle"}
{"cmd": "SET", "key": "ma_cle", "value": "ma_valeur"}
{"cmd": "DEL", "key": "ma_cle"}
{"cmd": "KEYS"}
{"cmd": "EXPIRE", "key": "ma_cle", "seconds": 30}
{"cmd": "TTL", "key": "ma_cle"}
{"cmd": "INCR", "key": "compteur"}
{"cmd": "DECR", "key": "compteur"}
{"cmd": "SAVE"}
```

### Format des réponses (serveur → client)

Chaque réponse contient un champ `"status"` (`"ok"` ou `"error"`) :

| Commande | Réponse succès | Notes |
|----------|---------------|-------|
| `PING` | `{"status": "ok"}` | |
| `SET` | `{"status": "ok"}` | |
| `GET` | `{"status": "ok", "value": "bar"}` | `"value": null` si la clé n'existe pas |
| `DEL` | `{"status": "ok", "count": 1}` | `0` si la clé n'existait pas |
| `KEYS` | `{"status": "ok", "keys": ["a", "b"]}` | Liste de toutes les clés (ordre quelconque) |
| `EXPIRE` | `{"status": "ok"}` | |
| `TTL` | `{"status": "ok", "ttl": 25}` | `-1` si pas d'expiration, `-2` si clé inexistante |
| `INCR` | `{"status": "ok", "value": 6}` | Valeur après incrémentation (entier) |
| `DECR` | `{"status": "ok", "value": 4}` | Valeur après décrémentation (entier) |
| `SAVE` | `{"status": "ok"}` | |
| Erreur | `{"status": "error", "message": "..."}` | Commande inconnue, JSON invalide, etc. |

---

## Palier 1 -- Commandes de base (10 pts)

Implémentez les commandes suivantes :

### PING

Retourne `{"status": "ok"}`. Sert de health-check.

### SET

Stocke une paire clé/valeur. Si la clé existe déjà, la valeur est écrasée.

### GET

Retourne la valeur associée à la clé, ou `null` si la clé n'existe pas.

### DEL

Supprime une clé. Retourne `"count": 1` si la clé existait, `"count": 0` sinon.

### Multi-client

Votre serveur **doit** gérer plusieurs connexions simultanées :
- Un `SET` effectué par le client A doit être visible par le client B via `GET`
- La déconnexion d'un client ne doit pas affecter les autres

### Gestion d'erreurs

- Si le JSON reçu est invalide → `{"status": "error", "message": "invalid json"}`
- Si la commande est inconnue → `{"status": "error", "message": "unknown command"}`
- La connexion doit **rester ouverte** après une erreur (pas de crash)

### Conseils pour le palier 1

```rust
// Structure suggérée pour l'état partagé :
type Store = Arc<Mutex<HashMap<String, String>>>;

// Accept loop classique :
loop {
    let (socket, addr) = listener.accept().await?;
    let store = store.clone();
    tokio::spawn(async move {
        handle_client(socket, store).await;
    });
}

// Lecture ligne par ligne avec BufReader :
use tokio::io::{AsyncBufReadExt, BufReader};
let reader = BufReader::new(read_half);
let mut line = String::new();
reader.read_line(&mut line).await?;
```

---

## Palier 2 -- KEYS, EXPIRE, TTL (6 pts)

### KEYS

Retourne la liste de **toutes** les clés présentes dans le store.
L'ordre n'a pas d'importance.

### EXPIRE

Associe un délai d'expiration (en secondes) à une clé existante.
Une fois le délai écoulé, la clé **doit** disparaître (un `GET` retourne `null`).

L'implémentation recommandée est une **tâche de fond** qui nettoie
périodiquement les clés expirées (ex : toutes les secondes avec `tokio::time::interval`).

**Note :** le test vérifie que la clé a aussi disparu de `KEYS` après expiration
(pas seulement de `GET`). Il faut donc implémenter `KEYS` pour obtenir tous les
points de ce palier.

### TTL

Retourne le temps restant avant expiration d'une clé (en secondes) :
- Clé avec expiration : un entier positif (secondes restantes)
- Clé sans expiration : `-1`
- Clé inexistante : `-2`

### Conseils pour le palier 2

```rust
use std::time::Instant;

// Vous pouvez stocker l'instant d'expiration avec la valeur :
struct Entry {
    value: String,
    expires_at: Option<Instant>,
}
```

---

## Palier 3 -- INCR, DECR, SAVE (4 pts)

### INCR / DECR (3 pts)

- `INCR` : incrémente la valeur associée à la clé (interprétée comme un entier).
  Si la clé n'existe pas, elle est créée avec la valeur `1` (resp. `-1` pour `DECR`).
- La réponse contient la nouvelle valeur : `{"status": "ok", "value": 42}`
- Si la valeur n'est pas un entier valide → `{"status": "error", "message": "not an integer"}`

### SAVE (1 pt)

- Sauvegarde l'état complet du store dans un fichier `dump.json` dans le
  répertoire courant du serveur
- Format du fichier : libre (un objet JSON `{"key": "value", ...}` suffit)
- La réponse est `{"status": "ok"}`

---

## Livrables

- Un crate Cargo (`mini_redis`) compilable avec `cargo build`
- `cargo fmt` et `cargo clippy` propres
- Au moins un test asynchrone (`#[tokio::test]`)
- Le serveur se lance avec `cargo run` et écoute sur `127.0.0.1:7878`

## Comment tester

### Test manuel avec netcat

```bash
# Dans un terminal :
cargo run

# Dans un autre terminal :
echo '{"cmd": "PING"}' | nc 127.0.0.1 7878
# → {"status":"ok"}

echo '{"cmd": "SET", "key": "hello", "value": "world"}' | nc 127.0.0.1 7878
# → {"status":"ok"}
```

### Test automatique

```bash
# Lancez votre serveur dans un terminal :
cd mini_redis && cargo run

# Dans un autre terminal, lancez le client de test :
./test_client_bin
# ou avec une adresse custom :
./test_client_bin 127.0.0.1:7878
```

Le client de test affiche le résultat de chaque test et un score final.

## Dépendances suggérées

Votre `Cargo.toml` est pré-rempli avec :

```toml
[dependencies]
tokio = { version = "1", features = ["full"] }
serde = { version = "1", features = ["derive"] }
serde_json = "1"
tracing = "0.1"
tracing-subscriber = { version = "0.3", features = ["env-filter"] }
```

Vous pouvez ajouter des dépendances supplémentaires si nécessaire
(`anyhow`, `thiserror`, etc.).

---

Bon courage !
