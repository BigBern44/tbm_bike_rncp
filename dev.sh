#!/usr/bin/env bash
# Démarrage des services de l'observatoire vélos TBM en une commande.
#
#   ./dev.sh            # démarre tout (MinIO + Dagster)
#   ./dev.sh stop       # arrête tout
#   ./dev.sh status     # état des services
#   ./dev.sh logs       # suit les logs des deux services
#
# Idempotent : relancer "start" ne double jamais un service déjà actif.

set -euo pipefail
cd "$(dirname "$0")"

MINIO_BIN="$HOME/.local/bin/minio"
MINIO_DATA="$HOME/minio-data"
RUN_DIR=".run"
LOG_DIR=".run/logs"
VENV=".venv"
# Instance Dagster persistante : sans DAGSTER_HOME, `dagster dev` crée un
# répertoire temporaire .tmp_dagster_home_* à chaque démarrage (jamais nettoyé,
# et historique des runs/sensors perdu). On le fixe à .dagster/ (déjà gitignoré).
DAGSTER_HOME="$PWD/.dagster"
export DAGSTER_HOME

mkdir -p "$RUN_DIR" "$LOG_DIR" "$DAGSTER_HOME"

# --- Environnement -----------------------------------------------------------

load_env() {
    if [[ ! -f .env ]]; then
        echo "ERREUR : .env absent. Copier .env.example et le remplir." >&2
        exit 1
    fi
    set -a
    # shellcheck disable=SC1091
    source .env
    set +a
}

ensure_venv() {
    if [[ ! -x "$VENV/bin/dagster" ]]; then
        echo "→ Environnement Python absent : installation (uv)…"
        uv venv --python 3.11 "$VENV"
        uv pip install --python "$VENV/bin/python" -e ".[dev]"
    fi
}

# --- Helpers process ---------------------------------------------------------

pid_alive() {
    local pidfile="$1"
    [[ -f "$pidfile" ]] && kill -0 "$(cat "$pidfile")" 2>/dev/null
}

stop_pid() {
    local pidfile="$1" nom="$2"
    if pid_alive "$pidfile"; then
        kill "$(cat "$pidfile")" 2>/dev/null || true
        echo "✓ $nom arrêté"
    else
        echo "- $nom déjà arrêté"
    fi
    rm -f "$pidfile"
}

# --- MinIO -------------------------------------------------------------------

minio_ok() {
    curl -sf -o /dev/null "http://${MINIO_ENDPOINT}/minio/health/live"
}

start_minio() {
    if minio_ok; then
        echo "- MinIO déjà actif (http://${MINIO_ENDPOINT})"
        return
    fi
    mkdir -p "$MINIO_DATA"
    MINIO_ROOT_USER="$MINIO_ACCESS_KEY" MINIO_ROOT_PASSWORD="$MINIO_SECRET_KEY" \
        nohup "$MINIO_BIN" server "$MINIO_DATA" --address ":9000" --console-address ":9001" \
        > "$LOG_DIR/minio.log" 2>&1 &
    echo $! > "$RUN_DIR/minio.pid"
    for _ in $(seq 1 20); do
        minio_ok && { echo "✓ MinIO démarré (API :9000, console :9001)"; return; }
        sleep 0.5
    done
    echo "ERREUR : MinIO ne répond pas — voir $LOG_DIR/minio.log" >&2
    exit 1
}

# --- Dagster -----------------------------------------------------------------

dagster_ok() {
    curl -sf -o /dev/null "http://localhost:3000/server_info"
}

start_dagster() {
    if dagster_ok; then
        echo "- Dagster déjà actif (http://localhost:3000)"
        return
    fi
    # Le venv doit être dans le PATH : dagster-dbt y cherche l'exécutable dbt.
    PATH="$PWD/$VENV/bin:$PATH" \
        nohup "$VENV/bin/dagster" dev -m orchestration.definitions --host 0.0.0.0 \
        > "$LOG_DIR/dagster.log" 2>&1 &
    echo $! > "$RUN_DIR/dagster.pid"
    for _ in $(seq 1 60); do
        dagster_ok && { echo "✓ Dagster démarré (http://localhost:3000)"; return; }
        sleep 1
    done
    echo "ERREUR : Dagster ne répond pas — voir $LOG_DIR/dagster.log" >&2
    exit 1
}

# --- Commandes ---------------------------------------------------------------

cmd_start() {
    load_env
    ensure_venv
    start_minio
    start_dagster
    echo
    echo "Tout est prêt :"
    echo "  Dagster UI    http://localhost:3000"
    echo "  Console MinIO http://localhost:9001"
}

cmd_stop() {
    # Dagster lance des processus fils (webserver, daemon) : on tue le groupe.
    if pid_alive "$RUN_DIR/dagster.pid"; then
        pkill -TERM -P "$(cat "$RUN_DIR/dagster.pid")" 2>/dev/null || true
    fi
    stop_pid "$RUN_DIR/dagster.pid" "Dagster"
    stop_pid "$RUN_DIR/minio.pid" "MinIO"
    pkill -f "minio server" 2>/dev/null || true
}

cmd_status() {
    load_env
    minio_ok   && echo "✓ MinIO   actif   http://${MINIO_ENDPOINT}" || echo "✗ MinIO   arrêté"
    dagster_ok && echo "✓ Dagster actif   http://localhost:3000"    || echo "✗ Dagster arrêté"
}

cmd_logs() {
    tail -f "$LOG_DIR"/minio.log "$LOG_DIR"/dagster.log
}

case "${1:-start}" in
    start)  cmd_start ;;
    stop)   cmd_stop ;;
    status) cmd_status ;;
    logs)   cmd_logs ;;
    *) echo "Usage : ./dev.sh [start|stop|status|logs]" >&2; exit 1 ;;
esac
