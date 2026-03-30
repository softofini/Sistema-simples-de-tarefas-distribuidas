#!/bin/bash
# ============================================================
# Script de Inicialização do Sistema Distribuído
# Plataforma Distribuída de Processamento Colaborativo de Tarefas
# ============================================================

echo "============================================================"
echo " Plataforma Distribuída de Processamento Colaborativo"
echo " Sistema de Orquestração de Tarefas"
echo "============================================================"

# Cores para output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

BASE_DIR="$(cd "$(dirname "$0")" && pwd)"
LOGS_DIR="$BASE_DIR/logs"

# Criar diretório de logs
mkdir -p "$LOGS_DIR"

# Função para limpar processos ao sair
cleanup() {
    echo -e "\n${YELLOW}[INFO] Encerrando todos os processos...${NC}"
    kill $(jobs -p) 2>/dev/null
    echo -e "${GREEN}[OK] Todos os processos encerrados.${NC}"
    exit 0
}

trap cleanup SIGINT SIGTERM

echo ""
echo -e "${GREEN}[1/5] Iniciando Orquestrador Principal...${NC}"
python3 "$BASE_DIR/orchestrator/orchestrator.py" \
    --host 0.0.0.0 --client-port 5000 --worker-port 5001 &
sleep 2

echo -e "${GREEN}[2/5] Iniciando Orquestrador Backup...${NC}"
python3 "$BASE_DIR/orchestrator/backup.py" \
    --host 0.0.0.0 --client-port 6000 --worker-port 6001 &
sleep 1

echo -e "${GREEN}[3/5] Iniciando Worker 1...${NC}"
python3 "$BASE_DIR/worker/worker.py" --id worker_1 --host 127.0.0.1 --port 5001 &
sleep 1

echo -e "${GREEN}[4/5] Iniciando Worker 2...${NC}"
python3 "$BASE_DIR/worker/worker.py" --id worker_2 --host 127.0.0.1 --port 5001 &
sleep 1

echo -e "${GREEN}[5/5] Iniciando Worker 3 (com simulação de falha)...${NC}"
python3 "$BASE_DIR/worker/worker.py" --id worker_3 --host 127.0.0.1 --port 5001 \
    --simulate-failure --failure-prob 0.3 &
sleep 1

echo ""
echo "============================================================"
echo -e "${GREEN} Sistema distribuído inicializado com sucesso!${NC}"
echo "============================================================"
echo ""
echo " Componentes ativos:"
echo "   • Orquestrador Principal: 0.0.0.0:5000 (clientes) / 5001 (workers)"
echo "   • Orquestrador Backup:    0.0.0.0:6000 (failover)"
echo "   • Worker 1:               conectado ao orquestrador"
echo "   • Worker 2:               conectado ao orquestrador"
echo "   • Worker 3:               conectado (com falhas simuladas)"
echo ""
echo " Para conectar um cliente:"
echo "   python3 client/client.py --interactive"
echo ""
echo " Logs disponíveis em: $LOGS_DIR/"
echo ""
echo " Pressione Ctrl+C para encerrar todos os componentes."
echo "============================================================"

# Aguardar todos os processos
wait
