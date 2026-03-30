# Plataforma Distribuída de Processamento Colaborativo de Tarefas

## 📋 Descrição

Sistema distribuído de orquestração de tarefas desenvolvido em Python, que permite a submissão de trabalhos por clientes autenticados, distribuição para múltiplos nós de processamento (workers), acompanhamento do estado global e recuperação em caso de falhas.

**Disciplina:** Sistemas Distribuídos  
**Instituição:** IFBA - Campus Santo Antônio de Jesus  
**Curso:** Tecnologia em Análise e Desenvolvimento de Sistemas

---

## 🏗️ Arquitetura do Sistema

O sistema é composto por quatro tipos de componentes:

```
┌─────────────┐     TCP      ┌──────────────────────┐     UDP Multicast
│   Cliente 1  │◄────────────►│  Orquestrador        │◄──────────────────►┌────────────────────┐
│   Cliente 2  │              │  Principal            │                    │  Orquestrador       │
│   Cliente N  │              │  (Coordenador)        │                    │  Backup (Secundário)│
└─────────────┘              └──────────┬───────────┘                    └────────────────────┘
                                        │ TCP
                         ┌──────────────┼──────────────┐
                         │              │              │
                    ┌────▼───┐    ┌────▼───┐    ┌────▼───┐
                    │Worker 1│    │Worker 2│    │Worker 3│
                    └────────┘    └────────┘    └────────┘
```

### Componentes

1. **Orquestrador Principal** - Recebe tarefas, distribui via Round Robin, monitora workers
2. **Orquestrador Backup** - Réplica sincronizada via UDP Multicast, failover automático
3. **Workers (3+)** - Executam tarefas, enviam heartbeats, podem falhar simuladamente
4. **Clientes** - Autenticam-se, submetem tarefas, consultam status em tempo real

---

## 🔧 Requisitos

- Python 3.8+
- Sistema operacional: Linux, macOS ou Windows
- Nenhuma dependência externa (apenas biblioteca padrão do Python)

---

## 🚀 Instalação e Execução

### 1. Clonar o repositório
```bash
git clone https://github.com/seu-usuario/plataforma-distribuida.git
cd plataforma-distribuida
```

### 2. Iniciar o sistema completo (script automático)
```bash
chmod +x start_system.sh
./start_system.sh
```

### 3. Ou iniciar componentes individualmente

**Terminal 1 - Orquestrador Principal:**
```bash
python3 orchestrator/orchestrator.py --host 0.0.0.0 --client-port 5000 --worker-port 5001
```

**Terminal 2 - Orquestrador Backup:**
```bash
python3 orchestrator/backup.py --host 0.0.0.0 --client-port 6000 --worker-port 6001
```

**Terminais 3, 4, 5 - Workers:**
```bash
python3 worker/worker.py --id worker_1 --host 127.0.0.1 --port 5001
python3 worker/worker.py --id worker_2 --host 127.0.0.1 --port 5001
python3 worker/worker.py --id worker_3 --host 127.0.0.1 --port 5001 --simulate-failure
```

**Terminal 6 - Cliente Interativo:**
```bash
python3 client/client.py --interactive
```

### 4. Executar testes automatizados
```bash
python3 test_system.py
```

---

## 👤 Usuários Pré-cadastrados

| Usuário    | Senha     |
|------------|-----------|
| admin      | admin123  |
| usuario1   | senha123  |
| usuario2   | senha456  |
| usuario3   | senha789  |

---

## 📡 Protocolos de Comunicação

| Comunicação                  | Protocolo      | Porta  |
|------------------------------|----------------|--------|
| Cliente ↔ Orquestrador       | TCP Sockets    | 5000   |
| Worker ↔ Orquestrador        | TCP Sockets    | 5001   |
| Orquestrador ↔ Backup        | UDP Multicast  | 5007   |

---

## ⚖️ Política de Balanceamento: Round Robin

O sistema utiliza a política **Round Robin** para distribuição de tarefas:

- **Vantagens:**
  - Implementação simples e determinística
  - Distribuição equitativa entre workers
  - Baixo overhead computacional
  - Previsibilidade na alocação

- **Limitações:**
  - Não considera carga real dos workers
  - Não diferencia tarefas por complexidade
  - Workers heterogêneos podem ficar desbalanceados

---

## 🔒 Segurança

- Autenticação baseada em usuário + senha
- Senhas armazenadas como hash SHA-256
- Tokens gerados aleatoriamente (hex 16 bytes)
- Validação de token em cada operação

---

## ⏰ Relógio Lógico de Lamport

Implementado para ordenação parcial de eventos:
- Incremento em eventos locais
- Inclusão do timestamp nas mensagens
- Ajuste ao receber: `max(local, recebido) + 1`

---

## 📁 Estrutura do Projeto

```
plataforma-distribuida/
├── orchestrator/
│   ├── __init__.py
│   ├── orchestrator.py          # Orquestrador principal
│   └── backup.py                # Orquestrador backup
├── worker/
│   ├── __init__.py
│   └── worker.py                # Nó de processamento
├── client/
│   ├── __init__.py
│   └── client.py                # Cliente interativo
├── utils/
│   ├── __init__.py
│   ├── lamport_clock.py         # Relógio de Lamport
│   ├── protocol.py              # Protocolo de mensagens
│   └── logger.py                # Sistema de logs
├── logs/                        # Arquivos de log
├── test_system.py               # Testes automatizados
├── start_system.sh              # Script de inicialização
└── README.md                    # Este arquivo
```

---

## 📊 Eventos Registrados nos Logs

- Submissão de tarefas
- Distribuição pelo orquestrador
- Conclusão ou falha em worker
- Failover para backup
- Reatribuição de tarefa
- Heartbeats e timeouts
- Autenticações (sucesso/falha)
- Sincronização de estado

---

## 🧪 Exemplos de Uso

### Submeter uma tarefa
```
--- Menu ---
1. Submeter tarefa
Opção: 1
Descrição da tarefa: Processamento de dados CSV
→ Resposta: {"success": true, "task_id": "a1b2c3d4", "status": "ASSIGNED"}
```

### Consultar status
```
--- Menu ---
2. Consultar status (todas as tarefas)
ID           Status         Worker       Descrição
--------------------------------------------------------------
a1b2c3d4     COMPLETED      worker_1     Processamento de dados CSV
e5f6g7h8     RUNNING        worker_2     Análise estatística
```

---

## 📄 Licença

Projeto acadêmico - IFBA Campus Santo Antônio de Jesus, 2025.
