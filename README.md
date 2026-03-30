# Plataforma Distribuída de Processamento Colaborativo de Tarefas

## Descrição

Sistema distribuído de orquestração de tarefas em Python com:

- autenticação por usuário e token
- distribuição de tarefas para múltiplos workers
- replicação de estado para orquestrador backup
- failover automático com recuperação de tarefas interrompidas
- rastreamento completo de execução por tarefa

Projeto acadêmico de Sistemas Distribuídos (IFBA - Campus Santo Antônio de Jesus).

## Arquitetura

```
Clientes (TCP 5000) <-> Orquestrador Primário <-> Workers (TCP 5001)
                              |
                              | UDP Multicast (224.1.1.1:5007)
                              v
                    Orquestrador Backup (passivo)
                    - aceita em 6000/6001 e envia REDIRECT
                    - assume automaticamente em falha do primário
```

## Funcionalidades Atuais

- Failover automático do backup para primário.
- Heartbeat explícito entre orquestradores (`ORCHESTRATOR_HEARTBEAT`) separado do `STATE_SYNC`.
- Monitoramento de workers com separação entre:
  - heartbeat (intervalo do worker: 3s)
  - atividade real (`activity_timeout`: 120s)
- Recuperação pós-failover:
  - espera curta por reconexão de workers (10s)
  - reatribuição automática de tarefas `ASSIGNED`/`RUNNING` interrompidas
- Distribuição Round Robin com filtro de workers já falhos para a tarefa (quando possível).
- Histórico de execução por tarefa (`execution_history`) com eventos `ASSIGNED`, `STARTED`, `FAILED`, `COMPLETED`.

## Requisitos

- Python 3.8+
- Windows, Linux ou macOS
- Sem dependências externas (somente biblioteca padrão)

## Execução Manual (Windows/PowerShell)

Use uma janela por processo.

1. Orquestrador primário:

```powershell
python orchestrator\orchestrator.py --host 0.0.0.0 --client-port 5000 --worker-port 5001
```

2. Orquestrador backup (passivo + replicação + redirect):

```powershell
python orchestrator\backup.py --host 0.0.0.0 --client-port 6000 --worker-port 6001 --primary-host 127.0.0.1 --primary-client-port 5000 --primary-worker-port 5001
```

3. Workers:

```powershell
python worker\worker.py --id worker_1 --host 127.0.0.1 --port 5001 --secondary-host 127.0.0.1 --secondary-port 6001
python worker\worker.py --id worker_2 --host 127.0.0.1 --port 5001 --secondary-host 127.0.0.1 --secondary-port 6001
python worker\worker.py --id worker_3 --host 127.0.0.1 --port 5001 --secondary-host 127.0.0.1 --secondary-port 6001 --simulate-failure --failure-prob 0.3
```

4. Cliente interativo:

```powershell
python client\client.py --interactive
```

## Testes

1. Teste completo (recomendado):

```powershell
python test_system.py
```

Valida autenticação, submissão, processamento, tolerância a falha de worker, reatribuição e `execution_history`.

Também valida failover real do orquestrador principal (encerrando o primário durante o teste), promoção automática do backup e continuidade operacional com submissão/consulta de tarefas após o failover.

2. Teste rápido:

```powershell
python test_quick.py
```

## Failover e Recuperação

- O backup recebe `STATE_SYNC` e `ORCHESTRATOR_HEARTBEAT` via multicast.
- Se o heartbeat do primário expira (timeout de 15s), o backup assume automaticamente.
- Ao assumir, tenta primeiro bind nas portas do primário (`5000/5001`).
- Se as portas estiverem ocupadas, permanece em `6000/6001` (clientes/workers com suporte a redirect continuam operando).
- Após assumir, executa rotina de recuperação para reatribuir tarefas interrompidas.
- Durante a janela de failover, podem ocorrer erros transitórios de conexão no Windows (ex.: `WinError 10053`), comportamento esperado até a promoção completa do backup.

## Usuários Padrão

| Usuário | Senha |
|---|---|
| admin | admin123 |
| usuario1 | senha123 |
| usuario2 | senha456 |
| usuario3 | senha789 |

## Protocolo (Resumo)

Tipos de mensagem implementados em `utils/protocol.py`:

- Autenticação: `AUTH_REQUEST`, `AUTH_RESPONSE`
- Tarefas: `TASK_SUBMIT`, `TASK_SUBMIT_ACK`, `TASK_ASSIGN`, `TASK_STARTED`, `TASK_COMPLETE`, `TASK_FAILED`, `TASK_STATUS_REQUEST`, `TASK_STATUS_RESPONSE`
- Cluster: `STATE_SYNC`, `ORCHESTRATOR_HEARTBEAT`, `REDIRECT`, `FAILOVER_ACTIVATE`
- Worker: `WORKER_REGISTER`, `WORKER_REGISTER_ACK`, `HEARTBEAT`, `HEARTBEAT_ACK`
- Teste: `SIMULATE_FAILURE`

Observação: tipos não utilizados anteriormente foram removidos do protocolo para manter consistência.

## Estrutura do Projeto

```
Sistema-simples-de-tarefas-distribuidas/
├── client/
│   ├── __init__.py
│   └── client.py
├── orchestrator/
│   ├── __init__.py
│   ├── backup.py
│   └── orchestrator.py
├── utils/
│   ├── __init__.py
│   ├── lamport_clock.py
│   ├── logger.py
│   └── protocol.py
├── worker/
│   ├── __init__.py
│   └── worker.py
├── test_quick.py
├── test_system.py
└── README.md
```

## Logs e Observabilidade

Eventos importantes registrados:

- autenticação e autorização
- submissão e distribuição de tarefas
- início real da execução (`TASK_STARTED`)
- falhas e reatribuições
- heartbeat de worker
- heartbeat explícito entre orquestradores
- sincronização de estado e failover
- recuperação pós-failover

## Licença

Projeto acadêmico.
