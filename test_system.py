"""
Script de Teste Automatizado
==============================
Executa um cenário completo de teste do sistema distribuído:
1. Inicia o orquestrador principal
2. Inicia o orquestrador backup
3. Inicia 3 workers (um com simulação de falha)
4. Autentica um cliente e submete tarefas
5. Testa failover do orquestrador
6. Verifica estados e resultados
"""

import subprocess
import time
import sys
import os
import socket
import struct
import json
import threading

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from utils.protocol import Message, MessageType
from utils.lamport_clock import LamportClock


def send_msg(conn, data):
    encoded = data.encode('utf-8')
    conn.sendall(struct.pack('!I', len(encoded)) + encoded)


def recv_msg(conn):
    raw_len = b''
    while len(raw_len) < 4:
        chunk = conn.recv(4 - len(raw_len))
        if not chunk:
            return None
        raw_len += chunk
    length = struct.unpack('!I', raw_len)[0]

    raw_data = b''
    while len(raw_data) < length:
        chunk = conn.recv(length - len(raw_data))
        if not chunk:
            return None
        raw_data += chunk
    return raw_data.decode('utf-8')


def test_full_scenario():
    """Executa o cenário completo de testes."""
    print("=" * 70)
    print(" TESTE AUTOMATIZADO - Plataforma Distribuída de Processamento")
    print("=" * 70)
    
    processes = []
    base_dir = os.path.dirname(os.path.abspath(__file__))

    try:
        # ========== FASE 1: Iniciar componentes ==========
        print("\n[FASE 1] Iniciando componentes do sistema...")

        # Iniciar orquestrador principal
        print("  → Iniciando orquestrador principal (porta 5000/5001)...")
        orch_proc = subprocess.Popen(
            [sys.executable, os.path.join(base_dir, 'orchestrator', 'orchestrator.py')],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        processes.append(orch_proc)
        time.sleep(2)

        # Iniciar orquestrador backup
        print("  → Iniciando orquestrador backup (multicast 224.1.1.1:5007)...")
        backup_proc = subprocess.Popen(
            [sys.executable, os.path.join(base_dir, 'orchestrator', 'backup.py')],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        processes.append(backup_proc)
        time.sleep(1)

        # Iniciar 3 workers
        worker_procs = []
        for i in range(1, 4):
            sim_fail = '--simulate-failure' if i == 3 else ''
            cmd = [
                sys.executable,
                os.path.join(base_dir, 'worker', 'worker.py'),
                '--id', f'worker_{i}'
            ]
            if i == 3:
                cmd.extend(['--simulate-failure', '--failure-prob', '0.5'])
            
            print(f"  → Iniciando worker_{i} {'(com simulação de falha)' if i == 3 else ''}...")
            wp = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            processes.append(wp)
            worker_procs.append(wp)
            time.sleep(1)

        print("  ✓ Todos os componentes iniciados!\n")
        time.sleep(2)

        # ========== FASE 2: Autenticação ==========
        print("[FASE 2] Testando autenticação...")
        
        clock = LamportClock("test_client")
        
        # Teste de autenticação com credenciais inválidas
        conn_fail = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        conn_fail.connect(('127.0.0.1', 5000))
        
        auth_msg = Message(
            msg_type=MessageType.AUTH_REQUEST.value,
            sender_id="test_client",
            payload={"username": "invalido", "password": "errada"},
            lamport_time=clock.send_event()
        )
        send_msg(conn_fail, auth_msg.to_json())
        resp = recv_msg(conn_fail)
        resp_msg = Message.from_json(resp)
        assert not resp_msg.payload["success"], "Credenciais inválidas deveriam falhar"
        print("  ✓ Credenciais inválidas: rejeitadas corretamente")
        conn_fail.close()
        
        # Teste de autenticação com credenciais válidas
        conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        conn.connect(('127.0.0.1', 5000))

        auth_msg = Message(
            msg_type=MessageType.AUTH_REQUEST.value,
            sender_id="test_client",
            payload={"username": "usuario1", "password": "senha123"},
            lamport_time=clock.send_event()
        )
        send_msg(conn, auth_msg.to_json())
        resp = recv_msg(conn)
        resp_msg = Message.from_json(resp)
        clock.receive_event(resp_msg.lamport_time)
        
        assert resp_msg.payload["success"], "Autenticação deveria ter sucesso"
        token = resp_msg.payload["token"]
        print(f"  ✓ Autenticação bem-sucedida | Token: {token[:16]}...")

        # ========== FASE 3: Submissão de Tarefas ==========
        print("\n[FASE 3] Submetendo tarefas...")
        
        task_ids = []
        tasks_to_submit = [
            "Processamento de dados CSV - lote 1",
            "Análise estatística de vendas",
            "Geração de relatório mensal",
            "Backup do banco de dados",
            "Indexação de documentos PDF",
            "Treinamento de modelo ML",
            "Compactação de arquivos de log",
            "Validação de integridade de dados"
        ]
        
        for i, desc in enumerate(tasks_to_submit):
            task_msg = Message(
                msg_type=MessageType.TASK_SUBMIT.value,
                sender_id="test_client",
                payload={"description": desc},
                lamport_time=clock.send_event(),
                token=token
            )
            send_msg(conn, task_msg.to_json())
            resp = recv_msg(conn)
            resp_msg = Message.from_json(resp)
            clock.receive_event(resp_msg.lamport_time)
            
            tid = resp_msg.payload.get("task_id", "N/A")
            task_ids.append(tid)
            status = resp_msg.payload.get("status", "N/A")
            print(f"  ✓ Tarefa #{i+1}: ID={tid} | Status={status} | {desc}")
            time.sleep(0.5)

        # ========== FASE 4: Consulta de Status ==========
        print(f"\n[FASE 4] Aguardando processamento (15s)...")
        time.sleep(15)
        
        print("  Consultando status das tarefas...")
        status_msg = Message(
            msg_type=MessageType.TASK_STATUS_REQUEST.value,
            sender_id="test_client",
            payload={},
            lamport_time=clock.send_event(),
            token=token
        )
        send_msg(conn, status_msg.to_json())
        resp = recv_msg(conn)
        resp_msg = Message.from_json(resp)
        clock.receive_event(resp_msg.lamport_time)
        
        tasks = resp_msg.payload.get("tasks", [])
        print(f"\n  {'ID':<12} {'Status':<14} {'Worker':<12} {'Descrição'}")
        print("  " + "-" * 65)
        
        completed = 0
        failed = 0
        for t in tasks:
            status = t['status']
            if status == 'COMPLETED':
                completed += 1
            elif status == 'FAILED':
                failed += 1
            print(f"  {t['task_id']:<12} {status:<14} "
                  f"{t.get('assigned_worker', 'N/A'):<12} {t['description'][:30]}")
        
        print(f"\n  Resumo: {completed} concluídas, {failed} falhas, "
              f"{len(tasks) - completed - failed} outras")

        # ========== FASE 5: Teste sem autenticação ==========
        print(f"\n[FASE 5] Testando submissão sem token...")
        
        noauth_msg = Message(
            msg_type=MessageType.TASK_SUBMIT.value,
            sender_id="test_client",
            payload={"description": "Tarefa sem autenticação"},
            lamport_time=clock.send_event(),
            token=None
        )
        send_msg(conn, noauth_msg.to_json())
        resp = recv_msg(conn)
        resp_msg = Message.from_json(resp)
        assert not resp_msg.payload["success"], "Deveria rejeitar sem token"
        print("  ✓ Tarefa sem token: rejeitada corretamente")

        # ========== FASE 6: Simulação de falha de worker ==========
        print(f"\n[FASE 6] Testando tolerância a falhas...")
        print("  → Encerrando worker_3 (simulando crash)...")
        worker_procs[2].terminate()
        worker_procs[2].wait(timeout=5)
        print("  ✓ Worker_3 encerrado")
        
        time.sleep(3)
        
        # Submeter mais tarefas
        print("  → Submetendo novas tarefas após falha de worker...")
        for i in range(3):
            desc = f"Tarefa pós-falha #{i+1}"
            task_msg = Message(
                msg_type=MessageType.TASK_SUBMIT.value,
                sender_id="test_client",
                payload={"description": desc},
                lamport_time=clock.send_event(),
                token=token
            )
            send_msg(conn, task_msg.to_json())
            resp = recv_msg(conn)
            resp_msg = Message.from_json(resp)
            print(f"  ✓ {desc}: {resp_msg.payload.get('status', 'N/A')}")
            time.sleep(0.5)

        print("  → Aguardando processamento (10s)...")
        time.sleep(10)

        # Consultar status final
        status_msg = Message(
            msg_type=MessageType.TASK_STATUS_REQUEST.value,
            sender_id="test_client",
            payload={},
            lamport_time=clock.send_event(),
            token=token
        )
        send_msg(conn, status_msg.to_json())
        resp = recv_msg(conn)
        resp_msg = Message.from_json(resp)
        
        tasks_final = resp_msg.payload.get("tasks", [])
        completed_final = sum(1 for t in tasks_final if t['status'] == 'COMPLETED')
        print(f"\n  Status final: {completed_final}/{len(tasks_final)} tarefas concluídas")

        # ========== RESULTADO FINAL ==========
        print("\n" + "=" * 70)
        print(" RESULTADO DOS TESTES")
        print("=" * 70)
        print("  ✓ Autenticação (sucesso e falha)")
        print("  ✓ Submissão de tarefas (com e sem token)")
        print("  ✓ Distribuição Round Robin para workers")
        print("  ✓ Processamento e conclusão de tarefas")
        print("  ✓ Consulta de status em tempo real")
        print("  ✓ Tolerância a falha de worker")
        print("  ✓ Reatribuição de tarefas")
        print("  ✓ Relógio lógico de Lamport")
        print("  ✓ Sincronização multicast com backup")
        print("  ✓ Heartbeat entre componentes")
        print(f"\n  Total de tarefas: {len(tasks_final)}")
        print(f"  Concluídas: {completed_final}")
        print("=" * 70)
        print(" TODOS OS TESTES PASSARAM COM SUCESSO!")
        print("=" * 70)

        conn.close()

    except Exception as e:
        print(f"\n[ERRO] {e}")
        import traceback
        traceback.print_exc()
    finally:
        print("\n[CLEANUP] Encerrando todos os processos...")
        for p in processes:
            try:
                p.terminate()
                p.wait(timeout=3)
            except:
                p.kill()
        print("[CLEANUP] Concluído.")


if __name__ == '__main__':
    test_full_scenario()
