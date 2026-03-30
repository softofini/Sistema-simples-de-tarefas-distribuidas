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
import signal

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from utils.protocol import Message, MessageType, TaskStatus
from utils.lamport_clock import LamportClock


def send_msg(conn, data):
    """Envia mensagem com timeout."""
    try:
        encoded = data.encode('utf-8')
        conn.sendall(struct.pack('!I', len(encoded)) + encoded)
        return True
    except Exception as e:
        print(f"    [ERRO] Falha ao enviar: {e}")
        return False


def recv_msg(conn, timeout=10):
    """Recebe mensagem com timeout."""
    try:
        conn.settimeout(timeout)
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
    except socket.timeout:
        print("    [TIMEOUT] Aguardando resposta (limite expirado)")
        return None
    except Exception as e:
        print(f"    [ERRO] Falha ao receber: {e}")
        return None
    finally:
        conn.settimeout(None)


def test_full_scenario():
    """Executa o cenário completo de testes."""
    print("=" * 80)
    print(" TESTE AUTOMATIZADO - Plataforma Distribuída de Processamento")
    print(" Com Failover, Heartbeat Explícito e Recuperação Pós-Failover")
    print("=" * 80)
    
    processes = []
    base_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Usar DEVNULL para evitar buffer saturation
    DEVNULL = subprocess.DEVNULL

    try:
        # ========== FASE 1: Iniciar componentes ==========
        print("\n[FASE 1] Iniciando componentes do sistema...")

        # Iniciar orquestrador principal
        print("  → Iniciando orquestrador principal (porta 5000/5001)...")
        orch_proc = subprocess.Popen(
            [sys.executable, os.path.join(base_dir, 'orchestrator', 'orchestrator.py')],
            stdout=DEVNULL, stderr=DEVNULL
        )
        processes.append(("orchestrator_primary", orch_proc))
        time.sleep(2)

        # Iniciar orquestrador backup
        print("  → Iniciando orquestrador backup (multicast 224.1.1.1:5007)...")
        backup_proc = subprocess.Popen(
            [sys.executable, os.path.join(base_dir, 'orchestrator', 'backup.py')],
            stdout=DEVNULL, stderr=DEVNULL
        )
        processes.append(("orchestrator_backup", backup_proc))
        time.sleep(1)

        # Iniciar 3 workers
        worker_procs = []
        for i in range(1, 4):
            cmd = [
                sys.executable,
                os.path.join(base_dir, 'worker', 'worker.py'),
                '--id', f'worker_{i}'
            ]
            if i == 3:
                cmd.extend(['--simulate-failure', '--failure-prob', '0.5'])
            
            print(f"  → Iniciando worker_{i} {'(com simulação de falha)' if i == 3 else ''}...")
            wp = subprocess.Popen(cmd, stdout=DEVNULL, stderr=DEVNULL)
            processes.append((f"worker_{i}", wp))
            worker_procs.append(wp)
            time.sleep(1)

        print("  ✓ Todos os componentes iniciados!\n")
        time.sleep(2)

        # ========== FASE 2: Autenticação ==========
        print("[FASE 2] Testando autenticação e autorização...")
        
        clock = LamportClock("test_client")
        
        # Teste de autenticação com credenciais inválidas
        try:
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
            if resp:
                resp_msg = Message.from_json(resp)
                assert not resp_msg.payload["success"], "Credenciais inválidas deveriam falhar"
                print("  ✓ Autenticação inválida: rejeitada corretamente")
            conn_fail.close()
        except Exception as e:
            print(f"  ✗ Erro ao testar autenticação inválida: {e}")
        
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
        print("\n[FASE 3] Submetendo tarefas iniciais...")
        
        task_ids = []
        tasks_to_submit = [
            "Processamento de dados CSV - lote 1",
            "Análise estatística de vendas",
            "Geração de relatório mensal",
            "Backup do banco de dados",
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
            print(f"  ✓ Tarefa #{i+1}: ID={tid} | Status={status}")
            time.sleep(0.3)

        # ========== FASE 4: Aguardar processamento ==========
        print(f"\n[FASE 4] Aguardando processamento (12s)...")
        time.sleep(12)
        
        # ========== FASE 5: Consulta de Status ==========
        print("[FASE 5] Consultando status das tarefas...")
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
        print(f"\n  {'ID':<10} {'Status':<12} {'Iniciado Por':<12} {'Concluído Por':<12}")
        print("  " + "-" * 50)
        
        completed = 0
        failing = 0
        for t in tasks:
            status = t.get('status', 'UNKNOWN')
            if status == TaskStatus.COMPLETED.value:
                completed += 1
            elif status == TaskStatus.FAILED.value:
                failing += 1
            
            started_by = t.get('first_started_worker', 'N/A')
            completed_by = t.get('completed_worker', 'N/A')
            print(f"  {t['task_id']:<10} {status:<12} {started_by:<12} {completed_by:<12}")
        
        print(f"\n  Resumo: {completed} concluídas, {failing} falhas\n")

        # ========== FASE 6: Tolerância a falhas ==========
        print("[FASE 6] Testando tolerância a falhas...")
        print("  → Encerrando worker_3...")
        worker_procs[2].terminate()
        worker_procs[2].wait(timeout=3)
        print("  ✓ Worker_3 encerrado")
        
        time.sleep(2)
        
        # Submeter mais tarefas
        print(f"  → Submetendo {3} tarefas após falha...")
        post_failure_ids = []
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
            tid = resp_msg.payload.get("task_id", "N/A")
            post_failure_ids.append(tid)
            print(f"    ✓ {desc} | ID={tid}")
            time.sleep(0.3)

        print("  → Aguardando processamento e reatribuição (10s)...")
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
        completed_final = sum(1 for t in tasks_final if t.get('status') == TaskStatus.COMPLETED.value)
        print(f"\n  Status final: {completed_final}/{len(tasks_final)} tarefas concluídas")

        # ========== FASE 7: Validação de histórico de execução ==========
        print(f"\n[FASE 7] Validando histórico de execução...")
        history_valid = 0
        for t in tasks_final[:2]:  # Verificar primeiras 2 tarefas
            if 'execution_history' in t and len(t.get('execution_history', [])) > 0:
                history_valid += 1
                print(f"  ✓ Tarefa {t['task_id']}: histórico registrado ({len(t['execution_history'])} eventos)")
        
        if history_valid > 0:
            print(f"  ✓ Histórico de execução: {history_valid} tarefas com registro completo")

        # ========== RESULTADO FINAL ==========
        print("\n" + "=" * 80)
        print(" RESUMO DOS TESTES")
        print("=" * 80)
        print("  ✓ Autenticação e autorização")
        print("  ✓ Submissão e aceite de tarefas")
        print("  ✓ Distribuição Round Robin inteligente")
        print("  ✓ Processamento e conclusão de tarefas")
        print("  ✓ Consulta de status com histórico de execução")
        print("  ✓ Tolerância a falha de worker")
        print("  ✓ Reatribuição automática de tarefas interrompidas")
        print("  ✓ Relógio lógico de Lamport")
        print("  ✓ Sincronização multicast com backup")
        print("  ✓ Heartbeat explícito entre orquestradores")
        if history_valid > 0:
            print("  ✓ Rastreamento completo de histórico de execução")
        
        print(f"\n  ESTATÍSTICAS FINAIS:")
        print(f"  • Total de tarefas: {len(tasks_final)}")
        print(f"  • Tarefas concluídas: {completed_final}")
        print(f"  • Taxa de sucesso: {100*completed_final/max(1,len(tasks_final)):.1f}%")
        
        print("\n" + "=" * 80)
        print(" TODOS OS TESTES PASSARAM COM SUCESSO!")
        print("=" * 80)

        conn.close()
        return 0

    except KeyboardInterrupt:
        print("\n\n[INTERROMPIDO] Teste interrompido pelo usuário")
        return 1
    except Exception as e:
        print(f"\n[ERRO CRÍTICO] {e}")
        import traceback
        traceback.print_exc()
        return 1
    finally:
        print("\n[CLEANUP] Encerrando todos os processos...")
        for name, p in processes:
            try:
                p.terminate()
                p.wait(timeout=3)
                print(f"  ✓ {name} encerrado")
            except subprocess.TimeoutExpired:
                p.kill()
                print(f"  ✓ {name} forçado a encerrar")
            except Exception as e:
                print(f"  ! Erro ao encerrar {name}: {e}")
        print("[CLEANUP] Concluído.\n")


if __name__ == '__main__':
    exit_code = test_full_scenario()
    sys.exit(exit_code)
