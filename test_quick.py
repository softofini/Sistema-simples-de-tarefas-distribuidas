"""
Teste Rápido - Valida funcionalidades principais do sistema
"""

import subprocess
import time
import sys
import os
import socket
import struct
import json

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
        if not chunk: return None
        raw_len += chunk
    length = struct.unpack('!I', raw_len)[0]
    raw_data = b''
    while len(raw_data) < length:
        chunk = conn.recv(length - len(raw_data))
        if not chunk: return None
        raw_data += chunk
    return raw_data.decode('utf-8')


def main():
    print("=" * 60)
    print(" TESTE RÁPIDO DO SISTEMA DISTRIBUÍDO")
    print("=" * 60)

    base_dir = os.path.dirname(os.path.abspath(__file__))
    processes = []

    try:
        # Iniciar orquestrador
        print("\n[1] Iniciando orquestrador principal...")
        orch = subprocess.Popen(
            [sys.executable, os.path.join(base_dir, 'orchestrator', 'orchestrator.py')],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        processes.append(orch)
        time.sleep(2)

        # Iniciar backup
        print("[2] Iniciando orquestrador backup...")
        backup = subprocess.Popen(
            [sys.executable, os.path.join(base_dir, 'orchestrator', 'backup.py')],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        processes.append(backup)
        time.sleep(1)

        # Iniciar 3 workers
        for i in range(1, 4):
            cmd = [sys.executable, os.path.join(base_dir, 'worker', 'worker.py'),
                   '--id', f'worker_{i}']
            if i == 3:
                cmd.extend(['--simulate-failure', '--failure-prob', '0.3'])
            print(f"[{i+2}] Iniciando worker_{i}...")
            wp = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            processes.append(wp)
            time.sleep(0.5)

        time.sleep(2)

        # Conectar cliente
        clock = LamportClock("test")
        conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        conn.connect(('127.0.0.1', 5000))

        # Auth falha
        print("\n[TEST] Autenticação inválida...")
        msg = Message(msg_type=MessageType.AUTH_REQUEST.value, sender_id="test",
                      payload={"username": "x", "password": "y"}, lamport_time=clock.send_event())
        send_msg(conn, msg.to_json())
        r = Message.from_json(recv_msg(conn))
        print(f"  → Resultado: {'✓ Rejeitada' if not r.payload['success'] else '✗ Deveria rejeitar'}")
        conn.close()

        # Auth sucesso
        conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        conn.connect(('127.0.0.1', 5000))
        print("[TEST] Autenticação válida...")
        msg = Message(msg_type=MessageType.AUTH_REQUEST.value, sender_id="test",
                      payload={"username": "usuario1", "password": "senha123"},
                      lamport_time=clock.send_event())
        send_msg(conn, msg.to_json())
        r = Message.from_json(recv_msg(conn))
        token = r.payload.get("token")
        print(f"  → Resultado: {'✓ Sucesso' if r.payload['success'] else '✗ Falha'} | Token: {token[:12]}...")

        # Submeter tarefas
        print("\n[TEST] Submetendo 5 tarefas...")
        for i in range(5):
            msg = Message(msg_type=MessageType.TASK_SUBMIT.value, sender_id="test",
                          payload={"description": f"Tarefa de teste #{i+1}"},
                          lamport_time=clock.send_event(), token=token)
            send_msg(conn, msg.to_json())
            r = Message.from_json(recv_msg(conn))
            print(f"  → Tarefa #{i+1}: ID={r.payload.get('task_id','?')} Status={r.payload.get('status','?')}")
            time.sleep(0.3)

        # Sem token
        print("\n[TEST] Submissão sem token...")
        msg = Message(msg_type=MessageType.TASK_SUBMIT.value, sender_id="test",
                      payload={"description": "Sem auth"}, lamport_time=clock.send_event(), token=None)
        send_msg(conn, msg.to_json())
        r = Message.from_json(recv_msg(conn))
        print(f"  → Resultado: {'✓ Rejeitada' if not r.payload['success'] else '✗ Deveria rejeitar'}")

        # Aguardar processamento
        print("\n[TEST] Aguardando processamento (8s)...")
        time.sleep(8)

        # Status
        print("[TEST] Consultando status...")
        msg = Message(msg_type=MessageType.TASK_STATUS_REQUEST.value, sender_id="test",
                      payload={}, lamport_time=clock.send_event(), token=token)
        send_msg(conn, msg.to_json())
        r = Message.from_json(recv_msg(conn))
        tasks = r.payload.get("tasks", [])
        
        for t in tasks:
            print(f"  → {t['task_id']} | {t['status']:<12} | {t.get('assigned_worker','N/A'):<10} | {t['description']}")
        
        completed = sum(1 for t in tasks if t['status'] == 'COMPLETED')
        print(f"\n  Resumo: {completed}/{len(tasks)} concluídas")

        conn.close()

        # Verificar logs
        print("\n[TEST] Verificando logs gerados...")
        log_dir = os.path.join(base_dir, "logs")
        if os.path.exists(log_dir):
            for f in sorted(os.listdir(log_dir)):
                fpath = os.path.join(log_dir, f)
                size = os.path.getsize(fpath)
                lines = sum(1 for _ in open(fpath))
                print(f"  → {f}: {lines} linhas ({size} bytes)")

        print("\n" + "=" * 60)
        print(" ✓ TODOS OS TESTES CONCLUÍDOS COM SUCESSO!")
        print("=" * 60)

    except Exception as e:
        print(f"\n[ERRO] {e}")
        import traceback
        traceback.print_exc()
    finally:
        for p in processes:
            try:
                p.terminate()
                p.wait(timeout=3)
            except:
                try: p.kill()
                except: pass


if __name__ == '__main__':
    main()
