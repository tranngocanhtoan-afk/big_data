# namenode.py

import socket
import threading
import json
import time

from functions_namenode import *

HOST = ''       # listen on all interfaces
PORT = 5001     # port for DataNode connections

# in-memory heartbeat timestamps
datanodes = {}  # { node_id: last_heartbeat_timestamp }
lock = threading.Lock()

HEARTBEAT_TIMEOUT = 15   # seconds without heartbeat → dead
MONITOR_INTERVAL  = 10   # seconds between status scans

def handle_client(conn, addr):
    """Handle a single DataNode connection."""
    with conn:
        print(f"[NameNode] New connection from {addr}")
        while True:
            try:
                raw = conn.recv(1024)
                if not raw:
                    break

                msg = json.loads(raw.decode('utf-8'))
                typ = msg.get('type')
                node_id = msg.get('id')

                with lock:
                    if typ == 'register':
                        # register node
                        datanodes[node_id] = time.time()
                        upsert_node(node_id, 'alive')
                        conn.sendall(b'{"status":"registered"}')
                        print(f"[NameNode] Registered DataNode '{node_id}'")

                    elif typ == 'compute':
                        file_base = msg.get('file')
                        process_file_tasks(file_base)
                    elif typ == 'heartbeat':
                        # refresh heartbeat
                        if node_id in datanodes:
                            datanodes[node_id] = time.time()
                            upsert_node(node_id, 'alive')
                            conn.sendall(b'{"status":"alive"}')
                            print(f"[NameNode] Heartbeat from '{node_id}'")
                        else:
                            conn.sendall(b'{"status":"unknown_node"}')

                    elif typ == 'compute':
                        # msg['file'] is the base filename (no .csv)
                        file_base = msg.get('file')
                        try:
                            # assign tasks for all blocks of this file
                            process_file_tasks(file_base)
                            resp = {'status':'ok', 'file': file_base}
                            conn.sendall(json.dumps(resp).encode('utf-8'))
                            print(f"[NameNode] Computed tasks for '{file_base}'")
                        except Exception as e:
                            err = {'status':'error', 'error': str(e)}
                            conn.sendall(json.dumps(err).encode('utf-8'))
                            print(f"[NameNode] Compute error: {e}")

                    else:
                        # unknown message type
                        conn.sendall(b'{"status":"bad_request"}')

            except Exception as e:
                print(f"[NameNode] Connection error: {e}")
                break

        # connection closed: clean up any dead nodes
        print(f"[NameNode] Disconnected {addr}")
        with lock:
            for nid, ts in list(datanodes.items()):
                if time.time() - ts > HEARTBEAT_TIMEOUT:
                    remove_node(nid)
                    reassign_leader_on_disconnect(node_id)
                    del datanodes[nid]
                    print(f"[NameNode] Removed dead DataNode '{nid}'")

def monitor_datanodes():
    """Periodically print status and purge timed-out nodes."""
    while True:
        time.sleep(MONITOR_INTERVAL)
        now = time.time()
        with lock:
            print("=== NameNode Status ===")
            for node, ts in list(datanodes.items()):
                age = now - ts
                state = "alive" if age < HEARTBEAT_TIMEOUT else "dead"
                print(f"  - {node}: last heartbeat {age:.1f}s ago → {state}")
                if state == "dead":
                    remove_node(node)
                    del datanodes[node]
                    print(f"    → Removed dead DataNode '{node}'")
            print("========================")

def main():
    # ensure the metadata table exists
    init_active_node_manager_table()

    # start monitor thread
    threading.Thread(target=monitor_datanodes, daemon=True).start()

    # start TCP server
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as srv:
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind((HOST, PORT))
        srv.listen()
        print(f"[NameNode] Listening on port {PORT}...")
        while True:
            conn, addr = srv.accept()
            threading.Thread(target=handle_client,
                             args=(conn, addr),
                             daemon=True).start()

if __name__ == '__main__':
    main()
    assign_task_auto(alogs)
