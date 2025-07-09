import socket
import threading
import json
import time

from functions_namenode import (
    init_active_node_manager_table,
    upsert_node,
    remove_node
)

HOST = ''      # Listen on all interfaces
PORT = 5001    # Port for DataNode to connect

# In-memory store: { node_id: last_heartbeat_timestamp }
datanodes = {}
lock = threading.Lock()

HEARTBEAT_TIMEOUT = 15  # nếu quá 30s không heartbeat → xem như dead

def handle_client(conn, addr):
    """Xử lý mỗi kết nối từ DataNode."""
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
                        datanodes[node_id] = time.time()
                        upsert_node(node_id, 'alive')
                        conn.sendall(b'{"status":"registered"}')
                        print(f"[NameNode] Registered DataNode '{node_id}'")

                    elif typ == 'heartbeat':
                        if node_id in datanodes:
                            datanodes[node_id] = time.time()
                            upsert_node(node_id, 'alive')
                            conn.sendall(b'{"status":"alive"}')
                            print(f"[NameNode] Heartbeat from '{node_id}'")
                        else:
                            conn.sendall(b'{"status":"unknown_node"}')

                    else:
                        conn.sendall(b'{"status":"bad_request"}')

            except Exception as e:
                print(f"[NameNode] Connection error: {e}")
                break

        # khi socket đóng
        print(f"[NameNode] Disconnected {addr}")
        # Xóa DataNode khỏi memory và DB
        with lock:
            # tìm node_id bằng addr? ở đây assume node_id độc lập
            # nếu bạn muốn remove dựa trên addr, cần map thêm
            # Ở ví dụ này, ta không có node_id nếu vừa disconnect trước khi register
            # Bỏ qua nếu không tìm thấy
            for nid, ts in list(datanodes.items()):
                # nếu timed out lâu hay đúng node?
                # đơn giản remove tất cả có timestamp cũ hơn
                if nid not in datanodes or time.time() - datanodes[nid] > HEARTBEAT_TIMEOUT:
                    remove_node(nid)
                    del datanodes[nid]
                    print(f"[NameNode] Removed DataNode '{nid}' due to disconnect")

def monitor_datanodes():
    """In trạng thái DataNode và loại bỏ những node đã dead."""
    while True:
        time.sleep(10)
        now = time.time()
        with lock:
            print("=== NameNode Status ===")
            for node, ts in list(datanodes.items()):
                age = now - ts
                state = "alive" if age < HEARTBEAT_TIMEOUT else "dead"
                print(f"  - {node}: last heartbeat {age:.1f}s ago → {state}")
                if state == "dead":
                    # xóa khỏi DB và memory
                    remove_node(node)
                    del datanodes[node]
                    print(f"    → Removed dead DataNode '{node}'")
            print("========================")

def main():
    # Tạo bảng quản lý DataNode nếu chưa có
    init_active_node_manager_table()

    # Khởi thread monitor
    threading.Thread(target=monitor_datanodes, daemon=True).start()

    # Khởi server socket
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as srv:
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind((HOST, PORT))
        srv.listen()
        print(f"[NameNode] Listening on port {PORT}...")
        while True:
            conn, addr = srv.accept()
            threading.Thread(
                target=handle_client,
                args=(conn, addr),
                daemon=True
            ).start()

if __name__ == '__main__':
    main()