# functions_datanode.py

import socket
import threading
import json
import os
import requests

# Cấu hình địa chỉ của Upload-Server (có thể override từ datanode.py nếu cần)
UPLOAD_SERVER_HOST = '127.0.0.1'
UPLOAD_SERVER_PORT = 5000

def get_local_node_id(sock: socket.socket) -> str:
    """
    Lấy node_id dạng 'ip:port' cho DataNode từ socket đã connect.
    """
    host, port = sock.getsockname()
    return f"{host}:{port}"

def send_message(sock: socket.socket, msg: dict) -> dict:
    """
    Gửi 1 dict (JSON) qua socket, nhận về dict (JSON) response.
    """
    data = json.dumps(msg).encode('utf-8')
    sock.sendall(data)
    raw = sock.recv(4096)
    try:
        return json.loads(raw.decode('utf-8'))
    except Exception:
        return {"_raw": raw.decode('utf-8')}

def download_block(server_ip: str,
                   server_port: int,
                   file_base: str,
                   block_id: str,
                   dest_dir: str) -> bool:
    """
    Tải block_id từ Upload-Server về thư mục dest_dir.
    - server_ip: IP của Upload-Server (host Flask upload_server.py)
    - server_port: port của Upload-Server (mặc định 5000)
    - file_base: tên file gốc không đuôi .csv (ví dụ 'alogs')
    - block_id: tên block file (ví dụ 'alogs_block1.csv')
    - dest_dir: thư mục local để lưu file này
    Trả về True nếu thành công, False nếu lỗi.
    """
    os.makedirs(dest_dir, exist_ok=True)
    url = f"http://{server_ip}:{server_port}/download/{file_base}.csv/blocks/{block_id}"
    local_path = os.path.join(dest_dir, block_id)
    try:
        resp = requests.get(url, stream=True, timeout=20)
        if resp.status_code == 200:
            with open(local_path, 'wb') as f:
                for chunk in resp.iter_content(32 * 1024):
                    if chunk:
                        f.write(chunk)
            print(f"[DataNode] Downloaded block {block_id} → {local_path}")
            return True
        else:
            print(f"[DataNode] ERROR {resp.status_code} when downloading block: {url}")
            return False
    except Exception as e:
        print(f"[DataNode] Download exception for {block_id}: {e}")
        return False

def handle_message(msg: dict):
    """
    Xử lý message JSON nhận từ NameNode.
    Nếu là 'task', sẽ tự động tải block về đúng thư mục.
    """
    mtype = msg.get('type')
    if mtype != 'task':
        print(f"[DataNode] Ignored message: {msg}")
        return

    # Các field: 'role', 'block_id', 'file'
    role      = msg.get('role')
    block_id  = msg.get('block_id')
    file_base = msg.get('file')

    if not all([role, block_id, file_base]):
        print(f"[DataNode] Malformed task message: {msg}")
        return

    if role == 'leader':
        # Tải về thư mục 'task/<file_base>/'
        dest_dir = os.path.join('task', file_base)
        download_block(
            server_ip=UPLOAD_SERVER_HOST,
            server_port=UPLOAD_SERVER_PORT,
            file_base=file_base,
            block_id=block_id,
            dest_dir=dest_dir
        )
        # TODO: xử lý data sau khi download tại đây
    elif role == 'storage':
        # Tải về thư mục 'storage/<file_base>/'
        dest_dir = os.path.join('storage', file_base)
        download_block(
            server_ip=UPLOAD_SERVER_HOST,
            server_port=UPLOAD_SERVER_PORT,
            file_base=file_base,
            block_id=block_id,
            dest_dir=dest_dir
        )
    else:
        print(f"[DataNode] Unknown role '{role}' in task message: {msg}")

def task_listener(listen_host: str, listen_port: int):
    """
    Lắng nghe task từ NameNode qua TCP, mỗi message là 1 JSON.
    Khi nhận được, gọi handle_message.
    """
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind((listen_host, listen_port))
    srv.listen()
    print(f"[DataNode] Listening for task assignment at {listen_host}:{listen_port}")
    while True:
        conn, addr = srv.accept()
        with conn:
            raw = conn.recv(8192)
            if not raw:
                continue
            try:
                msg = json.loads(raw.decode('utf-8'))
                handle_message(msg)
            except Exception as e:
                print(f"[DataNode] Error handling task from {addr}: {e}")

def start_task_listener_bg(listen_host='0.0.0.0', listen_port=7000):
    """
    Chạy task_listener trên 1 thread mới (background).
    """
    t = threading.Thread(target=task_listener, args=(listen_host, listen_port), daemon=True)
    t.start()
    return t
