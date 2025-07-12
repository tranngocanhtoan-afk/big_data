# functions_datanode.py

import socket
import threading
import json
import os
import requests
import time

# Cấu hình địa chỉ của Upload-Server (có thể override từ datanode.py nếu cần)
UPLOAD_SERVER_HOST = '192.168.1.14'
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
                   file_base: str,  #alogs
                   block_id: str,  #alogs_block1.csv
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
        resp = requests.get(url, stream=True )
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



def report_task_completion(block_id: str):
    """
    Report task completion to NameNode
    """
    try:
        # Connect to NameNode
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.connect(('127.0.0.1', 5001))  # NameNode address
            
            # Get node_id (we need to know the port)
            # For now, we'll use a placeholder - in real implementation, 
            # this should be passed from the main datanode.py
            node_id = f"127.0.0.1:7500"  # This should be dynamic
            
            # Send completion message
            completion_msg = {
                "type": "task_complete",
                "id": node_id,
                "block_id": block_id
            }
            resp = send_message(sock, completion_msg)
            print(f"[DataNode] Task completion reported → {resp}")
            
    except Exception as e:
        print(f"[DataNode] Failed to report task completion: {e}")

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

def start_task_listener_bg(listen_host='0.0.0.0', listen_port=7500):
    """
    Chạy task_listener trên 1 thread mới (background).
    """
    t = threading.Thread(target=task_listener, args=(listen_host, listen_port), daemon=True)
    t.start()
    return t

def report_node_free(file_base: str):
    """
    Report task completion to NameNode
    """
    node_id = f"127.0.0.1:7500" 
    try:
        # Connect to NameNode
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.connect(('127.0.0.1', 5001))  # NameNode address
            
            # Get node_id (we need to know the port)
            # For now, we'll use a placeholder - in real implementation, 
            # this should be passed from the main datanode.py  # This should be dynamic
            # Send completion message
            free_msg = {
                "type": "node_free",
                "node_id": node_id,
                "file": file_base
            }
            resp = send_message(sock, free_msg)
            print(f"[DataNode] Free node reported → {resp}")
            
    except Exception as e:
        print(f"[DataNode] Failed to report task completion: {e}")

def upload_block_to_server(server_ip, server_port, file_base, block_id, block_path):
    """
    Upload a block file (CSV or result file) to the Upload-Server.
    - server_ip/server_port: Flask server address
    - file_base: original file name (without .csv)
    - block_id: block file name (e.g., alogs_block1.csv or alogs_block1_analysis.txt)
    - block_path: path to the file on DataNode
    """
    url = f"http://{server_ip}:{server_port}/upload_block"
    try:
        # Determine content type based on file extension
        content_type = 'text/plain' if block_path.endswith('.txt') else 'text/csv'
        
        with open(block_path, 'rb') as f:
            files = {'file': (block_id, f, content_type)}
            data = {'file_base': file_base, 'block_id': block_id}
            resp = requests.post(url, files=files, data=data, timeout=20)
        
        if resp.status_code == 200:
            print(f"[DataNode] ✓ Uploaded {block_id} to server")
            return True
        else:
            print(f"[DataNode] ✗ Upload failed: {resp.text}")
            return False
    except Exception as e:
        print(f"[DataNode] ✗ Upload error: {e}")
        return False

def print_file_header(dest_dir: str, file_base: str, block_id: str):
    """
    Print header of the downloaded file
    """
    try:
        file_path = os.path.join(dest_dir, file_base)
        if os.path.exists(file_path):
            with open(file_path, 'r', encoding='utf-8') as f:
                # Read first few lines to show header
                lines = []
                for i, line in enumerate(f):
                    if i < 5:  # Show first 5 lines
                        lines.append(line.strip())
                        
                    else:
                        break
                print(f"[DataNode] 📄 File header for {block_id}:")
                for i, line in enumerate(lines):
                    print(f"  Line {i+1}: {line}")
                print(f"[DataNode] 📄 End of header for {block_id}")
        else:
            print(f"[DataNode] ⚠️ File not found: {file_path}")
    except Exception as e:
        print(f"[DataNode] ❌ Error reading file header: {e}")

def send_processing_heartbeat(namenode_socket, block_id: str, status: str, task_listen_port: int):
    """
    Send heartbeat to NameNode while processing
    """
    try:
        if namenode_socket:
            heartbeat_msg = {
                "type": "heartbeat",
                "id": f"{socket.gethostbyname(socket.gethostname())}:{task_listen_port}",
                "current_task": block_id,
                "processing_status": status
            }
            resp = send_message(namenode_socket, heartbeat_msg)
            print(f"[DataNode] 💓 Processing heartbeat sent → {resp}")
    except Exception as e:
        print(f"[DataNode] ❌ Failed to send processing heartbeat: {e}")

def generate_result_file(block_id: str, file_base: str) -> str:
    """
    Generate result file from processed data
    """
    results_dir = os.path.join('conclude', file_base)
    os.makedirs(results_dir, exist_ok=True)
    result_filename = f"{block_id}_result.txt"
    result_path = os.path.join(results_dir, result_filename)
    with open(result_path, 'w') as f:
        f.write(f"Processing result for {block_id}\n")
        f.write(f"Processed at: {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Status: COMPLETED\n")
        f.write(f"Data summary: Sample processed data from {block_id}\n")
    print(f"[DataNode] 📄 Generated result file: {result_path}")
    return result_path

def send_task_completion_status(namenode_socket, block_id: str, role: str, success: bool, task_listen_port: int):
    """
    Send task completion status to NameNode
    """
    try:
        if namenode_socket:
            completion_msg = {
                "type": "task_complete",
                "id": f"{socket.gethostbyname(socket.gethostname())}:{task_listen_port}",
                "block_id": block_id,
                "role": role,
                "success": success,
                "timestamp": time.time()
            }
            resp = send_message(namenode_socket, completion_msg)
            print(f"[DataNode] 📤 Task completion status sent → {resp}")
    except Exception as e:
        print(f"[DataNode] ❌ Failed to send completion status: {e}")