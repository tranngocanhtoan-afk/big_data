import sys
import time
import socket
import threading
import os
import json 
from functions_datanode import *
import requests

waiting_task = {}  # Store tasks that are waiting for processing

# Add parent directory to Python path BEFORE importing analyze_task
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)

# Now import from analyze_task
try:
    from analyze_task.processdata import process_single_file
    print("[DataNode] Successfully imported process_single_file")
except ImportError as e:
    print(f"[DataNode] Import error: {e}")
    # Fallback function if import fails
    def process_single_file(file_path, results_dir=None):
        print(f"[DataNode] Dummy processing for {file_path}")
        return None

# CLI: python datanode.py [<namenode_host>] [<namenode_port>] [<task_listen_port>]
NAMENODE_HOST      = sys.argv[1] if len(sys.argv) > 1 else '127.0.0.1'
NAMENODE_PORT      = int(sys.argv[2]) if len(sys.argv) > 2 else 5001
TASK_LISTEN_PORT   = int(sys.argv[3]) if len(sys.argv) > 3 else 7500
HEARTBEAT_INTERVAL = 10  # seconds

# Global variables for task processing
current_task = None
namenode_socket = None
processing_lock = threading.Lock()
node_id = None  # Add this global variable

# Fix: dict -> {}
dead_tasks = {}  # LIST TO STORE DEAD_TASK_needs_processing

# Add these constants (you may need to adjust these values)
UPLOAD_SERVER_HOST = '127.0.0.1'  # Adjust as needed
UPLOAD_SERVER_PORT = 8080         # Adjust as needed

def main():
    global namenode_socket, node_id
    
    # Kết nối tới NameNode để đăng ký + heartbeat
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.connect((NAMENODE_HOST, NAMENODE_PORT))
        namenode_socket = sock  # Store for task processing
        
        # Lấy node_id: "ip:port" (port chính là TASK_LISTEN_PORT)
        local_ip = sock.getsockname()[0]
        node_id = f"{local_ip}:{TASK_LISTEN_PORT}"
        print(f"[DataNode] My node_id = {node_id}")

        # Gửi register
        resp = send_message(sock, {"type": "register", "id": node_id})
        print(f"[DataNode] register → {resp}")

        # Khởi động background listener nhận task từ NameNode (luôn chạy)
        start_task_listener_bg_enhanced(listen_host='0.0.0.0', listen_port=TASK_LISTEN_PORT)
        print(f"[DataNode] Task listener started on 0.0.0.0:{TASK_LISTEN_PORT}")

        # Heartbeat loop with task status
        while True:
            time.sleep(HEARTBEAT_INTERVAL)
            
            # Send heartbeat with current task status
            heartbeat_msg = {
                "type": "heartbeat", 
                "id": node_id,
                "current_task": current_task,
                "processing_status": "processing" if current_task else "idle"
            }
            resp = send_message(sock, heartbeat_msg)
            print(f"[DataNode] heartbeat → {resp}")

def delete_block_file(block_id: str, role: str):
    """
    Xóa file block_id khỏi thư mục đúng vai trò (task hoặc storage)
    """
    file_base = get_file_base_from_block_id(block_id)
    if role == 'leader':
        path = os.path.join('task', file_base, block_id)
    elif role == 'storage':
        path = os.path.join('storage', file_base, block_id)
    else:
        print(f"[DataNode] Unknown role '{role}', cannot delete block")
        return

    if os.path.exists(path):
        os.remove(path)
        print(f"[DataNode] 🗑️ Deleted block file: {path}")
    else:
        print(f"[DataNode] ⚠️ Block file not found to delete: {path}")

def get_file_base_from_block_id(block_id: str) -> str:
    """
    Chuyển 'alogs_block1.csv' => 'alogs'
    """
    base = block_id.split('_block')[0] 
    return base

def handle_task_message(msg: dict):
    """
    Enhanced task message handler with proper workflow
    """
    global current_task, waiting_task, node_id
    
    mtype = msg.get('type')

    if mtype == 'task':
        role = msg.get('role')
        block_id = msg.get('block_id')
        file_base = msg.get('file')

        if not all([role, block_id, file_base]):
            print(f"[DataNode] Malformed task message: {msg}")
            return

        print(f"[DataNode] 📥 Received task: {role} for {block_id}")
        current_task = block_id  # Set current task
        
        if role == 'leader':
            # Tải về thư mục 'task/<file_base>/'
            dest_dir = os.path.join('task', file_base)
            success = download_block(
                server_ip=UPLOAD_SERVER_HOST,
                server_port=UPLOAD_SERVER_PORT,
                file_base=file_base,
                block_id=block_id,   
                dest_dir=dest_dir
            )
           
            if success:
                # Process the data (simulate processing)
                print(f"[DataNode] Processing {block_id}...")
                file_path = os.path.join(dest_dir, block_id)
                result_path = process_single_file(file_path)
                
                if result_path is not None:
                    # Extract the result file name and upload it
                    upload_success = upload_block_to_server(
                        server_ip=UPLOAD_SERVER_HOST,
                        server_port=UPLOAD_SERVER_PORT,
                        file_base=file_base,
                        block_id=f'{file_base}_analysis.txt',
                        block_path=result_path
                    )
                    print(f"[DataNode] Uploading result for {block_id} → {result_path} to server")
                    print(f"[DataNode] Upload success: {upload_success}")
                    print(f"[DataNode] Remove block file {block_id} from local storage")
                    
                    if upload_success:
                        # Report task completion to NameNode
                        threading.Thread(target=report_task_completion, args=(block_id,), daemon=True).start()
                        print(f"[DataNode] Task {block_id} completed successfully")
                        os.remove(file_path) 
                        
                        # Process waiting tasks (fix variable name)
                        while waiting_task:
                            block_id_waiting = next(iter(waiting_task))
                            file_base_waiting = waiting_task[block_id_waiting]
                            block_need_processing = os.path.join('storage', file_base_waiting, block_id_waiting)
                            try:
                                result_missing_block = process_single_file(block_need_processing)
                                if result_missing_block:
                                    upload_success = upload_block_to_server(
                                        server_ip=UPLOAD_SERVER_HOST,
                                        server_port=UPLOAD_SERVER_PORT,
                                        file_base=file_base_waiting,
                                        block_id=f'{file_base_waiting}_analysis.txt',
                                        block_path=result_missing_block
                                    )
                                    if upload_success:
                                        threading.Thread(target=report_task_completion, args=(block_id_waiting,), daemon=True).start()
                                        print(f"[DataNode] Task {block_id_waiting} promoted to leader and completed")
                                        os.remove(block_need_processing)
                                        del waiting_task[block_id_waiting]
                                        continue  # Xử lý tiếp task khác nếu có
                                    else:
                                        print(f"[DataNode] Failed to upload result for {block_id_waiting}, will retry after 2s.")
                                        time.sleep(2)
                                else:
                                    print(f"[DataNode] Failed to process {block_id_waiting}, removing from queue.")
                                    del waiting_task[block_id_waiting]
                            except Exception as e:
                                print(f"[DataNode] Error in processing/upload block {block_id_waiting}: {e}, will retry after 2s.")
                                time.sleep(2)
                        # Report node as free
                        threading.Thread(target=report_node_free, args=(file_base,), daemon=True).start()
                        print(f"[DataNode] Node {node_id} is free")
                    else:
                        print(f"[DataNode] Failed to upload result for {block_id}")
                else:
                    print(f"[DataNode] Failed to process {block_id}")
            else:
                print(f"[DataNode] Failed to download {block_id}")
                            
        elif role == 'storage':
            # Tải về thư mục 'storage/<file_base>/'
            dest_dir = os.path.join('storage', file_base)
            success = download_block(
                server_ip=UPLOAD_SERVER_HOST,
                server_port=UPLOAD_SERVER_PORT,
                file_base=file_base,
                block_id=block_id,
                dest_dir=dest_dir
            )
            
            if success:
                print(f"[DataNode] Storage task {block_id} completed")
            else:
                print(f"[DataNode] Failed to download storage {block_id}")
        else:
            print(f"[DataNode] Unknown role '{role}' in task message: {msg}")
    
    elif mtype == 'release':
        block_id = msg.get('block_id')
        role = msg.get('role', 'leader')  # Mặc định là leader nếu không truyền
        if block_id:
            delete_block_file(block_id, role)
            if current_task == block_id:
                current_task = None  # Clear current task
        return

    elif mtype == 'promote_to_leader':
        file_base = msg.get('file_base')  # Fix: 'file_base' -> 'file'
        block_id = msg.get('block_id')
        if block_id and file_base:
            waiting_task[block_id] = file_base  
            print(f"[DataNode] Waiting task queue now: {list(waiting_task.keys())}")

        
    else:
        print(f"[DataNode] Unknown message type: {msg}")

def task_listener_enhanced(listen_host: str, listen_port: int):
    """
    Enhanced task listener with proper message handling
    """
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind((listen_host, listen_port))
    srv.listen()
    print(f"[DataNode] Listening for task assignment at {listen_host}:{listen_port}")
    
    while True:
        try:
            conn, addr = srv.accept()
            with conn:
                raw = conn.recv(8192)
                if not raw:
                    continue
                try:
                    msg = json.loads(raw.decode('utf-8'))
                    handle_task_message(msg)
                except json.JSONDecodeError as e:
                    print(f"[DataNode] JSON decode error from {addr}: {e}")
                except Exception as e:
                    print(f"[DataNode] Error handling task from {addr}: {e}")
        except Exception as e:
            print(f"[DataNode] Error accepting connection: {e}")

def start_task_listener_bg_enhanced(listen_host='0.0.0.0', listen_port=7000):
    """
    Start enhanced task listener
    """
    t = threading.Thread(target=task_listener_enhanced, args=(listen_host, listen_port), daemon=True)
    t.start()
    return t

if __name__ == '__main__':
    main()


