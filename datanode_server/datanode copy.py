import sys
import time
import socket
import threading
import os
import json
from functions_datanode import *
import requests


# CLI: python datanode.py [<namenode_host>] [<namenode_port>] [<task_listen_port>]
NAMENODE_HOST      = sys.argv[1] if len(sys.argv) > 1 else '127.0.0.1'
NAMENODE_PORT      = int(sys.argv[2]) if len(sys.argv) > 2 else 5001
TASK_LISTEN_PORT   = int(sys.argv[3]) if len(sys.argv) > 3 else 7500
HEARTBEAT_INTERVAL = 10  # seconds

# Global variables for task processing
current_task = None
namenode_socket = None
processing_lock = threading.Lock()

def main():
    global namenode_socket
    
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

def process_task_complete(task_info):
    """
    Task processing: download block, print hello world, upload result
    """
    global current_task
    
    role = task_info.get('role')
    block_id = task_info.get('block_id')
    file_base = task_info.get('file')
    
    print(f"[DataNode] 🚀 Starting task processing: {role} for {block_id}")
    
    try:
        # Simple processing: just print hello world
        print(f"[DataNode] Hello World! Processing {block_id}")
        time.sleep(2)  # Small delay to simulate processing
        
        # Send completion status to NameNode
        send_task_completion_status(namenode_socket, block_id, role, True, TASK_LISTEN_PORT)
        
        # Reset current task
        with processing_lock:
            current_task = None
            
        print(f"[DataNode] 🎉 Task {block_id} completed successfully!")
        return True
        
    except Exception as e:
        print(f"[DataNode] ❌ Error processing task: {e}")
        send_task_completion_status(namenode_socket, block_id, role, False, TASK_LISTEN_PORT)
        with processing_lock:
            current_task = None
        return False



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
    Chuyển 'alogs_block1.csv' => 'alogs.csv'
    """
    base = block_id.split('_block')[0] 
    return base

def handle_task_message(msg: dict):
    """
    Enhanced task message handler with proper workflow
    """
    global current_task
    
    mtype = msg.get('type')

    if mtype == 'task':
        

        role = msg.get('role')
        block_id = msg.get('block_id')
        file_base = msg.get('file')

        if not all([role, block_id, file_base]):
            print(f"[DataNode] Malformed task message: {msg}")
            return

        print(f"[DataNode] 📥 Received task: {role} for {block_id}")
        
        
        if role == 'leader':
            # Tải về thư mục 'task/<file_base>/'
            dest_dir = os.path.join('task', file_base)
            success = download_block(
                server_ip=UPLOAD_SERVER_HOST,
                server_port=UPLOAD_SERVER_PORT,
                file_base=file_base, #alogs
                block_id=block_id,   
                dest_dir=dest_dir
            )
            res = analyze_block(dest_dir)
            upload_block_to_server(
            server_ip=UPLOAD_SERVER_HOST,
            server_port=UPLOAD_SERVER_PORT,
            file_base=file_base,
            block_id=block_id,
            block_path=res
        )
            
            if success:
                # Process the data (simulate processing)
                print(f"[DataNode] Processing {block_id}...")
                time.sleep(2)  # Simulate processing time
                
                # Report task completion to NameNode
                threading.Thread(target=report_task_completion, args=(block_id,), daemon=True).start()
                print(f"[DataNode] Task {block_id} completed successfully")
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
        block_id = msg['block_id']
        role = msg.get('role', 'leader')  # Mặc định là leader nếu không truyền
        delete_block_file(block_id, role)
        return

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
        conn, addr = srv.accept()
        with conn:
            raw = conn.recv(8192)
            if not raw:
                continue
            try:
                msg = json.loads(raw.decode('utf-8'))
                handle_task_message(msg)
            except Exception as e:
                print(f"[DataNode] Error handling task from {addr}: {e}")

def start_task_listener_bg_enhanced(listen_host='0.0.0.0', listen_port=7000):
    """
    Start enhanced task listener
    """
    t = threading.Thread(target=task_listener_enhanced, args=(listen_host, listen_port), daemon=True)
    t.start()
    return t

if __name__ == '__main__':
    main()
    

