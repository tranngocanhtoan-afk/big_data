import sys
import time
import socket
import threading
from functions_datanode import *
import requests

# CLI: python datanode.py [<namenode_host>] [<namenode_port>] [<task_listen_port>]
NAMENODE_HOST      = sys.argv[1] if len(sys.argv) > 1 else '127.0.0.1'
NAMENODE_PORT      = int(sys.argv[2]) if len(sys.argv) > 2 else 5001
TASK_LISTEN_PORT   = int(sys.argv[3]) if len(sys.argv) > 3 else 7000
HEARTBEAT_INTERVAL = 10  # seconds

def main():
    # Khởi động background listener nhận task từ NameNode (luôn chạy)
    start_task_listener_bg(listen_host='0.0.0.0', listen_port=TASK_LISTEN_PORT)
    print(f"[DataNode] Task listener started on 0.0.0.0:{TASK_LISTEN_PORT}")

    # Kết nối tới NameNode để đăng ký + heartbeat
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.connect((NAMENODE_HOST, NAMENODE_PORT))
        # Lấy node_id: "ip:port" (port chính là TASK_LISTEN_PORT)
        local_ip = sock.getsockname()[0]
        node_id = f"{local_ip}:{TASK_LISTEN_PORT}"
        print(f"[DataNode] My node_id = {node_id}")

        # Gửi register
        resp = send_message(sock, {"type": "register", "id": node_id})
        print(f"[DataNode] register → {resp}")

        download_block(server_ip ="192.168.1.14", 
                   server_port = 5000, 
                   file_base = "alogs", 
                   block_id = "alogs_block1.csv", 
                   dest_dir = os.path.join("task", "alogs.csv"))

        # Heartbeat loop
        while True:
            time.sleep(HEARTBEAT_INTERVAL)
            resp = send_message(sock, {"type": "heartbeat", "id": node_id})
            print(f"[DataNode] heartbeat → {resp}")
        
        

        
if __name__ == '__main__':
    main()
    

