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

# Node status tracking
node_statuses = {}  # { node_id: {status, current_task, processing_status, last_heartbeat} }
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
                            # assign tasks for all blocks of this file in parallel
                            process_file_tasks_parallel(file_base)
                            resp = {'status':'ok', 'file': file_base}
                            conn.sendall(json.dumps(resp).encode('utf-8'))
                            print(f"[NameNode] Computed tasks for '{file_base}'")
                        except Exception as e:
                            err = {'status':'error', 'error': str(e)}
                            conn.sendall(json.dumps(err).encode('utf-8'))
                            print(f"[NameNode] Compute error: {e}")

                    elif typ == 'task_complete':
                        # DataNode reports task completion with enhanced status
                        block_id = msg.get('block_id')
                        role = msg.get('role', 'leader')
                        success = msg.get('success', True)
                        timestamp = msg.get('timestamp', time.time())
                        node_id= msg.get("id")
                        
                        try:
                            print("[NAMENODE] start handle task complete")
                            if success:
                                handle_task_completion(node_id, block_id, role)
                                
                                conn.sendall(b'{"status":"task_complete_ack"}')
                                print(f"[NameNode] ✅ Task completion acknowledged for {node_id}: {block_id} ({role})")
                                print("NAMENODE Handle successfully")
                            else:
                                handle_task_failure(node_id, block_id, role)
                                conn.sendall(b'{"status":"task_failed_ack"}')
                                print(f"[NameNode] ❌ Task failure acknowledged for {node_id}: {block_id} ({role})")
                            print("auto assign tasks")
                        except Exception as e:
                            err = {'status':'error', 'error': str(e)}
                            conn.sendall(json.dumps(err).encode('utf-8'))
                            print(f"[NameNode] Task completion error: {e}")

                    elif typ == 'heartbeat':
                        # Enhanced heartbeat with task status and processing status
                        if node_id in datanodes:
                            datanodes[node_id] = time.time()
                            upsert_node(node_id, 'alive')
                            
                            # Update task status if provided
                            current_task = msg.get('current_task')
                            processing_status = msg.get('processing_status', 'idle')
                            
                            if current_task:
                                update_node_task_status(node_id, current_task)
                            
                            # Update processing status in node_statuses
                            if node_id not in node_statuses:
                                node_statuses[node_id] = {
                                    "status": "active",
                                    "current_task": current_task,
                                    "processing_status": processing_status,
                                    "last_heartbeat": time.time()
                                }
                            else:
                                node_statuses[node_id]["current_task"] = current_task
                                node_statuses[node_id]["processing_status"] = processing_status
                                node_statuses[node_id]["last_heartbeat"] = time.time()
                            
                            conn.sendall(b'{"status":"alive"}')
                            print(f"[NameNode] 💓 Heartbeat from '{node_id}' - Task: {current_task} - Processing: {processing_status}")
                        else:
                            conn.sendall(b'{"status":"unknown_node"}')

                    else:
                        # unknown message type
                        conn.sendall(b'{"status":"bad_request"}')

            except Exception as e:
                print(f"[NameNode] Connection error: {e}")
                break

        # connection closed: clean up any dead nodes
        print(f"[NameNode] Disconnected {addr}")
      
def handle_task_completion(node_id: str, block_id: str, role: str = 'leader'):
    """
    Handle task completion - reset node status and immediately assign new task if available
    """
    conn_meta = psycopg2.connect(**DB)
    try:
        with conn_meta.cursor() as cur:
            print(f"[DEBUG] Starting leader task completion for node_id: {node_id}, block_id: {block_id}")
                
                # Reset leader node status to free
            cur.execute(
                    "UPDATE active_node_manager SET task = 'free' WHERE node_id = %s",
                    (node_id,)
                )
        
            print(f"[DEBUG] Updated node {node_id} status to 'free' in active_node_manager")
                
            send_release_to_datanode(node_id, block_id, role = 'leader')
            print(f"[DEBUG] Sent release signal to datanode {node_id} for block {block_id} (leader role)")
                
                # Get followers for this block to reset their storage
            file_base = block_id.rsplit('.csv',1)[0].rsplit('_block',1)[0]
            print(f"[DEBUG] Extracted file_base: {file_base} from block_id: {block_id}")
                
            conn_file = get_file_conn(file_base)
            try:
                with conn_file.cursor() as cur_file:
                    tbl = sql.Identifier(file_base)
                    cur_file.execute(
                            sql.SQL("SELECT followers FROM {} WHERE block_id = %s").format(tbl),
                            (block_id,)
                        )
                    result = cur_file.fetchone()
                    followers = result[0] if result and result[0] else []
                    print(f"[DEBUG] Retrieved followers for block {block_id}: {followers}")
                        
                        # Update block status to completed
                    cur_file.execute(
                            sql.SQL("""
                                UPDATE {} SET status = 'completed',
                                    leader = NULL,
                                    followers = NULL
                                WHERE block_id = %s;
                                 
                            """).format(tbl),
                            (block_id,)
                        )
                    print(f"[DEBUG] Updated block {block_id} status to 'completed' and cleared leader/followers")
                    conn_file.commit()
                    print(f"[DEBUG] Successfully committed changes to file database for {file_base}")
            except Exception as e:
                    print(f"[DEBUG ERROR] Exception in file database operations: {e}")
                    raise
            finally:
                    conn_file.close()
                    print(f"[DEBUG] Closed file database connection for {file_base}")
                
                # Reset storage for followers (remove this block from their storage)
            for follower in followers:
                send_release_to_datanode(follower, block_id, role = 'storage')
                cur.execute(
                        """
                        UPDATE active_node_manager SET
                          storage = CASE
                                      WHEN storage = %s THEN ''
                                      WHEN storage LIKE %s THEN REPLACE(storage, %s, '')
                                      WHEN storage LIKE %s THEN REPLACE(storage, %s, '')
                                      ELSE storage
                                    END
                         WHERE node_id = %s;
                        """, 
                    (block_id, f"{block_id},%", f"{block_id},", f"%,{block_id}", f",{block_id}", follower)
                    )
                    
                print(f"[NameNode] Task {block_id} completed by {node_id} (leader), node reset to free")
                if followers:
                    print(f"[NameNode] Reset storage for followers: {followers}")
                
                # Immediately try to assign new task to this node
                assign_next_available_task(node_id, file_base)
                
            
        conn_meta.commit()
        
    finally:
        conn_meta.close()


def send_release_to_datanode(node_id: str, block_id: str, role: str):
    """
    Gửi tín hiệu yêu cầu DataNode xóa block_id với vai trò (leader/storage)
    node_id: dạng '127.0.0.1:7000'
    """
    host, port = node_id.split(':')
    port = int(port)
    msg = {
        "type": "release",     # loại message
        "block_id": block_id,  # tên block
        "role": role           # 'leader' hoặc 'storage'
    }
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((host, port))
            s.sendall(json.dumps(msg).encode('utf-8'))
        print(f"[NameNode] Đã gửi release block {block_id} ({role}) đến {node_id}")
    except Exception as e:
        print(f"[NameNode] Gửi release block lỗi: {e}")

        
def handle_task_failure(node_id: str, block_id: str, role: str = 'leader'):
    """
    Handle task failure - reassign task to another node
    """
    print(f"[NameNode] ❌ Task {block_id} failed by {node_id} ({role})")
    
    if role == 'leader':
        # Reset node status to free
        conn_meta = psycopg2.connect(**DB)
        try:
            with conn_meta.cursor() as cur:
                cur.execute(
                    "UPDATE active_node_manager SET task = 'free' WHERE node_id = %s",
                    (node_id,)
                )
            conn_meta.commit()
        finally:
            conn_meta.close()
        
        # Try to reassign the failed task to another node
        file_base = block_id.rsplit('.csv',1)[0].rsplit('_block',1)[0]
        print(f"[NameNode] 🔄 Attempting to reassign failed task {block_id}")
        
        # Reset block status to pending for reassignment
        conn_file = get_file_conn(file_base)
        try:
            with conn_file.cursor() as cur:
                tbl = sql.Identifier(file_base)
                cur.execute(
                    sql.SQL("""
                        UPDATE {} SET 
                          leader = NULL,
                          followers = NULL,
                          status = 'pending'
                         WHERE block_id = %s;
                    """).format(tbl),
                    (block_id,)
                )
            conn_file.commit()
        finally:
            conn_file.close()
        
        # Try to assign to another available node
        assign_next_available_task(node_id, file_base)

def update_node_task_status(node_id: str, current_task: str):
    """
    Update node's current task status in database
    """
    try:
        conn_meta = psycopg2.connect(**DB)
        with conn_meta.cursor() as cur:
            if current_task:
                cur.execute(
                    "UPDATE active_node_manager SET task = %s WHERE node_id = %s",
                    (current_task, node_id)
                )
            else:
                cur.execute(
                    "UPDATE active_node_manager SET task = 'free' WHERE node_id = %s",
                    (node_id,)
                )
        conn_meta.commit()
    except Exception as e:
        print(f"[NameNode] Error updating task status: {e}")
    finally:
        if 'conn_meta' in locals():
            conn_meta.close()

def assign_next_available_task(node_id: str, file_base: str):
    """
    Immediately assign the next available unassigned block to this node
    """
    try:
        # Get unassigned blocks for this file
        unassigned_blocks = get_unassigned_blocks(file_base)
        
        if unassigned_blocks:
            next_block = unassigned_blocks[0]
            if assign_task_to_node(node_id, next_block):
                print(f"[NameNode] ✅ Immediately assigned {next_block} to {node_id}")
            else:
                print(f"[NameNode] ❌ Failed to assign {next_block} to {node_id}")
        else:
            print(f"[NameNode] No more unassigned blocks for {file_base}")
            
    except Exception as e:
        print(f"[NameNode] Error assigning next task: {e}")

def get_unassigned_blocks(file_base: str) -> list:
    """
    Get list of unassigned blocks for a file
    """
    try:
        conn_file = get_file_conn(file_base)
        with conn_file.cursor() as cur:
            tbl = sql.Identifier(file_base)
            cur.execute(
                sql.SQL("SELECT block_id FROM {} WHERE (leader IS NULL OR leader = '') AND status = 'pending' ORDER BY block_id").format(tbl)

            )
            return [row[0] for row in cur.fetchall()]
    except Exception as e:
        print(f"[NameNode] Error getting unassigned blocks: {e}")
        return []
    finally:
        if 'conn_file' in locals():
            conn_file.close()

def assign_task_to_node(node_id: str, block_id: str) -> bool:
    """
    Assign a specific block to a specific node
    """
    try:
        conn_meta = psycopg2.connect(**DB)
        with conn_meta.cursor() as cur:
            # Check if node is free
            cur.execute(
                "SELECT task FROM active_node_manager WHERE node_id = %s",
                (node_id,)
            )
            result = cur.fetchone()
            if not result or result[0] != 'free':
                return False
            
            # Select followers for backup (nodes with least storage load)
            cur.execute("""
                SELECT node_id FROM active_node_manager
                 WHERE status='alive' AND node_id <> %s
                 ORDER BY
                   CASE WHEN storage IS NULL OR storage = '' THEN 0
                        ELSE cardinality(string_to_array(storage, ',')) END
                 LIMIT 2;
            """, (node_id,))
            followers = [r[0] for r in cur.fetchall()]
            
            # Assign task to leader node
            cur.execute(
                "UPDATE active_node_manager SET task = %s WHERE node_id = %s",
                (block_id, node_id)
            )
            
            # Assign storage task to followers
            for follower in followers:
                cur.execute(
                    """
                    UPDATE active_node_manager SET
                      storage = CASE
                                  WHEN storage IS NULL OR storage = ''
                                    THEN %s
                                  ELSE storage || ',' || %s
                                END
                     WHERE node_id = %s;
                    """, (block_id, block_id, follower)
                )
                
        conn_meta.commit()
        
        # Update block status with leader and followers
        file_base = block_id.rsplit('.csv',1)[0].rsplit('_block',1)[0]
        conn_file = get_file_conn(file_base)
        try:
            with conn_file.cursor() as cur_file:
                tbl = sql.Identifier(file_base)
                cur_file.execute(
                    sql.SQL("""
                        UPDATE {} SET
                          leader    = %s,
                          followers = %s,
                          status    = 'processing'
                         WHERE block_id = %s;
                    """).format(tbl),
                    (node_id, followers, block_id)
                )
            conn_file.commit()
        finally:
            conn_file.close()
        
        # Send leader task to main node
        send_to_datanode(node_id, {
            'type': 'task',
            'role': 'leader',
            'block_id': block_id,
            'file': file_base  #alogs
        })
        
        # Send storage tasks to followers
        for follower in followers:
            send_to_datanode(follower, {
                'type': 'task',
                'role': 'storage',
                'block_id': block_id,
                'file': file_base
            })
        
        print(f"[NameNode] Assigned {block_id} to {node_id} with {len(followers)} followers: {followers}")
        
        return True
        
    except Exception as e:
        print(f"[NameNode] Error assigning task to node: {e}")
        return False
    finally:
        if 'conn_meta' in locals():
            conn_meta.close()

def process_file_tasks_parallel(file_base: str):
    """
    PARALLEL: Assign all available blocks to all available nodes immediately
    No waiting for completion - nodes will get new tasks as they finish
    """
    block_ids = get_file_block_ids(file_base)
    print(f"[NameNode] 🚀 Starting PARALLEL processing of {len(block_ids)} blocks for {file_base}")
    
    # Get all free nodes
    conn_meta = psycopg2.connect(**DB)
    try:
        with conn_meta.cursor() as cur:
            cur.execute("""
                SELECT node_id FROM active_node_manager
                 WHERE status='alive' AND task='free'
                 ORDER BY node_id;
            """)
            free_nodes = [r[0] for r in cur.fetchall()]
    finally:
        conn_meta.close()
    
    print(f"[NameNode] Found {len(free_nodes)} free nodes: {free_nodes}")
    
    # Assign blocks to available nodes immediately
    assigned_count = 0
    for i, block_id in enumerate(block_ids):
        if i < len(free_nodes):
            # Assign to available node
            if assign_task_to_node(free_nodes[i], block_id):
                assigned_count += 1
                print(f"[NameNode] ✅ Assigned {block_id} to {free_nodes[i]}")
            else:
                print(f"[NameNode] ❌ Failed to assign {block_id} to {free_nodes[i]}")
        else:
            print(f"[NameNode] ⏳ No more free nodes for {block_id} - will be assigned when nodes complete tasks")
    
    print(f"[NameNode] Initial parallel assignment: {assigned_count}/{len(block_ids)} blocks assigned")
    
    # Start background thread to handle remaining blocks
    if assigned_count < len(block_ids):
        threading.Thread(target=process_remaining_blocks_parallel, 
                        args=(file_base, block_ids), 
                        daemon=True).start()

def process_remaining_blocks_parallel(file_base: str, all_block_ids: list):
    """
    Background thread to continuously assign remaining blocks as nodes become free
    """
    print(f"[NameNode] 🔄 Background processor started for {file_base}")
    
    while True:
        time.sleep(2)  # Check every 2 seconds
        
        # Get remaining unassigned blocks
        remaining_blocks = get_unassigned_blocks(file_base)
        
        if not remaining_blocks:
            print(f"[NameNode] ✅ All blocks for {file_base} have been assigned")
            break
        
        # Get free nodes
        conn_meta = psycopg2.connect(**DB)
        try:
            with conn_meta.cursor() as cur:
                cur.execute("""
                    SELECT node_id FROM active_node_manager
                     WHERE status='alive' AND task='free'
                     ORDER BY node_id;
                """)
                free_nodes = [r[0] for r in cur.fetchall()]
        finally:
            conn_meta.close()
        
        if free_nodes and remaining_blocks:
            print(f"[NameNode] 🔄 Found {len(free_nodes)} free nodes, {len(remaining_blocks)} remaining blocks")
            
            # Assign blocks to free nodes
            for i, node_id in enumerate(free_nodes):
                if i < len(remaining_blocks):
                    block_id = remaining_blocks[i]
                    if assign_task_to_node(node_id, block_id):
                        print(f"[NameNode] ✅ Late assignment: {block_id} → {node_id}")
                    else:
                        print(f"[NameNode] ❌ Failed late assignment: {block_id} → {node_id}")

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
                    reassign_leader_on_disconnect(node)
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
