import psycopg2
from psycopg2 import pool, sql
from config import DB
import time
import socket
import json

# ─── Connection Pool ───────────────────────────────────────────────────────────
# Khởi connection pool khi module load
# minconn=1, maxconn=10 (tùy nhu cầu)
db_pool = psycopg2.pool.SimpleConnectionPool(
    minconn=1,
    maxconn=10,
    **DB
)


def get_pooled_conn():
    """
    Lấy connection từ pool, tắt synchronous_commit để COMMIT nhanh.
    Caller phải putconn lại sau khi xong.
    """
    conn = db_pool.getconn()
    conn.autocommit = False
    with conn.cursor() as cur:
        cur.execute("SET LOCAL synchronous_commit = OFF;")
    return conn


# ─── Metadata Table ──────────────────────────────────────────────────────────

def init_active_node_manager_table():
    """
    Tạo bảng active_node_manager nếu chưa tồn tại:
      - node_id TEXT PRIMARY KEY
      - status  VARCHAR(10) NOT NULL
      - task    TEXT DEFAULT 'free'
      - storage TEXT
    """
    conn = get_pooled_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS active_node_manager (
                    node_id TEXT PRIMARY KEY,
                    status  VARCHAR(10) NOT NULL,
                    task    TEXT DEFAULT 'free',
                    storage TEXT
                );
            """)
        conn.commit()
    finally:
        db_pool.putconn(conn)


def upsert_node(node_id: str, status: str = 'alive'):
    """
    Chèn mới hoặc cập nhật status của datanode.
    """
    conn = get_pooled_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                sql.SQL("""
                    INSERT INTO active_node_manager (node_id, status)
                    VALUES (%s, %s)
                    ON CONFLICT (node_id) DO UPDATE
                      SET status = EXCLUDED.status
                """),
                (node_id, status)
            )
        conn.commit()
    finally:
        db_pool.putconn(conn)


def remove_node(node_id: str):
    """
    Xóa entry datanode theo node_id.
    """
    conn = get_pooled_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                sql.SQL("DELETE FROM active_node_manager WHERE node_id = %s"),
                (node_id,)
            )
        conn.commit()
    finally:
        db_pool.putconn(conn)


def has_free_node() -> bool:
    """
    Trả về True nếu còn ít nhất 1 datanode alive mà đang free (task='free').
    """
    conn = psycopg2.connect(**DB)
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(*)
                  FROM active_node_manager
                 WHERE status='alive'
                   AND task = 'free';
            """)
            return cur.fetchone()[0] > 0
    finally:
        conn.close()


# ─── File DB Connection ──────────────────────────────────────────────────────

def get_file_conn(file_base: str):
    """
    Kết nối tới database tương ứng file_base.
    """
    params = DB.copy()
    params['dbname'] = file_base
    return psycopg2.connect(**params)


def get_file_block_ids(file_base: str) -> list[str]:
    """
    Lấy danh sách block_id từ bảng file_base.
    """
    conn = get_file_conn(file_base)
    try:
        with conn.cursor() as cur:
            tbl = sql.Identifier(file_base)
            cur.execute(
                sql.SQL("SELECT block_id FROM {} ORDER BY block_id;").format(tbl)
            )
            return [row[0] for row in cur.fetchall()]
    finally:
        conn.close()


# ─── Task Assignment ──────────────────────────────────────────────────────────

def assign_task_auto(task: str) -> bool:
    """
    Tự động chọn 1 leader free + 2 followers ít storage,
    cập nhật task/storage trên active_node_manager
    và cập nhật leader/followers/status trên bảng block-manager.
    Trả về True nếu thành công, False nếu không có node free.
    """
    print("run assigntask auto")
    # 1) Kiểm tra free node
    if not has_free_node():
        print("no free node")
        return False

    # 2) Chọn leader & followers
    conn_meta = psycopg2.connect(**DB)
    try:
        with conn_meta.cursor() as cur:
            cur.execute("""
                SELECT node_id FROM active_node_manager
                 WHERE status='alive' AND task='free'
                 ORDER BY node_id LIMIT 1;
            """)
            row = cur.fetchone()
            leader = row[0]
            cur.execute("""
                SELECT node_id FROM active_node_manager
                 WHERE status='alive' AND node_id <> %s
                 ORDER BY
                   CASE WHEN storage IS NULL OR storage = '' THEN 0
                        ELSE cardinality(string_to_array(storage, ',')) END
                 LIMIT 2;
            """, (leader,))
            followers = [r[0] for r in cur.fetchall()]
    finally:
        conn_meta.close()

    # 3) Cập nhật metadata (chờ nếu không free? Already checked)
    conn_meta = psycopg2.connect(**DB)
    try:
        with conn_meta.cursor() as cur:
            cur.execute(
                "UPDATE active_node_manager SET task=%s WHERE node_id=%s;",
                (task, leader)
            )
            for nd in followers:
                cur.execute(
                    """
                    UPDATE active_node_manager SET
                      storage = CASE
                                  WHEN storage IS NULL OR storage = ''
                                    THEN %s
                                  ELSE storage || ',' || %s
                                END
                     WHERE node_id = %s;
                    """, (task, task, nd)
                )
        conn_meta.commit()
    finally:
        conn_meta.close()

    # 4) Cập nhật block-manager table
    file_base = task.rsplit('.csv',1)[0].rsplit('_block',1)[0]
    conn_file = get_file_conn(file_base)
    try:
        with conn_file.cursor() as cur:
            tbl = sql.Identifier(file_base)
            cur.execute(
                sql.SQL("""
                    UPDATE {} SET
                      leader    = %s,
                      followers = %s,
                      status    = 'processing'
                     WHERE block_id = %s;
                """).format(tbl),
                (leader, followers, task)
            )
        conn_file.commit()
    finally:
        conn_file.close()


    send_to_datanode(leader, {
        'type': 'task',
        'role': 'leader',
        'block_id': task,  #task la alogs_block1.csv
        'file': file_base   #filebase o day la alogs
    })
    for nd in followers:
        send_to_datanode(nd, {
            'type': 'task',
            'role': 'storage',
            'block_id': task,
            'file': file_base
        })

    return True


def send_to_datanode(node_id: str, payload: dict):
    """
    Mở kết nối tới DataNode và gửi payload JSON.
    node_id có định dạng 'host_port', ví dụ '127.0.0.1_6001'.
    """
    host, port = node_id.split(':')
    port = int(port)
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((host, port))
        s.sendall(json.dumps(payload).encode('utf-8'))
        # (nếu DataNode cần ACK, bạn có thể đọc lại ở đây)


def get_table_name_from_block_id(block_id: str) -> str:
    """
    Chuyển block_id ('alogs_block1.csv') -> table_name ('alogs')
    """
    file_base = block_id.rsplit('.csv', 1)[0].rsplit('_block', 1)[0]
    table_name = file_base.replace('.', '_')
    return table_name

def reassign_leader_on_disconnect(old_leader_id: str):
    """
    Khi node leader disconnect, chuyển quyền leader cho follower đầu tiên (nếu có),
    và remove node này khỏi followers của tất cả các block trong mọi file DB.
    """
    import psycopg2
    from psycopg2 import sql
    import glob, os

    print(f"[DEBUG] === Starting reassign_leader_on_disconnect ===")
    print(f"[DEBUG] Old leader ID: {old_leader_id}")

    conn_meta = None
    try:
        print(f"[DEBUG] Connecting to metadata database...")
        conn_meta = psycopg2.connect(**DB)
        print(f"[DEBUG] Successfully connected to metadata database")

        # === 1. Kiểm tra node đang làm leader block nào không ===
        block_id = None
        file_base = None
        table = None
        new_leader = None
        try:
            with conn_meta.cursor() as cur:
                cur.execute("SELECT task FROM active_node_manager WHERE node_id = %s", (old_leader_id,))
                row = cur.fetchone()
                if row and row[0] and row[0] != 'free':
                    block_id = row[0]
                    file_base = block_id.rsplit('.csv', 1)[0].rsplit('_block', 1)[0]
                    table = file_base
        except Exception as e:
            print(f"[ERROR] Metadata query failed: {e}")

        # === 2. Nếu đang làm leader, chuyển quyền leader cho follower đầu tiên ===
        if block_id and file_base:
            print(f"[DEBUG] Node is leader of block {block_id}, file_base {file_base}")

            try:
                conn_file = get_file_conn(file_base)
                with conn_file.cursor() as cur_file:
                    cur_file.execute(
                        sql.SQL("SELECT followers FROM {} WHERE block_id = %s").format(sql.Identifier(table)),
                        (block_id,)
                    )
                    result = cur_file.fetchone()
                    followers = result[0] if result and result[0] else []

                    if followers:
                        new_leader = followers[0]
                        new_followers = followers[1:] if len(followers) > 1 else []

                        # Update leader, followers của block này
                        cur_file.execute(
                            sql.SQL("UPDATE {} SET leader = %s, followers = %s, status = 'pending' WHERE block_id = %s")
                            .format(sql.Identifier(table)),
                            (new_leader, new_followers, block_id)
                        )
                        conn_file.commit()

                        # Update active_node_manager: free old leader, assign task cho new leader
                        with conn_meta.cursor() as cur:
                            cur.execute("UPDATE active_node_manager SET task = 'free' WHERE node_id = %s", (old_leader_id,))
                            cur.execute("UPDATE active_node_manager SET task = %s WHERE node_id = %s", (block_id, new_leader))
                            conn_meta.commit()

                        print(f"[DEBUG] Reassigned {block_id}: new leader = {new_leader}, followers = {new_followers}")

                        # Notify new leader
                        try:
                            send_to_datanode(new_leader, {
                                'type': 'promote_to_leader',
                                'block_id': block_id,
                                'file_base': file_base,
                            })
                            print(f"[DEBUG] Promotion notification sent to {new_leader}")
                        except Exception as e:
                            print(f"[WARNING] Failed to notify new leader {new_leader}: {e}")
                    else:
                        print(f"[DEBUG] Block {block_id} has no followers to promote.")
                conn_file.close()
            except Exception as e:
                print(f"[ERROR] FileDB leader reassignment error: {e}")

        # === 3. Remove old_leader_id khỏi mọi followers trong toàn bộ file DB ===
        print(f"[DEBUG] Start removing {old_leader_id} from all followers lists in ALL files...")
        db_folder = "."  # Change this if your databases are in a subfolder
        db_files = glob.glob(os.path.join(db_folder, "*.db")) + glob.glob(os.path.join(db_folder, "*.sqlite")) + glob.glob(os.path.join(db_folder, "*.pgsql")) + glob.glob(os.path.join(db_folder, "*")) # edit pattern if needed

        # Nếu dùng Postgres multi-database (thường mỗi file_base là 1 database tên 'alogs', 'plogs'...), có thể quét qua danh sách tên file_base hoặc lưu lại ở đâu đó
        # Ở đây ví dụ: bạn có một list các file_base (table/database) cần check, hoặc scan qua các file_name trong thư mục dữ liệu nếu dùng SQLite
        # Nếu Postgres, có thể truy vấn pg_database để lấy danh sách db, rồi lặp

        # Dưới đây là ví dụ quét qua các file_base đã biết
        # Sửa lại đoạn này cho phù hợp hệ thống của bạn!
        all_file_bases = []
        try:
            # Cách 1: Lưu sẵn danh sách file_base ở config hoặc db
            # all_file_bases = ['alogs', 'plogs', ...] 
            
            # Cách 2: Truy vấn tất cả db từ Postgres (nếu bạn dùng multi-db)
            with psycopg2.connect(**DB) as conn_sys:
                with conn_sys.cursor() as cur:
                    cur.execute("SELECT datname FROM pg_database WHERE datistemplate = false AND datname NOT IN ('postgres')")  # exclude template DB
                    all_file_bases = [row[0] for row in cur.fetchall()]

            print(f"[DEBUG] All file_bases found: {all_file_bases}")
        except Exception as e:
            print(f"[WARNING] Could not get all file_bases: {e}")

        for file_base in all_file_bases:
            print(f"[DEBUG] Processing file_base {file_base}")
            try:
                conn_file = get_file_conn(file_base)
                with conn_file.cursor() as cur_file:
                    # Xóa node khỏi followers ở mọi block
                    cur_file.execute(
                        sql.SQL("UPDATE {} SET followers = array_remove(followers, %s) WHERE %s = ANY(followers)")
                        .format(sql.Identifier(file_base)),
                        (old_leader_id, old_leader_id)
                    )
                    affected = cur_file.rowcount
                    if affected:
                        print(f"[DEBUG] Removed {old_leader_id} from followers in {affected} blocks of {file_base}")
                conn_file.commit()
                conn_file.close()
            except Exception as e:
                print(f"[WARNING] Could not remove {old_leader_id} from followers in {file_base}: {e}")

    except Exception as e:
        print(f"[ERROR] Exception in reassign_leader_on_disconnect: {e}")
        if conn_meta:
            conn_meta.rollback()
        raise
    finally:
        if conn_meta:
            conn_meta.close()
        print(f"[DEBUG] === End reassign_leader_on_disconnect ===")
