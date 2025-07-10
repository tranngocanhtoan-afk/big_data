import psycopg2
from psycopg2 import pool, sql
from config import DB
import time

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
        'block_id': task,
        'file': file_base
    })
    for nd in followers:
        send_to_datanode(nd, {
            'type': 'task',
            'role': 'storage',
            'block_id': task,
            'file': file_base
        })

    return True


def process_file_tasks(file_base: str, poll_interval: float = 2.0) -> None:
    """
    Với mỗi block trong file_base, chờ đến khi có node free rồi gọi assign_task_auto.
    """
    block_ids = get_file_block_ids(file_base)
    for blk in block_ids:
        # đợi cho đến khi assign thành công
        while True:
            if assign_task_auto(blk):
                print(f"Assigned {blk}")
                break
            # chưa có node free, chờ
            time.sleep(poll_interval)


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
    Khi node leader disconnect, chuyển quyền leader cho follower đầu tiên,
    cập nhật lại bảng block-manager và active_node_manager.
    """
    conn_meta = psycopg2.connect(**DB)
    try:
        with conn_meta.cursor() as cur:
            # 1. Lấy block node này đang làm leader (task)
            cur.execute("""
                SELECT task FROM active_node_manager
                 WHERE node_id = %s
            """, (old_leader_id,))
            row = cur.fetchone()
            if not row or not row[0]:
                print(f"Node {old_leader_id} không có task để reassign.")
                return
            block_id = row[0]

            # 2. Xác định table
            table = get_table_name_from_block_id(block_id)

            # 3. Lấy followers hiện tại
            cur.execute(
                sql.SQL('SELECT followers FROM {} WHERE block_id = %s').format(
                    sql.Identifier(table)),
                (block_id,))
            result = cur.fetchone()
            if not result or not result[0]:
                print(f"Block {block_id} không có followers.")
                return
            followers = result[0]
            if not followers:
                print(f"Block {block_id} không có followers.")
                return

            # 4. Chọn follower đầu tiên làm leader mới
            new_leader = followers[0]
            new_followers = followers[1:] if len(followers) > 1 else []

            # 5. Update block-manager table
            cur.execute(
                sql.SQL("""
                    UPDATE {} SET leader = %s, followers = %s, status = 'processing'
                     WHERE block_id = %s
                """).format(sql.Identifier(table)),
                (new_leader, new_followers, block_id)
            )

            # 6. Update active_node_manager cho leader mới
            cur.execute(
                "UPDATE active_node_manager SET task = %s WHERE node_id = %s",
                (block_id, new_leader)
            )
            print(f"Reassigned leader of {block_id} to {new_leader}")

            # 7. Optionally xóa task của node cũ (vì đã disconnect)
            cur.execute(
                "UPDATE active_node_manager SET task = 'free' WHERE node_id = %s",
                (old_leader_id,)
            )

        conn_meta.commit()
    finally:
        conn_meta.close()
