import os
import psycopg2
from psycopg2 import sql, errors
from werkzeug.security import generate_password_hash, check_password_hash
from config import DB



#==========================================================================================================

def split_csv_to_blocks(input_path: str, block_size: int =   10  * 1024 * 1024) -> int:
    """
    Split a CSV file into multiple blocks, each no larger than block_size bytes (including header).
    - Các block sẽ được lưu trong thư mục 'blocks' nằm trong cùng thư mục chứa file gốc.
    - Mỗi block được đặt tên <basename>_block<N>.csv.
    - Mỗi block có header giống file gốc.
    - Những dòng quá dài để nằm vừa block mới sẽ bị bỏ qua.
    Trả về số lượng block đã tạo.
    """
    base, ext = os.path.splitext(input_path)
    if ext.lower() != '.csv':
        print(f"[Warning] File '{input_path}' không có phần mở rộng .csv, vẫn tiếp tục…")

    # Đọc header
    with open(input_path, 'r', encoding='utf-8', newline='') as f:
        header = f.readline()
    header_bytes = header.encode('utf-8')
    header_size = len(header_bytes)

    # Thư mục chứa file gốc, và blocks/
    container_dir = os.path.dirname(input_path)
    blocks_dir = os.path.join(container_dir, 'blocks')
    os.makedirs(blocks_dir, exist_ok=True)

    # Tên cơ bản để ghép block
    basename = os.path.basename(base)
    block_num = 1
    current_size = header_size

    # Mở block đầu tiên và ghi header
    out_path = os.path.join(blocks_dir, f"{basename}_block{block_num}.csv")
    outfile = open(out_path, 'w', encoding='utf-8', newline='')
    outfile.write(header)

    # Xử lý các dòng còn lại
    with open(input_path, 'r', encoding='utf-8', newline='') as f:
        next(f)
        for lineno, line in enumerate(f, start=2):
            line_bytes = line.encode('utf-8')
            line_size = len(line_bytes)

            if line_size > block_size - header_size:
                print(f"[Warn] Dòng {lineno} ({line_size} bytes) quá dài, bỏ qua.")
                continue

            if current_size + line_size > block_size:
                outfile.close()
                block_num += 1
                current_size = header_size
                out_path = os.path.join(blocks_dir, f"{basename}_block{block_num}.csv")
                outfile = open(out_path, 'w', encoding='utf-8', newline='')
                outfile.write(header)

            outfile.write(line)
            current_size += line_size

    outfile.close()
    print(f"Hoàn thành: tạo được {block_num} block trong '{blocks_dir}'.")
    return block_num



#==========================================================================================================

def create_database_and_user(
    db_name: str,
    db_user: str,
    db_password: str,
    superuser: str = 'postgres',
    super_password: str = None,
    host: str = 'localhost',
    port: int = 5432
) -> None:
    """
    Tạo database và user mới trong PostgreSQL.

    1. Kết nối tới database 'postgres' với quyền superuser.
    2. Tạo database `db_name` nếu chưa tồn tại.
    3. Tạo user `db_user` với password `db_password` nếu chưa tồn tại.
    4. Gán toàn quyền trên `db_name` cho `db_user`.

    Raises:
        psycopg2.Error nếu có lỗi kết nối hoặc thực thi SQL.
    """
    # Kết nối với superuser
    conn = psycopg2.connect(
        dbname='postgres',
        user=superuser,
        password=super_password,
        host=host,
        port=port
    )
    conn.autocommit = True
    cur = conn.cursor()

    # 1. Tạo database
    try:
        cur.execute(
            sql.SQL("CREATE DATABASE {};").format(sql.Identifier(db_name))
        )
        print(f"✔ Database '{db_name}' created.")
    except errors.DuplicateDatabase:
        print(f"Database '{db_name}' already exists, skipping.")
    
    # 2. Tạo user
    try:
        cur.execute(
            sql.SQL("CREATE USER {} WITH PASSWORD %s;").format(sql.Identifier(db_user)),
            [db_password]
        )
        print(f"✔ User '{db_user}' created.")
    except errors.DuplicateObject:
        print(f" User '{db_user}' already exists, skipping.")

    # 3. Gán quyền
    cur.execute(
        sql.SQL("GRANT ALL PRIVILEGES ON DATABASE {} TO {};").format(
            sql.Identifier(db_name),
            sql.Identifier(db_user)
        )
    )
    print(f"✔ Granted all privileges on '{db_name}' to '{db_user}'.")

    # Đóng kết nối
    cur.close()
    conn.close()


def register_blocks_in_db(db_name: str, block_ids: list[str], db_user: str, db_password: str, host: str, port: int):
    """
    Kết nối tới database `db_name`, tạo table (tên = db_name.replace('.', '_')),
    và insert tất cả block_ids với status='pending'.
    """
    import psycopg2
    from psycopg2.extras import execute_values
    from psycopg2 import sql

    # Kết nối đến riêng database mới
    conn = psycopg2.connect(
        dbname=db_name,
        user=db_user,
        password=db_password,
        host=host,
        port=port
    )
    conn.autocommit = True
    cur = conn.cursor()

    tbl = db_name.replace('.', '_')
    # 1) Tạo table nếu chưa có
    cur.execute(f"""
      CREATE TABLE IF NOT EXISTS "{tbl}" (
        block_id TEXT PRIMARY KEY,
        status    VARCHAR(10) NOT NULL DEFAULT 'pending',
        leader    TEXT,
        followers TEXT[]
      );
    """)

    # 2) Insert các block
    rows = [(bid, 'pending', None, []) for bid in block_ids]
    execute_values(cur,
      f"""
      INSERT INTO "{tbl}" (block_id, status, leader, followers)
      VALUES %s
      ON CONFLICT (block_id) DO NOTHING
      """,
      rows
    )

    cur.close()
    conn.close()


def create_users_table():
    """
    Create the users table if it does not exist.
    """
    conn = psycopg2.connect(
        dbname=DB['dbname'],
        user=DB['user'],
        password=DB['password'],
        host=DB['host'],
        port=DB['port']
    )
    cur = conn.cursor()
    cur.execute('''
        CREATE TABLE IF NOT EXISTS users (
            id SERIAL PRIMARY KEY,
            username VARCHAR(150) UNIQUE NOT NULL,
            password_hash VARCHAR(255) NOT NULL
        );
    ''')
    conn.commit()
    cur.close()
    conn.close()


def add_user(username: str, password: str) -> bool:
    """
    Add a new user with a hashed password. Returns True if successful, False if username exists.
    """
    password_hash = generate_password_hash(password)
    conn = None
    cur = None
    try:
        conn = psycopg2.connect(
            dbname=DB['dbname'],
            user=DB['user'],
            password=DB['password'],
            host=DB['host'],
            port=DB['port']
        )
        cur = conn.cursor()
        cur.execute('INSERT INTO users (username, password_hash) VALUES (%s, %s);', (username, password_hash))
        conn.commit()
        cur.close()
        conn.close()
        return True
    except psycopg2.IntegrityError as e:
        # Duplicate username or other integrity error
        if conn:
            conn.rollback()
        if cur:
            cur.close()
        if conn:
            conn.close()
        return False
    except Exception as e:
        print(f"Error adding user: {e}")
        if conn:
            conn.rollback()
        if cur:
            cur.close()
        if conn:
            conn.close()
        return False


def verify_user(username: str, password: str) -> bool:
    """
    Verify a user's credentials. Returns True if correct, False otherwise.
    """
    try:
        conn = psycopg2.connect(
            dbname=DB['dbname'],
            user=DB['user'],
            password=DB['password'],
            host=DB['host'],
            port=DB['port']
        )
        cur = conn.cursor()
        cur.execute('SELECT password_hash FROM users WHERE username = %s;', (username,))
        row = cur.fetchone()
        cur.close()
        conn.close()
        if row:
            return check_password_hash(row[0], password)
        return False
    except Exception as e:
        print(f"Error verifying user: {e}")
        return False
