# upload_server.py

import os
import shutil
import socket
import json
from flask import Flask, request, render_template_string, jsonify, send_from_directory, redirect, url_for, session
from werkzeug.utils import secure_filename
from flask import send_from_directory

from functions.functions import (
    split_csv_to_blocks,
    create_database_and_user,
    register_blocks_in_db,
    create_users_table,
    add_user,
    verify_user
)
from config import DB, SUPERUSER, SUPERUSER_PW, NAMENODE_HOST, NAMENODE_PORT

import psycopg2
from psycopg2 import sql

app = Flask(__name__)
app.secret_key = 'your-very-secret-key'  # Needed for session management

# ─────────── Thiết lập thư mục upload ───────────
BASE_DIR    = os.path.dirname(os.path.abspath(__file__))
UPLOAD_ROOT = os.path.join(BASE_DIR, 'data', 'uploads')
os.makedirs(UPLOAD_ROOT, exist_ok=True)
# ──────────────────────────────────────────────────

ALLOWED_EXT = {'csv', 'json'}

def allowed(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXT

HTML = '''
<!doctype html>
<html lang="vi">
<head>
  <meta charset="UTF-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1.0"/>
  <title>Upload JSON/CSV</title>
  <style>
    * { box-sizing:border-box; margin:0; padding:0; }
    body { background:#f0f2f5; font-family:Arial,sans-serif; color:#333; }
    .container {
      max-width:800px; margin:40px auto; background:#fff;
      padding:30px; border-radius:8px; box-shadow:0 4px 12px rgba(0,0,0,0.1);
    }
    h1 { text-align:center; margin-bottom:20px; color:#444; }
    input[type=file] { display:block; margin:0 auto 20px; }
    button {
      margin-left:5px; padding:4px 8px;
      border:none; border-radius:4px; cursor:pointer;
    }
    .btn-delete { background:#e74c3c; color:#fff; }
    .btn-compute { background:#3498db; color:#fff; }
    ul#fileList { margin-top:20px; }
    ul#fileList li {
      list-style: none; padding:8px; border-bottom:1px solid #ddd;
      display:flex; align-items:center;
    }
    ul#fileList li span.name { flex:1; }
  </style>
</head>
<body>
  <div class="container">
    <h1>Upload JSON/CSV Files</h1>
    <input type="file" id="fileInput" multiple accept=".json,.csv"/>
    <button id="uploadBtn">Upload</button>
    <div id="progressContainer"></div>
    <div id="messages"></div>
    <h2>Danh sách files đã upload</h2>
    <ul id="fileList">
      {% for fname in uploads %}
        <li>
          <span class="name">{{ fname }}</span>
          <button class="btn-compute" onclick="computeFile('{{fname}}')">Compute</button>
          <button class="btn-delete"  onclick="deleteFile('{{fname}}')">Delete</button>
        </li>
      {% endfor %}
    </ul>
  </div>
  <script>
    function refreshList(){
      fetch('/').then(r=>r.text()).then(html=>{
        const tmp=document.createElement('div');
        tmp.innerHTML = html;
        document.getElementById('fileList').innerHTML =
          tmp.querySelector('#fileList').innerHTML;
      });
    }

    function deleteFile(name){
      if(!confirm(`Xóa file và database "${name}" ?`)) return;
      fetch('/delete?file='+encodeURIComponent(name), {method:'DELETE'})
        .then(r=>r.json()).then(res=>{
          alert(res.status==='ok'
            ? `Đã xóa ${name}`
            : `Lỗi: ${res.error}`);
          refreshList();
        });
    }

    function computeFile(name){
      fetch('/compute', {
        method:'POST',
        headers:{'Content-Type':'application/json'},
        body: JSON.stringify({file:name})
      })
      .then(r=>r.json()).then(res=>{
        if(res.status==='ok')
          alert(`Compute "${name}" gửi NameNode thành công`);
        else
          alert(`Compute lỗi: ${res.error}`);
      });
    }

    document.getElementById('uploadBtn').onclick = () => {
      const files = document.getElementById('fileInput').files;
      const prog = document.getElementById('progressContainer');
      const msg  = document.getElementById('messages');
      prog.innerHTML = ''; msg.innerHTML = '';

      for(let i=0; i<files.length; i++){
        const file = files[i];
        const form = new FormData(); form.append('files', file);
        const wrapper=document.createElement('div');
        wrapper.innerHTML = `<p>${file.name}</p>
          <div class="progress"><div class="progress-bar" id="bar${i}"></div></div>`;
        prog.appendChild(wrapper);

        const xhr=new XMLHttpRequest();
        xhr.open('POST','/upload',true);
        xhr.upload.onprogress = e => {
          if(e.lengthComputable){
            document.getElementById(`bar${i}`)
              .style.width = (e.loaded/e.total*100)+'%';
          }
        };
        xhr.onload = ()=>{
          if(xhr.status===200){
            const res=JSON.parse(xhr.responseText);
            res.forEach(r=>{
              msg.innerHTML += `<p>${r.status==='success'
                ? `Upload ${r.filename} thành công`
                : `Lỗi ${r.filename}: ${r.error||r.status}`}</p>`;
            });
            refreshList();
          } else {
            msg.innerHTML += `<p>Lỗi khi upload ${file.name}</p>`;
          }
        };
        xhr.send(form);
      }
    };
  </script>
</body>
</html>
'''

# Ensure users table exists at startup
create_users_table()

# --- Login required decorator ---
def login_required(f):
    from functools import wraps
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'username' not in session:
            return redirect(url_for('login', next=request.url))
        return f(*args, **kwargs)
    return decorated_function

# --- Simple HTML login form ---
LOGIN_HTML = '''
<!doctype html>
<html lang="en">
<head>
  <meta charset="UTF-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1.0"/>
  <title>Login</title>
  <style>
    body { background:#f0f2f5; font-family:Arial,sans-serif; color:#333; }
    .container { max-width:400px; margin:60px auto; background:#fff; padding:30px; border-radius:8px; box-shadow:0 4px 12px rgba(0,0,0,0.1); }
    h1 { text-align:center; margin-bottom:20px; color:#444; }
    input[type=text], input[type=password] { width:100%; padding:10px; margin-bottom:15px; border:1px solid #ccc; border-radius:4px; }
    button { width:100%; padding:10px; background:#3498db; color:#fff; border:none; border-radius:4px; cursor:pointer; }
    .error { color:#e74c3c; margin-bottom:10px; text-align:center; }
    .success { color:#27ae60; margin-bottom:10px; text-align:center; }
    .signup-link { display:block; text-align:center; margin-top:10px; }
  </style>
</head>
<body>
  <div class="container">
    <h1>Login</h1>
    {% if error %}<div class="error">{{ error }}</div>{% endif %}
    <form method="post">
      <input type="text" name="username" placeholder="Username" required autofocus/>
      <input type="password" name="password" placeholder="Password" required/>
      <button type="submit">Login</button>
    </form>
    <a class="signup-link" href="/signup">Don't have an account? Sign up</a>
  </div>
</body>
</html>
'''

# --- Login page (GET: form, POST: process) ---
@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        username = request.form.get('username')
        password = request.form.get('password')
        if not username or not password:
            return render_template_string(LOGIN_HTML, error='Username and password required')
        if verify_user(username, password):
            session['username'] = username
            next_url = request.args.get('next')
            return redirect(next_url or url_for('index'))
        else:
            return render_template_string(LOGIN_HTML, error='Invalid username or password')
    return render_template_string(LOGIN_HTML, error=None)

# --- Logout ---
@app.route('/logout')
def logout():
    session.pop('username', None)
    return redirect(url_for('login'))

# --- Signup page (GET: form, POST: process) ---
SIGNUP_HTML = '''
<!doctype html>
<html lang="en">
<head>
  <meta charset="UTF-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1.0"/>
  <title>Sign Up</title>
  <style>
    body { background:#f0f2f5; font-family:Arial,sans-serif; color:#333; }
    .container { max-width:400px; margin:60px auto; background:#fff; padding:30px; border-radius:8px; box-shadow:0 4px 12px rgba(0,0,0,0.1); }
    h1 { text-align:center; margin-bottom:20px; color:#444; }
    input[type=text], input[type=password] { width:100%; padding:10px; margin-bottom:15px; border:1px solid #ccc; border-radius:4px; }
    button { width:100%; padding:10px; background:#27ae60; color:#fff; border:none; border-radius:4px; cursor:pointer; }
    .error { color:#e74c3c; margin-bottom:10px; text-align:center; }
    .success { color:#27ae60; margin-bottom:10px; text-align:center; }
    .login-link { display:block; text-align:center; margin-top:10px; }
  </style>
</head>
<body>
  <div class="container">
    <h1>Sign Up</h1>
    {% if error %}<div class="error">{{ error }}</div>{% endif %}
    {% if success %}<div class="success">{{ success }}</div>{% endif %}
    <form method="post">
      <input type="text" name="username" placeholder="Username" required autofocus/>
      <input type="password" name="password" placeholder="Password" required/>
      <button type="submit">Sign Up</button>
    </form>
    <a class="login-link" href="/login">Already have an account? Login</a>
  </div>
</body>
</html>
'''

@app.route('/signup', methods=['GET', 'POST'])
def signup():
    if request.method == 'POST':
        username = request.form.get('username')
        password = request.form.get('password')
        if not username or not password:
            return render_template_string(SIGNUP_HTML, error='Username and password required', success=None)
        success = add_user(username, password)
        if success:
            return render_template_string(SIGNUP_HTML, error=None, success='User registered! Please log in.')
        else:
            return render_template_string(SIGNUP_HTML, error='Username already exists', success=None)
    return render_template_string(SIGNUP_HTML, error=None, success=None)

# --- Protect all routes except login/signup ---
@app.before_request
def require_login():
    allowed_routes = {'login', 'signup', 'static'}
    if request.endpoint not in allowed_routes and not session.get('username'):
        return redirect(url_for('login', next=request.url))

# --- danh sách upload ---
@app.route('/', methods=['GET'])
@login_required
def index():
    uploads = [d for d in sorted(os.listdir(UPLOAD_ROOT))
               if os.path.isdir(os.path.join(UPLOAD_ROOT, d))]
    return render_template_string(HTML, uploads=uploads)

# --- upload như trước ---
@app.route('/upload', methods=['POST'])
def upload():
    files = request.files.getlist('files')
    results=[]
    for f in files:
        name = secure_filename(f.filename)
        if not name or not allowed(name):
            results.append({'filename':name or'unknown','status':'invalid','error':'không hợp lệ','blocks':0})
            continue
        dest = os.path.join(UPLOAD_ROOT,name)
        os.makedirs(dest,exist_ok=True)
        fp = os.path.join(dest,name)
        try: f.save(fp)
        except Exception as e:
            results.append({'filename':name,'status':'save_error','error':str(e),'blocks':0})
            continue
        # tạo db + blocks + meta
        db_name = os.path.splitext(name)[0]
        try:
            create_database_and_user(db_name,DB['user'],DB['password'],
                                     SUPERUSER, SUPERUSER_PW,
                                     DB['host'],DB['port'])
            n=0
            if name.lower().endswith('.csv'):
                n=split_csv_to_blocks(fp)
                block_ids=[f"{db_name}_block{i}.csv" for i in range(1,n+1)]
                register_blocks_in_db(db_name,block_ids,
                                     DB['user'],DB['password'],
                                     DB['host'],DB['port'])
        except Exception as e:
            results.append({'filename':name,'status':'error','error':str(e),'blocks':0})
            continue
        results.append({'filename':name,'status':'success','blocks':n})
    return jsonify(results)

# --- delete cả folder + drop database ---
@app.route('/delete', methods=['DELETE'])
def delete():
    fn = secure_filename(request.args.get('file',''))
    if not fn:
        return jsonify({'status':'error','error':'chưa chỉ định file'}),400
    folder = os.path.join(UPLOAD_ROOT,fn)
    if os.path.isdir(folder):
        try:
            shutil.rmtree(folder)
        except Exception as e:
            return jsonify({'status':'error','error':str(e)}),500
    # drop database
    db_base = os.path.splitext(fn)[0]
    try:
        conn = psycopg2.connect(dbname='postgres',
                                user=SUPERUSER,
                                password=SUPERUSER_PW,
                                host=DB['host'],port=DB['port'])
        conn.autocommit=True
        cur=conn.cursor()
        # terminate connections
        cur.execute(sql.SQL("""
            SELECT pg_terminate_backend(pid)
              FROM pg_stat_activity
             WHERE datname = %s
        """),[db_base])
        cur.execute(sql.SQL("DROP DATABASE IF EXISTS {};").format(
                         sql.Identifier(db_base)))
        cur.close(); conn.close()
    except Exception as e:
        return jsonify({'status':'error','error':str(e)}),500

    return jsonify({'status':'ok'})

# --- compute: gửi yêu cầu đến NameNode qua socket TCP ---
@app.route('/compute', methods=['POST'])
def compute():
    data = request.get_json(force=True)
    fn = secure_filename(data.get('file',''))
    if not fn:
        return jsonify({'status':'error','error':'không có file'}),400
    db_base = os.path.splitext(fn)[0]
    try:
        s = socket.socket()
        s.connect((NAMENODE_HOST, NAMENODE_PORT))
        msg = {'type':'compute','file':db_base}
        s.sendall(json.dumps(msg).encode())
        resp = s.recv(1024)
        s.close()
    except Exception as e:
        return jsonify({'status':'error','error':str(e)}),500

    return jsonify({'status':'ok','namenode':resp.decode()})

# --- Route phục vụ download block --from flask import send_from_directory

@app.route('/download/<filename>/blocks/<block_id>')
def download_block(filename, block_id):
    # filename: tên folder, VD: alogs.csv
    # blockfile: VD: alogs_block1.csv
    blocks_dir = os.path.join(UPLOAD_ROOT, filename, 'blocks')
    print (blocks_dir)
    return send_from_directory(blocks_dir, block_id, as_attachment=True)


@app.route('/upload_block', methods=['POST'])
def upload_block():
    file = request.files.get('file')
    file_base = request.form.get('file_base')     # Ví dụ: 'alogs'
    block_id  = request.form.get('block_id')      # Ví dụ: 'alogs_block1.csv'

    if not file or not file_base or not block_id:
        return jsonify({"status": "error", "msg": "Missing parameters"}), 400

    # ===> ĐƯỜNG DẪN MỚI: server/data/results/alogs/alogs_block1.csv
    base_dir = os.path.dirname(os.path.abspath(__file__))
    results_dir = os.path.join(base_dir, "data", "results", file_base)
    os.makedirs(results_dir, exist_ok=True)
    save_path = os.path.join(results_dir, block_id)
    file.save(save_path)

    return jsonify({"status": "success", "msg": f"Uploaded {block_id} to {file_base}"})

# --- User Signup ---
# This route is now handled by the new signup function

# --- User Login ---
# This route is now handled by the new login function


if __name__=='__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)






