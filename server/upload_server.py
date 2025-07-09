# upload_server.py

import os
import shutil
import socket
import json
from flask import Flask, request, render_template_string, jsonify
from werkzeug.utils import secure_filename

from functions.functions import (
    split_csv_to_blocks,
    create_database_and_user,
    register_blocks_in_db
)
from config import DB, SUPERUSER, SUPERUSER_PW, NAMENODE_HOST, NAMENODE_PORT

import psycopg2
from psycopg2 import sql

app = Flask(__name__)

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

# --- danh sách upload ---
@app.route('/', methods=['GET'])
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

if __name__=='__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)


@app.route('/download/<file>/<subdir>/<block>')
def download(file, subdir, block):
    return send_file(f"data/uploads/{file}/{subdir}/{block}", as_attachment=True)