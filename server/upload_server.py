# upload_server.py

import os
import shutil
import socket
import json
import importlib.util
from flask import Flask, request, render_template_string, jsonify, send_from_directory
from werkzeug.utils import secure_filename
import sys
from flask import send_from_directory

# Add parent directory to Python path BEFORE importing analyze_task
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)

# Now import from analyze_task
try:
    from analyze_task.aggerate import * 
    print(" Successfully imported aggerate")
except ImportError as e:
    print(f"Import error: {e}")
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
    .btn-results { background:#f39c12; color:#fff; }
    .btn-aggregate { background:#27ae60; color:#fff; }
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
          <button class="btn-results" onclick="viewResults('{{fname}}')">View Results</button>
          <button class="btn-aggregate" onclick="viewAggregate('{{fname}}')">Aggregate</button>
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

    function viewResults(name){
      const fileBase = name.replace('.csv', '');
      window.open(`/results/${fileBase}`, '_blank');
    }

    function viewAggregate(name){
      const fileBase = name.replace('.csv', '');
      window.open(`/aggregate/${fileBase}`, '_blank');
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

# --- Route phục vụ download block ---
@app.route('/download/<filename>/blocks/<block_id>')
def download_block(filename, block_id):
    # filename: tên folder, VD: alogs.csv
    # blockfile: VD: alogs_block1.csv
    blocks_dir = os.path.join(UPLOAD_ROOT, filename, 'blocks')
    print (blocks_dir)
    return send_from_directory(blocks_dir, block_id, as_attachment=True)

@app.route('/aggregate/<file_base>')
def aggregate_results(file_base):
    """
    Aggregate all analysis results for a specific file and display them.
    """
    try:
        # Path to results directory
        base_dir = os.path.dirname(os.path.abspath(__file__))
        results_dir = os.path.join(base_dir, "data", "results", file_base)
        
        if not os.path.exists(results_dir):
            return jsonify({"status": "error", "msg": f"No results found for {file_base}"}), 404
        
        # Import the aggregate function
        import sys
        analyze_task_path = os.path.join(os.path.dirname(base_dir), 'analyze_task')
        if analyze_task_path not in sys.path:
            sys.path.insert(0, analyze_task_path)
        
        try:
            from analyze_task.aggerate import aggregate_ensemble_results
        except ImportError as e:
            # Handle the relative import issue by creating a simple local implementation
            import pandas as pd
            import re
            
            def aggregate_ensemble_results(results_dir):
                """
                Local implementation to avoid import issues.
                """
                pattern = re.compile(
                    r"(?P<column>.+?)\s+(?P<count>\d+\.?\d*)\s+(?P<mean>[-+]?\d*\.?\d+)\s+"
                    r"(?P<std>[-+]?\d*\.?\d+)\s+(?P<min>[-+]?\d*\.?\d+)\s+(?P<q25>[-+]?\d*\.?\d+)\s+"
                    r"(?P<q50>[-+]?\d*\.?\d+)\s+(?P<q75>[-+]?\d*\.?\d+)\s+(?P<max>[-+]?\d*\.?\d+)"
                )
                numeric_cols = ['count', 'mean', 'std', 'min', 'q25', 'q50', 'q75', 'max']
                all_stats = []

                for fname in os.listdir(results_dir):
                    if not fname.endswith(".txt"):
                        continue
                    filepath = os.path.join(results_dir, fname)
                    try:
                        with open(filepath, "r", encoding="utf-8") as f:
                            content = f.read()
                        section = re.search(r"Basic statistics:(.*?)(?:Unique value statistics per column:|$)", content, re.S)
                        if not section:
                            continue
                        for line in section.group(1).strip().split("\n"):
                            line = line.strip()
                            if not line or line.startswith(('count', 'mean', 'std')):
                                continue
                            match = pattern.match(line)
                            if match:
                                data = match.groupdict()
                                data["source_file"] = fname
                                all_stats.append(data)
                    except Exception as file_error:
                        print(f"Error processing file {fname}: {file_error}")
                        continue

                if not all_stats:
                    raise ValueError("No valid stats found in any file.")

                df = pd.DataFrame(all_stats)
                try:
                    df[numeric_cols] = df[numeric_cols].astype(float)
                    ensemble = df.groupby("column")[numeric_cols].agg(["mean", "std"])
                    
                    # Flatten the column names (keep both mean and std columns)
                    ensemble.columns = ['_'.join(col).strip() for col in ensemble.columns.values]
                    ensemble = ensemble.dropna()
                    
                    return ensemble
                except Exception as calc_error:
                    print(f"Error in calculations: {calc_error}")
                    # Return a simple summary if calculation fails
                    return pd.DataFrame({"info": [f"Found {len(all_stats)} statistics from {len(set(s['source_file'] for s in all_stats))} files"]})
            
            print(f"[Server] Using local aggregate implementation due to import error: {e}")
        # Aggregate the results
        ensemble_results = aggregate_ensemble_results(results_dir)
        
        # Convert to HTML for display
        html_table = ensemble_results.to_html(classes='table table-striped table-bordered')
        
        # Save ensemble results to CSV
        ensemble_csv_path = os.path.join(results_dir, f"{file_base}_ensemble_results.csv")
        ensemble_results.to_csv(ensemble_csv_path)
        
        # Create a simple HTML page to display results
        aggregate_html = f'''
        <!DOCTYPE html>
        <html>
        <head>
            <title>Aggregated Results for {file_base}</title>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 20px; }}
                .container {{ max-width: 1200px; margin: 0 auto; }}
                .header {{ background: #f8f9fa; padding: 20px; border-radius: 5px; margin-bottom: 20px; }}
                .table {{ border-collapse: collapse; width: 100%; }}
                .table th, .table td {{ padding: 8px 12px; text-align: left; border: 1px solid #ddd; }}
                .table th {{ background-color: #f2f2f2; }}
                .table-striped tbody tr:nth-of-type(odd) {{ background-color: rgba(0,0,0,.05); }}
                .download-btn {{ 
                    background: #007bff; color: white; padding: 10px 20px; 
                    text-decoration: none; border-radius: 5px; display: inline-block; margin: 10px 0;
                }}
                .back-btn {{ 
                    background: #6c757d; color: white; padding: 10px 20px; 
                    text-decoration: none; border-radius: 5px; display: inline-block; margin: 10px 0;
                }}
            </style>
        </head>
        <body>
            <div class="container">
                <div class="header">
                    <h1>📊 Aggregated Analysis Results</h1>
                    <h2>File: {file_base}</h2>
                    <p>Combined statistical analysis from all processed blocks</p>
                </div>
                
                <div>
                    <a href="/download_aggregate/{file_base}" class="download-btn">📥 Download CSV Results</a>
                    <a href="/" class="back-btn">← Back to Files</a>
                </div>
                
                <h3>Ensemble Statistics (Mean across all blocks)</h3>
                {html_table}
                
                <div style="margin-top: 20px;">
                    <h4>Interpretation:</h4>
                    <ul>
                        <li><strong>Mean columns:</strong> Average statistics across all blocks</li>
                        <li><strong>Column values:</strong> Aggregated mean values from all processed blocks</li>
                        <li><strong>Consistent results:</strong> Shows the overall statistical summary</li>
                    </ul>
                </div>
            </div>
        </body>
        </html>
        '''
        
        return aggregate_html
        
    except Exception as e:
        return jsonify({"status": "error", "msg": f"Error aggregating results: {str(e)}"}), 500

@app.route('/download_aggregate/<file_base>')
def download_aggregate(file_base):
    """
    Download the aggregated results as CSV file.
    """
    try:
        base_dir = os.path.dirname(os.path.abspath(__file__))
        results_dir = os.path.join(base_dir, "data", "results", file_base)
        csv_filename = f"{file_base}_ensemble_results.csv"
        
        return send_from_directory(results_dir, csv_filename, as_attachment=True)
        
    except Exception as e:
        return jsonify({"status": "error", "msg": f"Error downloading file: {str(e)}"}), 500

@app.route('/results/<file_base>')
def view_results(file_base):
    """
    View all individual analysis results for a file.
    """
    try:
        base_dir = os.path.dirname(os.path.abspath(__file__))
        results_dir = os.path.join(base_dir, "data", "results", file_base)
        
        if not os.path.exists(results_dir):
            return jsonify({"status": "error", "msg": f"No results found for {file_base}"}), 404
        
        # Get all result files
        result_files = [f for f in os.listdir(results_dir) if f.endswith('.txt')]
        
        results_html = f'''
        <!DOCTYPE html>
        <html>
        <head>
            <title>Analysis Results for {file_base}</title>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 20px; }}
                .container {{ max-width: 1200px; margin: 0 auto; }}
                .header {{ background: #f8f9fa; padding: 20px; border-radius: 5px; margin-bottom: 20px; }}
                .file-list {{ list-style: none; padding: 0; }}
                .file-item {{ 
                    background: white; border: 1px solid #ddd; margin: 10px 0; 
                    padding: 15px; border-radius: 5px; box-shadow: 0 2px 4px rgba(0,0,0,0.1);
                }}
                .file-link {{ 
                    color: #007bff; text-decoration: none; font-weight: bold; 
                }}
                .aggregate-btn {{ 
                    background: #28a745; color: white; padding: 10px 20px; 
                    text-decoration: none; border-radius: 5px; display: inline-block; margin: 10px 0;
                }}
                .back-btn {{ 
                    background: #6c757d; color: white; padding: 10px 20px; 
                    text-decoration: none; border-radius: 5px; display: inline-block; margin: 10px 0;
                }}
            </style>
        </head>
        <body>
            <div class="container">
                <div class="header">
                    <h1>📋 Individual Analysis Results</h1>
                    <h2>File: {file_base}</h2>
                    <p>Individual analysis results for each processed block</p>
                </div>
                
                <div>
                    <a href="/aggregate/{file_base}" class="aggregate-btn">📊 View Aggregated Results</a>
                    <a href="/" class="back-btn">← Back to Files</a>
                </div>
                
                <ul class="file-list">
        '''
        
        for result_file in sorted(result_files):
            results_html += f'''
                    <li class="file-item">
                        <a href="/download_result/{file_base}/{result_file}" class="file-link">
                            📄 {result_file}
                        </a>
                        <p>Individual analysis result file</p>
                    </li>
            '''
        
        results_html += '''
                </ul>
            </div>
        </body>
        </html>
        '''
        
        return results_html
        
    except Exception as e:
        return jsonify({"status": "error", "msg": f"Error viewing results: {str(e)}"}), 500

@app.route('/download_result/<file_base>/<filename>')
def download_result(file_base, filename):
    """
    Download individual result file.
    """
    try:
        base_dir = os.path.dirname(os.path.abspath(__file__))
        results_dir = os.path.join(base_dir, "data", "results", file_base)
        
        return send_from_directory(results_dir, filename, as_attachment=True)
        
    except Exception as e:
        return jsonify({"status": "error", "msg": f"Error downloading file: {str(e)}"}), 500

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

if __name__=='__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)






