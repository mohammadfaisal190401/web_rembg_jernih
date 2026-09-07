import os
import uuid
import json
import threading
import queue
import time

# Tambahkan ini untuk production
import os
port = int(os.environ.get('PORT', 5000))

from datetime import datetime
from flask import Flask, request, render_template, send_file, jsonify, Response, stream_with_context
from werkzeug.utils import secure_filename
from rembg import remove


app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = 'uploads'
app.config['RESULT_FOLDER'] = 'results'
app.config['MAX_CONTENT_LENGTH'] = 100 * 1024 * 1024
app.config['HISTORY_FILE'] = 'history.json'

os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)
os.makedirs(app.config['RESULT_FOLDER'], exist_ok=True)

ALLOWED_EXTENSIONS = {'png', 'jpg', 'jpeg', 'webp', 'bmp', 'tiff'}

# Session store: session_id -> {queue, files, total, done, cancel_event, thread}
sessions = {}

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def load_history():
    if os.path.exists(app.config['HISTORY_FILE']):
        try:
            with open(app.config['HISTORY_FILE'], 'r') as f:
                return json.load(f)
        except:
            return []
    return []

def save_history(history):
    with open(app.config['HISTORY_FILE'], 'w') as f:
        json.dump(history, f, indent=2)

def format_date(timestamp_str):
    dt = datetime.fromisoformat(timestamp_str)
    now = datetime.now()
    hari = ['Senin', 'Selasa', 'Rabu', 'Kamis', 'Jumat', 'Sabtu', 'Minggu'][dt.weekday()]
    bulan = ['Januari', 'Februari', 'Maret', 'April', 'Mei', 'Juni', 'Juli', 'Agustus', 'September', 'Oktober', 'November', 'Desember'][dt.month-1]
    if dt.year == now.year:
        return f"{hari}. {dt.day:02d}/{bulan}"
    else:
        return f"{hari}. {dt.day:02d}/{bulan}/{dt.year}"

# ---------- PROSES BACKGROUND DENGAN CANCEL ----------
def process_images(session_id):
    session = sessions.get(session_id)
    if not session:
        return
    files = session['files']
    total = len(files)
    q = session['queue']
    cancel_event = session['cancel_event']

    history = load_history()

    for idx, file_data in enumerate(files, start=1):
        # Cek apakah dibatalkan
        if cancel_event.is_set():
            q.put({'type': 'cancelled'})
            break

        try:
            original_filename = file_data['filename']
            file_bytes = file_data['bytes']
            unique_id = uuid.uuid4().hex[:8]
            base_name = os.path.splitext(original_filename)[0]

            # Simpan asli
            upload_filename = f"{base_name}_{unique_id}.jpg"
            upload_path = os.path.join(app.config['UPLOAD_FOLDER'], upload_filename)
            with open(upload_path, 'wb') as f:
                f.write(file_bytes)

            # Proses remove background
            output_data = remove(file_bytes)

            result_filename = f"{base_name}_{unique_id}.png"
            result_path = os.path.join(app.config['RESULT_FOLDER'], result_filename)
            with open(result_path, 'wb') as f:
                f.write(output_data)

            # Entry history
            entry = {
                'id': unique_id,
                'original_filename': original_filename,
                'upload_path': upload_path,
                'result_path': result_path,
                'timestamp': datetime.now().isoformat()
            }
            history.append(entry)
            save_history(history)

            # Kirim progress
            progress_msg = {
                'type': 'file_done',
                'index': idx,
                'total': total,
                'percent': round((idx / total) * 100),
                'file_id': unique_id,
                'filename': original_filename,
                'download_url': f'/download/{unique_id}',
                'entry': entry
            }
            q.put(progress_msg)

        except Exception as e:
            q.put({
                'type': 'error',
                'index': idx,
                'total': total,
                'filename': original_filename,
                'error': str(e)
            })

    # Kirim sinyal selesai atau batal
    if not cancel_event.is_set():
        q.put({'type': 'complete'})
    else:
        q.put({'type': 'cancelled'})

    # Hapus session setelah selesai
    sessions.pop(session_id, None)

# ---------- ROUTES ----------
@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/history', methods=['GET'])
def api_history():
    history = load_history()
    return jsonify(history)

@app.route('/upload', methods=['POST'])
def upload():
    if 'files' not in request.files:
        return jsonify({'error': 'Tidak ada file'}), 400

    files = request.files.getlist('files')
    if not files or files[0].filename == '':
        return jsonify({'error': 'Tidak ada file dipilih'}), 400

    file_data_list = []
    for file in files:
        if file and allowed_file(file.filename):
            file_bytes = file.read()
            file_data_list.append({
                'filename': secure_filename(file.filename),
                'bytes': file_bytes
            })
        else:
            return jsonify({'error': f'Format file {file.filename} tidak didukung'}), 400

    session_id = uuid.uuid4().hex[:12]
    q = queue.Queue()
    cancel_event = threading.Event()

    sessions[session_id] = {
        'queue': q,
        'files': file_data_list,
        'total': len(file_data_list),
        'done': 0,
        'cancel_event': cancel_event
    }

    # Jalankan thread
    thread = threading.Thread(target=process_images, args=(session_id,))
    thread.daemon = True
    thread.start()
    sessions[session_id]['thread'] = thread

    return jsonify({'session_id': session_id, 'total': len(file_data_list)})

@app.route('/cancel/<session_id>', methods=['POST'])
def cancel(session_id):
    if session_id not in sessions:
        return jsonify({'error': 'Session tidak ditemukan'}), 404
    # Set event cancel
    sessions[session_id]['cancel_event'].set()
    return jsonify({'success': True})

@app.route('/progress/<session_id>')
def progress(session_id):
    if session_id not in sessions:
        return jsonify({'error': 'Session tidak ditemukan'}), 404

    def generate():
        q = sessions[session_id]['queue']
        while True:
            try:
                data = q.get(timeout=30)
                yield f"data: {json.dumps(data)}\n\n"
                if data.get('type') in ('complete', 'cancelled'):
                    break
            except queue.Empty:
                yield f"data: {json.dumps({'type': 'ping'})}\n\n"
                continue
        # Hapus session
        sessions.pop(session_id, None)

    return Response(stream_with_context(generate()), mimetype='text/event-stream')

@app.route('/delete/<entry_id>', methods=['DELETE'])
def delete_entry(entry_id):
    history = load_history()
    entry = next((e for e in history if e['id'] == entry_id), None)
    if not entry:
        return jsonify({'error': 'Entry tidak ditemukan'}), 404

    if os.path.exists(entry['upload_path']):
        os.remove(entry['upload_path'])
    if os.path.exists(entry['result_path']):
        os.remove(entry['result_path'])

    history = [e for e in history if e['id'] != entry_id]
    save_history(history)
    return jsonify({'success': True})

@app.route('/delete_all', methods=['DELETE'])
def delete_all():
    history = load_history()
    for entry in history:
        if os.path.exists(entry['upload_path']):
            os.remove(entry['upload_path'])
        if os.path.exists(entry['result_path']):
            os.remove(entry['result_path'])
    save_history([])
    return jsonify({'success': True})

@app.route('/download/<entry_id>')
def download_result(entry_id):
    history = load_history()
    entry = next((e for e in history if e['id'] == entry_id), None)
    if not entry or not os.path.exists(entry['result_path']):
        return 'File tidak ditemukan', 404
    # Kirim file dengan nama asli + .png
    original_name = entry['original_filename'].rsplit('.', 1)[0] + '.png'
    return send_file(entry['result_path'], as_attachment=True, download_name=original_name)

@app.route('/image/<entry_id>/<type>')
def get_image(entry_id, type):
    history = load_history()
    entry = next((e for e in history if e['id'] == entry_id), None)
    if not entry:
        return 'Entry tidak ditemukan', 404
    if type == 'original':
        path = entry['upload_path']
    elif type == 'result':
        path = entry['result_path']
    else:
        return 'Tipe tidak valid', 400
    if not os.path.exists(path):
        return 'File tidak ditemukan', 404
    return send_file(path)

# if __name__ == '__main__':
#     app.run(host='0.0.0.0', port=5000, debug=True, threaded=True)


# if __name__ == '__main__':
#     app.run(host='0.0.0.0', port=port, debug=False)


# ... semua kode di atas ...

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)






