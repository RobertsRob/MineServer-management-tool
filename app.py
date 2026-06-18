import os
import json
import subprocess
import threading
import time
import shutil
import requests
import uuid
from flask import Flask, render_template, request, jsonify, Response
from flask_sock import Sock
import queue

app = Flask(__name__)
sock = Sock(app)

SERVERS_DIR = os.path.join(os.path.dirname(__file__), 'servers')
DATA_FILE = os.path.join(os.path.dirname(__file__), 'servers.json')
JAVA_MIN_RAM = "512M"

# In-memory state
server_processes = {}   # server_id -> subprocess.Popen
server_logs = {}        # server_id -> list of log lines
server_log_queues = {}  # server_id -> list of queue.Queue (websocket listeners)
playit_processes = {}   # server_id -> subprocess.Popen
playit_tunnels = {}     # server_id -> {"ip": ..., "port": ...}

os.makedirs(SERVERS_DIR, exist_ok=True)


def load_servers():
    if not os.path.exists(DATA_FILE):
        return {}
    with open(DATA_FILE) as f:
        return json.load(f)


def save_servers(data):
    with open(DATA_FILE, 'w') as f:
        json.dump(data, f, indent=2)


def get_server_dir(server_id):
    return os.path.join(SERVERS_DIR, server_id)


def broadcast_log(server_id, line):
    if server_id not in server_logs:
        server_logs[server_id] = []
    server_logs[server_id].append(line)
    # Keep last 500 lines
    if len(server_logs[server_id]) > 500:
        server_logs[server_id] = server_logs[server_id][-500:]
    # Broadcast to all websocket listeners
    for q in server_log_queues.get(server_id, []):
        try:
            q.put_nowait(line)
        except Exception:
            pass


def stream_output(server_id, proc):
    """Background thread: read stdout from MC process and broadcast."""
    for raw in proc.stdout:
        line = raw.decode('utf-8', errors='replace').rstrip()
        broadcast_log(server_id, line)
        # Detect playit tunnel lines if running embedded
    proc.stdout.close()
    broadcast_log(server_id, "[Panel] Server process exited.")
    # Update status
    servers = load_servers()
    if server_id in servers:
        servers[server_id]['status'] = 'stopped'
        save_servers(servers)


def stream_playit_output(server_id, proc):
    """Background thread: read playit stdout to capture tunnel address."""
    for raw in proc.stdout:
        line = raw.decode('utf-8', errors='replace').rstrip()
        broadcast_log(server_id, f"[playit] {line}")
        # Try to detect assigned address lines like:
        # "TCP tunnel: 1.2.3.4:25565" or "address: sg1.joinmc.link:12345"
        import re
        m = re.search(r'(\d+\.\d+\.\d+\.\d+):(\d+)', line)
        if m:
            playit_tunnels[server_id] = {"ip": m.group(1), "port": m.group(2)}
            servers = load_servers()
            if server_id in servers:
                servers[server_id]['tunnel_ip'] = m.group(1)
                servers[server_id]['tunnel_port'] = m.group(2)
                save_servers(servers)
        # hostname:port pattern
        m2 = re.search(r'address[:\s]+([a-zA-Z0-9.\-]+):(\d+)', line, re.IGNORECASE)
        if m2:
            playit_tunnels[server_id] = {"ip": m2.group(1), "port": m2.group(2)}
            servers = load_servers()
            if server_id in servers:
                servers[server_id]['tunnel_ip'] = m2.group(1)
                servers[server_id]['tunnel_port'] = m2.group(2)
                save_servers(servers)
    proc.stdout.close()


# ─── Routes ────────────────────────────────────────────────────────────────────

@app.route('/')
def index():
    servers = load_servers()
    return render_template('index.html', servers=servers)


@app.route('/server/<server_id>')
def server_detail(server_id):
    servers = load_servers()
    if server_id not in servers:
        return "Server not found", 404
    server = servers[server_id]
    logs = server_logs.get(server_id, [])
    return render_template('server.html', server=server, server_id=server_id, logs=logs)


@app.route('/api/servers', methods=['GET'])
def api_list_servers():
    servers = load_servers()
    # Inject live status
    for sid, s in servers.items():
        proc = server_processes.get(sid)
        if proc and proc.poll() is None:
            s['status'] = 'running'
        else:
            s['status'] = s.get('status', 'stopped')
        if sid in playit_tunnels:
            s['tunnel_ip'] = playit_tunnels[sid]['ip']
            s['tunnel_port'] = playit_tunnels[sid]['port']
    return jsonify(servers)


@app.route('/api/servers', methods=['POST'])
def api_create_server():
    data = request.json
    name = data.get('name', '').strip()
    subdomain = data.get('subdomain', '').strip().lower()
    version = data.get('version', '1.21.1').strip()
    ram = data.get('ram', '1024').strip()
    port = int(data.get('port', 25565))
    server_type = data.get('server_type', 'vanilla')

    if not name or not subdomain:
        return jsonify({'error': 'Name and subdomain are required'}), 400

    servers = load_servers()
    # Check subdomain uniqueness
    for s in servers.values():
        if s.get('subdomain') == subdomain:
            return jsonify({'error': 'Subdomain already in use'}), 400
    # Check port uniqueness
    for s in servers.values():
        if s.get('port') == port:
            return jsonify({'error': f'Port {port} already in use'}), 400

    server_id = str(uuid.uuid4())[:8]
    server_dir = get_server_dir(server_id)
    os.makedirs(server_dir, exist_ok=True)

    # Download server jar
    jar_path = os.path.join(server_dir, 'server.jar')
    broadcast_log(server_id, f"[Panel] Downloading Minecraft {version} ({server_type})...")

    jar_url = _get_jar_url(version, server_type)

    servers[server_id] = {
        'id': server_id,
        'name': name,
        'subdomain': subdomain,
        'version': version,
        'ram': ram,
        'port': port,
        'server_type': server_type,
        'status': 'installing',
        'jar_url': jar_url,
        'jar_path': jar_path,
        'dir': server_dir,
        'tunnel_ip': None,
        'tunnel_port': None,
    }
    save_servers(servers)

    # Install in background
    t = threading.Thread(target=_install_server, args=(server_id, jar_url, jar_path, server_dir, port, ram), daemon=True)
    t.start()

    return jsonify({'id': server_id, 'message': 'Server creation started'})


def _get_jar_url(version, server_type):
    """Get download URL for server jar."""
    if server_type == 'paper':
        # PaperMC API
        try:
            builds_url = f"https://api.papermc.io/v2/projects/paper/versions/{version}/builds"
            r = requests.get(builds_url, timeout=10)
            builds = r.json()['builds']
            latest = builds[-1]
            build_num = latest['build']
            jar_name = latest['downloads']['application']['name']
            return f"https://api.papermc.io/v2/projects/paper/versions/{version}/builds/{build_num}/downloads/{jar_name}"
        except Exception:
            pass
    # Fallback: vanilla via Mojang manifest
    try:
        manifest = requests.get("https://launchermeta.mojang.com/mc/game/version_manifest.json", timeout=10).json()
        for v in manifest['versions']:
            if v['id'] == version:
                ver_data = requests.get(v['url'], timeout=10).json()
                return ver_data['downloads']['server']['url']
    except Exception:
        pass
    return None


def _install_server(server_id, jar_url, jar_path, server_dir, port, ram):
    servers = load_servers()
    try:
        if jar_url:
            broadcast_log(server_id, f"[Panel] Fetching: {jar_url}")
            r = requests.get(jar_url, timeout=120, stream=True)
            with open(jar_path, 'wb') as f:
                for chunk in r.iter_content(65536):
                    f.write(chunk)
            broadcast_log(server_id, "[Panel] Download complete.")
        else:
            broadcast_log(server_id, "[Panel] ERROR: Could not find jar URL for this version.")
            servers[server_id]['status'] = 'error'
            save_servers(servers)
            return

        # Accept EULA
        with open(os.path.join(server_dir, 'eula.txt'), 'w') as f:
            f.write("eula=true\n")

        # Write server.properties
        with open(os.path.join(server_dir, 'server.properties'), 'w') as f:
            f.write(f"""server-port={port}
motd=\\u00A7aManaged by MC Panel
max-players=20
level-name=world
online-mode=true
""")

        servers[server_id]['status'] = 'stopped'
        save_servers(servers)
        broadcast_log(server_id, "[Panel] Installation complete. Ready to start.")
    except Exception as e:
        broadcast_log(server_id, f"[Panel] Install error: {e}")
        servers[server_id]['status'] = 'error'
        save_servers(servers)


@app.route('/api/servers/<server_id>/start', methods=['POST'])
def api_start_server(server_id):
    servers = load_servers()
    if server_id not in servers:
        return jsonify({'error': 'Not found'}), 404

    proc = server_processes.get(server_id)
    if proc and proc.poll() is None:
        return jsonify({'error': 'Already running'}), 400

    s = servers[server_id]
    ram = s.get('ram', '1024')
    jar_path = s.get('jar_path')
    server_dir = s.get('dir')

    if not os.path.exists(jar_path):
        return jsonify({'error': 'Server jar not found. Is it installed?'}), 400

    cmd = [
        'java', f'-Xmx{ram}M', f'-Xms{JAVA_MIN_RAM}',
        '-jar', jar_path, '--nogui'
    ]
    proc = subprocess.Popen(
        cmd, cwd=server_dir,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    server_processes[server_id] = proc
    servers[server_id]['status'] = 'running'
    save_servers(servers)

    t = threading.Thread(target=stream_output, args=(server_id, proc), daemon=True)
    t.start()

    broadcast_log(server_id, f"[Panel] Started server PID {proc.pid}")
    return jsonify({'status': 'started'})


@app.route('/api/servers/<server_id>/stop', methods=['POST'])
def api_stop_server(server_id):
    proc = server_processes.get(server_id)
    if not proc or proc.poll() is not None:
        return jsonify({'error': 'Not running'}), 400
    try:
        proc.stdin.write(b'stop\n')
        proc.stdin.flush()
    except Exception:
        proc.terminate()
    broadcast_log(server_id, "[Panel] Stop command sent.")
    return jsonify({'status': 'stopping'})


@app.route('/api/servers/<server_id>/command', methods=['POST'])
def api_send_command(server_id):
    cmd = request.json.get('command', '').strip()
    proc = server_processes.get(server_id)
    if not proc or proc.poll() is not None:
        return jsonify({'error': 'Server not running'}), 400
    try:
        proc.stdin.write((cmd + '\n').encode())
        proc.stdin.flush()
        broadcast_log(server_id, f"> {cmd}")
        return jsonify({'status': 'sent'})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/servers/<server_id>/restart', methods=['POST'])
def api_restart_server(server_id):
    api_stop_server(server_id)
    time.sleep(4)
    return api_start_server(server_id)


@app.route('/api/servers/<server_id>/new-world', methods=['POST'])
def api_new_world(server_id):
    """Delete the world folder and restart."""
    servers = load_servers()
    if server_id not in servers:
        return jsonify({'error': 'Not found'}), 404

    proc = server_processes.get(server_id)
    running = proc and proc.poll() is None

    if running:
        try:
            proc.stdin.write(b'stop\n')
            proc.stdin.flush()
            time.sleep(5)
        except Exception:
            pass

    server_dir = servers[server_id]['dir']
    world_dir = os.path.join(server_dir, 'world')
    world_nether = os.path.join(server_dir, 'world_nether')
    world_end = os.path.join(server_dir, 'world_the_end')

    for d in [world_dir, world_nether, world_end]:
        if os.path.exists(d):
            shutil.rmtree(d)
            broadcast_log(server_id, f"[Panel] Deleted {os.path.basename(d)}")

    broadcast_log(server_id, "[Panel] World deleted. Starting fresh...")

    if running:
        return api_start_server(server_id)
    return jsonify({'status': 'world deleted'})


@app.route('/api/servers/<server_id>/tunnel/start', methods=['POST'])
def api_start_tunnel(server_id):
    """Start a playit.gg tunnel for the given server."""
    servers = load_servers()
    if server_id not in servers:
        return jsonify({'error': 'Not found'}), 404

    existing = playit_processes.get(server_id)
    if existing and existing.poll() is None:
        return jsonify({'error': 'Tunnel already running'}), 400

    port = servers[server_id].get('port', 25565)
    secret = request.json.get('secret', '')  # playit.gg secret key

    # Check if playit is available
    playit_bin = shutil.which('playit') or '/usr/local/bin/playit'

    cmd_args = [playit_bin]
    if secret:
        cmd_args += ['--secret', secret]

    try:
        proc = subprocess.Popen(
            cmd_args,
            cwd=get_server_dir(server_id),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )
        playit_processes[server_id] = proc
        t = threading.Thread(target=stream_playit_output, args=(server_id, proc), daemon=True)
        t.start()
        broadcast_log(server_id, f"[Panel] playit.gg tunnel started (PID {proc.pid}) for port {port}")
        return jsonify({'status': 'tunnel starting'})
    except FileNotFoundError:
        return jsonify({'error': 'playit binary not found. Install playit.gg on this machine first.'}), 500


@app.route('/api/servers/<server_id>/tunnel/stop', methods=['POST'])
def api_stop_tunnel(server_id):
    proc = playit_processes.get(server_id)
    if proc and proc.poll() is None:
        proc.terminate()
        broadcast_log(server_id, "[Panel] Tunnel stopped.")
        playit_tunnels.pop(server_id, None)
        servers = load_servers()
        if server_id in servers:
            servers[server_id]['tunnel_ip'] = None
            servers[server_id]['tunnel_port'] = None
            save_servers(servers)
        return jsonify({'status': 'stopped'})
    return jsonify({'error': 'Not running'}), 400


@app.route('/api/servers/<server_id>', methods=['DELETE'])
def api_delete_server(server_id):
    servers = load_servers()
    if server_id not in servers:
        return jsonify({'error': 'Not found'}), 404

    proc = server_processes.get(server_id)
    if proc and proc.poll() is None:
        proc.terminate()

    tp = playit_processes.get(server_id)
    if tp and tp.poll() is None:
        tp.terminate()

    server_dir = servers[server_id].get('dir')
    if server_dir and os.path.exists(server_dir):
        shutil.rmtree(server_dir)

    del servers[server_id]
    save_servers(servers)
    return jsonify({'status': 'deleted'})


@app.route('/api/servers/<server_id>/logs', methods=['GET'])
def api_get_logs(server_id):
    return jsonify(server_logs.get(server_id, []))


@app.route('/api/servers/<server_id>/status', methods=['GET'])
def api_get_status(server_id):
    servers = load_servers()
    if server_id not in servers:
        return jsonify({'error': 'Not found'}), 404
    proc = server_processes.get(server_id)
    running = proc and proc.poll() is None
    tunnel_proc = playit_processes.get(server_id)
    tunnel_running = tunnel_proc and tunnel_proc.poll() is None
    t_info = playit_tunnels.get(server_id, {})
    return jsonify({
        'running': running,
        'tunnel_running': tunnel_running,
        'tunnel_ip': t_info.get('ip', servers[server_id].get('tunnel_ip')),
        'tunnel_port': t_info.get('port', servers[server_id].get('tunnel_port')),
        'status': servers[server_id].get('status'),
    })


# ─── WebSocket console ─────────────────────────────────────────────────────────

@sock.route('/ws/console/<server_id>')
def ws_console(ws, server_id):
    q = queue.Queue()
    if server_id not in server_log_queues:
        server_log_queues[server_id] = []
    server_log_queues[server_id].append(q)

    # Send backlog
    for line in server_logs.get(server_id, []):
        try:
            ws.send(line)
        except Exception:
            break

    try:
        while True:
            try:
                line = q.get(timeout=30)
                ws.send(line)
            except queue.Empty:
                try:
                    ws.send('')  # ping
                except Exception:
                    break
    except Exception:
        pass
    finally:
        server_log_queues[server_id].remove(q)


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8467, debug=False)