#!/usr/bin/env python3
"""
atem_superjoy_bridge.py
Raspberry Pi service that monitors a Blackmagic ATEM switcher for preview
input changes and forwards camera-select commands to a PTZOptics SuperJoy
via its official HTTP-CGI API.

Also exposes a web UI + REST API on port 8080 for monitoring and configuration.

SuperJoy HTTP-CGI API reference:
  Camera select:  GET /cgi-bin/joyctrl.cgi?f=camselect&group=<g>&camid=<c>
  Status inquiry: GET /cgi-bin/joyctrl.cgi?f=inquiry&action=status
  Preset recall:  GET /cgi-bin/joyctrl.cgi?f=directpresets&action=recall&group=<g>&camid=<c>&preset=<p>&presetspeed=<s>
  Custom button:  GET /cgi-bin/joyctrl.cgi?f=custom&action=trigger&buttonid=<1-4>
  HDMI output:    GET /cgi-bin/joyctrl.cgi?f=hdmiout&action=<on|off|toggle>

Dependencies:
    pip install PyATEMMax flask requests

Usage:
    python3 atem_superjoy_bridge.py
    Then open http://<pi-ip>:8080 in a browser.
"""

import time
import logging
import json
import threading
import datetime
from collections import deque
from typing import Optional

import requests
import PyATEMMax
from flask import Flask, jsonify, request, abort

# ──────────────────────────────────────────────────────────────────────────────
# Default configuration
# ──────────────────────────────────────────────────────────────────────────────

CONFIG_FILE = "config.json"

DEFAULT_CONFIG = {
    "atem_ip":        "192.168.1.240",
    "superjoy_ip":    "192.168.1.100",
    "superjoy_port":  80,
    "superjoy_group": 1,          # SuperJoy camera group (1-5)
    "http_port":      8080,
    "poll_interval":  0.05,
    "http_timeout":   3.0,        # seconds for SuperJoy HTTP requests
    "log_level":      "INFO",
    # ATEM input number -> SuperJoy camera ID within the group
    "camera_map": {
        "1": 1,
        "2": 2,
        "3": 3,
        "4": 4,
        "5": 5,
        "6": 6,
    },
}

# ──────────────────────────────────────────────────────────────────────────────
# Logging
# ──────────────────────────────────────────────────────────────────────────────

LOG_FILE = "atem_superjoy.log"

_log_formatter = logging.Formatter(
    fmt="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# Console handler
_console_handler = logging.StreamHandler()
_console_handler.setFormatter(_log_formatter)

# Rotating file handler — 1 MB per file, keep 5 old files
from logging.handlers import RotatingFileHandler
_file_handler = RotatingFileHandler(
    LOG_FILE, maxBytes=1_000_000, backupCount=5, encoding="utf-8"
)
_file_handler.setFormatter(_log_formatter)

logging.basicConfig(level=logging.INFO, handlers=[_console_handler, _file_handler])
log = logging.getLogger("atem_superjoy")


# ──────────────────────────────────────────────────────────────────────────────
# Config manager
# ──────────────────────────────────────────────────────────────────────────────

class Config:
    def __init__(self):
        self._lock = threading.Lock()
        self._data = dict(DEFAULT_CONFIG)
        self._data["camera_map"] = dict(DEFAULT_CONFIG["camera_map"])
        self._load()

    def _load(self):
        try:
            with open(CONFIG_FILE) as f:
                saved = json.load(f)
            with self._lock:
                self._data.update(saved)
            log.info("Config loaded from %s", CONFIG_FILE)
        except FileNotFoundError:
            pass
        except Exception as e:
            log.warning("Could not load config: %s", e)

    def save(self):
        try:
            with self._lock:
                data = dict(self._data)
            with open(CONFIG_FILE, "w") as f:
                json.dump(data, f, indent=2)
        except Exception as e:
            log.error("Could not save config: %s", e)

    def get(self, key, default=None):
        with self._lock:
            return self._data.get(key, default)

    def set(self, key, value):
        with self._lock:
            self._data[key] = value

    def as_dict(self):
        with self._lock:
            return dict(self._data)

    @property
    def camera_map(self) -> dict:
        with self._lock:
            return {int(k): int(v) for k, v in self._data["camera_map"].items()}

    def set_camera_map(self, m: dict):
        with self._lock:
            self._data["camera_map"] = {str(k): int(v) for k, v in m.items()}


config = Config()
logging.getLogger().setLevel(getattr(logging, config.get("log_level", "INFO"), logging.INFO))


# ──────────────────────────────────────────────────────────────────────────────
# Shared state
# ──────────────────────────────────────────────────────────────────────────────

class State:
    def __init__(self):
        self._lock               = threading.Lock()
        self.atem_connected      = False
        self.superjoy_reachable  = False
        self.superjoy_group      = None   # last reported group from inquiry
        self.superjoy_camid      = None   # last reported camid from inquiry
        self.preview_input       = None
        self.program_input       = None
        self.last_camera_sent    = None
        self.commands_sent       = 0
        self.started_at          = datetime.datetime.now().isoformat()
        self.event_log: deque    = deque(maxlen=200)

    def log_event(self, msg: str):
        with self._lock:
            self.event_log.appendleft({
                "time": datetime.datetime.now().strftime("%H:%M:%S"),
                "msg":  msg,
            })

    def snapshot(self) -> dict:
        with self._lock:
            return {
                "atem_connected":     self.atem_connected,
                "superjoy_reachable": self.superjoy_reachable,
                "superjoy_group":     self.superjoy_group,
                "superjoy_camid":     self.superjoy_camid,
                "preview_input":      self.preview_input,
                "program_input":      self.program_input,
                "last_camera_sent":   self.last_camera_sent,
                "commands_sent":      self.commands_sent,
                "started_at":         self.started_at,
                "uptime_seconds":     int(
                    (datetime.datetime.now() -
                     datetime.datetime.fromisoformat(self.started_at)).total_seconds()
                ),
                "recent_events":      list(self.event_log)[:50],
            }

state = State()


# ──────────────────────────────────────────────────────────────────────────────
# SuperJoy HTTP-CGI client
# ──────────────────────────────────────────────────────────────────────────────

class SuperJoyClient:
    """
    Sends commands to the PTZOptics SuperJoy via its HTTP-CGI API.
    All commands are stateless GET requests — no persistent connection needed.

    API base: http://<superjoy_ip>/cgi-bin/joyctrl.cgi
    """

    def __init__(self):
        self._error_suppressed = False  # True after first failure, reset on reconnect

    @property
    def base_url(self) -> str:
        return f"http://{config.get('superjoy_ip')}:{int(config.get('superjoy_port', 80))}/cgi-bin/joyctrl.cgi"

    @property
    def timeout(self) -> float:
        return float(config.get("http_timeout", 3.0))

    def _get(self, params: dict) -> Optional[requests.Response]:
        try:
            r = requests.get(self.base_url, params=params, timeout=self.timeout)
            r.raise_for_status()
            if not state.superjoy_reachable:
                msg = f"SuperJoy reconnected at {config.get('superjoy_ip')}"
                log.info(msg)
                state.log_event(msg)
                self._error_suppressed = False
            return r
        except requests.RequestException as e:
            if not self._error_suppressed:
                msg = f"SuperJoy unreachable: {e}"
                log.error(msg)
                state.log_event(msg)
                self._error_suppressed = True
            state.superjoy_reachable = False
            return None

    # ── Camera selection ──────────────────────────────────────────────────────

    def select_camera(self, group: int, camid: int) -> bool:
        """
        Select a camera by group and camera ID.
        GET /cgi-bin/joyctrl.cgi?f=camselect&group=<group>&camid=<camid>
        """
        r = self._get({"f": "camselect", "group": group, "camid": camid})
        if r is not None:
            state.superjoy_reachable = True
            state.last_camera_sent   = camid
            state.commands_sent     += 1
            msg = f"SuperJoy <- camselect(group={group}, camid={camid})"
            log.info(msg)
            state.log_event(msg)
            return True
        return False

    # ── Status inquiry ────────────────────────────────────────────────────────

    def inquiry(self) -> Optional[dict]:
        """
        Query current SuperJoy state.
        GET /cgi-bin/joyctrl.cgi?f=inquiry&action=status
        Returns dict with keys: group, camid, preset, hdmi  (or None on error)
        """
        r = self._get({"f": "inquiry", "action": "status"})
        if r is None:
            return None
        try:
            data = r.json()
            state.superjoy_reachable = True
            state.superjoy_group  = data.get("group")
            state.superjoy_camid  = data.get("camid")
            return data
        except Exception as e:
            log.warning("SuperJoy inquiry parse error: %s (body: %s)", e, r.text)
            return None


# ──────────────────────────────────────────────────────────────────────────────
# Background SuperJoy poller (keeps superjoy_reachable up to date)
# ──────────────────────────────────────────────────────────────────────────────

def superjoy_poll_loop(client: SuperJoyClient):
    while True:
        client.inquiry()
        time.sleep(5)


# ──────────────────────────────────────────────────────────────────────────────
# ATEM monitor
# ──────────────────────────────────────────────────────────────────────────────

class ATEMMonitor:
    def __init__(self, superjoy: SuperJoyClient):
        self.superjoy          = superjoy
        self._switcher         = PyATEMMax.ATEMMax()
        self._last_preview: Optional[int] = None
        self._running          = False

    @property
    def atem_ip(self): return config.get("atem_ip")

    def _on_connect(self, params):
        state.atem_connected = True
        msg = f"ATEM connected — {self.atem_ip}"
        log.info(msg); state.log_event(msg)

    def _on_disconnect(self, params):
        state.atem_connected = False
        msg = "ATEM disconnected"
        log.warning(msg); state.log_event(msg)

    def _on_receive(self, params):
        try:
            preview_source = self._switcher.previewInput[0].videoSource.value
            program_source = self._switcher.programInput[0].videoSource.value
        except (AttributeError, IndexError):
            return

        try:
            state.program_input = int(program_source)
        except Exception:
            pass

        if preview_source != self._last_preview:
            msg = f"ATEM preview: input {self._last_preview} -> {preview_source}"
            log.info(msg); state.log_event(msg)
            self._last_preview  = preview_source
            state.preview_input = int(preview_source)
            self._handle_preview_change(int(preview_source))

    def _handle_preview_change(self, atem_input: int):
        camid = config.camera_map.get(atem_input)
        if camid is None:
            log.debug("No SuperJoy mapping for ATEM input %d — ignoring", atem_input)
            return
        group = int(config.get("superjoy_group", 1))
        if not self.superjoy.select_camera(group, camid):
            log.error("Failed to select camera (group=%d, camid=%d)", group, camid)

    def run(self):
        self._running = True
        self._switcher.registerEvent(self._switcher.atem.events.receive,    self._on_receive)
        self._switcher.registerEvent(self._switcher.atem.events.connect,    self._on_connect)
        self._switcher.registerEvent(self._switcher.atem.events.disconnect, self._on_disconnect)

        log.info("Connecting to ATEM at %s ...", self.atem_ip)
        self._switcher.connect(self.atem_ip)
        self._switcher.waitForConnection(infinite=True)

        try:
            while self._running:
                time.sleep(config.get("poll_interval", 0.05))
        except KeyboardInterrupt:
            pass

        self.stop()

    def stop(self):
        self._running = False
        self._switcher.disconnect()
        log.info("Stopped.")


# ──────────────────────────────────────────────────────────────────────────────
# Flask HTTP interface
# ──────────────────────────────────────────────────────────────────────────────

app = Flask(__name__)
logging.getLogger("werkzeug").setLevel(logging.WARNING)

HTML_UI = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<link rel="icon" href="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHZpZXdCb3g9IjAgMCAxMDAgMTAwIj4KICA8cmVjdCB3aWR0aD0iMTAwIiBoZWlnaHQ9IjEwMCIgZmlsbD0iI2NjMDAwMCIvPgogIDx0ZXh0IHg9IjI4IiB5PSI1MiIgZm9udC1mYW1pbHk9IkFyaWFsIEJsYWNrLCBBcmlhbCwgc2Fucy1zZXJpZiIgZm9udC13ZWlnaHQ9IjkwMCIgZm9udC1zaXplPSI1MiIgZmlsbD0iI2ZmZmZmZiIgdGV4dC1hbmNob3I9Im1pZGRsZSI+UDwvdGV4dD4KICA8dGV4dCB4PSI3MiIgeT0iNTIiIGZvbnQtZmFtaWx5PSJBcmlhbCBCbGFjaywgQXJpYWwsIHNhbnMtc2VyaWYiIGZvbnQtd2VpZ2h0PSI5MDAiIGZvbnQtc2l6ZT0iNTIiIGZpbGw9IiNmZmZmZmYiIHRleHQtYW5jaG9yPSJtaWRkbGUiPlQ8L3RleHQ+CiAgPHRleHQgeD0iNTAiIHk9Ijk3IiBmb250LWZhbWlseT0iQXJpYWwgQmxhY2ssIEFyaWFsLCBzYW5zLXNlcmlmIiBmb250LXdlaWdodD0iOTAwIiBmb250LXNpemU9IjUyIiBmaWxsPSIjZmZmZmZmIiB0ZXh0LWFuY2hvcj0ibWlkZGxlIj5aPC90ZXh0Pgo8L3N2Zz4=">
<title>ATEM SuperJoy Bridge</title>
<style>
  :root{
    --bg:#111;--bg2:#1e1e1e;--bg3:#252525;--border:#3a3a3a;--border2:#252525;
    --text:#eee;--text2:#bbb;--muted:#555;--log-text:#777;--log-time:#3a3a3a;
    --log-border:#191919;--sec-btn:#2a2a2a;--sec-btn-h:#333;
  }
  body.light{
    --bg:#f0f0f0;--bg2:#fff;--bg3:#f5f5f5;--border:#ccc;--border2:#e0e0e0;
    --text:#111;--text2:#333;--muted:#888;--log-text:#555;--log-time:#aaa;
    --log-border:#e0e0e0;--sec-btn:#e0e0e0;--sec-btn-h:#d0d0d0;
  }
  *{box-sizing:border-box;margin:0;padding:0}
  body{font-family:system-ui,sans-serif;background:var(--bg);color:var(--text);padding:1.5rem;transition:background .2s,color .2s}
  h1{font-size:1.3rem;margin-bottom:1.2rem;color:var(--text)}
  h2{font-size:.75rem;margin:1.2rem 0 .5rem;color:var(--muted);text-transform:uppercase;letter-spacing:.1em}
  .grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(260px,1fr));gap:1rem}
  .card{background:var(--bg2);border-radius:8px;padding:1rem}
  .status-row{display:flex;align-items:center;gap:.5rem;margin-bottom:.45rem;font-size:.88rem}
  .dot{width:9px;height:9px;border-radius:50%;flex-shrink:0;transition:background .3s}
  .dot.ok{background:#4caf50}.dot.err{background:#f44336}.dot.unk{background:var(--muted)}
  .stat{font-size:.8rem;color:var(--muted);margin-bottom:.2rem}
  .stat span{color:var(--text2)}
  table{width:100%;border-collapse:collapse;font-size:.84rem}
  th,td{padding:.32rem .45rem;text-align:left;border-bottom:1px solid var(--border2)}
  th{color:var(--muted);font-weight:normal}
  input[type=number],input[type=text],select{
    background:var(--bg3);border:1px solid var(--border);color:var(--text);
    padding:.28rem .45rem;border-radius:4px;font-size:.84rem;width:100%}
  button{background:#0066cc;color:#fff;border:none;border-radius:4px;
    padding:.32rem .75rem;cursor:pointer;font-size:.8rem;margin-top:.35rem}
  button:hover{background:#0055aa}
  button.sm{margin-top:0;padding:.22rem .5rem}
  button.sec{background:var(--sec-btn);border:1px solid var(--border);color:var(--text)}
  button.sec:hover{background:var(--sec-btn-h)}
  button.danger{background:#c0392b;color:#fff}
  button.danger:hover{background:#a93226}
  .row{display:flex;gap:.4rem;margin-bottom:.38rem;align-items:center}
  .row label{font-size:.8rem;color:var(--muted);width:120px;flex-shrink:0}
  .log-box{font-family:monospace;font-size:.73rem;color:var(--log-text);max-height:190px;overflow-y:auto}
  .log-box .e{padding:.1rem 0;border-bottom:1px solid var(--log-border)}
  .log-box .t{color:var(--log-time);margin-right:.4rem}
  .flash{font-size:.76rem;color:#4caf50;margin-left:.5rem;display:none}
  .actions{display:flex;flex-wrap:wrap;gap:.35rem;margin-top:.5rem}
  select{width:auto;min-width:60px}
  #theme-btn{float:right;margin-top:0;background:var(--sec-btn);border:1px solid var(--border);color:var(--text);font-size:.8rem;padding:.28rem .65rem;border-radius:4px;cursor:pointer}
  #theme-btn:hover{background:var(--sec-btn-h)}
</style>
</head>
<body>
<h1>ATEM &rarr; SuperJoy Bridge<button id="theme-btn" onclick="toggleTheme()">&#9788; Light</button></h1>

<div class="grid">

  <!-- Status -->
  <div class="card">
    <h2>Status</h2>
    <div class="status-row"><div class="dot unk" id="dot-atem"></div><span id="lbl-atem">ATEM —</span></div>
    <div class="status-row"><div class="dot unk" id="dot-joy"></div><span id="lbl-joy">SuperJoy —</span></div>
    <br>
    <div class="stat">ATEM program input <span id="program-input">—</span></div>
    <div class="stat">ATEM preview input <span id="preview-input">—</span></div>
    <div class="stat">SuperJoy group / cam <span id="joy-gcam">—</span></div>
    <div class="stat">Last cam sent &nbsp;&nbsp;&nbsp;&nbsp;<span id="last-cam">—</span></div>
    <div class="stat">Commands sent &nbsp;&nbsp;&nbsp;<span id="cmd-count">0</span></div>
    <div class="stat">Uptime &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;<span id="uptime">—</span></div>
  </div>

  <!-- Connection settings -->
  <div class="card">
    <h2>Connection Settings</h2>
    <div class="row"><label>ATEM IP</label><input type="text" id="cfg-atem-ip"></div>
    <div class="row"><label>SuperJoy IP</label><input type="text" id="cfg-joy-ip"></div>
    <div class="row"><label>SuperJoy Port</label><input type="number" id="cfg-joy-port" min="1" max="65535" style="width:80px"></div>
    <div class="row"><label>SuperJoy Group</label>
      <select id="cfg-joy-group">
        <option>1</option><option>2</option><option>3</option><option>4</option><option>5</option>
      </select>
    </div>
    <div class="row"><label>HTTP timeout (s)</label><input type="number" id="cfg-timeout" step="0.5" min="0.5" style="width:70px"></div>
    <div class="row"><label>Poll interval (s)</label><input type="number" id="cfg-poll" step="0.01" min="0.01" style="width:70px"></div>
    <button onclick="saveConnConfig()">Save &amp; Apply</button>
    <span class="flash" id="save-msg">Saved!</span>
  </div>

  <!-- Manual SuperJoy controls -->
  <div class="card">
    <h2>Manual SuperJoy Controls</h2>
    <div class="row"><label>Group</label>
      <select id="m-group"><option>1</option><option>2</option><option>3</option><option>4</option><option>5</option></select>
    </div>
    <div class="row"><label>Camera ID</label><input type="number" id="m-camid" value="1" min="1" max="170" style="width:70px"></div>
    <div class="actions">
      <button onclick="manualCamSelect()">Select Camera</button>
    </div>
    <span class="flash" id="ctrl-msg">Sent!</span>
  </div>

</div>

<!-- Camera map -->
<h2>Camera Map — ATEM Input &rarr; SuperJoy Camera ID</h2>
<div class="card">
  <table>
    <thead><tr><th style="width:38%">ATEM Input</th><th style="width:38%">SuperJoy Camera ID</th><th></th></tr></thead>
    <tbody id="cam-tbody"></tbody>
  </table>
  <div style="margin-top:.6rem;display:flex;gap:.4rem;align-items:center">
    <button onclick="addRow()">+ Add Row</button>
    <button onclick="saveCameraMap()">Save Map</button>
    <span class="flash" id="map-save-msg">Saved!</span>
  </div>
</div>

<!-- Event log -->
<h2>Event Log</h2>
<div class="card">
  <div class="log-box" id="log-box"></div>
</div>

<script>
async function refresh(){
  const [s,c] = await Promise.all([
    fetch('/api/state').then(r=>r.json()),
    fetch('/api/config').then(r=>r.json()),
  ]).catch(()=>[null,null]);
  if(!s||!c) return;

  dot('dot-atem', s.atem_connected);
  dot('dot-joy',  s.superjoy_reachable);
  setText('lbl-atem', `ATEM (${c.atem_ip}) — ${s.atem_connected?'connected':'disconnected'}`);
  setText('lbl-joy',  `SuperJoy (${c.superjoy_ip}:${c.superjoy_port}) — ${s.superjoy_reachable?'reachable':'unreachable'}`);
  setText('program-input', s.program_input??'—');
  setText('preview-input', s.preview_input??'—');
  setText('joy-gcam',   s.superjoy_group!=null ? `${s.superjoy_group} / ${s.superjoy_camid}` : '—');
  setText('last-cam',   s.last_camera_sent??'—');
  setText('cmd-count',  s.commands_sent);
  setText('uptime',     fmtUptime(s.uptime_seconds));

  if(!configDirty){
    setVal('cfg-atem-ip',  c.atem_ip);
    setVal('cfg-joy-ip',   c.superjoy_ip);
    setVal('cfg-joy-port', c.superjoy_port);
    setVal('cfg-timeout',  c.http_timeout);
  setVal('cfg-poll',     c.poll_interval);
    setSel('cfg-joy-group', c.superjoy_group);
  }

  renderMap(c.camera_map);

  document.getElementById('log-box').innerHTML =
    (s.recent_events||[]).map(e=>`<div class="e"><span class="t">${e.time}</span>${e.msg}</div>`).join('');
}

function renderMap(m){
  if(document.activeElement?.closest('#cam-tbody')) return;
  const tbody = document.getElementById('cam-tbody');
  tbody.innerHTML='';
  Object.entries(m).sort((a,b)=>+a[0]-+b[0]).forEach(([inp,cam])=>{
    const tr=document.createElement('tr');
    tr.innerHTML=
      `<td><input type="number" value="${inp}" class="inp-k" min="1"></td>`+
      `<td><input type="number" value="${cam}" class="inp-v" min="1" max="170"></td>`+
      `<td><button class="sm danger" onclick="this.closest('tr').remove()">Remove</button></td>`;
    tbody.appendChild(tr);
  });
}

function addRow(){
  const tbody=document.getElementById('cam-tbody');
  const tr=document.createElement('tr');
  tr.innerHTML=
    `<td><input type="number" class="inp-k" min="1" placeholder="ATEM input"></td>`+
    `<td><input type="number" class="inp-v" min="1" max="170" placeholder="cam id"></td>`+
    `<td><button class="sm danger" onclick="this.closest('tr').remove()">Remove</button></td>`;
  tbody.appendChild(tr);
  tr.querySelector('.inp-k').focus();
}

async function saveCameraMap(){
  const map={};
  document.querySelectorAll('#cam-tbody tr').forEach(tr=>{
    const k=tr.querySelector('.inp-k').value.trim();
    const v=tr.querySelector('.inp-v').value.trim();
    if(k&&v) map[k]=parseInt(v);
  });
  await patch({camera_map:map});
  clearDirty();
  flash('map-save-msg');
}

let configDirty = false;
document.querySelectorAll('#cfg-atem-ip,#cfg-joy-ip,#cfg-joy-port,#cfg-timeout,#cfg-joy-group').forEach(el=>{
  el.addEventListener('input', ()=>{ configDirty=true; });
});

async function saveConnConfig(){
  clearDirty();
  await patch({
    atem_ip:        document.getElementById('cfg-atem-ip').value.trim(),
    superjoy_ip:    document.getElementById('cfg-joy-ip').value.trim(),
    superjoy_port:  parseInt(document.getElementById('cfg-joy-port').value),
    superjoy_group: parseInt(document.getElementById('cfg-joy-group').value),
    http_timeout:   parseFloat(document.getElementById('cfg-timeout').value),
    poll_interval:  parseFloat(document.getElementById('cfg-poll').value),
  });
  configDirty = false;
  flash('save-msg');
}

// Manual controls
async function manualCamSelect(){
  const g=parseInt(document.getElementById('m-group').value);
  const c=parseInt(document.getElementById('m-camid').value);
  await fetch(`/api/superjoy/camselect?group=${g}&camid=${c}`,{method:'POST'});
  flash('ctrl-msg');
}
async function patch(body){
  await fetch('/api/config',{method:'PATCH',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)});
}

const dirty = new Set();
document.addEventListener('input', e=>{ if(e.target.id) dirty.add(e.target.id); });

function dot(id,ok){ document.getElementById(id).className='dot '+(ok?'ok':'err'); }
function setText(id,v){ document.getElementById(id).textContent=v; }
function setVal(id,v){ const el=document.getElementById(id); if(el && !dirty.has(id)) el.value=v; }
function setSel(id,v){ const el=document.getElementById(id); if(el && !dirty.has(id)) el.value=String(v); }
function clearDirty(){ dirty.clear(); }
function fmtUptime(s){ return `${Math.floor(s/3600)}h ${Math.floor(s%3600/60)}m ${s%60}s`; }
function flash(id){ const el=document.getElementById(id); el.style.display='inline'; setTimeout(()=>el.style.display='none',2000); }

function toggleTheme(){
  const light = document.body.classList.toggle('light');
  document.getElementById('theme-btn').textContent = light ? '\u2600 Dark' : '\u2688 Light';
  localStorage.setItem('theme', light ? 'light' : 'dark');
}
(function(){
  if(localStorage.getItem('theme')==='light'){
    document.body.classList.add('light');
    document.getElementById('theme-btn').textContent='\u2600 Dark';
  }
})();

refresh();
setInterval(refresh, 2000);
</script>
</body>
</html>
"""

# Global SuperJoy client (set in main)
_superjoy: Optional[SuperJoyClient] = None


@app.route("/")
def index():
    return HTML_UI


@app.route("/api/state")
def api_state():
    return jsonify(state.snapshot())


@app.route("/api/config", methods=["GET"])
def api_config_get():
    return jsonify(config.as_dict())


@app.route("/api/config", methods=["PATCH"])
def api_config_patch():
    data = request.get_json(force=True, silent=True) or {}
    allowed = {"atem_ip", "superjoy_ip", "superjoy_port", "superjoy_group", "http_timeout",
               "poll_interval", "log_level", "camera_map", "http_port"}
    for key, value in data.items():
        if key not in allowed:
            abort(400, f"Unknown config key: {key}")
        if key == "camera_map":
            config.set_camera_map(value)
        else:
            config.set(key, value)
    config.save()
    log.info("Config updated via HTTP: %s", list(data.keys()))
    state.log_event(f"Config updated: {', '.join(data.keys())}")
    return jsonify({"ok": True, "config": config.as_dict()})


@app.route("/api/config", methods=["PUT"])
def api_config_put():
    data = request.get_json(force=True, silent=True) or {}
    for key, value in data.items():
        if key == "camera_map":
            config.set_camera_map(value)
        else:
            config.set(key, value)
    config.save()
    return jsonify({"ok": True})


# ── SuperJoy control endpoints ────────────────────────────────────────────────

@app.route("/api/superjoy/status")
def api_superjoy_status():
    data = _superjoy.inquiry()
    return jsonify(data or {})


@app.route("/api/superjoy/camselect", methods=["POST"])
def api_superjoy_camselect():
    group = int(request.args.get("group", config.get("superjoy_group", 1)))
    camid = int(request.args.get("camid", 1))
    ok = _superjoy.select_camera(group, camid)
    return jsonify({"ok": ok})


def run_http(port: int):
    app.run(host="0.0.0.0", port=port, debug=False, use_reloader=False)


# ──────────────────────────────────────────────────────────────────────────────
# Entry point
# ──────────────────────────────────────────────────────────────────────────────

def main():
    global _superjoy

    log.info("ATEM -> SuperJoy bridge starting")
    log.info("  ATEM:          %s", config.get("atem_ip"))
    log.info("  SuperJoy:      %s  (group %s)", config.get("superjoy_ip"), config.get("superjoy_group"))
    log.info("  Camera map:    %s", config.camera_map)

    _superjoy = SuperJoyClient()
    monitor   = ATEMMonitor(_superjoy)

    # Background SuperJoy status poller
    threading.Thread(target=superjoy_poll_loop, args=(_superjoy,), daemon=True).start()

    # HTTP UI
    http_port = int(config.get("http_port", 8080))
    log.info("  HTTP UI:       http://0.0.0.0:%d", http_port)
    threading.Thread(target=run_http, args=(http_port,), daemon=True).start()

    try:
        monitor.run()
    except KeyboardInterrupt:
        monitor.stop()


if __name__ == "__main__":
    main()


# ──────────────────────────────────────────────────────────────────────────────
# systemd service unit (save as /etc/systemd/system/atem-superjoy.service)
# ──────────────────────────────────────────────────────────────────────────────
#
# [Unit]
# Description=ATEM -> PTZOptics SuperJoy bridge
# After=network-online.target
# Wants=network-online.target
#
# [Service]
# ExecStart=/usr/bin/python3 /home/pi/atem_superjoy_bridge.py
# WorkingDirectory=/home/pi
# Restart=always
# RestartSec=5
# User=pi
#
# [Install]
# WantedBy=multi-user.target
#
# Enable with:
#   sudo systemctl daemon-reload
#   sudo systemctl enable --now atem-superjoy
