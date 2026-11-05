"""IoT Gateway (ban 0.2): Sink Node <-> MQTT Gateway API cua CoreIOT, ca 2 chieu.

Chieu len : dong JSON 'tele'/'ble'/'ack' -> v1/gateway/telemetry, attributes.
Chieu xuong: v1/gateway/rpc (setPump/setFan/setLed/get*/getStatus) va
             shared attribute 'mode' -> dong 'cmd' xuong Sink, cho 'ack' roi tra loi RPC.

Cach chay:
  python gateway.py --port socket://127.0.0.1:7777   # voi fake_sink.py (2 cua so)
  python gateway.py --port COM5                      # voi Sink Node that
  them --dry de chi in ra, khong gui MQTT
"""
import argparse
import json
import os
import threading
import time

import paho.mqtt.client as mqtt
import serial
from dotenv import load_dotenv

load_dotenv()
HOST = os.getenv("COREIOT_HOST", "app.coreiot.io")
PORT = int(os.getenv("COREIOT_PORT", "1883"))
TOKEN = os.getenv("GW_TOKEN", "")

# node_id -> (ten device tren CoreIOT, Device Profile)
DEVICES = {1: ("GH_NODE_01", "greenhouse-node"), 2: ("SOIL_BLE_01", "soil-ble-node")}
NODE_OF = {name: nid for nid, (name, _) in DEVICES.items()}
TELE_KEYS = {
    1: ["temp", "humi", "light", "ai_label", "ai_conf", "rssi"],
    2: ["soil_moist", "batt", "rssi"],
}
ATTR_KEYS = ["pump", "fan", "led", "fw_ver"]  # client attribute
SETTERS = {"setPump": "pump", "setFan": "fan", "setLed": "led"}
GETTERS = {"getPump": "pump", "getFan": "fan", "getLed": "led"}
OFFLINE_S = {1: 15, 2: 60}   # khong co goi trong N giay -> coi la offline
ACK_TIMEOUT_S = 3.0          # Sink tu gui lai 3 lan x 300 ms, nen 3 s la du
ACK_ERR = {1: "bad_param", 2: "locked_auto", 3: "node_offline"}


class Gateway:
    def __init__(self, port_url: str, baud: int, dry: bool):
        self.port_url, self.baud, self.dry = port_url, baud, dry
        self.ser = None
        self.ser_lock = threading.Lock()
        self.lock = threading.Lock()          # bao ve cac bien trang thai ben duoi
        self.connected: set[str] = set()      # device da gui v1/gateway/connect
        self.last_attrs: dict[str, dict] = {}
        self.last_seq: dict[int, int] = {}
        self.lost: dict[int, int] = {}        # so goi mat theo node (so lieu cho Ch.4)
        self.last_seen: dict[int, float] = {}
        self.latest: dict[str, dict] = {}     # gia tri moi nhat theo device
        self.mode: dict[str, str] = {"GH_NODE_01": "manual"}
        self.cmd_seq = 0
        self.pending: dict[int, dict] = {}    # seq lenh -> thong tin RPC dang cho ack

        if dry:
            return
        if not TOKEN:
            raise SystemExit("Thieu GW_TOKEN trong file .env")
        self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="gh-gateway")
        self.client.username_pw_set(TOKEN)
        self.client.on_connect = self._on_connect
        self.client.on_message = self._on_message
        self.client.on_disconnect = lambda *a: print("# MQTT mat ket noi, dang thu lai...")
        self.client.reconnect_delay_set(min_delay=1, max_delay=30)
        self.client.connect_async(HOST, PORT, keepalive=60)
        self.client.loop_start()

    # ================= MQTT =================
    def _on_connect(self, client, userdata, flags, reason_code, properties):
        print(f"# MQTT ket noi {HOST}:{PORT}: {reason_code}")
        with self.lock:
            self.connected.clear()  # bao lai cac node sau khi ket noi lai
        client.subscribe("v1/gateway/rpc", qos=1)
        client.subscribe("v1/gateway/attributes", qos=1)
        client.subscribe("v1/gateway/attributes/response", qos=1)

    def _on_message(self, client, userdata, msg):
        try:
            data = json.loads(msg.payload)
        except json.JSONDecodeError:
            return
        if msg.topic == "v1/gateway/rpc":
            self.on_rpc(data)
        elif msg.topic == "v1/gateway/attributes":
            self.on_shared(data.get("device"), data.get("data", {}))
        elif msg.topic == "v1/gateway/attributes/response" and data.get("value") is not None:
            self.on_shared(data.get("device"), {"mode": data["value"]})

    def publish(self, topic: str, payload) -> None:
        if self.dry:
            print(f"[dry] {topic} {json.dumps(payload)}")
        else:
            self.client.publish(topic, json.dumps(payload), qos=1)

    def rpc_reply(self, device: str, rpc_id, data) -> None:
        if rpc_id is not None:
            self.publish("v1/gateway/rpc", {"device": device, "id": rpc_id, "data": data})
            print(f"-> tra loi RPC {device} id={rpc_id}: {data}")

    # ================= chieu xuong =================
    def is_online(self, node: int) -> bool:
        return time.time() - self.last_seen.get(node, 0) < OFFLINE_S[node]

    def on_shared(self, device, data: dict) -> None:
        mode = data.get("mode")
        if isinstance(mode, bool):  # cong tac tren dashboard gui true/false
            mode = "auto" if mode else "manual"
        if device not in NODE_OF or mode not in ("auto", "manual"):
            return
        with self.lock:
            self.mode[device] = mode
        print(f"# {device}: che do -> {mode}")
        self.send_cmd(device, "mode", 1 if mode == "auto" else 0, rpc_id=None)

    def on_rpc(self, data: dict) -> None:
        device = data.get("device")
        req = data.get("data", {})
        rpc_id, method, params = req.get("id"), req.get("method"), req.get("params")
        print(f"<- RPC {device} {method} {params}")
        if device not in NODE_OF:
            return

        if method in GETTERS:
            with self.lock:
                value = bool(self.latest.get(device, {}).get(GETTERS[method], False))
            self.rpc_reply(device, rpc_id, value)
            return

        if method == "getStatus":
            with self.lock:
                status = dict(self.latest.get(device, {}))
                status["mode"] = self.mode.get(device, "manual")
            status["online"] = self.is_online(NODE_OF[device])
            self.rpc_reply(device, rpc_id, status)
            return

        if method in SETTERS:
            # params: true/false (widget Switch) hoac {"value": true, "by": "rule"} (RuleChain)
            if isinstance(params, dict):
                value, by = params.get("value"), params.get("by", "user")
            else:
                value, by = params, "user"
            if isinstance(value, str):
                value = value.strip().lower() == "true"
            if not isinstance(value, bool):
                self.rpc_reply(device, rpc_id, {"ok": False, "err": "bad_param"})
                return
            mode = self.mode.get(device, "manual")
            if mode == "auto" and by != "rule":
                self.rpc_reply(device, rpc_id, {"ok": False, "err": "locked_auto"})
                return
            if mode == "manual" and by == "rule":
                self.rpc_reply(device, rpc_id, {"ok": False, "err": "locked_manual"})
                return
            if not self.is_online(NODE_OF[device]):
                self.rpc_reply(device, rpc_id, {"ok": False, "err": "node_offline"})
                return
            self.send_cmd(device, SETTERS[method], 1 if value else 0, rpc_id)
            return

        self.rpc_reply(device, rpc_id, {"ok": False, "err": "bad_param"})

    def send_cmd(self, device: str, target: str, value: int, rpc_id) -> None:
        with self.lock:
            self.cmd_seq = (self.cmd_seq + 1) % 65536
            seq = self.cmd_seq
            self.pending[seq] = {"device": device, "rpc_id": rpc_id, "target": target, "t": time.time()}
        line = json.dumps({"t": "cmd", "node": NODE_OF[device], "seq": seq,
                           "target": target, "value": value}, separators=(",", ":"))
        print(f"-> {line}")
        self.write_line(line)

    def write_line(self, line: str) -> None:
        with self.ser_lock:
            if self.ser is None:
                print("# Chua co ket noi Serial, bo lenh")
                return
            try:
                self.ser.write((line + "\n").encode())
            except Exception as e:  # noqa: BLE001
                print(f"# Loi ghi Serial: {e}")

    def on_ack(self, msg: dict) -> None:
        node = msg.get("node")
        if node in DEVICES:
            self.update_state(node, msg)
        with self.lock:
            p = self.pending.pop(msg.get("ack_seq"), None)
        if p is None:
            return
        status = msg.get("status", 1)
        if status == 0:
            data = {"ok": True}
            if p["target"] in msg:
                data[p["target"]] = msg[p["target"]]
            self.rpc_reply(p["device"], p["rpc_id"], data)
        else:
            self.rpc_reply(p["device"], p["rpc_id"], {"ok": False, "err": ACK_ERR.get(status, "bad_param")})

    # ================= chieu len =================
    def ensure_connected(self, name: str, profile: str) -> None:
        with self.lock:
            if name in self.connected:
                return
            self.connected.add(name)
        self.publish("v1/gateway/connect", {"device": name, "type": profile})
        if name in self.mode:  # hoi CoreIOT gia tri 'mode' hien tai
            self.publish("v1/gateway/attributes/request",
                         {"id": 1, "device": name, "client": False, "key": "mode"})

    def update_state(self, node: int, msg: dict) -> None:
        name, profile = DEVICES[node]
        self.ensure_connected(name, profile)
        attrs = {k: msg[k] for k in ATTR_KEYS if k in msg}
        with self.lock:
            self.latest.setdefault(name, {}).update(attrs)
            changed = attrs and attrs != self.last_attrs.get(name)
            if changed:
                self.last_attrs[name] = attrs
        if changed:
            self.publish("v1/gateway/attributes", {name: attrs})

    def on_tele(self, msg: dict) -> None:
        node = msg.get("node")
        if node not in DEVICES:
            print(f"# Bo qua node la: {node}")
            return
        name, profile = DEVICES[node]

        seq = msg.get("seq")
        prev = self.last_seq.get(node)
        if prev is not None and seq is not None:
            if seq == prev:
                return  # goi trung
            gap = (seq - prev) % 65536 - 1
            if gap > 0:
                self.lost[node] = self.lost.get(node, 0) + gap
                print(f"# {name}: mat {gap} goi (tong {self.lost[node]})")
        self.last_seq[node] = seq
        self.last_seen[node] = time.time()

        self.ensure_connected(name, profile)
        values = {k: msg[k] for k in TELE_KEYS[node] if k in msg}
        with self.lock:
            self.latest.setdefault(name, {}).update(values)
        self.publish("v1/gateway/telemetry",
                     {name: [{"ts": int(time.time() * 1000), "values": values}]})
        self.update_state(node, msg)
        print(f"-> {name} {values}")

    def handle_line(self, line: str) -> None:
        line = line.strip()
        if not line or line.startswith("#"):
            return
        try:
            msg = json.loads(line)
        except json.JSONDecodeError:
            print(f"# Dong khong phai JSON: {line[:80]}")
            return
        t = msg.get("t")
        if t == "hello":
            print(f"# Sink khoi dong, fw {msg.get('fw_ver')}")
        elif t in ("tele", "ble"):
            self.on_tele(msg)
        elif t == "ack":
            self.on_ack(msg)

    # ================= dinh ky =================
    def periodic(self) -> None:
        now = time.time()
        with self.lock:
            expired = [s for s, p in self.pending.items() if now - p["t"] > ACK_TIMEOUT_S]
            expired_p = [self.pending.pop(s) for s in expired]
        for p in expired_p:
            print(f"# Het thoi gian cho ack: {p['target']}")
            self.rpc_reply(p["device"], p["rpc_id"], {"ok": False, "err": "node_offline"})
        for node, (name, _) in DEVICES.items():
            if node in self.last_seen and not self.is_online(node):
                with self.lock:
                    was = name in self.connected
                    self.connected.discard(name)
                if was:
                    print(f"# {name} offline")
                    self.publish("v1/gateway/disconnect", {"device": name})

    # ================= vong lap chinh =================
    def open_port(self):
        while True:
            try:
                s = serial.serial_for_url(self.port_url, baudrate=self.baud, timeout=0.5)
                print(f"# Da mo {self.port_url}")
                return s
            except Exception as e:  # noqa: BLE001
                print(f"# Khong mo duoc {self.port_url}: {e}. Thu lai sau 2 s")
                time.sleep(2)

    def run(self) -> None:
        buf = b""
        while True:
            if self.ser is None:
                port = self.open_port()
                with self.ser_lock:
                    self.ser = port
                buf = b""
            try:
                data = self.ser.read(self.ser.in_waiting or 1)
            except Exception as e:  # noqa: BLE001
                print(f"# Mat ket noi Sink: {e}")
                with self.ser_lock:
                    try:
                        self.ser.close()
                    except Exception:  # noqa: BLE001
                        pass
                    self.ser = None
                time.sleep(2)
                continue
            buf += data
            while b"\n" in buf:
                raw, buf = buf.split(b"\n", 1)
                self.handle_line(raw.decode("utf-8", errors="replace"))
            self.periodic()


def main() -> None:
    ap = argparse.ArgumentParser(description="Gateway nha kinh <-> CoreIOT")
    ap.add_argument("--port", default="socket://127.0.0.1:7777",
                    help="COM5 cho Sink that, hoac socket://127.0.0.1:7777 cho fake_sink")
    ap.add_argument("--baud", type=int, default=115200)
    ap.add_argument("--dry", action="store_true", help="Chi in ra, khong gui MQTT")
    args = ap.parse_args()
    gw = Gateway(args.port, args.baud, args.dry)
    try:
        gw.run()
    except KeyboardInterrupt:
        print(f"# Dung. Goi mat theo node: {gw.lost}")


if __name__ == "__main__":
    main()
