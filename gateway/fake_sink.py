"""Gia lap Sink Node (ban 0.2): giao tiep 2 chieu qua TCP thay cho cong Serial.

- Gui len: dong JSON 'hello', 'tele', 'ble' giong het Sink that.
- Nhan xuong: dong 'cmd', doi trang thai relay gia, tra 'ack'.
Dinh dang theo tai lieu "Dac ta dinh dang du lieu" (chang 3 - Serial JSON).

Cach dung (2 cua so terminal):
  1)  python fake_sink.py                         # mo cong TCP 127.0.0.1:7777
  2)  python gateway.py --port socket://127.0.0.1:7777
"""
import argparse
import json
import random
import socket
import threading
import time

PERIOD_S = 5.0        # Node nha kinh gui moi 5 s
SOIL_PERIOD_S = 10.0  # Node dat cap nhat moi 10 s
TARGETS = ("pump", "fan", "led")


class FakeSink:
    def __init__(self, drop: float):
        self.drop = drop  # ti le gia lap mat goi lenh (0..1)
        self.state = {"pump": False, "fan": False, "led": False, "mode": "manual"}
        self.temp, self.humi, self.light, self.soil = 29.0, 68.0, 55, 45
        self.seq = self.soil_seq = 0
        self.lock = threading.Lock()
        self.conn = None

    # ---------- gui ----------
    def emit(self, msg: dict) -> None:
        line = json.dumps(msg, separators=(",", ":")) + "\n"
        with self.lock:
            if self.conn:
                try:
                    self.conn.sendall(line.encode())
                except OSError:
                    self.conn = None

    def send_tele(self) -> None:
        if self.temp > 34 and self.humi < 50:
            label = "hot_dry"
        elif self.temp < 22 and self.humi > 85:
            label = "cold_humid"
        else:
            label = "optimal"
        self.seq = (self.seq + 1) % 65536
        self.emit({
            "t": "tele", "node": 1, "seq": self.seq,
            "temp": round(self.temp, 1), "humi": round(self.humi, 1), "light": self.light,
            "ai_label": label, "ai_conf": round(random.uniform(0.7, 0.99), 2),
            **self.state, "rssi": random.randint(-70, -45),
        })

    def step(self) -> None:
        """Gia tri dao dong quanh muc trung binh; quat lam mat, bom lam am dat."""
        t_target = 26.0 if self.state["fan"] else 29.0
        self.temp = min(45.0, max(18.0, self.temp + 0.1 * (t_target - self.temp) + random.uniform(-0.5, 0.5)))
        self.humi = min(95.0, max(30.0, self.humi + 0.1 * (68.0 - self.humi) + random.uniform(-1.5, 1.5)))
        self.light = int(min(100, max(0, self.light + 0.1 * (55 - self.light) + random.randint(-5, 5))))
        s_target = 80 if self.state["pump"] else 45
        self.soil = int(min(100, max(0, self.soil + 0.15 * (s_target - self.soil) + random.randint(-2, 2))))

    def sender_loop(self) -> None:
        last_soil = 0.0
        while True:
            self.step()
            self.send_tele()
            if time.time() - last_soil >= SOIL_PERIOD_S:
                last_soil = time.time()
                self.soil_seq = (self.soil_seq + 1) % 65536
                self.emit({"t": "ble", "node": 2, "seq": self.soil_seq, "soil_moist": self.soil,
                           "batt": 3.7, "rssi": random.randint(-85, -60)})
            time.sleep(PERIOD_S)

    # ---------- nhan ----------
    def on_cmd(self, msg: dict) -> None:
        if msg.get("node") != 1:
            return
        if random.random() < self.drop:
            print(f"# (gia lap) mat lenh seq={msg.get('seq')}")
            return
        target, value = msg.get("target"), msg.get("value")
        status = 0
        if target in TARGETS and value in (0, 1):
            self.state[target] = bool(value)
        elif target == "mode" and value in (0, 1):
            self.state["mode"] = "auto" if value else "manual"
        else:
            status = 1  # sai tham so
        print(f"# Node nhan lenh {target}={value} -> {self.state}")
        self.emit({"t": "ack", "node": 1, "ack_seq": msg.get("seq"), "status": status, **self.state})
        if status == 0:
            self.send_tele()  # trang thai doi -> gui telemetry ngay

    def serve(self, host: str, port: int) -> None:
        threading.Thread(target=self.sender_loop, daemon=True).start()
        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind((host, port))
        srv.listen(1)
        print(f"# fake_sink dang cho Gateway tai socket://{host}:{port}  (Ctrl+C de dung)")
        while True:
            conn, addr = srv.accept()
            print(f"# Gateway da ket noi tu {addr}")
            with self.lock:
                self.conn = conn
            self.emit({"t": "hello", "fw_ver": "fake-0.2"})
            buf = b""
            while True:
                try:
                    data = conn.recv(1024)
                except OSError:
                    data = b""
                if not data:
                    print("# Gateway ngat ket noi, cho ket noi lai...")
                    with self.lock:
                        self.conn = None
                    break
                buf += data
                while b"\n" in buf:
                    raw, buf = buf.split(b"\n", 1)
                    try:
                        msg = json.loads(raw)
                    except json.JSONDecodeError:
                        continue
                    if msg.get("t") == "cmd":
                        self.on_cmd(msg)


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Gia lap Sink Node qua TCP")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=7777)
    ap.add_argument("--drop", type=float, default=0.0, help="Ti le mat lenh gia lap, vd 0.3")
    args = ap.parse_args()
    try:
        FakeSink(args.drop).serve(args.host, args.port)
    except KeyboardInterrupt:
        pass
