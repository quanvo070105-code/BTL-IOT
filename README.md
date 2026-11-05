# Smart Greenhouse – BTL IoT (HK 261)

He thong nha kinh thong minh: ESP32-S3 (YoloUNO) + ESP-NOW + BLE + Gateway Python + CoreIOT.

## Cau truc

| Thu muc | Noi dung | Phu trach |
|---|---|---|
| `firmware/` | Node nha kinh, Sink Node, node dat BLE | Nguoi A |
| `gateway/` | Gateway Python (Serial <-> MQTT CoreIOT), fake_sink | Nguoi B |
| `ml/` | Thu du lieu, train va export TinyML | Nguoi B |
| `coreiot/` | File export dashboard, RuleChain | Nguoi B |
| `shared/` | Dinh dang goi tin dung chung (sua phai bao ca nhom) | Ca hai |
| `docs/evidence/` | Anh, log lam bang chung theo ma T/N | Ca hai |

## Chay Gateway voi du lieu gia lap

```
cd gateway
python -m venv ../.venv
../.venv/bin/python -m pip install -r requirements.txt   # Windows python.org: ..\.venv\Scripts\python
copy .env.example .env    # roi dien GW_TOKEN cua device GH_GATEWAY

# Cua so 1
python fake_sink.py
# Cua so 2
python gateway.py                 # mac dinh socket://127.0.0.1:7777
python gateway.py --port COM5     # khi dung Sink Node that
```

Dinh dang goi tin: xem `shared/serial_samples.jsonl` va tai lieu "Dac ta dinh dang du lieu".
