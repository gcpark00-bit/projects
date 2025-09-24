import json, time, sys
from pathlib import Path

try:
    import serial
    from serial.tools import list_ports
except Exception as e:
    print("pyserial not available:", e)
    sys.exit(1)

cfg_path = Path('ports_config.json')
try:
    cfg = json.loads(cfg_path.read_text(encoding='utf-8'))
except Exception as e:
    print(f"Failed to read {cfg_path}: {e}")
    sys.exit(1)

baud = int(cfg.get('baud', 9600))

cfg_co2 = cfg.get('co2_ports') or []

ports = [p.device for p in list_ports.comports()]
print('Discovered ports:', ports)

co2_ports = cfg_co2 if cfg_co2 else ports[:6]
co2_ports = list(dict.fromkeys(co2_ports))[:6]
print('Selected CO2 ports:', co2_ports)

co2_serials = []
for i, port in enumerate(co2_ports, start=1):
    try:
        s = serial.Serial(port, baudrate=baud, timeout=1)
        co2_serials.append((i, port, s))
    except Exception as e:
        print(f"CO2 {i} open failed on {port}: {e}")

try:
    for tick in range(5):
        ts = time.strftime('%H:%M:%S')
        msgs = [f"[{ts}] tick {tick+1}"]
        if not co2_serials:
            msgs.append('No CO2 serials available')
        for i, port, s in co2_serials:
            try:
                s.write(b'\xFF\x01\x86\x00\x00\x00\x00\x00\x79')
                resp = s.read(9)
                if len(resp) == 9 and resp[0] == 0xFF and resp[1] == 0x86:
                    co2 = (resp[2] << 8) | resp[3]
                    msgs.append(f"CO2{i}@{port}={co2}ppm")
                else:
                    msgs.append(f"CO2{i}@{port}=invalid({resp.hex()})")
            except Exception as e:
                msgs.append(f"CO2{i}@{port}=err({e})")
        print(' | '.join(msgs))
        time.sleep(1)
finally:
    for _, _, s in co2_serials:
        try:
            s.close()
        except Exception:
            pass
