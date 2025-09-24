import json, time, sys
from pathlib import Path
import re
import tkinter as tk
from tkinter import ttk

try:
    import serial
    from serial.tools import list_ports
except Exception as e:
    print("pyserial not available:", e)
    sys.exit(1)


def classify_port(p):
    dev = p.device
    blob = ' '.join(str(x or '') for x in (p.description, getattr(p, 'manufacturer', None), getattr(p, 'product', None))).lower() + ' ' + dev.lower()
    vid = getattr(p, 'vid', None)
    # Exclusions aligned with logger
    if 'cu.usbserial' in dev.lower() or 'cu.bluetooth' in dev.lower() or 'bluetooth' in blob or 'debug' in blob:
        return 'Excluded'
    if isinstance(vid, int):
        if vid == 0x10C4:
            return 'CO2'
        if vid == 0x1A86:
            return 'TH'
    if 'cu.slab' in blob or 'cp210' in blob or 'silicon labs' in blob or 'slab' in blob:
        return 'CO2'
    if 'cu.wch' in blob or 'wch' in blob or 'ch340' in blob or 'wchusb' in blob:
        return 'TH'
    return 'Unknown'


def robust_read_once(port, baud=9600, timeout=1.0):
    try:
        with serial.Serial(port, baudrate=baud, timeout=timeout) as s:
            try:
                s.reset_input_buffer()
            except Exception:
                pass
            s.write(b'\xFF\x01\x86\x00\x00\x00\x00\x00\x79')
            time.sleep(0.1)
            buf = s.read(18)
            if len(buf) >= 9:
                for k in range(0, len(buf)-8):
                    if buf[k] == 0xFF and buf[k+1] == 0x86:
                        frame = buf[k:k+9]
                        return (frame[2] << 8) | frame[3], frame.hex()
            resp = s.read(9)
            if len(resp) == 9 and resp[0] == 0xFF and resp[1] == 0x86:
                return (resp[2] << 8) | resp[3], resp.hex()
            return None, (buf.hex() if buf else (resp.hex() if resp else ''))
    except Exception as e:
        return f"ERR: {e}", ''


class DiagApp(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title('CO2 Port Diagnostic')
        self.geometry('1200x620')
        self._load_cfg()
        self._build_ui()
        self._rescan()

    def _load_cfg(self):
        cfg_path = Path('ports_config.json')
        try:
            self.cfg = json.loads(cfg_path.read_text(encoding='utf-8'))
        except Exception:
            self.cfg = {}
        self.baud = int(self.cfg.get('baud', 9600))

    def _build_ui(self):
        top = ttk.Frame(self)
        top.pack(fill=tk.X, padx=10, pady=8)
        ttk.Label(top, text='CO2 Port Diagnostic', font=('Segoe UI', 14, 'bold')).pack(side=tk.LEFT)
        ttk.Button(top, text='Rescan', command=self._rescan).pack(side=tk.RIGHT, padx=6)
        ttk.Button(top, text='EXIT', command=self.destroy).pack(side=tk.RIGHT)

        hdr = ttk.Frame(self)
        hdr.pack(fill=tk.X, padx=10)
        for text, w in [('Sel', 4), ('Class', 8), ('Port', 24), ('VID:PID', 12), ('Result', 30), ('', 8)]:
            ttk.Label(hdr, text=text, width=w).pack(side=tk.LEFT)

        self.rows_frame = ttk.Frame(self)
        self.rows_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=(4, 8))

        bulk = ttk.Frame(self)
        bulk.pack(fill=tk.X, padx=10, pady=(0, 8))
        ttk.Label(bulk, text='Test selected:').pack(side=tk.LEFT)
        ttk.Button(bulk, text='CO2 Quick Test', command=self._test_selected).pack(side=tk.LEFT, padx=6)

        self._rows = []

    def _rescan(self):
        for w in self.rows_frame.pack_slaves():
            w.destroy()
        self._rows.clear()
        # Build CO2 list using logger-like discovery
        co2_like = self._discover_co2_like_logger()
        co2_set = set(co2_like)
        for p in list_ports.comports():
            if p.device not in co2_set:
                continue
            cls = 'CO2'
            row = ttk.Frame(self.rows_frame)
            row.pack(fill=tk.X, pady=2)
            var_sel = tk.BooleanVar(value=(cls == 'CO2'))
            ttk.Checkbutton(row, variable=var_sel).pack(side=tk.LEFT, padx=(0, 6))
            ttk.Label(row, text=cls, width=8).pack(side=tk.LEFT)
            ttk.Label(row, text=p.device, width=24).pack(side=tk.LEFT)
            vid = getattr(p, 'vid', None); pid = getattr(p, 'pid', None)
            vidpid = (f"{vid:04X}:{pid:04X}" if isinstance(vid, int) and isinstance(pid, int) else '-')
            ttk.Label(row, text=vidpid, width=12).pack(side=tk.LEFT)
            res = ttk.Label(row, text='-')
            res.pack(side=tk.LEFT, padx=6)
            btn = ttk.Button(row, text='Test', command=lambda port=p.device, lab=res: self._test_one(port, lab))
            btn.pack(side=tk.LEFT)
            self._rows.append({'sel': var_sel, 'port': p.device, 'cls': cls, 'label': res})

    def _discover_co2_like_logger(self):
        ports = list(list_ports.comports())
        co2, th, unknown = [], [], []
        for p in ports:
            dev = p.device
            blob = ' '.join(str(x or '') for x in (p.description, getattr(p,'manufacturer',None), getattr(p,'product',None))).lower() + ' ' + dev.lower()
            vid = getattr(p, 'vid', None)
            # Exclude unwanted
            if ('cu.usbserial' in dev.lower()) or ('cu.bluetooth' in dev.lower()) or ('bluetooth' in blob) or ('debug' in blob):
                continue
            if isinstance(vid, int):
                if vid == 0x10C4:  # CP210x
                    co2.append(dev); continue
                if vid == 0x1A86:  # CH340
                    th.append(dev); continue
            if 'cu.slab' in blob or 'cp210' in blob or 'silicon labs' in blob or 'slab' in blob:
                co2.append(dev)
            elif 'cu.wch' in blob or 'wch' in blob or 'ch340' in blob or 'wchusb' in blob:
                th.append(dev)
            else:
                unknown.append(dev)
        for d in unknown:
            if len(co2) < 6:
                co2.append(d)
            elif len(th) < 2:
                th.append(d)
        # Sort
        def sortkey(s):
            m = re.search(r"(.*?)(\d+)$", s)
            return (m.group(1), int(m.group(2))) if m else (s, -1)
        co2 = sorted(list(dict.fromkeys(co2)), key=sortkey)[:6]
        return co2

    def _test_one(self, port, label):
        label.configure(text='Testing...')
        self.after(50, lambda: self._do_test(port, label))

    def _do_test(self, port, label):
        val, raw = robust_read_once(port, baud=self.baud, timeout=1.0)
        if isinstance(val, str) and val.startswith('ERR:'):
            label.configure(text=val)
        elif val is None:
            label.configure(text=f'Invalid ({raw})')
        else:
            label.configure(text=f'{val} ppm')

    def _test_selected(self):
        for r in self._rows:
            if r['sel'].get():
                self._test_one(r['port'], r['label'])


if __name__ == '__main__':
    app = DiagApp()
    app.mainloop()
