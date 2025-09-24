import json, time, sys
from pathlib import Path
import re
import glob
import tkinter as tk
from tkinter import ttk
import tkinter.messagebox as messagebox

try:
    import serial
    from serial.tools import list_ports
except Exception as e:
    print("pyserial not available:", e)
    sys.exit(1)

# Optional: PyUSB (libusb backend) for raw CH34x access when no COM port is present
try:
    import usb.core
    import usb.util
    import usb.backend.libusb1 as libusb1
    from ctypes.util import find_library as _find_lib
    HAS_PYUSB = True
except Exception:
    HAS_PYUSB = False

# Optional: smbus2 for kernel /dev/i2c-* access (Linux, i2c-ch341-usb)
try:
    import smbus2 as smbus
    HAS_SMBUS = True
except Exception:
    HAS_SMBUS = False

def _get_libusb_backend():
    """Try to obtain a libusb backend for PyUSB on macOS/Linux without Homebrew.

    Attempts default discovery, ctypes find_library, and common install paths
    (MacPorts, Homebrew, system) to locate libusb-1.0.
    """
    if not HAS_PYUSB:
        return None
    # 1) Default backend
    try:
        be = libusb1.get_backend()
        if be is not None:
            return be
    except Exception:
        pass
    # 2) Try ctypes find_library
    try:
        libpath = _find_lib('usb-1.0')
        if libpath:
            be = libusb1.get_backend(find_library=lambda name: libpath)
            if be is not None:
                return be
    except Exception:
        pass
    # 3) Common install paths
    candidates = [
        '/opt/local/lib/libusb-1.0.dylib',   # MacPorts
        '/opt/homebrew/lib/libusb-1.0.dylib',# Homebrew (Apple Silicon)
        '/usr/local/lib/libusb-1.0.dylib',   # Homebrew (Intel)
        '/usr/lib/libusb-1.0.dylib',         # System
        '/usr/lib/x86_64-linux-gnu/libusb-1.0.so',
        '/usr/lib/aarch64-linux-gnu/libusb-1.0.so',
    ]
    for p in candidates:
        try:
            be = libusb1.get_backend(find_library=lambda name, _p=p: _p)
            if be is not None:
                return be
        except Exception:
            continue
    return None

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


def th_read_once(port, baud=9600, timeout=1.0, pattern: str | None = None):
    """Read one temperature/humidity record.

    Returns (temp, hum, raw) where temp/hum may be None on parse failure.
    """
    try:
        rgx = re.compile(pattern) if pattern else None
    except Exception:
        rgx = None
    try:
        with serial.Serial(port, baudrate=baud, timeout=timeout) as s:
            try:
                s.reset_input_buffer()
            except Exception:
                pass
            try:
                s.write(b"READ\n")
            except Exception:
                pass
            raw = s.readline().decode('utf-8', errors='ignore').strip()
            if not raw:
                return None, None, ''
            if rgx:
                m = rgx.search(raw)
                if m:
                    try:
                        return float(m.group('temp')), float(m.group('rh')), raw
                    except Exception:
                        return None, None, raw
            parts = [p.strip() for p in raw.split(',')]
            if len(parts) >= 2:
                try:
                    return float(parts[0]), float(parts[1]), raw
                except Exception:
                    return None, None, raw
            return None, None, raw
    except Exception as e:
        return None, None, f"ERR: {e}"


class DiagApp(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title('CO2/TH Port Diagnostic')
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
        ttk.Label(top, text='CO2/TH Port Diagnostic', font=('Segoe UI', 14, 'bold')).pack(side=tk.LEFT)
        ttk.Button(top, text='Rescan', command=self._rescan).pack(side=tk.RIGHT, padx=6)
        ttk.Button(top, text='EXIT', command=self.destroy).pack(side=tk.RIGHT)

        hdr = ttk.Frame(self)
        hdr.pack(fill=tk.X, padx=10)
        for text, w in [('Sel', 4), ('Class', 8), ('Port/USB', 24), ('VID:PID', 12), ('Result', 36), ('', 10)]:
            ttk.Label(hdr, text=text, width=w).pack(side=tk.LEFT)

        self.rows_frame = ttk.Frame(self)
        self.rows_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=(4, 8))

        bulk = ttk.Frame(self)
        bulk.pack(fill=tk.X, padx=10, pady=(0, 8))
        ttk.Label(bulk, text='Test/Scan:').pack(side=tk.LEFT)
        ttk.Button(bulk, text='CO2 (UART) Quick Test', command=lambda: self._test_selected('CO2')).pack(side=tk.LEFT, padx=6)
        # Setup backend
        self._usb_backend = _get_libusb_backend() if HAS_PYUSB else None
        if HAS_PYUSB and self._usb_backend is not None:
            # We focus on TH via I2C only here
            ttk.Button(bulk, text='I2C Scan (CH341A)', command=self._scan_i2c_ch341a_all).pack(side=tk.LEFT, padx=6)
        # Kernel I2C (/dev/i2c-*) scan controls
        i2c_kernel_box = ttk.Frame(self)
        i2c_kernel_box.pack(fill=tk.X, padx=10, pady=(0, 8))
        ttk.Label(i2c_kernel_box, text='Kernel I2C (/dev/i2c-*):').pack(side=tk.LEFT)
        self.cmb_i2c_bus = ttk.Combobox(i2c_kernel_box, state='readonly', width=18)
        self.cmb_i2c_bus.pack(side=tk.LEFT, padx=6)
        ttk.Button(i2c_kernel_box, text='Refresh', command=self._refresh_i2c_buses).pack(side=tk.LEFT)
        ttk.Button(i2c_kernel_box, text='Scan Bus', command=self._scan_selected_i2c_bus).pack(side=tk.LEFT, padx=6)
        ttk.Button(i2c_kernel_box, text='Scan All', command=self._scan_all_i2c_buses).pack(side=tk.LEFT)
        if not HAS_SMBUS:
            ttk.Label(i2c_kernel_box, text='(smbus2 not installed)', foreground='#888888').pack(side=tk.LEFT, padx=8)

        self._rows = []
        # populate initial kernel I2C bus list
        try:
            self._refresh_i2c_buses()
        except Exception:
            pass

    def _rescan(self):
        for w in self.rows_frame.pack_slaves():
            w.destroy()
        self._rows.clear()
        # Build CO2 UART-only list (exclude TH serial when using I2C-only for TH)
        co2_like, th_like = self._discover_ports_like_logger()
        wanted = set(co2_like)
        for p in list_ports.comports():
            if p.device not in wanted:
                continue
            cls = 'CO2'
            row = ttk.Frame(self.rows_frame)
            row.pack(fill=tk.X, pady=2)
            var_sel = tk.BooleanVar(value=True)
            ttk.Checkbutton(row, variable=var_sel).pack(side=tk.LEFT, padx=(0, 6))
            ttk.Label(row, text=cls, width=10).pack(side=tk.LEFT)
            ttk.Label(row, text=p.device, width=24).pack(side=tk.LEFT)
            vid = getattr(p, 'vid', None); pid = getattr(p, 'pid', None)
            vidpid = (f"{vid:04X}:{pid:04X}" if isinstance(vid, int) and isinstance(pid, int) else '-')
            ttk.Label(row, text=vidpid, width=12).pack(side=tk.LEFT)
            res = ttk.Label(row, text='-')
            res.pack(side=tk.LEFT, padx=6)
            btn = ttk.Button(row, text='Test', command=lambda port=p.device, lab=res: self._test_one_co2(port, lab))
            btn.pack(side=tk.LEFT)
            self._rows.append({'sel': var_sel, 'port': p.device, 'cls': cls, 'label': res})

        # Add CH341A (USB-I2C) entries for TH
        if HAS_PYUSB and self._usb_backend is not None:
            try:
                devs = list(usb.core.find(find_all=True, idVendor=0x1A86, backend=self._usb_backend))  # QinHeng/WCH
            except Exception:
                devs = []
            for d in devs:
                try:
                    pid = int(d.idProduct)
                    vid = int(d.idVendor)
                except Exception:
                    pid = d.idProduct; vid = d.idVendor
                # keep only common CH341A PIDs
                if int(pid) not in (0x5512, 0x5523):
                    continue
                row = ttk.Frame(self.rows_frame)
                row.pack(fill=tk.X, pady=2)
                var_sel = tk.BooleanVar(value=False)
                ttk.Checkbutton(row, variable=var_sel).pack(side=tk.LEFT, padx=(0, 6))
                ttk.Label(row, text='TH(I2C)', width=10).pack(side=tk.LEFT)
                # Show location (bus:addr) as identifier
                ident = f'bus{getattr(d, "bus", "?")}:addr{getattr(d, "address", "?")}'
                ttk.Label(row, text=ident, width=24).pack(side=tk.LEFT)
                ttk.Label(row, text=f"{vid:04X}:{pid:04X}", width=12).pack(side=tk.LEFT)
                res = ttk.Label(row, text='-')
                res.pack(side=tk.LEFT, padx=6)
                ttk.Button(row, text='I2C Scan', command=lambda dev=d, lab=res: self._scan_i2c_ch341a(dev, lab)).pack(side=tk.LEFT, padx=4)
                self._rows.append({'sel': var_sel, 'usb': d, 'cls': 'USB', 'label': res})

    # -------- Kernel I2C helpers (Linux /dev/i2c-*) --------
    def _list_i2c_buses(self):
        try:
            paths = sorted(glob.glob('/dev/i2c-*'))
            return paths
        except Exception:
            return []

    def _refresh_i2c_buses(self):
        buses = self._list_i2c_buses()
        try:
            self.cmb_i2c_bus['values'] = buses
            if buses:
                self.cmb_i2c_bus.current(0)
        except Exception:
            pass

    def _scan_selected_i2c_bus(self):
        try:
            path = self.cmb_i2c_bus.get()
        except Exception:
            path = ''
        if not path:
            messagebox.showinfo('I2C Scan', 'No /dev/i2c-* bus selected.')
            return
        msg = self._scan_kernel_i2c_bus(path)
        messagebox.showinfo('I2C Scan', msg)

    def _scan_all_i2c_buses(self):
        buses = self._list_i2c_buses()
        if not buses:
            messagebox.showinfo('I2C Scan', 'No /dev/i2c-* buses found.')
            return
        lines = []
        for b in buses:
            lines.append(self._scan_kernel_i2c_bus(b))
        messagebox.showinfo('I2C Scan (All)', '\n'.join(lines))

    def _scan_kernel_i2c_bus(self, path):
        if not HAS_SMBUS:
            return 'smbus2 not installed.'
        # Extract bus number from /dev/i2c-N
        try:
            busno = int(Path(path).name.split('-')[-1])
        except Exception:
            return f'Invalid bus path: {path}'
        found = []
        try:
            with smbus.SMBus(busno) as bus:
                for addr in range(0x03, 0x78):
                    try:
                        # Probe by attempting a read of one byte
                        # Many devices NACK non-implemented registers, so use quick test
                        bus.write_quick(addr)
                        found.append(addr)
                    except Exception:
                        # Try a harmless read as fallback
                        try:
                            bus.read_byte(addr)
                            found.append(addr)
                        except Exception:
                            pass
        except Exception as e:
            return f'{path}: ERR {e}'
        if found:
            hexlist = ' '.join(f'0x{a:02X}' for a in found)
            return f'{path}: found {len(found)} device(s): {hexlist}'
        return f'{path}: no devices found in 0x03-0x77'

    def _discover_ports_like_logger(self):
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
        th = sorted(list(dict.fromkeys(th)), key=sortkey)[:2]
        return co2, th

    def _test_one_co2(self, port, label):
        label.configure(text='Testing...')
        self.after(50, lambda: self._do_test_co2(port, label))

    def _do_test_co2(self, port, label):
        val, raw = robust_read_once(port, baud=self.baud, timeout=1.0)
        if isinstance(val, str) and val.startswith('ERR:'):
            label.configure(text=val)
        elif val is None:
            label.configure(text=f'Invalid ({raw})')
        else:
            label.configure(text=f'{val} ppm')

    def _test_one_th(self, port, label):
        label.configure(text='Testing...')
        self.after(50, lambda: self._do_test_th(port, label))

    def _do_test_th(self, port, label):
        thr_pattern = None
        try:
            thr_pattern = self.cfg.get('thr_pattern')
        except Exception:
            thr_pattern = None
        t, h, raw = th_read_once(port, baud=int(self.cfg.get('thr_baud', 9600)), timeout=1.0, pattern=thr_pattern)
        if isinstance(raw, str) and raw.startswith('ERR:'):
            label.configure(text=raw)
        elif t is None and h is None:
            label.configure(text=f'Invalid ({raw})')
        else:
            label.configure(text=f'{t:.1f} C, {h:.1f} %RH')

    def _test_selected(self, kind='CO2'):
        for r in self._rows:
            if r['sel'].get() and r['cls'] == kind:
                if kind == 'CO2':
                    self._test_one_co2(r['port'], r['label'])
                else:
                    self._test_one_th(r['port'], r['label'])

    # -------- USB (libusb/PyUSB) probing for CH34x --------
    def _probe_usb_ch34x(self, dev, label):
        if not HAS_PYUSB:
            label.configure(text='PyUSB missing')
            return
        try:
            # Ensure configured
            try:
                dev.set_configuration()
            except Exception:
                pass
            cfg = dev.get_active_configuration()
            intf = cfg[(0, 0)]  # interface 0 alt 0
            # Detach kernel driver if needed
            try:
                if dev.is_kernel_driver_active(intf.bInterfaceNumber):
                    dev.detach_kernel_driver(intf.bInterfaceNumber)
            except Exception:
                pass
            # Find a bulk IN endpoint
            ep_in = None
            for ep in intf:
                if usb.util.endpoint_direction(ep.bEndpointAddress) == usb.util.ENDPOINT_IN and usb.util.endpoint_type(ep.bmAttributes) == usb.util.ENDPOINT_TYPE_BULK:
                    ep_in = ep; break
            # Find a bulk OUT endpoint (optional)
            ep_out = None
            for ep in intf:
                if usb.util.endpoint_direction(ep.bEndpointAddress) == usb.util.ENDPOINT_OUT and usb.util.endpoint_type(ep.bmAttributes) == usb.util.ENDPOINT_TYPE_BULK:
                    ep_out = ep; break
            # Try a short non-blocking read to see if device responds
            payload = None
            if ep_in is not None:
                try:
                    payload = dev.read(ep_in.bEndpointAddress, ep_in.wMaxPacketSize or 64, timeout=100)
                except Exception as e:
                    payload = None
            msg = f"EPs IN={getattr(ep_in,'bEndpointAddress',None)} OUT={getattr(ep_out,'bEndpointAddress',None)}"
            if payload is not None and len(payload) > 0:
                label.configure(text=f"OK {msg} len={len(payload)}")
            else:
                label.configure(text=f"Probed {msg}")
        except Exception as e:
            label.configure(text=f"ERR: {e}")

    # -------- CH341A I2C scanning (skeleton with PyUSB) --------
    def _scan_i2c_ch341a_all(self):
        if not HAS_PYUSB:
            messagebox.showerror('Missing PyUSB', 'PyUSB/libusb not available.')
            return
        found = list(usb.core.find(find_all=True, idVendor=0x1A86, backend=self._usb_backend)) if (HAS_PYUSB and self._usb_backend) else []
        ch341a = [d for d in found if int(getattr(d, 'idProduct', 0)) in (0x5512, 0x5523)]
        if not ch341a:
            messagebox.showinfo('I2C Scan', 'No CH341A devices found via USB.')
            return
        res = []
        for d in ch341a:
            r = self._scan_i2c_ch341a(d, None, silent=True)
            res.append(r or 'no result')
        messagebox.showinfo('I2C Scan', '\n'.join(str(x) for x in res))

    def _scan_i2c_ch341a(self, dev, label, silent=False):
        """Attempt an I2C address sweep on CH341A (requires vendor protocol; skeleton only)."""
        if not HAS_PYUSB:
            if label: label.configure(text='PyUSB missing')
            return None
        try:
            # Prepare device (config/claim)
            try:
                dev.set_configuration()
            except Exception:
                pass
            cfg = dev.get_active_configuration()
            intf = cfg[(0, 0)]
            try:
                if dev.is_kernel_driver_active(intf.bInterfaceNumber):
                    dev.detach_kernel_driver(intf.bInterfaceNumber)
            except Exception:
                pass
            # NOTE: Real I2C scan requires CH341A vendor-specific control transfers
            # to set I2C mode, clock, and perform START/ADDR/STOP sequences per address.
            # This skeleton does not execute those undocumented sequences.
            msg = 'CH341A detected (USB). I2C scan requires vendor protocol; not executed.'
            if label:
                label.configure(text=msg)
            if not silent:
                messagebox.showinfo('CH341A I2C', msg)
            return msg
        except Exception as e:
            if label:
                label.configure(text=f'ERR: {e}')
            return f'ERR: {e}'


if __name__ == '__main__':
    app = DiagApp()
    app.mainloop()
