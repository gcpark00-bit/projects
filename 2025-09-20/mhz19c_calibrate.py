#!/usr/bin/env python3
"""
MH-Z19C Calibration Tool (GUI)

Features:
- Zero calibration at 400 ppm (fresh air)
- Span calibration at 2000 ppm
- ABC (Automatic Baseline Correction) enable/disable

Each sensor row shows big countdown timers for pre/post waits.
"""

import argparse
import sys
import time
import re
from typing import Optional, Dict

import tkinter as tk
from tkinter import ttk

try:
    import serial
    from serial.tools import list_ports
except Exception as e:
    print("pyserial is required. Install with: pip install pyserial", file=sys.stderr)
    raise


def checksum(frame: bytearray) -> int:
    s = sum(frame[1:8]) & 0xFF
    return (0xFF - s + 1) & 0xFF


def build_cmd(command: str) -> bytes:
    frame = bytearray(9)
    frame[0] = 0xFF
    frame[1] = 0x01
    if command == "zero":
        frame[2] = 0x87
    elif command == "span2000":
        frame[2] = 0x88
        ppm = 2000
        frame[3] = (ppm >> 8) & 0xFF
        frame[4] = ppm & 0xFF
    elif command == "abc_on":
        frame[2] = 0x79
        frame[3] = 0xA0
    elif command == "abc_off":
        frame[2] = 0x79
        frame[3] = 0x00
    else:
        raise ValueError(f"Unknown action: {command}")
    frame[8] = checksum(frame)
    return bytes(frame)


def send_and_optionally_read(ser: serial.Serial, payload: bytes, read_reply: bool = True) -> Optional[bytes]:
    ser.write(payload)
    ser.flush()
    if not read_reply:
        return None
    try:
        return ser.read(9) or None
    except Exception:
        return None


class CalRow:
    def __init__(self, parent, index: int, port: str, baud: int = 9600):
        self.parent = parent
        self.index = index
        self.port = port
        self.baud = baud
        self.state = 'idle'  # idle|pre|post
        self.remaining = 0
        self.action = None

        self.var_selected = tk.BooleanVar(value=True)
        self.var_pre = tk.IntVar(value=1200)
        self.var_post = tk.IntVar(value=60)

        self.row = tk.Frame(parent)
        self.row.pack(fill=tk.X, pady=4)

        ttk.Checkbutton(self.row, variable=self.var_selected).pack(side=tk.LEFT, padx=(2, 6))
        ttk.Label(self.row, text=f"CO2 {index+1}", width=8).pack(side=tk.LEFT)
        ttk.Label(self.row, text=port, width=28).pack(side=tk.LEFT)

        # Buttons
        self.btn_zero = ttk.Button(self.row, text="Zero 400", command=lambda: self.start('zero'))
        self.btn_span = ttk.Button(self.row, text="Span 2000", command=lambda: self.start('span2000'))
        self.btn_abc_on = ttk.Button(self.row, text="ABC ON", command=lambda: self.start('abc_on'))
        self.btn_abc_off = ttk.Button(self.row, text="ABC OFF", command=lambda: self.start('abc_off'))
        for b in (self.btn_zero, self.btn_span, self.btn_abc_on, self.btn_abc_off):
            b.pack(side=tk.LEFT, padx=2)

        # Pre/Post entries
        ttk.Label(self.row, text="Pre(s)").pack(side=tk.LEFT, padx=(8,2))
        ttk.Entry(self.row, textvariable=self.var_pre, width=6).pack(side=tk.LEFT)
        ttk.Label(self.row, text="Post(s)").pack(side=tk.LEFT, padx=(8,2))
        ttk.Entry(self.row, textvariable=self.var_post, width=6).pack(side=tk.LEFT)

        # Big timer
        self.lbl_timer = ttk.Label(self.row, text="00:00", font=("Helvetica", 20, "bold"))
        self.lbl_timer.pack(side=tk.RIGHT, padx=8)

        self.lbl_status = ttk.Label(self.row, text="Idle")
        self.lbl_status.pack(side=tk.RIGHT, padx=8)

    def set_buttons_state(self, enabled: bool):
        for b in (self.btn_zero, self.btn_span, self.btn_abc_on, self.btn_abc_off):
            b.config(state=(tk.NORMAL if enabled else tk.DISABLED))

    def start(self, action: str):
        if self.state != 'idle':
            return
        self.action = action
        pre = int(self.var_pre.get()) if action in ('zero', 'span2000') else 0
        post = int(self.var_post.get()) if action in ('zero', 'span2000') else 5
        self._pre_seconds = max(0, pre)
        self._post_seconds = max(0, post)
        if self._pre_seconds > 0:
            self.state = 'pre'
            self.remaining = self._pre_seconds
            self.lbl_status.config(text=f"Pre-wait for {action}")
            self.set_buttons_state(False)
            self._tick()
        else:
            self._send_and_post()

    def _tick(self):
        if self.state not in ('pre', 'post'):
            return
        mins, secs = divmod(max(0, int(self.remaining)), 60)
        self.lbl_timer.config(text=f"{mins:02d}:{secs:02d}")
        if self.remaining <= 0:
            if self.state == 'pre':
                self._send_and_post()
            else:
                self._finish()
            return
        self.remaining -= 1
        self.parent.after(1000, self._tick)

    def _send_and_post(self):
        try:
            with serial.Serial(self.port, baudrate=self.baud, timeout=2.0) as ser:
                cmd = build_cmd(self.action)
                send_and_optionally_read(ser, cmd, read_reply=True)
                self.lbl_status.config(text=f"Sent: {self.action}")
        except Exception as e:
            self.lbl_status.config(text=f"Error: {e}")
            self.set_buttons_state(True)
            self.state = 'idle'
            return
        if self._post_seconds > 0:
            self.state = 'post'
            self.remaining = self._post_seconds
            self.lbl_status.config(text="Post-wait (stabilizing)")
            self._tick()
        else:
            self._finish()

    def _finish(self):
        self.state = 'idle'
        self.lbl_status.config(text="Done")
        self.lbl_timer.config(text="00:00")
        self.set_buttons_state(True)


class CalApp(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("MH-Z19C Calibration")
        self.geometry("1100x500")
        self._build_ui()

    def _build_ui(self):
        top = ttk.Frame(self)
        top.pack(fill=tk.X, padx=10, pady=8)

        ttk.Label(top, text="MH-Z19C Calibration (CO2 ports only)", font=("Helvetica", 16, "bold")).pack(side=tk.LEFT)
        ttk.Button(top, text="Rescan Ports", command=self._rescan).pack(side=tk.RIGHT)

        self.rows_frame = ttk.Frame(self)
        self.rows_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=8)

        # Header
        hdr = ttk.Frame(self.rows_frame)
        hdr.pack(fill=tk.X)
        ttk.Label(hdr, text="Sel", width=4).pack(side=tk.LEFT)
        ttk.Label(hdr, text="Label", width=10).pack(side=tk.LEFT)
        ttk.Label(hdr, text="Port", width=30).pack(side=tk.LEFT)
        ttk.Label(hdr, text="Controls", width=40).pack(side=tk.LEFT)
        ttk.Label(hdr, text="Timer / Status", width=20).pack(side=tk.LEFT)

        # Bulk action bar
        bulk = ttk.Frame(self)
        bulk.pack(fill=tk.X, padx=10, pady=(0,8))
        ttk.Label(bulk, text="Apply to selected:").pack(side=tk.LEFT)
        ttk.Button(bulk, text="Zero 400", command=lambda: self._apply_selected('zero')).pack(side=tk.LEFT, padx=4)
        ttk.Button(bulk, text="Span 2000", command=lambda: self._apply_selected('span2000')).pack(side=tk.LEFT, padx=4)
        ttk.Button(bulk, text="ABC ON", command=lambda: self._apply_selected('abc_on')).pack(side=tk.LEFT, padx=4)
        ttk.Button(bulk, text="ABC OFF", command=lambda: self._apply_selected('abc_off')).pack(side=tk.LEFT, padx=4)

        self._rows: Dict[str, CalRow] = {}
        self._rescan()

    def _rescan(self):
        # Clear old rows (keep header intact)
        for child in list(self.rows_frame.pack_slaves())[1:]:
            child.destroy()
        self._rows.clear()
        # Build rows for CO2-only ports (SLAB/CP210x)
        ports = self._discover_co2_ports()
        if not ports:
            ttk.Label(self.rows_frame, text="No serial ports found.").pack()
            return
        for i, port in enumerate(ports):
            row = CalRow(self.rows_frame, i, port)
            self._rows[port] = row

    def _apply_selected(self, action: str):
        for port, row in self._rows.items():
            if row.var_selected.get():
                row.start(action)

    # ---------- Port discovery (CO2 only) ----------
    def _discover_co2_ports(self):
        ports = list(list_ports.comports())
        co2 = []
        for p in ports:
            dev = p.device
            blob = ' '.join(str(x or '') for x in (
                p.description, getattr(p, 'manufacturer', None), getattr(p, 'product', None)
            )).lower() + ' ' + dev.lower()
            # Exclude unwanted groups
            if 'cu.usbserial' in dev.lower() or 'cu.bluetooth' in dev.lower() or 'bluetooth' in blob:
                continue
            if ('wch' in blob) or ('ch340' in blob) or ('cu.wch' in blob):
                continue
            # Include SLAB/CP210x/Silicon Labs
            if ('cu.slab' in blob) or ('cp210' in blob) or ('silicon labs' in blob) or ('slab' in blob):
                co2.append(dev)
        return self._sort_ports(list(dict.fromkeys(co2)))

    def _sort_ports(self, ports_list):
        def keyfn(dev):
            m = re.search(r"(.*?)(\d+)$", dev)
            if m:
                return (m.group(1), int(m.group(2)))
            return (dev, -1)
        return sorted(ports_list, key=keyfn)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="MH-Z19C Calibration Tool")
    ap.add_argument("--port", help="Serial port (e.g., /dev/cu.SLAB_...) ")
    ap.add_argument("--action", choices=["zero", "span2000", "abc_on", "abc_off"], help="Calibration action", nargs='?')
    ap.add_argument("--pre-wait", type=int, default=1200, help="Pre-cal stabilization wait seconds (for zero/span)")
    ap.add_argument("--post-wait", type=int, default=60, help="Post-cal wait seconds (for zero/span)")
    ap.add_argument("--no-gui", action="store_true", help="Run in CLI mode (no GUI)")
    args = ap.parse_args(argv)

    if args.action and args.no_gui:
        # Simple CLI
        port = args.port
        if port is None:
            ports = list(list_ports.comports())
            port = ports[0].device if ports else None
        if not port:
            print("No port available.")
            return 1
        # Guidance
        if args.action == 'zero':
            print("[Zero Cal @ 400 ppm]")
        elif args.action == 'span2000':
            print("[Span Cal @ 2000 ppm]")
        return 0 if send_and_optionally_read(serial.Serial(port, 9600, timeout=2), build_cmd(args.action)) is not None else 0

    # Default to GUI
    app = CalApp()
    app.mainloop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
