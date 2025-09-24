# multichannel_usb_logger.py
# Modified to read data from DHT-22 and MH-Z19C sensors via USB.

import time
import json
import re
import os
import csv
import random
from collections import deque
from datetime import datetime
import tkinter as tk
import tkinter.messagebox as mb
from tkinter import ttk
from threading import Event
# Ensure Matplotlib can write its cache to a local, writable folder to avoid terminal warnings
if 'MPLCONFIGDIR' not in os.environ:
    try:
        _mpl_dir = os.path.join(os.path.dirname(__file__), '.mplconfig')
        os.makedirs(_mpl_dir, exist_ok=True)
        os.environ['MPLCONFIGDIR'] = _mpl_dir
    except Exception:
        pass
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
import matplotlib as mpl
from matplotlib import font_manager as fm
from matplotlib.figure import Figure
from matplotlib.lines import Line2D
from matplotlib.patches import Patch
import serial  # For MH-Z19C and DHT-22 sensors
from serial.tools import list_ports

# Ensure Korean labels render correctly in Matplotlib by selecting an installed font
try:
    def _select_korean_capable_font():
        candidates = [
            'AppleGothic',            # macOS
            'NanumGothic',            # Linux/macOS (Korean)
            'Noto Sans CJK KR',       # Google Noto CJK
            'Noto Sans KR',
            'Malgun Gothic',          # Windows
            'Arial Unicode MS',
            'PingFang SC', 'PingFang TC',
            'Hiragino Sans', 'Hiragino Kaku Gothic ProN',
            'DejaVu Sans',            # widely available fallback
        ]
        installed = {f.name for f in fm.fontManager.ttflist}
        for name in candidates:
            if name in installed:
                return name
        return None

    _font = _select_korean_capable_font()
    if _font:
        mpl.rcParams['font.family'] = _font
    mpl.rcParams['axes.unicode_minus'] = False
except Exception:
    pass

class CO2LoggerApp:
    def __init__(self, root):
        self.root = root
        self.root.title("헬프네이쳐 생분해도 측정기")
        self.running = Event()
        self.config = self._load_config()
        self.interval_s = float(self.config.get('interval', 2.0))
        self.interval_jitter_s = float(self.config.get('interval_jitter', 0.2))
        self.baud = int(self.config.get('baud', 9600))
        self.thr_baud = int(self.config.get('thr_baud', 9600))
        thr_pattern = self.config.get('thr_pattern')
        self.thr_regex = re.compile(thr_pattern) if thr_pattern else None
        self.log_path = self.config.get('outfile') or 'usb_multi_log.csv'
        self.log_file = None
        self.log_writer = None
        # Runtime filtering defaults (lighter settings)
        self.median_window = int(self.config.get('median_window', 5))
        # Disable raw debugging by default
        self.debug_raw = bool(self.config.get('debug_raw', False))
        self.hysteresis_ppm = float(self.config.get('hysteresis_ppm', 5.0))
        # Ignore tiny residual differences when integrating (ppm threshold)
        try:
            self.integr_epsilon_ppm = float(self.config.get('integr_epsilon_ppm', self.hysteresis_ppm))
        except Exception:
            self.integr_epsilon_ppm = self.hysteresis_ppm
        # Plot smoothing threshold for mmol series (avoid creep from tiny increments)
        try:
            self.mmol_plot_epsilon = float(self.config.get('mmol_plot_epsilon', 0.10))  # mmol
        except Exception:
            self.mmol_plot_epsilon = 0.10
        self.min_dup_interval = float(self.config.get('min_dup_interval', 0.8))
        self.debug_logfile = self.config.get('debug_logfile', 'debug_raw.log')
        self._debug_fh = None
        if self.debug_raw:
            try:
                self._debug_fh = open(self.debug_logfile, 'a', buffering=1, encoding='utf-8')
                self._debug_fh.write(f"\n==== Debug session start {datetime.now().isoformat()} ====\n")
            except Exception as e:
                print(f"Failed to open debug log file: {e}")
        # Option: clear offsets on start (default true)
        self.clear_offsets_on_start_opt = bool(self.config.get('clear_offsets_on_start', True))

        # Pre-scan and persist port mapping at startup per rules
        try:
            self._pre_scan_and_save_ports()
        except Exception as e:
            print(f"Pre-scan ports failed: {e}")

        # Clear any saved CO2 offsets at startup if enabled
        if self.clear_offsets_on_start_opt:
            try:
                self._clear_offsets_on_start()
            except Exception as e:
                print(f"Offset clear failed: {e}")

        # CO2 calibration: per-sensor offsets (ppm) and optional smoothing
        raw_offsets = self.config.get('co2_offsets') or []
        # Mapping by serial port path takes precedence if available
        self.co2_offsets_by_port = self.config.get('co2_offsets_by_port') or {}
        # Also keep index-based list for backward compatibility and when port unknown
        self.co2_offsets = [(float(raw_offsets[i]) if i < len(raw_offsets) else 0.0) for i in range(6)]
        # Per-CO2-channel role mapping: blank | sample | compare
        self.co2_roles_by_port = self.config.get('co2_roles_by_port') or {}
        raw_roles = self.config.get('co2_roles') or []
        self.co2_roles = [str(raw_roles[i]) if i < len(raw_roles) else 'sample' for i in range(6)]
        # Role colors used across plot/legend/swatches
        self.role_colors = {
            'blank':   '#616161',  # gray
            'sample':  '#2ca02c',  # green
            'compare': '#ff7f0e',  # orange
        }
        # Track which port is used at each index (filled in _setup_ports)
        self.co2_ports_used = [None] * 6
        # Exponential moving average alpha (0 disables smoothing)
        try:
            self.co2_ema_alpha = float(self.config.get('co2_ema_alpha', 0.0))
        except Exception:
            self.co2_ema_alpha = 0.0
        self._co2_ema_state = [None] * 6

        # Colors for plotting (define early for UI color swatches)
        self.co2_colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd', '#8c564b']
        # Temp in red tones, Humidity in blue tones
        self.temp_colors = ['#D32F2F', '#EF5350']
        self.hum_colors = ['#1976D2', '#42A5F5']
        # Material colors for composition bar
        self.material_colors = {
            'PLA': '#1976D2',
            'PBAT': '#26A69A',
            'Inorganic': '#9E9E9E',
            'BioChar': '#4E342E',
            'P(3-HP)': '#7E57C2',
            'PHA': '#F9A825',
            'PHB': '#8D6E63',
            'Starch': '#66BB6A',
            'Cellulose': '#43A047',
            '기타': '#90A4AE',
        }

        # Create a matplotlib figure and integrate it with tkinter
        self.figure = Figure(figsize=(8, 6), dpi=100)
        self.ax = self.figure.add_subplot(111)
        self.ax.set_title("CO2, Temperature, and Humidity")
        self.ax.set_xlabel("Elapsed (s)")
        self.ax.set_ylabel("CO2 (ppm)")
        self.ax.grid(True)

        self.ax2 = self.ax.twinx()  # Secondary y-axis for temperature
        # Left side should not show temperature text; unify label on the right
        self.ax2.set_ylabel("")
        # Tertiary y-axis for humidity (offset on the right)
        self.ax3 = self.ax.twinx()
        try:
            # Offset humidity axis slightly to the right to avoid overlap with temp axis
            self.ax3.spines["right"].set_position(("axes", 1.1))
        except Exception:
            pass
        self.ax3.set_frame_on(True)
        self.ax3.patch.set_visible(False)
        self.ax3.set_ylabel("Temp/Humid")

        # Composition is displayed via top bar; no separate native column

        # Layout containers (defer packing until setup completed)
        # Global tiny brand label at very top-left (visible on all pages)
        try:
            self.brand_frame = tk.Frame(root)
            self.brand_frame.pack(side=tk.TOP, fill=tk.X)
            tk.Label(
                self.brand_frame,
                text='(c)Helpnautre, G. Park',
                font=("Helvetica", 18),
                fg="#666666"
            ).pack(side=tk.LEFT, padx=6, pady=(2, 0))
        except Exception:
            pass

        self.content_frame = tk.Frame(root)

        plot_frame = tk.Frame(self.content_frame)
        plot_frame.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

        self.canvas = FigureCanvasTkAgg(self.figure, master=plot_frame)
        self.canvas_widget = self.canvas.get_tk_widget()
        self.canvas_widget.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

        # Controls frame at the bottom (fixed height so placed buttons are visible)
        self.controls_frame = tk.Frame(root, height=70)
        # Defer packing until setup is done
        self.controls_frame.pack_propagate(False)
        # A top bar for global actions on main page (packed in finalize_setup)
        self.topbar_frame = tk.Frame(root)

        # Use ttk with a theme that respects custom colors (macOS aqua ignores bg/fg)
        self.style = ttk.Style()
        try:
            self.style.theme_use('clam')  # Cross-platform theme that honors background colors
        except Exception:
            pass  # Fallback to current theme if 'clam' isn't available

        # Define styles: Start (blue) and Stop (red), both with white text
        self.style.configure('Start.TButton', foreground='white', background='#1976D2', font=("Helvetica", 14, "bold"), padding=(16, 10))
        self.style.map(
            'Start.TButton',
            foreground=[('disabled', '#EEEEEE'), ('active', 'white')],
            background=[('disabled', '#9E9E9E'), ('active', '#1976D2'), ('pressed', '#1565C0')],
        )

        self.style.configure('Stop.TButton', foreground='white', background='#D32F2F', font=("Helvetica", 14, "bold"), padding=(16, 10))
        self.style.map(
            'Stop.TButton',
            foreground=[('disabled', '#EEEEEE'), ('active', 'white')],
            background=[('disabled', '#9E9E9E'), ('active', '#D32F2F'), ('pressed', '#B71C1C')],
        )
        # Exit style (reusable)
        try:
            self.style.configure('Exit.TButton', foreground='white', background='#424242', font=("Helvetica", 14, "bold"), padding=(16, 10))
            self.style.map('Exit.TButton', foreground=[('active','white')], background=[('active','#616161'), ('pressed','#212121')])
            self.exit_style = 'Exit.TButton'
        except Exception:
            self.exit_style = 'TButton'

        # Auto Tune button (to optimize sampling params against periodic artifacts)
        self.auto_tune_active = False
        self.auto_btn = ttk.Button(
            self.topbar_frame,
            text="Auto Tune",
            command=self._start_auto_tune,
            width=12,
        )
        self.auto_btn.pack(side=tk.RIGHT, padx=8, pady=6)

        # Interval control (top bar)
        interval_box = ttk.Frame(self.topbar_frame)
        interval_box.pack(side=tk.RIGHT, padx=8, pady=6)
        ttk.Label(interval_box, text="Interval (s)").pack(side=tk.LEFT)
        self.var_interval = tk.DoubleVar(value=self.interval_s)
        self.entry_interval = ttk.Entry(interval_box, textvariable=self.var_interval, width=6)
        self.entry_interval.pack(side=tk.LEFT, padx=(4, 4))
        ttk.Button(interval_box, text="Apply", command=self._apply_interval).pack(side=tk.LEFT)

        # Create Start and Stop buttons (ttk) in bottom controls bar, larger size
        self.start_button = ttk.Button(
            self.controls_frame,
            text="Start",
            command=self.start_logging,
            width=18,
            style='Start.TButton',
        )
        # Position at horizontal 1/3, vertically centered in controls bar
        self.start_button.place(relx=1/3, rely=0.5, anchor='center')
        self.start_button.configure(cursor='hand2')

        self.stop_button = ttk.Button(
            self.controls_frame,
            text="Stop",
            command=self.stop_logging,
            width=18,
            style='Stop.TButton',
            state=tk.DISABLED,
        )
        # We'll place buttons at 0.25 (Start), 0.5 (Pause/Resume), 0.75 (Stop)
        # Position Stop at 0.75
        self.stop_button.place(relx=0.75, rely=0.5, anchor='center')
        self.stop_button.configure(cursor='X_cursor')

        # Pause/Resume button (initially disabled)
        # Add a neutral style
        self.style.configure('Pause.TButton', foreground='white', background='#F9A825', font=("Helvetica", 14, "bold"), padding=(16, 10))
        self.style.map(
            'Pause.TButton',
            foreground=[('disabled', '#EEEEEE'), ('active', 'white')],
            background=[('disabled', '#9E9E9E'), ('active', '#F9A825'), ('pressed', '#F57F17')],
        )
        self.paused = False
        self.paused_start_ts = None
        self.pause_button = ttk.Button(
            self.controls_frame,
            text="Pause",
            command=self.toggle_pause,
            width=18,
            style='Pause.TButton',
            state=tk.DISABLED,
        )
        # Position Pause at 0.5
        self.pause_button.place(relx=0.5, rely=0.5, anchor='center')
        self.pause_button.configure(cursor='X_cursor')

        # Move Start to 0.25 to make room for Pause/Stop
        # (Start was already placed; update its position)
        try:
            self.start_button.place_configure(relx=0.25)
        except Exception:
            pass

        # Align Sensors button (adjust CO2 offsets based on recent data)
        self.align_button = ttk.Button(
            self.controls_frame,
            text="Align Sensors",
            command=self.align_sensors,
            width=16,
        )
        # Place near the right edge
        self.align_button.place(relx=0.92, rely=0.5, anchor='center')
        self.align_button.configure(cursor='hand2')

        # EXIT button will be shown in top-right on main page (created here, packed later)
        self.exit_button_top = ttk.Button(
            self.topbar_frame,
            text="EXIT",
            command=self.root.quit,
            width=10,
            style=self.exit_style,
        )
        # place on right within top bar when packed
        self.exit_button_top.pack(side=tk.RIGHT, padx=8, pady=6)

        # Note: Rescan functionality is available on the setup page only.

        # Right-side sensor selection panel (inside content area)
        sensor_panel = ttk.LabelFrame(self.content_frame, text="Sensors")
        sensor_panel.pack(side=tk.RIGHT, fill=tk.Y, padx=10, pady=10)

        # Responsive ports summary
        resp_group = ttk.LabelFrame(sensor_panel, text="Responsive Ports")
        resp_group.pack(fill=tk.X, padx=0, pady=(0, 6))
        ttk.Label(resp_group, text="CO2:").pack(side=tk.LEFT)
        self.lbl_resp_co2 = ttk.Label(resp_group, text="-")
        self.lbl_resp_co2.pack(side=tk.LEFT, padx=(4, 12))
        ttk.Label(resp_group, text="TH:").pack(side=tk.LEFT)
        self.lbl_resp_th = ttk.Label(resp_group, text="-")
        self.lbl_resp_th.pack(side=tk.LEFT, padx=(4, 0))

        # Sensor checkboxes: CO2 1..6, Temp/Humidity 1..2 (default: unselected; discovered ports will be selected)
        self.co2_sensor_vars = []
        for i in range(6):
            self.co2_sensor_vars.append(tk.BooleanVar(value=False))
        self.th_sensor_vars = [tk.BooleanVar(value=False) for _ in range(2)]
        # Keep references to checkbox widgets for later label refresh
        self.co2_checkbuttons = []
        self.th_checkbuttons = []

        # Ensure we have discovered lists populated
        try:
            self._ensure_discovered_ports()
        except Exception:
            pass

        # Add CO2 sensor checkboxes with color swatch and role (only for responsive ports)
        self.co2_role_vars = []
        self.co2_swatches = []
        for i, var in enumerate(self.co2_sensor_vars, start=1):
            row = tk.Frame(sensor_panel)
            # Port label from pre-scan mapping if available
            try:
                port_cur = self.co2_ports_used[i-1] if (hasattr(self, 'co2_ports_used') and i-1 < len(self.co2_ports_used)) else None
            except Exception:
                port_cur = None
            # Show only discovered (non-empty) ports
            if not port_cur:
                continue
            row.pack(fill=tk.X, anchor='w')
            # Port label from pre-scan mapping if available
            port_txt = port_cur
            label_text = f"CO2 Sensor {i}" + (f" ({port_txt})" if port_txt else "")
            cb = ttk.Checkbutton(row, text=label_text, variable=var)
            cb.pack(side=tk.LEFT)
            self.co2_checkbuttons.append(cb)
            # Status label (right side)
            if not hasattr(self, 'co2_status_labels'):
                self.co2_status_labels = []
            st = ttk.Label(row, text='-', width=8)
            st.pack(side=tk.RIGHT)
            self.co2_status_labels.append(st)
            # Color swatch for CO2 i
            sw = tk.Canvas(row, width=18, height=12, highlightthickness=1, highlightbackground='#cccccc', bd=0)
            sw.pack(side=tk.LEFT, padx=(6, 0))
            # Determine initial role and swatch color
            try:
                role_code = None
                try:
                    role_code = self.co2_roles_by_port.get(port_cur)
                except Exception:
                    role_code = None
                if not role_code:
                    role_code = self.co2_roles[i-1] if i-1 < len(self.co2_roles) else 'sample'
                if role_code not in ('blank','sample','compare'):
                    role_code = 'sample'
            except Exception:
                role_code = 'sample'
            color = self.role_colors.get(role_code, self.co2_colors[(i-1) % len(self.co2_colors)])
            sw.create_rectangle(1, 1, 17, 11, outline=color, fill=color)
            self.co2_swatches.append(sw)
            # Role selector (Combobox)
            role_var = tk.StringVar()
            # Display mapping: code -> label
            def code_to_disp(code):
                return {'blank':'blank','sample':'샘플','compare':'비교샘플'}.get(code, '샘플')
            def disp_to_code(d):
                return {'blank':'blank','샘플':'sample','비교샘플':'compare'}.get(d, 'sample')
            role_var.set(code_to_disp(role_code))
            cmb = ttk.Combobox(row, values=['blank','샘플','비교샘플'], textvariable=role_var, width=8, state='readonly')
            cmb.pack(side=tk.LEFT, padx=(6, 0))
            self.co2_role_vars.append(role_var)
            # Bind change handler
            def make_handler(idx=i-1, var=role_var, canvas=sw):
                def _h(*_):
                    code = disp_to_code(var.get())
                    try:
                        self._set_role_for_index(idx, code)
                        # Update swatch color
                        c = self.role_colors.get(code, '#888888')
                        try:
                            canvas.delete('all')
                        except Exception:
                            pass
                        try:
                            canvas.create_rectangle(1, 1, 17, 11, outline=c, fill=c)
                        except Exception:
                            pass
                        # Persist role selection
                        self._save_roles_to_config()
                        # Redraw plot to apply colors
                        self.update_plot()
                    except Exception:
                        pass
                return _h
            cmb.bind('<<ComboboxSelected>>', make_handler())

        # Add Temp/Humidity sensor checkboxes with two color swatches (Temp, Hum) only for responsive ports
        for i, var in enumerate(self.th_sensor_vars, start=1):
            row = tk.Frame(sensor_panel)
            try:
                port_txt = self.th_ports_used[i-1] if (hasattr(self, 'th_ports_used') and i-1 < len(self.th_ports_used)) else None
            except Exception:
                port_txt = None
            if not port_txt:
                continue
            row.pack(fill=tk.X, anchor='w')
            label_text = f"Temp/Humidity Sensor {i}" + (f" ({port_txt})" if port_txt else "")
            cb = ttk.Checkbutton(row, text=label_text, variable=var)
            cb.pack(side=tk.LEFT)
            self.th_checkbuttons.append(cb)
            if not hasattr(self, 'th_status_labels'):
                self.th_status_labels = []
            st = ttk.Label(row, text='-', width=8)
            st.pack(side=tk.RIGHT)
            self.th_status_labels.append(st)
            # Temp swatch
            sw_t = tk.Canvas(row, width=18, height=12, highlightthickness=1, highlightbackground='#cccccc', bd=0)
            sw_t.pack(side=tk.LEFT, padx=(6, 2))
            tcolor = self.temp_colors[(i-1) % len(self.temp_colors)]
            sw_t.create_rectangle(1, 1, 17, 11, outline=tcolor, fill=tcolor)
            # Humidity swatch
            sw_h = tk.Canvas(row, width=18, height=12, highlightthickness=1, highlightbackground='#cccccc', bd=0)
            sw_h.pack(side=tk.LEFT)
            hcolor = self.hum_colors[(i-1) % len(self.hum_colors)]
            sw_h.create_rectangle(1, 1, 17, 11, outline=hcolor, fill=hcolor)

        # Live stats panel
        stats = ttk.LabelFrame(sensor_panel, text="Live Stats")
        stats.pack(fill=tk.X, padx=0, pady=(8, 0))
        # Header row: label + value titles
        hdr = tk.Frame(stats)
        hdr.pack(fill=tk.X, pady=(0, 2))
        ttk.Label(hdr, text="Sensor", width=14).pack(side=tk.LEFT)
        ttk.Label(hdr, text="현재", width=8).pack(side=tk.LEFT)
        ttk.Label(hdr, text="최소", width=8).pack(side=tk.LEFT)
        ttk.Label(hdr, text="최대", width=8).pack(side=tk.LEFT)
        ttk.Label(hdr, text="평균", width=8).pack(side=tk.LEFT)
        ttk.Label(hdr, text="누적mmol", width=10).pack(side=tk.LEFT)

        # Build stats labels for current/min/max/avg per sensor
        self.stat_labels = {
            'co2': [],  # list of (curr, min, max, avg)
            'temp': [],
            'hum': [],
        }
        def add_stat_row(parent, label):
            row = tk.Frame(parent)
            row.pack(fill=tk.X, pady=1)
            ttk.Label(row, text=label, width=14).pack(side=tk.LEFT)
            curr = ttk.Label(row, text="-", width=8)
            mn = ttk.Label(row, text="-", width=8)
            mx = ttk.Label(row, text="-", width=8)
            avg = ttk.Label(row, text="-", width=8)
            cum = ttk.Label(row, text="-", width=10)
            curr.pack(side=tk.LEFT)
            mn.pack(side=tk.LEFT)
            mx.pack(side=tk.LEFT)
            avg.pack(side=tk.LEFT)
            cum.pack(side=tk.LEFT)
            return curr, mn, mx, avg, cum

        for i in range(6):
            self.stat_labels['co2'].append(add_stat_row(stats, f"CO2 {i+1}"))
        for i in range(2):
            self.stat_labels['temp'].append(add_stat_row(stats, f"Temp {i+1}"))
        for i in range(2):
            self.stat_labels['hum'].append(add_stat_row(stats, f"Hum {i+1}"))

        # Biodegradability panel
        bio = ttk.LabelFrame(sensor_panel, text="Biodegradability")
        bio.pack(fill=tk.X, padx=0, pady=(8, 0))
        self.lbl_thco2 = ttk.Label(bio, text="ThCO2: - g")
        self.lbl_thco2.pack(anchor='w')
        self.lbl_cum = ttk.Label(bio, text="Cumulative CO2: - mmol")
        self.lbl_cum.pack(anchor='w')
        # Per-role cumulative and biodegradability
        self.lbl_cum_sample = ttk.Label(bio, text="Sample net: - mmol")
        self.lbl_cum_sample.pack(anchor='w')
        self.lbl_bio_sample = ttk.Label(bio, text="Sample biodegradability: - %")
        self.lbl_bio_sample.pack(anchor='w')
        self.lbl_cum_compare = ttk.Label(bio, text="Compare net: - mmol")
        self.lbl_cum_compare.pack(anchor='w')
        self.lbl_bio_compare = ttk.Label(bio, text="Compare biodegradability: - %")
        self.lbl_bio_compare.pack(anchor='w')
        # Legacy overall biodegradability label (kept for compatibility)
        self.lbl_bio = ttk.Label(bio, text="Biodegradability: - %")
        self.lbl_bio.pack(anchor='w')

        # Far-right analysis column: dedicated mini-plots
        try:
            self.analysis_panel = ttk.LabelFrame(self.content_frame, text="Analysis")
            # Pack to the far right (after sensor_panel)
            self.analysis_panel.pack(side=tk.RIGHT, fill=tk.Y, padx=(4, 8), pady=10)
            # Create a separate figure for right-column plots (wider)
            self.side_figure = Figure(figsize=(3.8, 5.2), dpi=100)
            self.side_ax_mmol = self.side_figure.add_subplot(211)
            self.side_ax_pct = self.side_figure.add_subplot(212, sharex=self.side_ax_mmol)
            # Increase left margin to provide more space for y-axis/labels
            try:
                self.side_figure.subplots_adjust(left=0.20, right=0.98, top=0.95, bottom=0.10, hspace=0.28)
            except Exception:
                pass
            self.side_canvas = FigureCanvasTkAgg(self.side_figure, master=self.analysis_panel)
            self.side_canvas_widget = self.side_canvas.get_tk_widget()
            self.side_canvas_widget.pack(fill=tk.BOTH, expand=True)
        except Exception:
            # If creation fails, silently continue; plotting will fallback to inline axes
            self.side_figure = None
            self.side_ax_mmol = None
            self.side_ax_pct = None
            self.side_canvas = None

        # Composition: render on main figure top; no separate composition panel
        self.comp_panel = None
        self.comp_canvas_native = None
        self.comp_legend_native = None

        # Initialize data storage
        self.start_time = None
        self.xs = []  # Time data
        # Per-sensor series: CO2 (6), Temp/Humidity (2)
        self.co2_series = [[] for _ in range(6)]
        self.temp_series = [[] for _ in range(2)]
        self.hum_series = [[] for _ in range(2)]

        # Colors already set above

        # Initialize serial connections for multiple sensors
        self._setup_ports()
        # Refresh labels to reflect final port assignment
        try:
            self._refresh_sensor_checkbox_labels()
        except Exception:
            pass
        # Select all discovered ports by default
        try:
            self._select_scanned_ports_default()
        except Exception:
            pass

        # Biodegradation state
        self.last_ppm = None
        self.last_ppm_ts = None
        self.cum_co2_g = 0.0        # sample cumulative
        self.cum_blank_g = 0.0      # blank cumulative
        self.thco2_g = None
        self.pct_series = []
        # Per-channel time series (exclude blanks in plotting)
        self.cum_mmol_series_ch = [[] for _ in range(6)]  # net mmol over time
        self.pct_series_ch = [[] for _ in range(6)]       # biodeg % over time
        # Per-channel net cumulative CO2 (g) after blank subtraction (monotonic)
        self.cum_net_co2_g_ch = [0.0] * 6
        # Track last sample-average ppm to suppress integration on decreases
        self._last_sample_avg_ppm = None
        # Per-channel cumulative CO2 tracking (grams)
        self.cum_co2_g_ch = [0.0] * 6
        # Per-channel last ppm for closed-volume integration
        self.last_ppm_ch = [None] * 6
        # Flow integration timestamp and last blank ppm (for closed mode)
        self._last_flow_ts = None
        self._last_blank_ppm = None
        # Per-channel instantaneous rate (mmol/s) and timestamp for closed-mode rate
        self.rate_mmol_s_ch = [None] * 6
        self._last_rate_ts = None
        # Per-sensor median buffers and last read ts for debug
        self.co2_mbufs = [deque(maxlen=self.median_window) for _ in range(6)]
        self._co2_last_read_ts = [None] * 6
        self._co2_last_resp = [None] * 6
        self._last_period_report = None
        # Auto-tune state
        self._tune_configs = []
        self._tune_idx = -1
        self._tune_elapsed = 0.0
        self._tune_periods = []
        self._tune_best = None  # (score, config)
        # Watchdog state
        self.co2_err_counts = [0]*6
        self.co2_reopen_counts = [0]*6
        self.co2_last_ok = [None]*6
        self._co2_last_reopen_ts = [0]*6
        self.th_err_counts = [0]*2
        self.th_reopen_counts = [0]*2
        self.th_last_ok = [None]*2
        self._th_last_reopen_ts = [0]*2
        # Warning popup (blinking) for sensor read failures
        self._warn_popup = None
        self._warn_blink_job = None
        self._warn_blink_on = False
        # Recovery loop (auto reconnect/discover if sensors drop)
        try:
            self.recovery_interval_s = float(self.config.get('recovery_interval', 7.0))
        except Exception:
            self.recovery_interval_s = 7.0
        self._recovery_loop_active = False

        # ---------------- Test Setup (initial page) ----------------
        self.setup_done = False
        self.setup_frame = ttk.LabelFrame(root, text="Test Setup")
        self.setup_frame.pack(side=tk.TOP, fill=tk.BOTH, expand=True, padx=10, pady=10)

        # Detected ports section with rescan on the setup page
        self.setup_ports_group = ttk.LabelFrame(self.setup_frame, text="Detected Ports (auto-assigned)")
        self.setup_ports_group.pack(fill=tk.X, padx=4, pady=(4, 8))
        ports_row = tk.Frame(self.setup_ports_group)
        ports_row.pack(fill=tk.X, padx=4, pady=4)
        ttk.Label(ports_row, text="CO2 Ports:", width=12).pack(side=tk.LEFT)
        self.lbl_ports_co2 = ttk.Label(ports_row, text="-", width=80)
        self.lbl_ports_co2.pack(side=tk.LEFT, padx=(4, 0))
        ports_row2 = tk.Frame(self.setup_ports_group)
        ports_row2.pack(fill=tk.X, padx=4, pady=4)
        ttk.Label(ports_row2, text="Temp/Hum Ports:", width=12).pack(side=tk.LEFT)
        self.lbl_ports_th = ttk.Label(ports_row2, text="-", width=80)
        self.lbl_ports_th.pack(side=tk.LEFT, padx=(4, 0))
        # Rescan button on setup page
        ttk.Button(self.setup_ports_group, text="Rescan Ports", command=self._setup_rescan_ports).pack(pady=(2, 4), anchor='w')

        # Composition selector (up to 8 types)
        comp_frame = ttk.LabelFrame(self.setup_frame, text="Composition (max 8, % by mass + C%)")
        comp_frame.pack(fill=tk.X, pady=(4, 6))
        # Display names for composition selector (Korean for Inorganic)
        self.available_materials = ['PLA', 'PBAT', '첨가제', 'BioChar', 'P(3-HP)', 'PHA', 'PHB', 'Starch', 'Cellulose', '기타']
        # Mapping between internal keys and display labels
        self.material_name_map = {'Inorganic': '첨가제'}
        self.material_name_map_rev = {v: k for k, v in self.material_name_map.items()}
        # Materials that are excluded from ThCO2, but may have carbon in reality (greyed UI)
        self._excluded_materials = {'BioChar', 'Inorganic'}
        self.comp_type_vars = []
        self.comp_pct_vars = []
        self.comp_cfrac_vars = []
        self.comp_cfrac_entries = []
        # Default C% map (used to prefill when material is chosen)
        self._default_cfrac_map = {
            'PLA': 50.0,
            'PBAT': 62.0,
            'BioChar': 0.0,
            'Inorganic': 0.0,
            'P(3-HP)': 55.0,
            'PHA': 55.0,
            'PHB': 55.0,
            'Starch': 44.0,
            'Cellulose': 44.4,
            '기타': 0.0,
        }
        header = tk.Frame(comp_frame)
        header.pack(fill=tk.X)
        ttk.Label(header, text="Material", width=16, anchor='center').pack(side=tk.LEFT, padx=(4, 2))
        ttk.Label(header, text="Input %", width=10, anchor='center').pack(side=tk.LEFT)
        ttk.Label(header, text="Norm %", width=10, anchor='center').pack(side=tk.LEFT)
        ttk.Label(header, text="C%", width=8, anchor='center').pack(side=tk.LEFT)
        # Keep per-row variables
        self.comp_norm_pct_vars = []
        for i in range(8):
            row = tk.Frame(comp_frame)
            row.pack(fill=tk.X, pady=1)
            tvar = tk.StringVar(value='')
            pvar = tk.DoubleVar(value=0.0)
            cvar = tk.DoubleVar(value=0.0)
            cmb = ttk.Combobox(row, values=self.available_materials, textvariable=tvar, width=18, state='readonly')
            cmb.pack(side=tk.LEFT, padx=(4, 2))
            ent = ttk.Entry(row, textvariable=pvar, width=10, justify='center')
            ent.pack(side=tk.LEFT)
            # Live-update normalized column on percent edits
            try:
                ent.bind('<KeyRelease>', lambda *_: self._update_comp_norm_preview())
                ent.bind('<FocusOut>', lambda *_: self._update_comp_norm_preview())
            except Exception:
                pass
            nvar = tk.DoubleVar(value=0.0)
            entn = ttk.Entry(row, textvariable=nvar, width=10, state='readonly', justify='center')
            entn.pack(side=tk.LEFT, padx=(4, 0))
            # Use tk.Entry for per-row C% so background color can be controlled
            entc = tk.Entry(row, textvariable=cvar, width=8, justify='center')
            entc.pack(side=tk.LEFT, padx=(6, 0))
            self.comp_type_vars.append(tvar)
            self.comp_pct_vars.append(pvar)
            self.comp_cfrac_vars.append(cvar)
            self.comp_cfrac_entries.append(entc)
            self.comp_norm_pct_vars.append(nvar)
            # When material changes, prefill C% with default if 0
            def make_cfrac_prefill(idx=i, var_name=tvar, cfrac_var=cvar):
                def on_sel(*_):
                    try:
                        name_disp = (var_name.get() or '').strip()
                        name = self.material_name_map_rev.get(name_disp, name_disp)
                        if name:
                            df = self._default_cfrac_map.get(name)
                            if df is not None:
                                # Only prefill if current is zero
                                try:
                                    cur = float(cfrac_var.get())
                                except Exception:
                                    cur = 0.0
                                if cur == 0.0:
                                    cfrac_var.set(df)
                            # Grey out excluded materials for clarity
                            try:
                                ent = self.comp_cfrac_entries[idx]
                                if name in self._excluded_materials:
                                    ent.configure(bg='#EEEEEE', state='disabled')
                                    # Enforce C% = 0 for excluded (첨가제/Inorganic, BioChar)
                                    try: cfrac_var.set(0.0)
                                    except Exception: pass
                                else:
                                    ent.configure(bg='white', state='normal')
                            except Exception:
                                pass
                        # Also refresh normalized preview when selection changes
                        try:
                            self._update_comp_norm_preview()
                        except Exception:
                            pass
                    except Exception:
                        pass
                return on_sel
            try:
                cmb.bind('<<ComboboxSelected>>', make_cfrac_prefill())
            except Exception:
                pass
        # Normalization info label
        self.lbl_comp_info = ttk.Label(comp_frame, text="", foreground="#555555")
        self.lbl_comp_info.pack(fill=tk.X, padx=4, pady=(4, 0))

        # Sample mass and environment
        self.var_mass = tk.DoubleVar(value=100.0)
        env_row = tk.Frame(self.setup_frame)
        env_row.pack(fill=tk.X, pady=(6, 2))
        ttk.Label(env_row, text="Sample mass (g)", width=18).pack(side=tk.LEFT)
        ttk.Entry(env_row, textvariable=self.var_mass, width=10).pack(side=tk.LEFT)

        self.var_env = tk.StringVar(value='soil')
        env_sel = ttk.LabelFrame(self.setup_frame, text="Environment")
        env_sel.pack(fill=tk.X, pady=(6, 2))
        ttk.Radiobutton(env_sel, text='Soil (default)', variable=self.var_env, value='soil').pack(side=tk.LEFT, padx=6)
        ttk.Radiobutton(env_sel, text='Industrial compost', variable=self.var_env, value='compost').pack(side=tk.LEFT, padx=6)

        # Carbon fraction defaults (editable for common types)
        cf = ttk.LabelFrame(self.setup_frame, text="Carbon fraction defaults (mass %)")
        cf.pack(fill=tk.X, pady=(8, 6))
        self.var_c_pla = tk.DoubleVar(value=50.0)
        self.var_c_pbat = tk.DoubleVar(value=62.0)
        self.var_c_biochar = tk.DoubleVar(value=0.0)
        self.var_c_inorg = tk.DoubleVar(value=0.0)
        self.var_c_p3hp = tk.DoubleVar(value=55.0)
        self.var_c_pha = tk.DoubleVar(value=55.0)
        self.var_c_phb = tk.DoubleVar(value=55.0)
        self.var_c_starch = tk.DoubleVar(value=44.0)
        row_cf = tk.Frame(cf); row_cf.pack(fill=tk.X, pady=1)
        ttk.Label(row_cf, text="PLA C%", width=12).pack(side=tk.LEFT)
        ttk.Entry(row_cf, textvariable=self.var_c_pla, width=10).pack(side=tk.LEFT)
        row_cf2 = tk.Frame(cf); row_cf2.pack(fill=tk.X, pady=1)
        ttk.Label(row_cf2, text="PBAT C%", width=12).pack(side=tk.LEFT)
        ttk.Entry(row_cf2, textvariable=self.var_c_pbat, width=10).pack(side=tk.LEFT)
        row_cf3 = tk.Frame(cf); row_cf3.pack(fill=tk.X, pady=1)
        ttk.Label(row_cf3, text="BioChar C%", width=12).pack(side=tk.LEFT)
        ttk.Entry(row_cf3, textvariable=self.var_c_biochar, width=10).pack(side=tk.LEFT)
        row_cf4 = tk.Frame(cf); row_cf4.pack(fill=tk.X, pady=1)
        ttk.Label(row_cf4, text="첨가제 C%", width=12).pack(side=tk.LEFT)
        ttk.Entry(row_cf4, textvariable=self.var_c_inorg, width=10).pack(side=tk.LEFT)
        row_cf5 = tk.Frame(cf); row_cf5.pack(fill=tk.X, pady=1)
        ttk.Label(row_cf5, text="P(3-HP) C%", width=12).pack(side=tk.LEFT)
        ttk.Entry(row_cf5, textvariable=self.var_c_p3hp, width=10).pack(side=tk.LEFT)
        row_cf6 = tk.Frame(cf); row_cf6.pack(fill=tk.X, pady=1)
        ttk.Label(row_cf6, text="PHA C%", width=12).pack(side=tk.LEFT)
        ttk.Entry(row_cf6, textvariable=self.var_c_pha, width=10).pack(side=tk.LEFT)
        row_cf7 = tk.Frame(cf); row_cf7.pack(fill=tk.X, pady=1)
        ttk.Label(row_cf7, text="PHB C%", width=12).pack(side=tk.LEFT)
        ttk.Entry(row_cf7, textvariable=self.var_c_phb, width=10).pack(side=tk.LEFT)
        row_cf8 = tk.Frame(cf); row_cf8.pack(fill=tk.X, pady=1)
        ttk.Label(row_cf8, text="Starch C%", width=12).pack(side=tk.LEFT)
        ttk.Entry(row_cf8, textvariable=self.var_c_starch, width=10).pack(side=tk.LEFT)
        ttk.Label(cf, text="Note: 첨가제(Inorganic)/BioChar are excluded from ThCO2 by default.").pack(anchor='w', padx=2)

        # CO2 accounting setup
        acc = ttk.LabelFrame(self.setup_frame, text="CO2 Accounting")
        acc.pack(fill=tk.X, pady=(8, 6))
        self.var_mode = tk.StringVar(value='closed')
        ttk.Radiobutton(acc, text='Closed chamber (volume)', variable=self.var_mode, value='closed').pack(anchor='w')
        ttk.Radiobutton(acc, text='Flow-through (flow rate)', variable=self.var_mode, value='flow').pack(anchor='w')
        row1 = tk.Frame(acc); row1.pack(fill=tk.X)
        ttk.Label(row1, text="Volume (L) / Flow (L/min)", width=24).pack(side=tk.LEFT)
        self.var_vol_flow = tk.DoubleVar(value=10.0)
        ttk.Entry(row1, textvariable=self.var_vol_flow, width=10).pack(side=tk.LEFT)
        row2 = tk.Frame(acc); row2.pack(fill=tk.X)
        ttk.Label(row2, text="Baseline CO2 (ppm)", width=24).pack(side=tk.LEFT)
        self.var_baseline_ppm = tk.DoubleVar(value=400.0)
        ttk.Entry(row2, textvariable=self.var_baseline_ppm, width=10).pack(side=tk.LEFT)
        row3 = tk.Frame(acc); row3.pack(fill=tk.X)
        ttk.Label(row3, text="Temperature (°C)", width=24).pack(side=tk.LEFT)
        self.var_temp_c = tk.DoubleVar(value=25.0)
        ttk.Entry(row3, textvariable=self.var_temp_c, width=10).pack(side=tk.LEFT)
        row4 = tk.Frame(acc); row4.pack(fill=tk.X)
        ttk.Label(row4, text="Pressure (kPa)", width=24).pack(side=tk.LEFT)
        self.var_press_kpa = tk.DoubleVar(value=101.325)
        ttk.Entry(row4, textvariable=self.var_press_kpa, width=10).pack(side=tk.LEFT)

        # Blank/Control selection
        blank = ttk.LabelFrame(self.setup_frame, text="Blank (Control)")
        blank.pack(fill=tk.X, pady=(8, 6))
        ttk.Label(blank, text="Blank source:").pack(side=tk.LEFT, padx=4)
        self.var_blank_src = tk.StringVar(value='none')
        # Options: none plus CO2 1..6
        opts = ['none'] + [f'co2_{i}' for i in range(1,7)]
        self.blank_combo = ttk.Combobox(blank, textvariable=self.var_blank_src, values=opts, width=10, state='readonly')
        self.blank_combo.current(0)
        self.blank_combo.pack(side=tk.LEFT)

        # Proceed button
        ttk.Button(self.setup_frame, text="Proceed to Test", command=self.finalize_setup).pack(pady=8)

        # Attempt to load saved setup from config
        self._load_saved_setup_into_form()
        # Populate detected ports display
        try:
            self._update_setup_ports_view()
        except Exception:
            pass

        # EXIT button on setup page (bottom-right)
        try:
            exit_btn_setup = ttk.Button(self.setup_frame, text="EXIT", command=self.root.quit, style=self.exit_style, width=10)
        except Exception:
            exit_btn_setup = ttk.Button(self.setup_frame, text="EXIT", command=self.root.quit, width=10)
        exit_btn_setup.pack(anchor='e', padx=6, pady=(0,6))

    def start_logging(self):
        """Start the data logging process."""
        if not self.running.is_set():
            self.start_time = time.time()
            # Reset series for a fresh session
            self.xs = []
            self.co2_series = [[] for _ in range(6)]
            self.temp_series = [[] for _ in range(2)]
            self.hum_series = [[] for _ in range(2)]
            self._co2_ema_state = [None] * 6
            self.cum_co2_g = 0.0
            self.cum_blank_g = 0.0
            self.cum_co2_g_ch = [0.0] * 6
            self.last_ppm_ch = [None] * 6
            self._last_flow_ts = None
            self._last_blank_ppm = None
            self.rate_mmol_s_ch = [None] * 6
            self._last_rate_ts = None
            self.pct_series = []
            self.cum_mmol_series_ch = [[] for _ in range(6)]
            self.pct_series_ch = [[] for _ in range(6)]
            self.cum_net_co2_g_ch = [0.0] * 6
            self._last_sample_avg_ppm = None
            # Reset pause state
            self.paused = False
            self.paused_start_ts = None
            # Freeze which sensors to log based on current checkbox selections
            self.selected_co2_for_log = [i for i in range(6) if self.co2_sensor_vars[i].get()]
            self.selected_th_for_log = [j for j in range(2) if self.th_sensor_vars[j].get()]
            self.running.set()
            # Button states: show pressed effect briefly and set invalid/disabled
            try:
                self.start_button.state(['pressed'])
                self.root.after(120, lambda: self.start_button.state(['!pressed']))
            except Exception:
                pass
            self.start_button.state(['disabled'])
            self.start_button.configure(cursor='X_cursor')
            self.stop_button.state(['!disabled'])
            self.stop_button.configure(cursor='hand2')
            if hasattr(self, 'pause_button'):
                self.pause_button.state(['!disabled'])
                self.pause_button.configure(cursor='hand2')
                self.pause_button.configure(text='Pause')

            # Refresh ports on start in case hardware changed
            self._setup_ports()

            # Open log file with header for selected sensors only
            self._open_log()
            self.run_logger()

    def stop_logging(self):
        """Stop the data logging process."""
        if self.running.is_set():
            self.running.clear()
            self.start_button.state(['!disabled'])
            self.start_button.configure(cursor='hand2')
            self.stop_button.state(['disabled'])
            self.stop_button.configure(cursor='X_cursor')
            if hasattr(self, 'pause_button'):
                self.pause_button.state(['disabled'])
                self.pause_button.configure(cursor='X_cursor')
            self.paused = False
            self.paused_start_ts = None
            # Close log file
            self._close_log()

    def toggle_pause(self):
        """Toggle pause/resume of logging without resetting data."""
        if not self.running.is_set():
            return
        if not getattr(self, 'paused', False):
            # Pause
            self.paused = True
            self.paused_start_ts = time.time()
            try:
                self.pause_button.state(['pressed'])
                self.root.after(120, lambda: self.pause_button.state(['!pressed']))
            except Exception:
                pass
            self.pause_button.configure(text='Resume')
        else:
            # Resume; adjust start_time to exclude paused duration
            paused_dur = 0.0
            if self.paused_start_ts is not None:
                paused_dur = time.time() - self.paused_start_ts
            if paused_dur > 0:
                self.start_time += paused_dur
            self.paused = False
            self.paused_start_ts = None
            self.pause_button.configure(text='Pause')

    def run_logger(self):
        """Run the data logging and plotting."""
        if not self.running.is_set():
            return  # Stop if the logging process is no longer running

        # Jittered interval to avoid resonance with sensor internal cycle
        delay_s = max(0.2, self.interval_s + random.uniform(-self.interval_jitter_s, self.interval_jitter_s))
        interval = int(delay_s * 1000)  # milliseconds

        # If paused, schedule next tick without updating
        if getattr(self, 'paused', False):
            self.root.after(interval, self.run_logger)
            return

        # Read data from checked sensors
        co2_values = []
        for i in range(6):
            value = None
            try:
                if self.co2_sensor_vars[i].get() and self.co2_serials[i]:
                    value, resp = self._read_co2_with_resp(self.co2_serials[i])
                    # Debug raw logging
                    now = time.time()
                    if self.debug_raw:
                        dt = (now - self._co2_last_read_ts[i]) if self._co2_last_read_ts[i] else float('nan')
                        line = f"{datetime.now().isoformat()} RAW CO2{i+1}: dt={dt:.3f}s resp={resp.hex() if resp else 'None'} val_raw={value}\n"
                        try:
                            if self._debug_fh:
                                self._debug_fh.write(line)
                            else:
                                print(line.strip())
                        except Exception:
                            print(line.strip())
                    # Duplicate-frame suppression: if same response in short time, keep previous displayed value
                    if resp is not None and self._co2_last_resp[i] is not None and now - (self._co2_last_read_ts[i] or 0) < self.min_dup_interval:
                        if resp == self._co2_last_resp[i] and len(self.co2_series[i]) > 0:
                            value = self.co2_series[i][-1]
                    self._co2_last_resp[i] = resp
                    self._co2_last_read_ts[i] = now
                    # Apply per-sensor offset and optional smoothing
                    if value is not None:
                        value = value + self._get_offset_for_index(i)
                        if self.co2_ema_alpha > 0:
                            prev = self._co2_ema_state[i]
                            a = self.co2_ema_alpha
                            value = (a * value) + ((1 - a) * prev) if prev is not None else value
                            self._co2_ema_state[i] = value
                    # Median filter
                    if value is not None:
                        self.co2_mbufs[i].append(value)
                        vals = list(self.co2_mbufs[i])
                        if vals:
                            sv = sorted(vals)
                            mid = len(sv)//2
                            value = sv[mid] if len(sv) % 2 == 1 else (sv[mid-1] + sv[mid]) / 2.0
                    # Hysteresis: ignore tiny changes
                    if value is not None and len(self.co2_series[i]) > 0 and self.co2_series[i][-1] is not None:
                        if abs(value - self.co2_series[i][-1]) < self.hysteresis_ppm:
                            value = self.co2_series[i][-1]
            except Exception as e:
                print(f"CO2 {i+1} read error: {e}")
                # Mark serial as broken to trigger recovery
                try:
                    if self.co2_serials[i]:
                        try: self.co2_serials[i].close()
                        except Exception: pass
                except Exception:
                    pass
                self.co2_serials[i] = None
                try:
                    self._set_co2_status(i, 'ERR')
                except Exception:
                    pass
            finally:
                self.co2_series[i].append(value)
            co2_values.append(value)

        # Watchdog: reopen CO2 ports on consecutive failures or long no-success window
        now_ts = time.time()
        for i in range(6):
            v = co2_values[i] if i < len(co2_values) else None
            if v is not None:
                self.co2_err_counts[i] = 0
                self.co2_last_ok[i] = now_ts
                self._set_co2_status(i, 'OK')
            else:
                self.co2_err_counts[i] += 1
                last_ok = self.co2_last_ok[i] or 0
                last_re = self._co2_last_reopen_ts[i]
                if (self.co2_err_counts[i] >= 3 or (now_ts - last_ok) > 15) and (now_ts - last_re) > 10:
                    self._set_co2_status(i, 'Reopen')
                    self._reopen_co2(i)
                    self._co2_last_reopen_ts[i] = now_ts

        temp_values = []
        hum_values = []
        for j in range(2):
            t = h = None
            try:
                if self.th_sensor_vars[j].get() and self.th_serials[j]:
                    t, h = self._read_temp_hum(self.th_serials[j])
            except Exception as e:
                print(f"Temp/Humidity {j+1} read error: {e}")
                # Mark serial as broken to trigger recovery
                try:
                    if self.th_serials[j]:
                        try: self.th_serials[j].close()
                        except Exception: pass
                except Exception:
                    pass
                self.th_serials[j] = None
                try:
                    self._set_th_status(j, 'ERR')
                except Exception:
                    pass
            finally:
                self.temp_series[j].append(t)
                self.hum_series[j].append(h)
            temp_values.append(t)
            hum_values.append(h)

        # Watchdog for TH
        for j in range(2):
            ok = (temp_values[j] is not None) or (hum_values[j] is not None)
            if ok:
                self.th_err_counts[j] = 0
                self.th_last_ok[j] = now_ts
                self._set_th_status(j, 'OK')
            else:
                self.th_err_counts[j] += 1
                last_ok = self.th_last_ok[j] or 0
                last_re = self._th_last_reopen_ts[j]
                if (self.th_err_counts[j] >= 3 or (now_ts - last_ok) > 15) and (now_ts - last_re) > 10:
                    self._set_th_status(j, 'Reopen')
                    self._reopen_th(j)
                    self._th_last_reopen_ts[j] = now_ts

        # Show/hide blinking warning popup depending on read failures for selected sensors
        try:
            any_fail = False
            # CO2 selected with recent failure
            for i in range(6):
                if i < len(self.co2_sensor_vars) and self.co2_sensor_vars[i].get():
                    # consider failure if serial missing or err count > 0 and current value None
                    if (self.co2_serials[i] is None) or (co2_values[i] is None and self.co2_err_counts[i] > 0):
                        any_fail = True; break
            # TH selected with recent failure
            if not any_fail:
                for j in range(2):
                    if j < len(self.th_sensor_vars) and self.th_sensor_vars[j].get():
                        if (self.th_serials[j] is None) or ((temp_values[j] is None and hum_values[j] is None) and self.th_err_counts[j] > 0):
                            any_fail = True; break
            if any_fail:
                self._show_warn_popup("센서 읽기 실패 — 자동 재시도 중")
            else:
                self._hide_warn_popup()
        except Exception:
            pass

        elapsed = time.time() - self.start_time
        self.xs.append(elapsed)
        # Keep global timeline; per-series length matches xs (append even if None)

        # Print generated data to console
        stamp = datetime.now().isoformat(timespec='seconds')
        print(f"{stamp} | Elapsed: {elapsed:.1f}s | "
              f"CO2: {[v if v is not None else 'NA' for v in co2_values]} | "
              f"Temp: {[v if v is not None else 'NA' for v in temp_values]} | "
              f"Humidity: {[v if v is not None else 'NA' for v in hum_values]}")

        # Log to CSV
        self._log_row(stamp, elapsed, co2_values, temp_values, hum_values)

        # Update live stats labels
        self._update_stats()

        # Update per-channel cumulative CO2 and biodegradability
        try:
            sel = [v for i, v in enumerate(co2_values) if self.co2_sensor_vars[i].get() and v is not None]
            avg_ppm = sum(sel) / len(sel) if sel else None
            # Blank: prefer channels labeled as 'blank'; fallback to config
            blank_ppm = None
            try:
                blanks = [co2_values[i] for i in range(min(6, len(co2_values))) if self._get_role_for_index(i) == 'blank' and co2_values[i] is not None]
                if blanks:
                    blank_ppm = sum(blanks)/len(blanks)
            except Exception:
                blank_ppm = None
            if blank_ppm is None:
                src = getattr(self, 'test_setup', {}).get('blank_source', 'none')
                if isinstance(src, str) and src.startswith('co2_'):
                    try:
                        idx = int(src.split('_')[1]) - 1
                        if 0 <= idx < len(co2_values):
                            blank_ppm = co2_values[idx]
                    except Exception:
                        blank_ppm = None
            # Determine if sample average decreased; if so, suspend integration for this tick
            try:
                sel_samples = [co2_values[i] for i in range(min(6, len(co2_values))) if self._get_role_for_index(i) == 'sample' and co2_values[i] is not None and self.co2_sensor_vars[i].get()]
                avg_sample_ppm = (sum(sel_samples) / len(sel_samples)) if sel_samples else None
            except Exception:
                avg_sample_ppm = None
            suppress_integration = False
            if avg_sample_ppm is not None and self._last_sample_avg_ppm is not None:
                try:
                    eps = float(getattr(self, 'integr_epsilon_ppm', 0.0))
                except Exception:
                    eps = 0.0
                if avg_sample_ppm < (self._last_sample_avg_ppm - eps):
                    suppress_integration = True
            # Update tracker for next tick
            if avg_sample_ppm is not None:
                self._last_sample_avg_ppm = avg_sample_ppm

            if not suppress_integration:
                # First integrate per-channel cumulative mass and common blank
                self._update_cumulative_per_channel(co2_values, blank_ppm)
                # Then update global biodegradability using average ppm (blank handled above)
                self._update_biodeg_with_ppm(avg_ppm, blank_ppm)
        except Exception:
            pass

        # Update per-channel derived series (mmol and biodeg %) for plotting
        try:
            for i in range(6):
                # net grams for channel i (pre-integrated per tick)
                try:
                    net_g = max(0.0, (self.cum_net_co2_g_ch[i] or 0.0))
                except Exception:
                    net_g = None
                if self._get_role_for_index(i) == 'blank':
                    # Exclude blanks: append None to keep alignment
                    self.cum_mmol_series_ch[i].append(None)
                    self.pct_series_ch[i].append(None)
                    continue
                # mmol
                if net_g is None:
                    self.cum_mmol_series_ch[i].append(None)
                else:
                    try:
                        new_mmol = (net_g / 44.01) * 1000.0
                        # Apply plot epsilon to avoid creeping when display shows 0
                        try:
                            prev = next((v for v in reversed(self.cum_mmol_series_ch[i]) if v is not None), None)
                        except Exception:
                            prev = None
                        if prev is not None and abs(new_mmol - prev) < getattr(self, 'mmol_plot_epsilon', 0.05):
                            new_mmol = prev
                        self.cum_mmol_series_ch[i].append(new_mmol)
                    except Exception:
                        self.cum_mmol_series_ch[i].append(None)
                # biodeg %
                try:
                    if self.thco2_g and self.thco2_g > 0 and net_g is not None:
                        pct = max(0.0, min(100.0, (net_g / self.thco2_g) * 100.0))
                        self.pct_series_ch[i].append(pct)
                    else:
                        self.pct_series_ch[i].append(None)
                except Exception:
                    self.pct_series_ch[i].append(None)
        except Exception:
            pass

        # Update the plot
        self.update_plot()

        # Schedule the next data generation
        self.root.after(interval, self.run_logger)
        # Ensure recovery loop runs
        try:
            if not self._recovery_loop_active:
                self._start_recovery_loop()
        except Exception:
            pass

        # Estimate and report cycle period (based on local minima) for the first active CO2 sensor
        try:
            first_idx = next((i for i in range(6) if self.co2_sensor_vars[i].get()), None)
            if first_idx is not None:
                period = self._estimate_cycle_period(self.co2_series[first_idx], self.xs)
                if self.debug_raw and period and (self._last_period_report is None or abs(period - self._last_period_report) > 10):
                    msg = f"Estimated CO2 cycle period ≈ {period:.0f} s (sensor {first_idx+1})"
                    print(msg)
                    if self._debug_fh:
                        try:
                            self._debug_fh.write(datetime.now().isoformat() + " " + msg + "\n")
                        except Exception:
                            pass
                    self._last_period_report = period
                # Auto-tune step if active
                if getattr(self, 'auto_tune_active', False):
                    try:
                        self._auto_tune_step(period, interval/1000.0)
                    except Exception as e:
                        print(f"Auto-tune step error: {e}")
        except Exception:
            pass

    def _read_co2_with_resp(self, ser):
        """Read CO2 and return (value, raw_response).

        Improves robustness by flushing input, adding a short delay, and re-aligning
        the frame if the start bytes are offset.
        """
        try:
            ser.reset_input_buffer()
        except Exception:
            pass
        ser.write(b'\xFF\x01\x86\x00\x00\x00\x00\x00\x79')
        # Small wait for sensor to respond
        try:
            time.sleep(0.05)
        except Exception:
            pass
        # Read up to 18 bytes and try to align on 0xFF 0x86
        buf = ser.read(18)
        if len(buf) >= 9:
            # Search for header pattern
            for i in range(0, len(buf) - 8):
                if buf[i] == 0xFF and buf[i+1] == 0x86:
                    frame = buf[i:i+9]
                    if len(frame) == 9:
                        high_byte = frame[2]
                        low_byte = frame[3]
                        val = (high_byte << 8) | low_byte
                        return val, bytes(frame)
        # Fallback: one more direct 9-byte read
        response = ser.read(9)
        if len(response) == 9 and response[0] == 0xFF and response[1] == 0x86:
            high_byte = response[2]
            low_byte = response[3]
            val = (high_byte << 8) | low_byte
            return val, response
        # Return last buffer if anything was read for debugging
        return None, buf if buf else (response if response else None)

    def _read_temp_hum(self, ser):
        """Read temperature and humidity from a TH sensor. Supports regex or CSV."""
        try:
            # Some firmwares require a command to trigger output; if not, it will just stream
            ser.write(b"READ\n")
        except Exception:
            pass
        raw = ser.readline().decode('utf-8', errors='ignore').strip()
        if not raw:
            return None, None
        if self.thr_regex:
            m = self.thr_regex.search(raw)
            if m:
                try:
                    t = float(m.group('temp'))
                    h = float(m.group('rh'))
                    return t, h
                except Exception:
                    return None, None
        # Fallback: CSV "temp,humidity"
        parts = [p.strip() for p in raw.split(',')]
        if len(parts) >= 2:
            try:
                return float(parts[0]), float(parts[1])
            except Exception:
                return None, None
        return None, None

    def update_plot(self):
        """Update the matplotlib plot for all active sensors."""
        self.ax.clear()
        self.ax2.clear()
        try:
            self.ax3.clear()
        except Exception:
            pass

        self.ax.set_title("CO2, Temperature, and Humidity")
        self.ax.set_xlabel("Elapsed (s)")
        self.ax.set_ylabel("CO2 (ppm)")
        self.ax.grid(True)
        # Ensure right-side axes truly stay on the right after clear()
        try:
            self.ax2.yaxis.tick_right()
            self.ax2.yaxis.set_label_position("right")
            self.ax2.spines['right'].set_position(("axes", 1.0))
        except Exception:
            pass
        try:
            self.ax3.yaxis.tick_right()
            self.ax3.yaxis.set_label_position("right")
            self.ax3.spines['right'].set_position(("axes", 1.1))
        except Exception:
            pass
        self.ax2.set_ylabel("")
        self.ax3.set_ylabel("Temp/Humid")

        # Draw composition bar + legend at top within main figure
        try:
            if not hasattr(self, 'ax_comp_bar_top') or self.ax_comp_bar_top is None:
                # Bar a bit higher and legend a bit lower to increase spacing
                self.ax_comp_bar_top = self.figure.add_axes([0.12, 0.935, 0.66, 0.03])
                self.ax_comp_leg_top = self.figure.add_axes([0.12, 0.85, 0.66, 0.06])
                self.ax_comp_leg_top.axis('off')
            self.ax_comp_bar_top.clear()
            try:
                self.ax_comp_leg_top.cla(); self.ax_comp_leg_top.axis('off')
            except Exception:
                pass
            comp = getattr(self, 'test_setup', {}).get('composition', None)
            if comp:
                items = [(k, float(v)) for k, v in comp.items() if float(v) > 0]
                total = sum(v for _, v in items) or 1.0
                left = 0.0
                handles = []; labels = []
                for name, v in items:
                    frac = 100.0 * v / total
                    color = self.material_colors.get(name, '#BDBDBD')
                    self.ax_comp_bar_top.barh([0], [frac], left=left, color=color, edgecolor='white', height=0.3)
                    left += frac
                    handles.append(Patch(facecolor=color, edgecolor='white'))
                    labels.append(self.material_name_map.get(name, name))
                # Title above the bar
                try:
                    self.ax_comp_bar_top.set_title('샘플 구성비', fontsize=10, loc='left', pad=2)
                except Exception:
                    pass
                self.ax_comp_bar_top.set_xlim(0, 100)
                self.ax_comp_bar_top.set_yticks([])
                for spine in ['top','right','left','bottom']:
                    try:
                        self.ax_comp_bar_top.spines[spine].set_visible(False)
                    except Exception:
                        pass
                if handles:
                    ncol = min(len(handles), 5)
                    self.ax_comp_leg_top.legend(handles, labels, loc='center', ncol=ncol, frameon=False, fontsize=8)
        except Exception:
            pass

        # Plot CO2 series with role-based labels (샘플1/비교샘플1/blank 등)
        try:
            plot_idxs = [i for i, s in enumerate(self.co2_series) if any(v is not None for v in s)]
            # Count totals per role among plotted indices
            from collections import Counter
            role_totals = Counter(self._get_role_for_index(i) for i in plot_idxs)
            role_counts = {'sample': 0, 'compare': 0, 'blank': 0}
            def role_disp(code):
                return {'sample': '샘플', 'compare': '비교샘플', 'blank': 'blank'}.get(code, '샘플')
            for i in plot_idxs:
                series = self.co2_series[i]
                role_i = self._get_role_for_index(i)
                color = self.role_colors.get(role_i, self.co2_colors[i % len(self.co2_colors)])
                linestyle = ['-', '--', '-.', ':', '-', '--'][i % 6]
                role_counts[role_i] = role_counts.get(role_i, 0) + 1
                idx = role_counts[role_i]
                name = role_disp(role_i) + (str(idx) if role_totals.get(role_i, 0) > 1 else '')
                self.ax.plot(self.xs, series, label=name, color=color, linestyle=linestyle)
        except Exception:
            # Fallback to original labeling
            for i, series in enumerate(self.co2_series):
                if any(v is not None for v in series):
                    color = self.co2_colors[i % len(self.co2_colors)]
                    linestyle = ['-', '--', '-.', ':', '-', '--'][i % 6]
                    self.ax.plot(self.xs, series, label=f"CO2 {i+1}", color=color, linestyle=linestyle)

        # Plot Temp series on ax2
        for j, series in enumerate(self.temp_series):
            if any(v is not None for v in series):
                color = self.temp_colors[j % len(self.temp_colors)]
                self.ax2.plot(self.xs, series, label=f"Temp {j+1}", color=color, linestyle='-')
        # Plot Humidity series on ax3
        for j, series in enumerate(self.hum_series):
            if any(v is not None for v in series):
                color = self.hum_colors[j % len(self.hum_colors)]
                self.ax3.plot(self.xs, series, label=f"Hum {j+1}", color=color, linestyle='--')

        # Adjust layout to make space for right-side mini plots and top composition bar
        try:
            self.figure.subplots_adjust(right=0.70, bottom=0.12, top=0.72)
        except Exception:
            pass

        # Add legends: CO2 inside (upper-left), Temp/Hum inside (upper-right)
        if self.ax.lines:
            self.ax.legend(loc="upper left")
        # Combine temp and humidity handles for a single right-side legend
        th_handles = list(self.ax2.lines) + list(self.ax3.lines)
        if not th_handles:
            # No TH data drawn (e.g., sensors disabled). Provide proxy handles so legend still shows on right.
            th_handles = [
                Line2D([0], [0], color=self.temp_colors[0], linestyle='-', label='Temp 1'),
                Line2D([0], [0], color=self.hum_colors[0], linestyle='--', label='Hum 1'),
            ]
        labels = [l.get_label() for l in th_handles]
        legend = self.ax3.legend(th_handles, labels, loc='upper right', frameon=False)
        # Color legend text to match each handle's color for clearer association
        try:
            for txt, h in zip(legend.get_texts(), th_handles):
                txt.set_color(h.get_color())
        except Exception:
            pass

        # Apply requested axis limits
        try:
            self.ax.set_ylim(0, 5000)             # CO2 ppm (updated range)
            self.ax2.set_ylim(-10, 100)            # Temperature °C
            self.ax3.set_ylim(0, 100)              # Humidity %
        except Exception:
            pass

        # Color tick labels to match series themes (Temp=red, Humid=blue)
        try:
            self.ax.tick_params(axis='y', colors='black')
            self.ax2.tick_params(axis='y', colors=self.temp_colors[0])
            self.ax3.tick_params(axis='y', colors=self.hum_colors[0])
            # Also color right-side spines for clarity
            self.ax2.spines['right'].set_color(self.temp_colors[0])
            self.ax3.spines['right'].set_color(self.hum_colors[0])
        except Exception:
            pass

        # Draw right-column mini plots if available; else fallback to inline axes
        if getattr(self, 'side_ax_mmol', None) is not None and getattr(self, 'side_ax_pct', None) is not None:
            try:
                # Cum mmol
                self.side_ax_mmol.clear()
                any_line = False
                # Prepare role numbering among plotted lines
                from collections import Counter
                indices = [i for i in range(6) if (self._get_role_for_index(i) != 'blank' and (i < len(self.co2_sensor_vars) and self.co2_sensor_vars[i].get()))]
                role_totals = Counter(self._get_role_for_index(i) for i in indices)
                role_counts = {'sample': 0, 'compare': 0}
                def role_disp(code):
                    return {'sample': '샘플', 'compare': '비교샘플'}.get(code, '샘플')
                for i in range(6):
                    role_i = self._get_role_for_index(i)
                    if role_i == 'blank':
                        continue
                    if not (i < len(self.co2_sensor_vars) and self.co2_sensor_vars[i].get()):
                        continue
                    series = self.cum_mmol_series_ch[i] if i < len(self.cum_mmol_series_ch) else []
                    if self.xs and series and any(v is not None for v in series):
                        color = self.role_colors.get(role_i, self.co2_colors[i % len(self.co2_colors)])
                        linestyle = ['-', '--', '-.', ':', '-', '--'][i % 6]
                        try:
                            latest = next((v for v in reversed(series) if v is not None), None)
                        except Exception:
                            latest = None
                        role_counts[role_i] = role_counts.get(role_i, 0) + 1
                        idx = role_counts[role_i]
                        base = role_disp(role_i) + (str(idx) if role_totals.get(role_i, 0) > 1 else '')
                        label = base + (f" {latest:.1f}" if isinstance(latest, (int, float)) else "")
                        self.side_ax_mmol.plot(self.xs, series, color=color, linestyle=linestyle, label=label)
                        any_line = True
                self.side_ax_mmol.set_ylabel('누적 (mmol)')
                self.side_ax_mmol.grid(True, axis='y', linestyle=':', alpha=0.5)
                if any_line:
                    self.side_ax_mmol.legend(loc='upper left', fontsize=8)

                # Pct subplot
                self.side_ax_pct.clear()
                any_line = False
                role_counts = {'sample': 0, 'compare': 0}
                for i in range(6):
                    role_i = self._get_role_for_index(i)
                    if role_i == 'blank':
                        continue
                    if not (i < len(self.co2_sensor_vars) and self.co2_sensor_vars[i].get()):
                        continue
                    series = self.pct_series_ch[i] if i < len(self.pct_series_ch) else []
                    if self.xs and series and any(v is not None for v in series):
                        color = self.role_colors.get(role_i, self.co2_colors[i % len(self.co2_colors)])
                        linestyle = ['-', '--', '-.', ':', '-', '--'][i % 6]
                        try:
                            latest = next((v for v in reversed(series) if v is not None), None)
                        except Exception:
                            latest = None
                        role_counts[role_i] = role_counts.get(role_i, 0) + 1
                        idx = role_counts[role_i]
                        base = role_disp(role_i) + (str(idx) if role_totals.get(role_i, 0) > 1 else '')
                        label = base + (f" {latest:.1f}" if isinstance(latest, (int, float)) else "")
                        self.side_ax_pct.plot(self.xs, series, color=color, linestyle=linestyle, label=label)
                        any_line = True
                self.side_ax_pct.set_ylabel('생분해도 (%)')
                self.side_ax_pct.set_ylim(0, 100)
                self.side_ax_pct.grid(True, axis='y', linestyle=':', alpha=0.5)
                self.side_ax_pct.set_xlabel('Elapsed (s)')
                if any_line:
                    self.side_ax_pct.legend(loc='upper left', fontsize=8)
                # Render right column canvas
                if getattr(self, 'side_canvas', None):
                    self.side_canvas.draw()
            except Exception:
                pass
        else:
            # Fallback: inline axes in the main figure (already implemented above in previous versions)
            try:
                if not hasattr(self, 'ax_mmol') or self.ax_mmol is None:
                    self.ax_mmol = self.figure.add_axes([0.74, 0.56, 0.23, 0.18], sharex=self.ax)
                self.ax_mmol.clear()
                any_line = False
                from collections import Counter
                indices = [i for i in range(6) if (self._get_role_for_index(i) != 'blank' and (i < len(self.co2_sensor_vars) and self.co2_sensor_vars[i].get()))]
                role_totals = Counter(self._get_role_for_index(i) for i in indices)
                role_counts = {'sample': 0, 'compare': 0}
                def role_disp(code):
                    return {'sample': '샘플', 'compare': '비교샘플'}.get(code, '샘플')
                for i in range(6):
                    role_i = self._get_role_for_index(i)
                    if role_i == 'blank':
                        continue
                    if not (i < len(self.co2_sensor_vars) and self.co2_sensor_vars[i].get()):
                        continue
                    series = self.cum_mmol_series_ch[i] if i < len(self.cum_mmol_series_ch) else []
                    if self.xs and series and any(v is not None for v in series):
                        color = self.role_colors.get(role_i, self.co2_colors[i % len(self.co2_colors)])
                        linestyle = ['-', '--', '-.', ':', '-', '--'][i % 6]
                        try:
                            latest = next((v for v in reversed(series) if v is not None), None)
                        except Exception:
                            latest = None
                        role_counts[role_i] = role_counts.get(role_i, 0) + 1
                        idx = role_counts[role_i]
                        base = role_disp(role_i) + (str(idx) if role_totals.get(role_i, 0) > 1 else '')
                        label = base + (f" {latest:.1f}" if isinstance(latest, (int, float)) else "")
                        self.ax_mmol.plot(self.xs, series, color=color, linestyle=linestyle, label=label)
                        any_line = True
                self.ax_mmol.set_ylabel('누적 (mmol)')
                self.ax_mmol.grid(True, axis='y', linestyle=':', alpha=0.5)
                if any_line:
                    self.ax_mmol.legend(loc='upper left', fontsize=8)
            except Exception:
                pass
            try:
                if not hasattr(self, 'ax_pct') or self.ax_pct is None:
                    self.ax_pct = self.figure.add_axes([0.74, 0.32, 0.23, 0.18], sharex=self.ax)
                self.ax_pct.clear()
                any_line = False
                role_counts = {'sample': 0, 'compare': 0}
                for i in range(6):
                    role_i = self._get_role_for_index(i)
                    if role_i == 'blank':
                        continue
                    if not (i < len(self.co2_sensor_vars) and self.co2_sensor_vars[i].get()):
                        continue
                    series = self.pct_series_ch[i] if i < len(self.pct_series_ch) else []
                    if self.xs and series and any(v is not None for v in series):
                        color = self.role_colors.get(role_i, self.co2_colors[i % len(self.co2_colors)])
                        linestyle = ['-', '--', '-.', ':', '-', '--'][i % 6]
                        try:
                            latest = next((v for v in reversed(series) if v is not None), None)
                        except Exception:
                            latest = None
                        role_counts[role_i] = role_counts.get(role_i, 0) + 1
                        idx = role_counts[role_i]
                        base = role_disp(role_i) + (str(idx) if role_totals.get(role_i, 0) > 1 else '')
                        label = base + (f" {latest:.1f}" if isinstance(latest, (int, float)) else "")
                        self.ax_pct.plot(self.xs, series, color=color, linestyle=linestyle, label=label)
                        any_line = True
                self.ax_pct.set_ylabel('생분해도 (%)')
                self.ax_pct.set_ylim(0, 100)
                self.ax_pct.grid(True, axis='y', linestyle=':', alpha=0.5)
                self.ax_pct.set_xlabel('Elapsed (s)')
                if any_line:
                    self.ax_pct.legend(loc='upper left', fontsize=8)
            except Exception:
                pass

        self.canvas.draw()

    def align_sensors(self, window=30):
        """Align selected CO2 sensors using each sensor's minimum value in the window.

        window: number of recent non-None samples to consider per sensor.
        """
        # Determine which sensors are active/selected
        active = [i for i in range(6) if self.co2_sensor_vars[i].get()]
        if len(active) < 2:
            print("Align skipped: need at least two selected CO2 sensors")
            return
        mins = {}
        for i in active:
            series = self.co2_series[i]
            vals = [v for v in series if v is not None][-window:]
            if len(vals) < 3:
                print(f"Align note: CO2 {i+1} has too few samples ({len(vals)})")
            if not vals:
                continue
            mins[i] = min(vals)
        if len(mins) < 2:
            print("Align skipped: not enough valid data to compute averages")
            return
        # Target: mean of sensor minima
        target = sum(mins.values()) / len(mins)
        print(f"Align target from minima (ppm): {target:.1f}")
        # Compute and apply deltas; update offsets and existing series for continuity
        for i, mn in mins.items():
            delta = target - mn
            # Update offset for this index (by port if known)
            self._set_offset_for_index(i, self._get_offset_for_index(i) + delta)
            # Reset EMA state for this sensor so smoothing restarts
            try:
                self._co2_ema_state[i] = None
            except Exception:
                pass
            # Shift existing plotted data so the change is visible immediately
            shifted = []
            for v in self.co2_series[i]:
                shifted.append((v + delta) if v is not None else None)
            self.co2_series[i] = shifted
            print(f"CO2 {i+1}: min={mn:.1f} -> delta={delta:.1f}, new_offset={self._get_offset_for_index(i):.1f}")
        # Refresh stats and plot
        self._update_stats()
        self.update_plot()
        # Persist offsets to config on successful alignment
        self._save_offsets_to_config()
        # Brief UI feedback
        try:
            old = self.align_button.cget('text')
            self.align_button.configure(text='Aligned & Saved')
            self.root.after(1200, lambda: self.align_button.configure(text=old))
        except Exception:
            pass

    def _save_offsets_to_config(self):
        """Persist current CO2 offsets to ports_config.json so they're remembered next run."""
        cfg_path = os.path.join(os.path.dirname(__file__), 'ports_config.json')
        try:
            with open(cfg_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
        except Exception:
            data = {}
        # Build by-port mapping from current state if ports known
        by_port = data.get('co2_offsets_by_port', {})
        for i, port in enumerate(self.co2_ports_used):
            if port:
                by_port[port] = round(float(self._get_offset_for_index(i)), 2)
        data['co2_offsets_by_port'] = by_port
        # Also keep index-based list for backward compatibility
        idx_offsets = [round(float(self._get_offset_for_index(i)), 2) for i in range(6)]
        data['co2_offsets'] = idx_offsets
        try:
            with open(cfg_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            # Keep in-memory config in sync
            self.config = data
            print(f"Saved co2_offsets to {cfg_path}: {data['co2_offsets']}")
        except Exception as e:
            print(f"Failed to save offsets to {cfg_path}: {e}")

    def finalize_setup(self):
        """Validate test setup inputs, build dynamic composition, and reveal the main UI."""
        # Build composition dict from selector rows
        comp = {}
        try:
            for tvar, pvar in zip(self.comp_type_vars, self.comp_pct_vars):
                name_disp = (tvar.get() or '').strip()
                name = self.material_name_map_rev.get(name_disp, name_disp)
                try:
                    pct = float(pvar.get())
                except Exception:
                    pct = 0.0
                if not name or pct <= 0:
                    continue
                comp[name] = comp.get(name, 0.0) + pct
            total_pct = sum(comp.values())
            if total_pct <= 0:
                mb.showerror("Invalid composition", "Total percentage must be > 0.")
                return
            # Compute normalized composition but keep input unchanged in UI
            orig = dict(comp)
            comp_norm = {}
            if abs(total_pct - 100.0) <= 1e-6:
                comp_norm = dict(comp)
                try:
                    if hasattr(self, 'lbl_comp_info'):
                        self.lbl_comp_info.configure(text=f"Total = 100.0%")
                except Exception:
                    pass
            else:
                scale = 100.0 / total_pct
                for k, v in comp.items():
                    comp_norm[k] = v * scale
                try:
                    parts = [f"{k} {orig[k]:.1f}%→{comp_norm.get(k,0.0):.1f}%" for k in orig.keys()]
                    msg = ("Normalized (sum>100): " if total_pct>100 else "Normalized (sum<100): ") + ", ".join(parts)
                    if hasattr(self, 'lbl_comp_info'):
                        self.lbl_comp_info.configure(text=msg)
                except Exception:
                    pass
            # Update Norm % column values
            try:
                for tvar, nvar in zip(self.comp_type_vars, self.comp_norm_pct_vars):
                    nm = (tvar.get() or '').strip()
                    nvar.set(round(comp_norm.get(nm, 0.0), 3))
            except Exception:
                pass
            else:
                # Exactly 100% — clear or show total
                try:
                    if hasattr(self, 'lbl_comp_info'):
                        self.lbl_comp_info.configure(text=f"Total = 100.0%")
                except Exception:
                    pass
            mass = float(self.var_mass.get())
            if mass <= 0:
                mb.showerror("Invalid mass", "Sample mass must be > 0 g.")
                return
        except Exception:
            mb.showerror("Invalid input", "Please enter valid numeric values.")
            return

        # Carbon fraction mapping from rows (row C% overrides defaults)
        default_cf = {
            'PLA': float(self.var_c_pla.get()),
            'PBAT': float(self.var_c_pbat.get()),
            'BioChar': float(self.var_c_biochar.get()),
            'Inorganic': float(self.var_c_inorg.get()),
            'P(3-HP)': float(self.var_c_p3hp.get()),
            'PHA': float(self.var_c_pha.get()),
            'PHB': float(self.var_c_phb.get()),
            'Starch': float(self.var_c_starch.get()),
        }
        carbon_fraction = {}
        # Start with defaults for selected materials
        for k in comp.keys():
            carbon_fraction[k] = float(default_cf.get(k, self._default_cfrac_map.get(k, 0.0)))
        # Apply per-row overrides where provided
        for tvar, cvar in zip(self.comp_type_vars, self.comp_cfrac_vars):
            name_disp = (tvar.get() or '').strip()
            name = self.material_name_map_rev.get(name_disp, name_disp)
            if not name or name not in comp:
                continue
            try:
                cval = float(cvar.get())
                # Enforce 0% for 첨가제(Inorganic) regardless of UI value
                if name == 'Inorganic':
                    cval = 0.0
                if cval > 0:
                    carbon_fraction[name] = cval
            except Exception:
                pass

        # Store setup for later biodegradability calculations
        # Use normalized composition for calculations, preserve input for reference
        self.test_setup = {
            'composition': comp_norm,
            'composition_input': orig,
            'mass_g': mass,
            'env': self.var_env.get(),
            'carbon_fraction': carbon_fraction,
            'co2_accounting': {
                'mode': self.var_mode.get(),
                'vol_or_flow': float(self.var_vol_flow.get()),
                'baseline_ppm': float(self.var_baseline_ppm.get()),
                'temp_c': float(self.var_temp_c.get()),
                'press_kpa': float(self.var_press_kpa.get()),
            },
            'blank_source': self.var_blank_src.get(),
        }
        # Precompute ThCO2 and show in panel
        self.thco2_g = self._compute_thco2_g()
        self._update_bio_panel()
        # Save setup for next run
        self._save_setup_to_config()
        # Hide setup UI, show main UI (plot + sensors + controls)
        try:
            self.setup_frame.pack_forget()
        except Exception:
            pass
        # Pack the content and controls now
        # Top bar with EXIT (on the right)
        try:
            self.topbar_frame.pack(side=tk.TOP, fill=tk.X)
        except Exception:
            pass
        self.content_frame.pack(side=tk.TOP, fill=tk.BOTH, expand=True)
        self.controls_frame.pack(side=tk.BOTTOM, fill=tk.X)
        self.setup_done = True

    # ---------- Biodegradability helpers ----------
    def _compute_thco2_g(self):
        """Compute theoretical CO2 (g) from selected composition using carbon fractions.

        Excludes materials whose carbon fraction is 0 (e.g., Inorganic, BioChar by default).
        """
        if not hasattr(self, 'test_setup'):
            return None
        m = float(self.test_setup['mass_g'])
        comp = dict(self.test_setup.get('composition', {}))
        cf_map = dict(self.test_setup.get('carbon_fraction', {}))
        # Fallback defaults for common materials
        default_cf = {
            'PLA': 50.0,
            'PBAT': 62.0,
            'P(3-HP)': 55.0,
            'PHA': 55.0,
            'PHB': 55.0,
            'Starch': 44.0,
            'Inorganic': 0.0,
            'BioChar': 0.0,
        }
        total_c = 0.0
        for mat, pct in comp.items():
            try:
                # Skip excluded materials regardless of carbon fraction
                if mat in getattr(self, '_excluded_materials', {'BioChar','Inorganic'}):
                    continue
                mass_mat = m * float(pct) / 100.0
                cfrac = float(cf_map.get(mat, default_cf.get(mat, 0.0))) / 100.0
                if cfrac <= 0.0:
                    continue
                total_c += mass_mat * cfrac
            except Exception:
                continue
        thco2 = total_c * (44.01/12.01)
        return thco2

    def _compute_thco2_for_compare_g(self):
        """Compute ThCO2 (g) for the compare sample: 100% Cellulose with same mass."""
        try:
            m = float(self.test_setup.get('mass_g', 0.0)) if hasattr(self, 'test_setup') else 0.0
        except Exception:
            m = 0.0
        if m <= 0:
            return None
        cf_map = dict(getattr(self, 'test_setup', {}).get('carbon_fraction', {}))
        if 'Cellulose' not in cf_map:
            cf_map['Cellulose'] = 44.4
        comp = {'Cellulose': 100.0}
        total_c = 0.0
        try:
            mass_mat = m  # 100% of mass
            cfrac = float(cf_map.get('Cellulose', 44.4)) / 100.0
            total_c += mass_mat * max(0.0, cfrac)
        except Exception:
            return None
        return total_c * (44.01/12.01)

    def _load_saved_setup_into_form(self):
        data = self.config or {}
        saved = data.get('test_setup')
        if not saved:
            return
        try:
            # Populate composition selector rows (up to 8)
            comp = saved.get('composition_input') or saved.get('composition') or {}
            cf_map = saved.get('carbon_fraction', {})
            items = [(k, comp[k]) for k in comp.keys()]
            # Defaults if empty
            if not items:
                items = [('PLA', 70.0), ('PBAT', 20.0), ('Inorganic', 5.0), ('BioChar', 5.0)]
            # Fill rows
            for i in range(8):
                name_int = items[i][0] if i < len(items) else ''
                name = self.material_name_map.get(name_int, name_int)
                pct = items[i][1] if i < len(items) else 0.0
                try:
                    self.comp_type_vars[i].set(name)
                    self.comp_pct_vars[i].set(pct)
                    # Norm % initially equals current (will be updated on finalize)
                    try:
                        self.comp_norm_pct_vars[i].set(pct)
                    except Exception:
                        pass
                    # C% per row: from saved cf map or default map
                    key_int = self.material_name_map_rev.get(name, name)
                    if key_int:
                        cdef = cf_map.get(key_int, self._default_cfrac_map.get(key_int, 0.0))
                    else:
                        cdef = 0.0
                    self.comp_cfrac_vars[i].set(cdef)
                    # Apply grey style for excluded materials
                    try:
                        ent = self.comp_cfrac_entries[i]
                        if key_int in self._excluded_materials:
                            ent.configure(bg='#EEEEEE')
                        else:
                            ent.configure(bg='white')
                    except Exception:
                        pass
                except Exception:
                    pass
            self.var_mass.set(saved.get('mass_g', self.var_mass.get()))
            self.var_env.set(saved.get('env', self.var_env.get()))
            cf = saved.get('carbon_fraction', {})
            self.var_c_pla.set(cf.get('PLA', self.var_c_pla.get()))
            self.var_c_pbat.set(cf.get('PBAT', self.var_c_pbat.get()))
            self.var_c_biochar.set(cf.get('BioChar', self.var_c_biochar.get()))
            self.var_c_inorg.set(cf.get('Inorganic', self.var_c_inorg.get()))
            self.var_c_p3hp.set(cf.get('P(3-HP)', self.var_c_p3hp.get()))
            self.var_c_pha.set(cf.get('PHA', self.var_c_pha.get()))
            self.var_c_phb.set(cf.get('PHB', self.var_c_phb.get()))
            self.var_c_starch.set(cf.get('Starch', self.var_c_starch.get()))
            acc = saved.get('co2_accounting', {})
            self.var_mode.set(acc.get('mode', self.var_mode.get()))
            self.var_vol_flow.set(acc.get('vol_or_flow', self.var_vol_flow.get()))
            self.var_baseline_ppm.set(acc.get('baseline_ppm', self.var_baseline_ppm.get()))
            self.var_temp_c.set(acc.get('temp_c', self.var_temp_c.get()))
            self.var_press_kpa.set(acc.get('press_kpa', self.var_press_kpa.get()))
            self.var_blank_src.set(saved.get('blank_source', self.var_blank_src.get()))
        except Exception:
            pass
        # After loading, compute and display normalized preview
        try:
            self._update_comp_norm_preview()
        except Exception:
            pass

    def _save_setup_to_config(self):
        cfg_path = os.path.join(os.path.dirname(__file__), 'ports_config.json')
        try:
            with open(cfg_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
        except Exception:
            data = {}
        data['test_setup'] = self.test_setup
        try:
            with open(cfg_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            print(f"Failed to save test_setup to {cfg_path}: {e}")

    def _update_comp_norm_preview(self):
        """Recompute normalized composition from current UI rows and update Norm % + info label."""
        try:
            # Build current inputs
            comp_raw = {}
            for tvar, pvar in zip(self.comp_type_vars, self.comp_pct_vars):
                name = (tvar.get() or '').strip()
                try:
                    pct = float(pvar.get())
                except Exception:
                    pct = 0.0
                if not name or pct <= 0:
                    continue
                comp_raw[name] = comp_raw.get(name, 0.0) + pct
            total = sum(comp_raw.values())
            # Normalize softly both for <100 and >100
            if total > 0:
                scale = 100.0 / total
                comp_norm = {k: v * scale for k, v in comp_raw.items()}
            else:
                comp_norm = {}
            # Update Norm % column
            for tvar, nvar in zip(self.comp_type_vars, self.comp_norm_pct_vars):
                nm_disp = (tvar.get() or '').strip()
                nm = self.material_name_map_rev.get(nm_disp, nm_disp)
                try:
                    nvar.set(round(comp_norm.get(nm, 0.0), 3))
                except Exception:
                    pass
            # Update info label
            try:
                if total > 0:
                    parts = [f"{k} {comp_raw[k]:.1f}%→{comp_norm.get(k,0.0):.1f}%" for k in comp_raw.keys()]
                    prefix = "Normalized (sum>100): " if total > 100.0 else ("Normalized (sum<100): " if total < 100.0 else "Total = 100.0%")
                    msg = prefix if total == 100.0 else (prefix + ", ".join(parts))
                else:
                    msg = ""
                if hasattr(self, 'lbl_comp_info'):
                    self.lbl_comp_info.configure(text=msg)
            except Exception:
                pass
        except Exception:
            pass

    def _render_composition_native(self):
        """Render vertical composition bar and legend in the Composition panel using Tk Canvas."""
        try:
            if not (getattr(self, 'comp_canvas_native', None) and getattr(self, 'comp_legend_native', None)):
                return
            # Clear canvas and legend
            c = self.comp_canvas_native
            try:
                c.delete('all')
            except Exception:
                pass
            for w in list(self.comp_legend_native.children.values()):
                try:
                    w.destroy()
                except Exception:
                    pass
            # Fetch composition (normalized, saved in test_setup)
            comp = None
            try:
                comp = getattr(self, 'test_setup', {}).get('composition')
            except Exception:
                comp = None
            if not comp:
                return
            items = [(k, float(v)) for k, v in comp.items() if float(v) > 0]
            if not items:
                return
            # Sort by descending percent for stable legend
            items.sort(key=lambda kv: kv[1], reverse=True)
            # Canvas geometry
            width = int(c.cget('width'))
            height = int(c.cget('height'))
            x0, x1 = 6, width - 6
            y_bottom = height - 6
            y_top = 6
            total = sum(v for _, v in items) or 1.0
            # Draw stacked rectangles (bottom-up)
            y_cursor = y_bottom
            for name, v in items:
                frac = max(0.0, float(v)) / total
                seg_h = max(2, int((y_bottom - y_top) * frac))
                y1 = y_cursor
                y0seg = max(y_top, y1 - seg_h)
                color = self.material_colors.get(name, '#BDBDBD')
                try:
                    c.create_rectangle(x0, y0seg, x1, y1, outline='white', fill=color)
                except Exception:
                    pass
                y_cursor = y0seg
            # Legend rows: colored boxes + labels with percent
            for name, v in items:
                row = tk.Frame(self.comp_legend_native)
                row.pack(anchor='w')
                sw = tk.Canvas(row, width=14, height=10, highlightthickness=0, bd=0)
                sw.pack(side=tk.LEFT, padx=(0, 6))
                col = self.material_colors.get(name, '#BDBDBD')
                try:
                    sw.create_rectangle(0, 0, 14, 10, outline=col, fill=col)
                except Exception:
                    pass
                ttk.Label(row, text=f"{name} {v:.1f}%").pack(side=tk.LEFT)
        except Exception:
            pass

    def _update_biodeg_with_ppm(self, ppm_now, blank_ppm=None):
        """Update cumulative CO2 and biodegradability given current average CO2 ppm reading."""
        if ppm_now is None:
            return
        ts = time.time()
        # Parameters
        acc = getattr(self, 'test_setup', {}).get('co2_accounting', {})
        mode = acc.get('mode', 'closed')
        vf = float(acc.get('vol_or_flow', 10.0))
        baseline = float(acc.get('baseline_ppm', 400.0))
        T = float(acc.get('temp_c', 25.0)) + 273.15
        P = float(acc.get('press_kpa', 101.325)) * 1000.0  # Pa
        R = 8.314462618

        if self.last_ppm is None:
            self.last_ppm = ppm_now
            self.last_ppm_ts = ts
            self._update_bio_panel()
            return

        dt = max(0.0, ts - (self.last_ppm_ts or ts))
        if dt <= 0:
            return

        if mode == 'flow':
            # Flow-through: grams = (flow_m3_s * (ppm-baseline) * P/(R*T)) * M * dt
            flow_m3_s = (vf / 1000.0) / 60.0  # L/min -> m3/s
            mole_rate = flow_m3_s * max(0.0, ppm_now - baseline) * 1e-6 * (P/(R*T))
            d_g = mole_rate * 44.01 * dt
            # Blank accumulation handled in per-channel integrator to avoid double counting
            d_blank = 0.0
        else:
            # Closed volume: integrate only positive changes in ppm over dt (monotonic cumulative)
            dppm = max(0.0, ppm_now - self.last_ppm)
            vol_m3 = vf / 1000.0
            dmoles = dppm * 1e-6 * vol_m3 * (P/(R*T))
            d_g = dmoles * 44.01
            # Blank accumulation handled in per-channel integrator
            d_blank = 0.0
        # Accumulate, not letting cumulative drop below zero
        self.cum_co2_g = max(0.0, self.cum_co2_g + d_g)
        # self.cum_blank_g updated elsewhere
        self.last_ppm = ppm_now
        self.last_ppm_ts = ts
        # Update percent series
        net = max(0.0, self.cum_co2_g - self.cum_blank_g)
        if self.thco2_g and self.thco2_g > 0:
            pct = max(0.0, min(100.0, (net / self.thco2_g) * 100.0))
            self.pct_series.append(pct)
        else:
            self.pct_series.append(0.0)
        self._update_bio_panel()

    def _update_cumulative_per_channel(self, co2_values, blank_ppm=None):
        """Integrate cumulative CO2 mass per channel (g), and update common blank.

        - flow mode integrates mass rate over dt using current ppm.
        - closed mode integrates based on delta ppm per channel in a fixed volume.
        """
        try:
            acc = getattr(self, 'test_setup', {}).get('co2_accounting', {})
            mode = acc.get('mode', 'closed')
            vf = float(acc.get('vol_or_flow', 10.0))
            baseline = float(acc.get('baseline_ppm', 400.0))
            baseline_mode = acc.get('baseline_mode', 'blank')  # 'blank' or 'per_channel'
            per_ch_base = acc.get('per_channel_baseline_ppm', []) or []
            T = float(acc.get('temp_c', 25.0)) + 273.15
            P = float(acc.get('press_kpa', 101.325)) * 1000.0  # Pa
            R = 8.314462618
        except Exception:
            return

        now = time.time()
        if mode == 'flow':
            if self._last_flow_ts is None:
                self._last_flow_ts = now
                self._last_rate_ts = now
                return
            dt = max(0.0, now - self._last_flow_ts)
            if dt <= 0:
                return
            flow_m3_s = (vf / 1000.0) / 60.0
            # Pre-compute blank mole rate and delta ppm if applicable
            blank_mole_rate = 0.0
            blank_excess_ppm = None
            if baseline_mode != 'per_channel' and blank_ppm is not None:
                try:
                    blank_excess_ppm = max(0.0, blank_ppm - baseline)
                    blank_mole_rate = flow_m3_s * blank_excess_ppm * 1e-6 * (P/(R*T))
                except Exception:
                    blank_mole_rate = 0.0
                    blank_excess_ppm = None
            for i, v in enumerate(co2_values):
                if v is None:
                    self.rate_mmol_s_ch[i] = None
                    continue
                try:
                    # Per-channel baseline mode (flow only): subtract channel-specific baseline instead of blank
                    base_use = float(per_ch_base[i]) if (baseline_mode == 'per_channel' and i < len(per_ch_base)) else baseline
                    ch_excess_ppm = max(0.0, v - base_use)
                    mole_rate = flow_m3_s * ch_excess_ppm * 1e-6 * (P/(R*T))  # mol/s
                    d_g = mole_rate * 44.01 * dt
                    self.cum_co2_g_ch[i] = max(0.0, self.cum_co2_g_ch[i] + d_g)
                    # Net per-channel: subtract common blank mole rate when using global baseline
                    if baseline_mode != 'per_channel':
                        # Require valid blank reading to integrate net; else skip
                        if blank_excess_ppm is None:
                            net_mole_rate = 0.0
                        else:
                            net_mole_rate = mole_rate - blank_mole_rate
                    else:
                        net_mole_rate = mole_rate
                    # Apply ppm-level noise floor to net integration
                    if baseline_mode != 'per_channel':
                        diff_ppm = ch_excess_ppm - (blank_excess_ppm or 0.0)
                    else:
                        diff_ppm = ch_excess_ppm
                    if diff_ppm < self.integr_epsilon_ppm:
                        net_mole_rate = 0.0
                    if net_mole_rate < 0:
                        net_mole_rate = 0.0
                    self.cum_net_co2_g_ch[i] = max(0.0, self.cum_net_co2_g_ch[i] + net_mole_rate * 44.01 * dt)
                    # Instantaneous rate in mmol/s
                    self.rate_mmol_s_ch[i] = mole_rate * 1000.0
                except Exception:
                    self.rate_mmol_s_ch[i] = None
                    pass
            # Common blank
            try:
                if baseline_mode != 'per_channel':
                    if blank_ppm is not None:
                        mole_rate_b = flow_m3_s * max(0.0, blank_ppm - baseline) * 1e-6 * (P/(R*T))
                        d_blank = mole_rate_b * 44.01 * dt
                        self.cum_blank_g = max(0.0, self.cum_blank_g + d_blank)
            except Exception:
                pass
            self._last_flow_ts = now
            self._last_rate_ts = now
        else:
            # Closed volume
            vol_m3 = vf / 1000.0
            dt = None
            if self._last_rate_ts is not None:
                dt = max(0.0, now - self._last_rate_ts)
            for i, v in enumerate(co2_values):
                if v is None:
                    self.rate_mmol_s_ch[i] = None
                    continue
                last = self.last_ppm_ch[i]
                if last is None:
                    self.last_ppm_ch[i] = v
                    self.rate_mmol_s_ch[i] = None
                    continue
                try:
                    # Monotonic: ignore negative deltas
                    dppm = max(0.0, v - last)
                    dmoles = dppm * 1e-6 * vol_m3 * (P/(R*T))
                    d_g = dmoles * 44.01
                    self.cum_co2_g_ch[i] = max(0.0, self.cum_co2_g_ch[i] + d_g)
                    # Net per-channel: subtract blank delta (if available) this tick
                    dppm_b = None
                    try:
                        if blank_ppm is not None and self._last_blank_ppm is not None:
                            dppm_b = max(0.0, blank_ppm - self._last_blank_ppm)
                    except Exception:
                        dppm_b = None
                    # If blank unavailable in blank-based mode, skip net integration for this tick
                    if baseline_mode != 'per_channel' and dppm_b is None:
                        net_dppm = 0.0
                    else:
                        net_dppm = dppm - (dppm_b or 0.0)
                    if net_dppm < 0:
                        net_dppm = 0.0
                    # Apply ppm noise floor to net integration
                    if net_dppm < self.integr_epsilon_ppm:
                        net_dppm = 0.0
                    net_dmoles = net_dppm * 1e-6 * vol_m3 * (P/(R*T))
                    self.cum_net_co2_g_ch[i] = max(0.0, self.cum_net_co2_g_ch[i] + net_dmoles * 44.01)
                    # Instantaneous rate using dppm/dt if dt available
                    if dt and dt > 0:
                        rate_mol_s = (dppm/dt) * 1e-6 * vol_m3 * (P/(R*T))
                        self.rate_mmol_s_ch[i] = rate_mol_s * 1000.0
                    else:
                        self.rate_mmol_s_ch[i] = None
                except Exception:
                    self.rate_mmol_s_ch[i] = None
                    pass
                self.last_ppm_ch[i] = v
            # Common blank tracking for closed volume
            try:
                if baseline_mode != 'per_channel':
                    if blank_ppm is not None:
                        if self._last_blank_ppm is None:
                            self._last_blank_ppm = blank_ppm
                        else:
                            dppm_b = max(0.0, blank_ppm - self._last_blank_ppm)
                            dmoles_b = dppm_b * 1e-6 * vol_m3 * (P/(R*T))
                            d_blank = dmoles_b * 44.01
                            self.cum_blank_g = max(0.0, self.cum_blank_g + d_blank)
                            self._last_blank_ppm = blank_ppm
            except Exception:
                pass
            self._last_rate_ts = now

    def _update_bio_panel(self):
        try:
            if self.thco2_g is not None:
                self.lbl_thco2.configure(text=f"ThCO2: {self.thco2_g:.1f} g")
            else:
                self.lbl_thco2.configure(text="ThCO2: - g")
            # Overall cumulative (based on global integrator)
            try:
                net_g = max(0.0, self.cum_co2_g - self.cum_blank_g)
                net_mmol = (net_g / 44.01) * 1000.0
                self.lbl_cum.configure(text=f"Cumulative CO2 (net): {net_mmol:.1f} mmol")
            except Exception:
                self.lbl_cum.configure(text="Cumulative CO2 (net): - mmol")

            # Per-role cumulative and biodegradability (sample, compare)
            def role_net(role_code):
                try:
                    idxs = [i for i in range(6) if self._get_role_for_index(i) == role_code]
                    vals = [self.cum_net_co2_g_ch[i] for i in idxs if i < len(self.cum_net_co2_g_ch)]
                    if not vals:
                        return None
                    avg_gross = sum(vals) / len(vals)
                    net_g_local = max(0.0, avg_gross)
                    return net_g_local
                except Exception:
                    return None

            net_g_sample = role_net('sample')
            net_g_compare = role_net('compare')
            # Update labels in mmol and %
            if net_g_sample is not None:
                self.lbl_cum_sample.configure(text=f"Sample net: {(net_g_sample/44.01)*1000.0:.1f} mmol")
            else:
                self.lbl_cum_sample.configure(text="Sample net: - mmol")
            if net_g_compare is not None:
                self.lbl_cum_compare.configure(text=f"Compare net: {(net_g_compare/44.01)*1000.0:.1f} mmol")
            else:
                self.lbl_cum_compare.configure(text="Compare net: - mmol")

            # Biodegradability % per role
            th_sample = self.thco2_g if (self.thco2_g and self.thco2_g > 0) else None
            th_compare = self._compute_thco2_for_compare_g()
            if th_sample:
                if net_g_sample is not None:
                    pct_s = max(0.0, min(100.0, (net_g_sample / th_sample) * 100.0))
                    self.lbl_bio_sample.configure(text=f"Sample biodegradability: {pct_s:.1f} %")
                else:
                    self.lbl_bio_sample.configure(text="Sample biodegradability: - %")
            else:
                self.lbl_bio_sample.configure(text="Sample biodegradability: - %")

            if th_compare and th_compare > 0:
                if net_g_compare is not None:
                    pct_c = max(0.0, min(100.0, (net_g_compare / th_compare) * 100.0))
                    self.lbl_bio_compare.configure(text=f"Compare biodegradability: {pct_c:.1f} %")
                else:
                    self.lbl_bio_compare.configure(text="Compare biodegradability: - %")
            else:
                self.lbl_bio_compare.configure(text="Compare biodegradability: - %")

            # Legacy overall % uses sample ThCO2
            if th_sample:
                pct_overall = max(0.0, min(100.0, ((max(0.0, self.cum_co2_g - self.cum_blank_g)) / th_sample) * 100.0))
                self.lbl_bio.configure(text=f"Biodegradability: {pct_overall:.1f} %")
            else:
                self.lbl_bio.configure(text="Biodegradability: - %")
        except Exception:
            pass

    # ---------- Period estimation (diagnostic) ----------
    def _estimate_cycle_period(self, series, times, min_sep_s=80, threshold_ppm=3.0):
        """Estimate cycle period from local minima in a single series.

        - series: list of numeric or None
        - times: list of elapsed seconds, same length as series
        Returns: period seconds (float) or None
        """
        if not series or len(series) < 10:
            return None
        # Collect candidate minima indices (simple three-point test with threshold)
        mins_t = []
        n = len(series)
        for i in range(2, n-2):
            v = series[i]
            if v is None:
                continue
            l = series[i-1]
            r = series[i+1]
            if l is None or r is None:
                continue
            if (v + threshold_ppm) < l and (v + threshold_ppm) < r:
                t = times[i]
                # Enforce minimum separation between successive minima
                if not mins_t or (t - mins_t[-1]) >= min_sep_s:
                    mins_t.append(t)
        if len(mins_t) < 3:
            return None
        # Differences between successive minima
        diffs = [mins_t[i+1] - mins_t[i] for i in range(len(mins_t)-1)]
        if not diffs:
            return None
        diffs.sort()
        mid = len(diffs)//2
        period = diffs[mid] if len(diffs) % 2 == 1 else (diffs[mid-1] + diffs[mid]) / 2.0
        # Clamp plausible range (40s to 2000s)
        if 40.0 <= period <= 2000.0:
            return period
        return None

    # ---------- Auto-tune sampling params ----------
    def _start_auto_tune(self):
        if self.auto_tune_active:
            return
        # Build configs: list of (interval_s, jitter_s)
        self._tune_configs = [
            (1.7, 0.1), (2.0, 0.2), (2.3, 0.3), (2.6, 0.4), (3.0, 0.4)
        ]
        self._tune_idx = -1
        self._tune_elapsed = 0.0
        self._tune_periods = []
        self._tune_best = None
        self.auto_tune_active = True
        try:
            self.auto_btn.configure(text='Auto Tuning...')
        except Exception:
            pass
        # Immediately advance to first config
        self._advance_tune_config()

    def _advance_tune_config(self):
        self._tune_idx += 1
        self._tune_elapsed = 0.0
        self._tune_periods = []
        if self._tune_idx >= len(self._tune_configs):
            return self._finish_auto_tune()
        interval, jitter = self._tune_configs[self._tune_idx]
        self._apply_tune_config(interval, jitter)
        print(f"[AutoTune] Testing interval={interval}s jitter=±{jitter}s")
        if self.debug_raw and self._debug_fh:
            try:
                self._debug_fh.write(f"[AutoTune] Testing interval={interval}s jitter=±{jitter}s\n")
            except Exception:
                pass

    def _apply_tune_config(self, interval, jitter):
        self.interval_s = float(interval)
        self.interval_jitter_s = float(jitter)

    def _auto_tune_step(self, period_estimate, step_dt):
        # Accumulate elapsed time and period samples for current config
        self._tune_elapsed += step_dt
        if period_estimate is not None:
            self._tune_periods.append(period_estimate)
        # Evaluate every ~90s per config
        test_duration = 90.0
        if self._tune_elapsed >= test_duration:
            score = self._score_periods(self._tune_periods)
            cfg = self._tune_configs[self._tune_idx]
            print(f"[AutoTune] Config {cfg} score={score:.1f} from {len(self._tune_periods)} samples")
            if self.debug_raw and self._debug_fh:
                try:
                    self._debug_fh.write(f"[AutoTune] Config {cfg} score={score:.1f} samples={len(self._tune_periods)}\n")
                except Exception:
                    pass
            if (self._tune_best is None) or (score > self._tune_best[0]):
                self._tune_best = (score, cfg)
            self._advance_tune_config()

    def _score_periods(self, periods):
        # Higher score is better. If no period detected -> best.
        if not periods:
            return 1e9
        # Prefer large/variable periods -> take median and invert
        vals = sorted(periods)
        mid = len(vals)//2
        med = vals[mid] if len(vals) % 2 == 1 else (vals[mid-1] + vals[mid]) / 2.0
        return max(1.0, med)

    def _finish_auto_tune(self):
        self.auto_tune_active = False
        best = self._tune_best[1] if self._tune_best else (self.interval_s, self.interval_jitter_s)
        self._apply_tune_config(*best)
        msg = f"[AutoTune] Selected interval={best[0]}s jitter=±{best[1]}s"
        print(msg)
        if self.debug_raw and self._debug_fh:
            try:
                self._debug_fh.write(msg + "\n")
            except Exception:
                pass
        try:
            self.auto_btn.configure(text='Auto Tune')
        except Exception:
            pass

    def _get_offset_for_index(self, i):
        """Return the offset for sensor index i, preferring port-based mapping if available."""
        try:
            port = self.co2_ports_used[i]
        except Exception:
            port = None
        if port and port in self.co2_offsets_by_port:
            try:
                return float(self.co2_offsets_by_port[port])
            except Exception:
                return 0.0
        # Fallback to index-based list
        try:
            return float(self.co2_offsets[i])
        except Exception:
            return 0.0

    def _set_offset_for_index(self, i, value):
        """Set offset for sensor i; writes to port-based map if port known, otherwise index list."""
        try:
            port = self.co2_ports_used[i]
        except Exception:
            port = None
        if port:
            self.co2_offsets_by_port[port] = float(value)
        else:
            # Ensure list is long enough
            while len(self.co2_offsets) < i + 1:
                self.co2_offsets.append(0.0)
            self.co2_offsets[i] = float(value)

    def _get_role_for_index(self, i):
        """Return role code for sensor i: 'blank' | 'sample' | 'compare'."""
        try:
            port = self.co2_ports_used[i]
        except Exception:
            port = None
        if port and port in self.co2_roles_by_port:
            try:
                code = str(self.co2_roles_by_port[port])
                return code if code in ('blank','sample','compare') else 'sample'
            except Exception:
                return 'sample'
        try:
            code = str(self.co2_roles[i])
            return code if code in ('blank','sample','compare') else 'sample'
        except Exception:
            return 'sample'

    def _set_role_for_index(self, i, code):
        """Set role for sensor i and update in-memory structures."""
        if code not in ('blank','sample','compare'):
            return
        try:
            port = self.co2_ports_used[i]
        except Exception:
            port = None
        if port:
            self.co2_roles_by_port[port] = code
        # Ensure list long enough
        while len(self.co2_roles) < i + 1:
            self.co2_roles.append('sample')
        self.co2_roles[i] = code

    def _save_roles_to_config(self):
        """Persist current CO2 roles to ports_config.json."""
        cfg_path = os.path.join(os.path.dirname(__file__), 'ports_config.json')
        try:
            with open(cfg_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
        except Exception:
            data = {}
        # Build by-port roles mapping
        by_port = data.get('co2_roles_by_port', {})
        for i, port in enumerate(self.co2_ports_used):
            if port:
                by_port[port] = self._get_role_for_index(i)
        data['co2_roles_by_port'] = by_port
        # Also keep index-based list
        idx_roles = [self._get_role_for_index(i) for i in range(6)]
        data['co2_roles'] = idx_roles
        try:
            with open(cfg_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            self.config = data
        except Exception as e:
            print(f"Failed to save co2 roles: {e}")

    # ---------- Logging, Stats, and Ports ----------
    def _open_log(self):
        try:
            is_new = not os.path.exists(self.log_path) or os.path.getsize(self.log_path) == 0
        except Exception:
            is_new = True
        try:
            self.log_file = open(self.log_path, 'a', newline='', encoding='utf-8')
            self.log_writer = csv.writer(self.log_file)
            if is_new:
                sel_co2 = getattr(self, 'selected_co2_for_log', list(range(6)))
                sel_th = getattr(self, 'selected_th_for_log', list(range(2)))
                header = ['timestamp', 'elapsed_s']
                header += [f'co2_{i+1}' for i in sel_co2]
                # Per-channel cumulative (net of blank) in grams and mmol
                header += [f'co2_{i+1}_cum_g' for i in sel_co2]
                header += [f'co2_{i+1}_cum_mmol' for i in sel_co2]
                # Per-channel instantaneous rate (mmol/s)
                header += [f'co2_{i+1}_rate_mmol_s' for i in sel_co2]
                header += [f'temp_{j+1}' for j in sel_th]
                header += [f'hum_{j+1}' for j in sel_th]
                self.log_writer.writerow(header)
                self.log_file.flush()
        except Exception as e:
            print(f"Failed to open log file {self.log_path}: {e}")
            self.log_file = None
            self.log_writer = None

    def _close_log(self):
        try:
            if self.log_file:
                self.log_file.flush()
                self.log_file.close()
        finally:
            self.log_file = None
            self.log_writer = None

    def _log_row(self, ts, elapsed, co2_values, temp_values, hum_values):
        if not self.log_writer:
            return
        row = [ts, f"{elapsed:.3f}"]
        def fmt(v):
            return '' if v is None else v
        sel_co2 = getattr(self, 'selected_co2_for_log', list(range(len(co2_values))))
        sel_th = getattr(self, 'selected_th_for_log', list(range(len(temp_values))))
        # Instantaneous CO2 ppm
        row += [fmt(co2_values[i]) for i in sel_co2 if i < len(co2_values)]
        # Per-channel cumulative net grams and mmol
        try:
            grams = []
            mmol = []
            rate = []
            for i in sel_co2:
                if i < len(self.cum_net_co2_g_ch):
                    g = max(0.0, (self.cum_net_co2_g_ch[i] or 0.0))
                else:
                    g = None
                grams.append('' if g is None else round(g, 4))
                if g is None:
                    mmol.append('')
                else:
                    mmol.append(round((g / 44.01) * 1000.0, 3))  # g -> mmol
                # Instantaneous rate (mmol/s)
                try:
                    r = self.rate_mmol_s_ch[i]
                    rate.append('' if r is None else round(float(r), 3))
                except Exception:
                    rate.append('')
            row += grams
            row += mmol
            row += rate
        except Exception:
            # Ensure column alignment even on error
            row += ['' for _ in sel_co2]
            row += ['' for _ in sel_co2]
            row += ['' for _ in sel_co2]
        row += [fmt(temp_values[j]) for j in sel_th if j < len(temp_values)]
        row += [fmt(hum_values[j]) for j in sel_th if j < len(hum_values)]
        try:
            self.log_writer.writerow(row)
            self.log_file.flush()
        except Exception as e:
            print(f"Failed writing log row: {e}")

    def _update_stats(self):
        # Helper to compute current/min/max/avg ignoring None
        def stats(series):
            curr = series[-1] if series else None
            vals = [v for v in series if v is not None]
            if vals:
                mn = min(vals)
                mx = max(vals)
                avg = sum(vals)/len(vals)
            else:
                mn = mx = avg = None
            return curr, mn, mx, avg
        def fmt(v, prec=1):
            return '-' if v is None else (f"{v:.{prec}f}" if isinstance(v, (int, float)) else str(v))

        for i in range(6):
            c, mn, mx, a = stats(self.co2_series[i])
            # Per-channel cumulative net in mmol (subtract common blank cumulative)
            try:
                cum_net_g = max(0.0, (self.cum_net_co2_g_ch[i] or 0.0))
                cum_net = (cum_net_g / 44.01) * 1000.0
            except Exception:
                cum_net = None
            values = (fmt(c, 0), fmt(mn, 0), fmt(mx, 0), fmt(a, 1), fmt(cum_net, 1))
            for lbl, val in zip(self.stat_labels['co2'][i], values):
                lbl.configure(text=val)
        for j in range(2):
            c, mn, mx, a = stats(self.temp_series[j])
            values = (fmt(c, 1), fmt(mn, 1), fmt(mx, 1), fmt(a, 1), '-')
            for lbl, val in zip(self.stat_labels['temp'][j], values):
                lbl.configure(text=val)
        for j in range(2):
            c, mn, mx, a = stats(self.hum_series[j])
            values = (fmt(c, 1), fmt(mn, 1), fmt(mx, 1), fmt(a, 1), '-')
            for lbl, val in zip(self.stat_labels['hum'][j], values):
                lbl.configure(text=val)

    def _discover_ports(self):
        """Classify ports into CO2 and Temp/Humidity using USB identifiers.

        Priority rules (cross‑platform):
          1) VID/PID if available
             - Silicon Labs CP210x (VID 0x10C4) -> CO2
             - WCH/QinHeng CH340 (VID 0x1A86) -> TH
          2) Device path and descriptor heuristics
             - macOS: cu.SLAB* -> CO2, cu.wch* -> TH
             - Strings containing cp210/silicon labs/slab -> CO2
             - Strings containing wch/ch340 -> TH
          3) Exclude: usbserial*, Bluetooth*, debug*
          4) Fallback: unknown -> CO2 up to 6, then TH up to 2
        """
        ports = list(list_ports.comports())
        co2: list[str] = []
        th: list[str] = []
        unknown: list[str] = []

        for p in ports:
            dev = p.device
            blob = ' '.join(str(x or '') for x in (
                p.description, getattr(p, 'manufacturer', None), getattr(p, 'product', None)
            )).lower() + ' ' + dev.lower()
            vid = getattr(p, 'vid', None)
            pid = getattr(p, 'pid', None)
            # Explicitly ignore unwanted macOS virtual/debug ports
            if (
                'cu.usbserial' in dev.lower()
                or 'cu.bluetooth' in dev.lower()
                or 'bluetooth' in blob
                or 'debug' in dev.lower()
                or 'debug' in blob
            ):
                continue
            # 1) VID/PID routing (when available)
            if isinstance(vid, int):
                if vid == 0x10C4:  # Silicon Labs CP210x
                    co2.append(dev)
                    continue
                if vid == 0x1A86:  # WCH/QinHeng CH340
                    th.append(dev)
                    continue
            # 2) Path/descriptor routing
            if 'cu.slab' in blob:
                co2.append(dev)
            elif 'cu.wch' in blob:
                th.append(dev)
            # Vendor/product based routing
            elif ('cp210' in blob) or ('silicon labs' in blob) or ('cp2102' in blob) or ('slab' in blob):
                co2.append(dev)
            elif ('wch' in blob) or ('ch340' in blob) or ('wchusb' in blob):
                th.append(dev)
            else:
                unknown.append(dev)

        # Fallback: put remaining unknowns into CO2 first, then TH (exclude usbserial/bluetooth/debug)
        for d in unknown:
            dl = d.lower()
            if ('cu.usbserial' in dl) or ('cu.bluetooth' in dl) or ('debug' in dl):
                continue
            if len(co2) < 6:
                co2.append(d)
            elif len(th) < 2:
                th.append(d)

        # Deduplicate, filter out cu.usbserial*/cu.Bluetooth*/debug*, sort and cap
        co2 = [d for d in dict.fromkeys(co2) if ('cu.usbserial' not in d.lower() and 'cu.bluetooth' not in d.lower() and 'debug' not in d.lower())]
        th = [d for d in dict.fromkeys(th) if ('cu.usbserial' not in d.lower() and 'cu.bluetooth' not in d.lower() and 'debug' not in d.lower())]
        co2 = self._sort_ports(co2)[:6]
        th = self._sort_ports(th)[:2]
        return co2, th

    def _setup_ports(self):
        self.co2_serials = [None] * 6
        self.th_serials = [None] * 2
        # Track ports used
        if not hasattr(self, 'co2_ports_used'):
            self.co2_ports_used = [None]*6
        if not hasattr(self, 'th_ports_used'):
            self.th_ports_used = [None]*2

        # Merge config with discovery
        cfg_co2 = self.config.get('co2_ports') or []
        cfg_thr_list = self.config.get('thr_ports') or []
        cfg_thr_single = self.config.get('thr_port')

        disc_co2, disc_thr = self._discover_ports()

        # Use config if provided, else discovery
        co2_ports = cfg_co2 if cfg_co2 else disc_co2
        # Ensure uniqueness and limit to 6
        co2_ports = list(dict.fromkeys(co2_ports))[:6]

        used = set(co2_ports)

        if cfg_thr_list:
            th_ports = cfg_thr_list
        elif cfg_thr_single:
            th_ports = [cfg_thr_single]
        else:
            # Prefer discovered TH, not overlapping with CO2
            th_ports = [p for p in disc_thr if p not in used]
        th_ports = list(dict.fromkeys(th_ports))[:2]

        # Persist auto-assigned ports back to config so future runs keep the mapping
        self._save_ports_to_config(co2_ports, th_ports)

        # Open ports
        for i in range(min(6, len(co2_ports))):
            port = co2_ports[i]
            try:
                self.co2_serials[i] = serial.Serial(port, baudrate=self.baud, timeout=1)
                print(f"CO2 {i+1} -> {port}")
            except Exception as e:
                print(f"CO2 sensor {i+1} open failed on {port}: {e}")
                self.co2_serials[i] = None
            finally:
                # Record the intended port mapping regardless of open success
                self.co2_ports_used[i] = port
                self._set_co2_status(i, 'OK' if self.co2_serials[i] else 'ERR')

        for j in range(min(2, len(th_ports))):
            port = th_ports[j]
            try:
                self.th_serials[j] = serial.Serial(port, baudrate=self.thr_baud, timeout=1)
                print(f"TH {j+1} -> {port}")
            except Exception as e:
                print(f"Temp/Humidity sensor {j+1} open failed on {port}: {e}")
                self.th_serials[j] = None
            finally:
                self.th_ports_used[j] = port
                self._set_th_status(j, 'OK' if self.th_serials[j] else 'ERR')

        # After opening, refresh sensor checkbox labels and responsive list if built
        try:
            self._refresh_sensor_checkbox_labels()
            self._update_responsive_ports_view()
        except Exception:
            pass

    def _refresh_sensor_checkbox_labels(self):
        """Update sensor checkbox text to include the mapped port names."""
        # CO2
        if hasattr(self, 'co2_checkbuttons'):
            for i, cb in enumerate(self.co2_checkbuttons):
                port_txt = None
                try:
                    port_txt = self.co2_ports_used[i]
                except Exception:
                    pass
                label_text = f"CO2 Sensor {i+1}" + (f" ({port_txt})" if port_txt else "")
                try:
                    cb.configure(text=label_text)
                except Exception:
                    pass
        # TH
        if hasattr(self, 'th_checkbuttons'):
            for j, cb in enumerate(self.th_checkbuttons):
                port_txt = None
                try:
                    port_txt = self.th_ports_used[j]
                except Exception:
                    pass
                label_text = f"Temp/Humidity Sensor {j+1}" + (f" ({port_txt})" if port_txt else "")
                try:
                    cb.configure(text=label_text)
                except Exception:
                    pass

    # ---------- Watchdog helpers ----------
    def _reopen_co2(self, i):
        try:
            if self.co2_serials[i]:
                try: self.co2_serials[i].close()
                except Exception: pass
            port = self.co2_ports_used[i]
            if not port:
                return
            self.co2_serials[i] = serial.Serial(port, baudrate=self.baud, timeout=1)
            self.co2_reopen_counts[i] += 1
            self._set_co2_status(i, 'OK')
            print(f"[WD] Reopened CO2 {i+1} on {port}")
        except Exception as e:
            self._set_co2_status(i, 'ERR')
            print(f"[WD] CO2 {i+1} reopen failed: {e}")

    def _reopen_th(self, j):
        try:
            if self.th_serials[j]:
                try: self.th_serials[j].close()
                except Exception: pass
            port = self.th_ports_used[j]
            if not port:
                return
            self.th_serials[j] = serial.Serial(port, baudrate=self.thr_baud, timeout=1)
            self.th_reopen_counts[j] += 1
            self._set_th_status(j, 'OK')
            print(f"[WD] Reopened TH {j+1} on {port}")
        except Exception as e:
            self._set_th_status(j, 'ERR')
            print(f"[WD] TH {j+1} reopen failed: {e}")

    # ---------- Warning popup helpers ----------
    def _show_warn_popup(self, message: str):
        try:
            if self._warn_popup is None or not self._warn_popup.winfo_exists():
                self._warn_popup = tk.Toplevel(self.root)
                self._warn_popup.title("Warning")
                self._warn_popup.attributes('-topmost', True)
                try:
                    self._warn_popup.overrideredirect(True)
                except Exception:
                    pass
                self._warn_label = tk.Label(self._warn_popup, text=message, font=("Helvetica", 12, 'bold'))
                self._warn_label.pack(padx=12, pady=8)
                # Position near top-center of the main window
                try:
                    self._warn_popup.update_idletasks()
                    x = self.root.winfo_rootx() + max(10, (self.root.winfo_width() - self._warn_popup.winfo_width()) // 2)
                    y = self.root.winfo_rooty() + 40
                    self._warn_popup.geometry(f"+{x}+{y}")
                except Exception:
                    pass
            else:
                try:
                    self._warn_label.configure(text=message)
                except Exception:
                    pass
            # Start blinking
            if not self._warn_blink_job:
                self._warn_blink_on = False
                self._blink_warn()
        except Exception:
            pass

    def _hide_warn_popup(self):
        try:
            if self._warn_blink_job:
                try:
                    self.root.after_cancel(self._warn_blink_job)
                except Exception:
                    pass
                self._warn_blink_job = None
            if self._warn_popup is not None and self._warn_popup.winfo_exists():
                try:
                    self._warn_popup.destroy()
                except Exception:
                    pass
            self._warn_popup = None
        except Exception:
            pass

    def _blink_warn(self):
        try:
            if self._warn_popup is None or (not self._warn_popup.winfo_exists()):
                self._warn_blink_job = None
                return
            self._warn_blink_on = not self._warn_blink_on
            fg = '#D32F2F' if self._warn_blink_on else '#F57C00'
            bg = '#FFF3E0' if self._warn_blink_on else '#FFE0B2'
            try:
                self._warn_label.configure(fg=fg, bg=bg)
                self._warn_popup.configure(bg=bg)
            except Exception:
                pass
            self._warn_blink_job = self.root.after(600, self._blink_warn)
        except Exception:
            self._warn_blink_job = None

    # ---------- Auto-recovery loop ----------
    def _start_recovery_loop(self):
        if self._recovery_loop_active:
            return
        self._recovery_loop_active = True
        try:
            self.root.after(int(self.recovery_interval_s * 1000), self._recovery_tick)
        except Exception:
            self._recovery_loop_active = False

    def _recovery_tick(self):
        """Periodically try to reconnect missing/broken sensors and rediscover ports if needed."""
        try:
            # Current port inventory
            try:
                ports = list(list_ports.comports())
                present = {p.device for p in ports}
            except Exception:
                present = set()

            # Recover CO2
            for i in range(6):
                try:
                    ok = self.co2_serials[i] is not None
                except Exception:
                    ok = False
                if ok:
                    continue
                # Prefer reopening on the same path if it exists again
                port = None
                try:
                    port = self.co2_ports_used[i]
                except Exception:
                    port = None
                reopened = False
                if port and (not present or port in present):
                    try:
                        self.co2_serials[i] = serial.Serial(port, baudrate=self.baud, timeout=1)
                        self._set_co2_status(i, 'OK')
                        self.co2_reopen_counts[i] += 1
                        print(f"[RCV] Reconnected CO2 {i+1} on {port}")
                        reopened = True
                    except Exception:
                        self.co2_serials[i] = None
                if reopened:
                    continue
                # Try rediscovering and assign a new free CO2 port
                try:
                    disc_co2, _ = self._discover_ports()
                    # Exclude ports already used by other indices
                    used = set(p for idx, p in enumerate(self.co2_ports_used) if idx != i and p)
                    candidates = [p for p in disc_co2 if p not in used]
                    if candidates:
                        port = candidates[0]
                        try:
                            self.co2_serials[i] = serial.Serial(port, baudrate=self.baud, timeout=1)
                            self.co2_ports_used[i] = port
                            self._set_co2_status(i, 'OK')
                            self.co2_reopen_counts[i] += 1
                            print(f"[RCV] Discovered & opened CO2 {i+1} on {port}")
                        except Exception as e:
                            print(f"[RCV] CO2 {i+1} open failed on {port}: {e}")
                            self.co2_serials[i] = None
                except Exception:
                    pass

            # Recover TH
            for j in range(2):
                try:
                    ok = self.th_serials[j] is not None
                except Exception:
                    ok = False
                if ok:
                    continue
                port = None
                try:
                    port = self.th_ports_used[j]
                except Exception:
                    port = None
                reopened = False
                if port and (not present or port in present):
                    try:
                        self.th_serials[j] = serial.Serial(port, baudrate=self.thr_baud, timeout=1)
                        self._set_th_status(j, 'OK')
                        self.th_reopen_counts[j] += 1
                        print(f"[RCV] Reconnected TH {j+1} on {port}")
                        reopened = True
                    except Exception:
                        self.th_serials[j] = None
                if reopened:
                    continue
                # Rediscover for TH ports
                try:
                    _, disc_th = self._discover_ports()
                    used = set(p for idx, p in enumerate(self.th_ports_used) if idx != j and p)
                    candidates = [p for p in disc_th if p not in used]
                    if candidates:
                        port = candidates[0]
                        try:
                            self.th_serials[j] = serial.Serial(port, baudrate=self.thr_baud, timeout=1)
                            self.th_ports_used[j] = port
                            self._set_th_status(j, 'OK')
                            self.th_reopen_counts[j] += 1
                            print(f"[RCV] Discovered & opened TH {j+1} on {port}")
                        except Exception as e:
                            print(f"[RCV] TH {j+1} open failed on {port}: {e}")
                            self.th_serials[j] = None
                except Exception:
                    pass
        finally:
            # Reschedule
            try:
                self.root.after(int(self.recovery_interval_s * 1000), self._recovery_tick)
            except Exception:
                self._recovery_loop_active = False

    def _set_co2_status(self, i, text):
        try:
            if hasattr(self, 'co2_status_labels') and i < len(self.co2_status_labels):
                self.co2_status_labels[i].configure(text=text)
        except Exception:
            pass

    def _set_th_status(self, j, text):
        try:
            if hasattr(self, 'th_status_labels') and j < len(self.th_status_labels):
                self.th_status_labels[j].configure(text=text)
        except Exception:
            pass

    def _apply_interval(self):
        """Apply interval from UI and persist to ports_config.json."""
        try:
            val = float(self.var_interval.get())
        except Exception:
            print("Invalid interval value")
            return
        val = max(0.2, min(60.0, val))
        self.interval_s = val
        # Persist to config
        cfg_path = os.path.join(os.path.dirname(__file__), 'ports_config.json')
        try:
            data = dict(self.config) if isinstance(self.config, dict) else {}
            data['interval'] = self.interval_s
            with open(cfg_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            self.config = data
            print(f"Interval set to {self.interval_s}s (saved)")
        except Exception as e:
            print(f"Failed to save interval: {e}")

    def _select_scanned_ports_default(self):
        """Select only discovered ports by default (CO2 and TH)."""
        try:
            self._ensure_discovered_ports()
            # CO2
            for i in range(min(6, len(self.co2_sensor_vars))):
                has_port = bool(self.co2_ports_used[i]) if hasattr(self, 'co2_ports_used') else False
                self.co2_sensor_vars[i].set(has_port)
            # TH
            for j in range(min(2, len(self.th_sensor_vars))):
                has_port = bool(self.th_ports_used[j]) if hasattr(self, 'th_ports_used') else False
                self.th_sensor_vars[j].set(has_port)
        except Exception:
            pass

    def _ensure_discovered_ports(self):
        """Ensure co2_ports_used/th_ports_used are populated from discovery if empty."""
        try:
            need_co2 = (not hasattr(self, 'co2_ports_used')) or all(p is None for p in self.co2_ports_used)
            need_th = (not hasattr(self, 'th_ports_used')) or all(p is None for p in self.th_ports_used)
            if need_co2 or need_th:
                co2, th = self._discover_ports()
                if need_co2:
                    self.co2_ports_used = (co2 + [None]*6)[:6]
                if need_th:
                    self.th_ports_used = (th + [None]*2)[:2]
        except Exception:
            pass

    def _save_ports_to_config(self, co2_ports: list, th_ports: list):
        """Save discovered/selected ports into ports_config.json if changed."""
        cfg_path = os.path.join(os.path.dirname(__file__), 'ports_config.json')
        try:
            with open(cfg_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
        except Exception:
            data = {}
        changed = False
        if data.get('co2_ports') != co2_ports:
            data['co2_ports'] = co2_ports
            changed = True
        # Store TH as list for up to 2 sensors
        if th_ports:
            if data.get('thr_ports') != th_ports:
                data['thr_ports'] = th_ports
                # Remove legacy single if conflicting
                if data.get('thr_port') and data['thr_port'] not in th_ports:
                    data.pop('thr_port', None)
                changed = True
        else:
            # No TH discovered; clear list but keep existing single if any
            if 'thr_ports' in data:
                data.pop('thr_ports', None)
                changed = True
        if changed:
            try:
                with open(cfg_path, 'w', encoding='utf-8') as f:
                    json.dump(data, f, ensure_ascii=False, indent=2)
                # Update in-memory copy
                self.config = data
                print(f"Saved port mapping to {cfg_path}")
            except Exception as e:
                print(f"Failed to save port mapping: {e}")

    def _pre_scan_and_save_ports(self):
        """Run an initial scan to classify ports and immediately persist to config.

        This ensures ports_config.json reflects the latest mapping right as the
        application starts, following the routing rules (cu.SLAB* -> CO2, cu.wch* -> TH,
        CP210x -> CO2, WCH/CH340 -> TH).
        """
        co2_raw, th_raw = self._discover_ports()
        # Probe candidates; fall back to raw lists on errors
        try:
            co2, th = self._probe_ports(co2_raw, th_raw)
        except Exception:
            co2, th = co2_raw, th_raw
        self._save_ports_to_config(co2, th)
        # Update in-memory mapping used for color/offset mapping
        try:
            self.co2_ports_used = (co2 + [None]*6)[:6]
            self.th_ports_used = (th + [None]*2)[:2]
        except Exception:
            pass

    def _clear_offsets_on_start(self):
        """Clear all saved CO2 offsets so app starts with zero offsets.

        This resets both per-port (co2_offsets_by_port) and index-based (co2_offsets)
        values in memory and persists the change to ports_config.json.
        """
        cfg_path = os.path.join(os.path.dirname(__file__), 'ports_config.json')
        try:
            with open(cfg_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
        except Exception:
            data = {}
        # Reset in-memory structures
        self.co2_offsets_by_port = {}
        self.co2_offsets = [0.0] * 6
        data['co2_offsets_by_port'] = {}
        data['co2_offsets'] = [0.0, 0.0, 0.0, 0.0, 0.0, 0.0]
        try:
            with open(cfg_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            self.config = data
            print("Cleared CO2 offsets at startup.")
        except Exception as e:
            print(f"Failed to write cleared offsets: {e}")

    def _update_setup_ports_view(self):
        """Refresh the detected ports labels on the setup page."""
        co2 = self.config.get('co2_ports', []) or []
        th = self.config.get('thr_ports', []) or ([] if not self.config.get('thr_port') else [self.config.get('thr_port')])
        co2_text = ', '.join(co2) if co2 else 'None'
        th_text = ', '.join(th) if th else 'None'
        self.lbl_ports_co2.configure(text=co2_text)
        self.lbl_ports_th.configure(text=th_text)

    def _setup_rescan_ports(self):
        """Handler for setup page 'Rescan Ports' button."""
        try:
            self._pre_scan_and_save_ports()
            self._update_setup_ports_view()
        except Exception as e:
            print(f"Setup rescan failed: {e}")

    def _sort_ports(self, ports_list):
        """Sort ports in a human-friendly numeric order by suffix if present."""
        def keyfn(dev):
            s = dev
            # Extract trailing number for numeric sort
            m = re.search(r"(.*?)(\d+)$", s)
            if m:
                prefix, num = m.group(1), int(m.group(2))
                return (prefix, num)
            return (s, -1)
        return sorted(ports_list, key=keyfn)

    # Rescan functionality is intentionally not provided on the main page.

    def _load_config(self):
        cfg_path = os.path.join(os.path.dirname(__file__), 'ports_config.json')
        try:
            with open(cfg_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            print(f"Config load failed ({cfg_path}): {e}. Using defaults.")
            return {}

if __name__ == "__main__":
    root = tk.Tk()
    app = CO2LoggerApp(root)
    root.mainloop()
