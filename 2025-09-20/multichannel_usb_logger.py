# multichannel_usb_logger.py
# Modified to read data from DHT-22 and MH-Z19C sensors via USB.

import time
import json
import re
import os
import csv
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
from matplotlib.figure import Figure
from matplotlib.lines import Line2D
import serial  # For MH-Z19C and DHT-22 sensors
from serial.tools import list_ports

class CO2LoggerApp:
    def __init__(self, root):
        self.root = root
        self.root.title("CO2 Logger with Temp/Humidity")
        self.running = Event()
        self.config = self._load_config()
        self.interval_s = float(self.config.get('interval', 1.0))
        self.baud = int(self.config.get('baud', 9600))
        self.thr_baud = int(self.config.get('thr_baud', 9600))
        thr_pattern = self.config.get('thr_pattern')
        self.thr_regex = re.compile(thr_pattern) if thr_pattern else None
        self.log_path = self.config.get('outfile') or 'usb_multi_log.csv'
        self.log_file = None
        self.log_writer = None

        # Pre-scan and persist port mapping at startup per rules
        try:
            self._pre_scan_and_save_ports()
        except Exception as e:
            print(f"Pre-scan ports failed: {e}")

        # CO2 calibration: per-sensor offsets (ppm) and optional smoothing
        raw_offsets = self.config.get('co2_offsets') or []
        # Mapping by serial port path takes precedence if available
        self.co2_offsets_by_port = self.config.get('co2_offsets_by_port') or {}
        # Also keep index-based list for backward compatibility and when port unknown
        self.co2_offsets = [(float(raw_offsets[i]) if i < len(raw_offsets) else 0.0) for i in range(6)]
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

        # Layout containers (defer packing until setup completed)
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

        # Note: Rescan functionality is available on the setup page only.

        # Right-side sensor selection panel (inside content area)
        sensor_panel = ttk.LabelFrame(self.content_frame, text="Sensors")
        sensor_panel.pack(side=tk.RIGHT, fill=tk.Y, padx=10, pady=10)

        # Sensor checkboxes: CO2 1..6 (default: only 1 & 2 ON), Temp/Humidity 1..2 (default OFF)
        self.co2_sensor_vars = []
        for i in range(6):
            self.co2_sensor_vars.append(tk.BooleanVar(value=(i < 2)))
        self.th_sensor_vars = [tk.BooleanVar(value=False) for _ in range(2)]

        # Add CO2 sensor checkboxes with color swatch
        for i, var in enumerate(self.co2_sensor_vars, start=1):
            row = tk.Frame(sensor_panel)
            row.pack(fill=tk.X, anchor='w')
            cb = ttk.Checkbutton(row, text=f"CO2 Sensor {i}", variable=var)
            cb.pack(side=tk.LEFT)
            # Color swatch for CO2 i
            sw = tk.Canvas(row, width=18, height=12, highlightthickness=1, highlightbackground='#cccccc', bd=0)
            sw.pack(side=tk.LEFT, padx=(6, 0))
            color = self.co2_colors[(i-1) % len(self.co2_colors)]
            sw.create_rectangle(1, 1, 17, 11, outline=color, fill=color)

        # Add Temp/Humidity sensor checkboxes with two color swatches (Temp, Hum)
        for i, var in enumerate(self.th_sensor_vars, start=1):
            row = tk.Frame(sensor_panel)
            row.pack(fill=tk.X, anchor='w')
            cb = ttk.Checkbutton(row, text=f"Temp/Humidity Sensor {i}", variable=var)
            cb.pack(side=tk.LEFT)
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

        # Build stats labels for current/avg/max per sensor
        self.stat_labels = {
            'co2': [],  # list of (curr, avg, max)
            'temp': [],
            'hum': [],
        }
        def add_stat_row(parent, label):
            row = tk.Frame(parent)
            row.pack(fill=tk.X, pady=1)
            ttk.Label(row, text=label, width=14).pack(side=tk.LEFT)
            curr = ttk.Label(row, text="-", width=8)
            avg = ttk.Label(row, text="-", width=8)
            mx = ttk.Label(row, text="-")
            curr.pack(side=tk.LEFT)
            avg.pack(side=tk.LEFT)
            mx.pack(side=tk.LEFT)
            return curr, avg, mx

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
        self.lbl_cum = ttk.Label(bio, text="Cumulative CO2: - g")
        self.lbl_cum.pack(anchor='w')
        self.lbl_bio = ttk.Label(bio, text="Biodegradability: - %")
        self.lbl_bio.pack(anchor='w')

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

        # Biodegradation state
        self.last_ppm = None
        self.last_ppm_ts = None
        self.cum_co2_g = 0.0        # sample cumulative
        self.cum_blank_g = 0.0      # blank cumulative
        self.thco2_g = None
        self.pct_series = []

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

        # Composition inputs (% by mass)
        comp_frame = tk.Frame(self.setup_frame)
        comp_frame.pack(fill=tk.X, pady=(4, 6))
        ttk.Label(comp_frame, text="Composition (%):", width=18).pack(side=tk.LEFT)
        # Defaults may be overwritten by saved config
        self.var_pla = tk.DoubleVar(value=70.0)
        self.var_pbat = tk.DoubleVar(value=20.0)
        self.var_inorg = tk.DoubleVar(value=5.0)
        self.var_biochar = tk.DoubleVar(value=5.0)
        def add_entry(parent, label, var):
            row = tk.Frame(parent)
            row.pack(fill=tk.X, pady=1)
            ttk.Label(row, text=label, width=12).pack(side=tk.LEFT)
            ent = ttk.Entry(row, textvariable=var, width=10)
            ent.pack(side=tk.LEFT)
        add_entry(self.setup_frame, "PLA", self.var_pla)
        add_entry(self.setup_frame, "PBAT", self.var_pbat)
        add_entry(self.setup_frame, "Inorganic", self.var_inorg)
        add_entry(self.setup_frame, "Biochar", self.var_biochar)

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

        # Carbon fractions (editable, defaults)
        cf = ttk.LabelFrame(self.setup_frame, text="Carbon fraction (mass % of component)")
        cf.pack(fill=tk.X, pady=(8, 6))
        self.var_c_pla = tk.DoubleVar(value=50.0)
        self.var_c_pbat = tk.DoubleVar(value=62.0)
        self.var_c_biochar = tk.DoubleVar(value=80.0)
        add_entry(cf, "PLA C%", self.var_c_pla)
        add_entry(cf, "PBAT C%", self.var_c_pbat)
        add_entry(cf, "Biochar C%", self.var_c_biochar)
        ttk.Label(cf, text="Note: Biochar is excluded from ThCO2 by default.").pack(anchor='w', padx=2)

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
            self.pct_series = []
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

        interval = int(self.interval_s * 1000)  # milliseconds

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
                    value = self._read_co2(self.co2_serials[i])
                    # Apply per-sensor offset and optional smoothing
                    if value is not None:
                        value = value + self._get_offset_for_index(i)
                        if self.co2_ema_alpha > 0:
                            prev = self._co2_ema_state[i]
                            a = self.co2_ema_alpha
                            value = (a * value) + ((1 - a) * prev) if prev is not None else value
                            self._co2_ema_state[i] = value
            except Exception as e:
                print(f"CO2 {i+1} read error: {e}")
            finally:
                self.co2_series[i].append(value)
            co2_values.append(value)

        temp_values = []
        hum_values = []
        for j in range(2):
            t = h = None
            try:
                if self.th_sensor_vars[j].get() and self.th_serials[j]:
                    t, h = self._read_temp_hum(self.th_serials[j])
            except Exception as e:
                print(f"Temp/Humidity {j+1} read error: {e}")
            finally:
                self.temp_series[j].append(t)
                self.hum_series[j].append(h)
            temp_values.append(t)
            hum_values.append(h)

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

        # Update biodegradability estimate using average of selected CO2 sensors
        try:
            sel = [v for i, v in enumerate(co2_values) if self.co2_sensor_vars[i].get() and v is not None]
            avg_ppm = sum(sel) / len(sel) if sel else None
            # Blank subtraction if configured
            blank_ppm = None
            src = getattr(self, 'test_setup', {}).get('blank_source', 'none')
            if isinstance(src, str) and src.startswith('co2_'):
                try:
                    idx = int(src.split('_')[1]) - 1
                    if 0 <= idx < len(co2_values):
                        blank_ppm = co2_values[idx]
                except Exception:
                    blank_ppm = None
            self._update_biodeg_with_ppm(avg_ppm, blank_ppm)
        except Exception:
            pass

        # Update the plot
        self.update_plot()

        # Schedule the next data generation
        self.root.after(interval, self.run_logger)

    def _read_co2(self, ser):
        """Read CO2 data from an MH-Z19C sensor on a given serial port."""
        ser.write(b'\xFF\x01\x86\x00\x00\x00\x00\x00\x79')
        response = ser.read(9)
        if len(response) == 9 and response[0] == 0xFF and response[1] == 0x86:
            high_byte = response[2]
            low_byte = response[3]
            return (high_byte << 8) | low_byte
        return None

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

        # Plot CO2 series with distinct styles
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

        # Adjust layout to make space for right-side Temp/Hum legend
        try:
            self.figure.subplots_adjust(right=0.82, bottom=0.22)
        except Exception:
            pass

        # Add legends: CO2 inside (upper-left), Temp/Hum outside on the right
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
        legend = self.ax3.legend(
            th_handles,
            labels,
            loc='upper left',
            bbox_to_anchor=(1.02, 1.0),
            borderaxespad=0.0,
            frameon=False,
        )
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

        # Draw biodegradability % subplot at the bottom
        try:
            if not hasattr(self, 'ax_pct') or self.ax_pct is None:
                # [left, bottom, width, height]
                self.ax_pct = self.figure.add_axes([0.12, 0.08, 0.66, 0.10], sharex=self.ax)
            self.ax_pct.clear()
            if self.xs and self.pct_series:
                self.ax_pct.plot(self.xs, self.pct_series, color='#555555', label='Biodeg %')
                self.ax_pct.set_ylabel('생분해도 (%)')
                self.ax_pct.set_ylim(0, 100)
                self.ax_pct.grid(True, axis='y', linestyle=':', alpha=0.5)
                if self.ax_pct.lines:
                    self.ax_pct.legend(loc='upper left')
            self.ax_pct.set_xlabel('Elapsed (s)')
        except Exception:
            pass

        self.canvas.draw()

    def align_sensors(self, window=30):
        """Align selected CO2 sensors by adjusting per-sensor offsets using recent data.

        window: number of recent non-None samples to average for each sensor.
        """
        # Determine which sensors are active/selected
        active = [i for i in range(6) if self.co2_sensor_vars[i].get()]
        if len(active) < 2:
            print("Align skipped: need at least two selected CO2 sensors")
            return
        avgs = {}
        for i in active:
            series = self.co2_series[i]
            vals = [v for v in series if v is not None][-window:]
            if len(vals) < 3:
                print(f"Align note: CO2 {i+1} has too few samples ({len(vals)})")
            if not vals:
                continue
            avgs[i] = sum(vals) / len(vals)
        if len(avgs) < 2:
            print("Align skipped: not enough valid data to compute averages")
            return
        # Target: mean of sensor means
        target = sum(avgs.values()) / len(avgs)
        print(f"Align target (ppm): {target:.1f}")
        # Compute and apply deltas; update offsets and existing series for continuity
        for i, avg in avgs.items():
            delta = target - avg
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
            print(f"CO2 {i+1}: avg={avg:.1f} -> delta={delta:.1f}, new_offset={self._get_offset_for_index(i):.1f}")
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
        """Validate test setup inputs, store them, and reveal the main UI."""
        try:
            pla = float(self.var_pla.get()); pbat = float(self.var_pbat.get()); inorg = float(self.var_inorg.get()); bio = float(self.var_biochar.get())
            total_pct = pla + pbat + inorg + bio
            if abs(total_pct - 100.0) > 1e-6:
                mb.showerror("Invalid composition", f"Percentages must sum to 100 (now {total_pct:.2f}).")
                return
            mass = float(self.var_mass.get())
            if mass <= 0:
                mb.showerror("Invalid mass", "Sample mass must be > 0 g.")
                return
        except Exception:
            mb.showerror("Invalid input", "Please enter valid numeric values.")
            return
        # Store setup for later biodegradability calculations
        self.test_setup = {
            'composition': {'PLA': pla, 'PBAT': pbat, 'Inorganic': inorg, 'Biochar': bio},
            'mass_g': mass,
            'env': self.var_env.get(),
            'carbon_fraction': {'PLA': float(self.var_c_pla.get()), 'PBAT': float(self.var_c_pbat.get()), 'Biochar': float(self.var_c_biochar.get())},
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
        self.content_frame.pack(side=tk.TOP, fill=tk.BOTH, expand=True)
        self.controls_frame.pack(side=tk.BOTTOM, fill=tk.X)
        self.setup_done = True

    # ---------- Biodegradability helpers ----------
    def _compute_thco2_g(self):
        """Compute theoretical CO2 (g) from composition, excluding inorganic and biochar."""
        if not hasattr(self, 'test_setup'):
            return None
        m = float(self.test_setup['mass_g'])
        comp = self.test_setup['composition']
        cf = self.test_setup['carbon_fraction']
        # Mass (g) of biodegradable fractions
        m_pla = m * comp.get('PLA', 0.0) / 100.0
        m_pbat = m * comp.get('PBAT', 0.0) / 100.0
        # Carbon mass (g)
        c_pla = m_pla * cf.get('PLA', 0.0) / 100.0
        c_pbat = m_pbat * cf.get('PBAT', 0.0) / 100.0
        total_c = c_pla + c_pbat
        thco2 = total_c * (44.01/12.01)
        return thco2

    def _load_saved_setup_into_form(self):
        data = self.config or {}
        saved = data.get('test_setup')
        if not saved:
            return
        try:
            comp = saved.get('composition', {})
            self.var_pla.set(comp.get('PLA', self.var_pla.get()))
            self.var_pbat.set(comp.get('PBAT', self.var_pbat.get()))
            self.var_inorg.set(comp.get('Inorganic', self.var_inorg.get()))
            self.var_biochar.set(comp.get('Biochar', self.var_biochar.get()))
            self.var_mass.set(saved.get('mass_g', self.var_mass.get()))
            self.var_env.set(saved.get('env', self.var_env.get()))
            cf = saved.get('carbon_fraction', {})
            self.var_c_pla.set(cf.get('PLA', self.var_c_pla.get()))
            self.var_c_pbat.set(cf.get('PBAT', self.var_c_pbat.get()))
            self.var_c_biochar.set(cf.get('Biochar', self.var_c_biochar.get()))
            acc = saved.get('co2_accounting', {})
            self.var_mode.set(acc.get('mode', self.var_mode.get()))
            self.var_vol_flow.set(acc.get('vol_or_flow', self.var_vol_flow.get()))
            self.var_baseline_ppm.set(acc.get('baseline_ppm', self.var_baseline_ppm.get()))
            self.var_temp_c.set(acc.get('temp_c', self.var_temp_c.get()))
            self.var_press_kpa.set(acc.get('press_kpa', self.var_press_kpa.get()))
            self.var_blank_src.set(saved.get('blank_source', self.var_blank_src.get()))
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
            d_blank = 0.0
            if blank_ppm is not None:
                mole_rate_b = flow_m3_s * max(0.0, blank_ppm - baseline) * 1e-6 * (P/(R*T))
                d_blank = mole_rate_b * 44.01 * dt
        else:
            # Closed volume: use change in ppm over dt
            dppm = ppm_now - self.last_ppm
            vol_m3 = vf / 1000.0
            dmoles = dppm * 1e-6 * vol_m3 * (P/(R*T))
            d_g = dmoles * 44.01
            d_blank = 0.0
            if blank_ppm is not None and self.last_ppm is not None:
                dppm_b = blank_ppm - self.last_ppm  # assume same last for simplicity
                dmoles_b = dppm_b * 1e-6 * vol_m3 * (P/(R*T))
                d_blank = dmoles_b * 44.01
        # Accumulate, not letting cumulative drop below zero
        self.cum_co2_g = max(0.0, self.cum_co2_g + d_g)
        self.cum_blank_g = max(0.0, self.cum_blank_g + d_blank)
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

    def _update_bio_panel(self):
        try:
            if self.thco2_g is not None:
                self.lbl_thco2.configure(text=f"ThCO2: {self.thco2_g:.1f} g")
            else:
                self.lbl_thco2.configure(text="ThCO2: - g")
            self.lbl_cum.configure(text=f"Cumulative CO2 (net): {max(0.0, self.cum_co2_g - self.cum_blank_g):.1f} g")
            if self.thco2_g and self.thco2_g > 0:
                pct = max(0.0, min(100.0, ((max(0.0, self.cum_co2_g - self.cum_blank_g)) / self.thco2_g) * 100.0))
                self.lbl_bio.configure(text=f"Biodegradability: {pct:.1f} %")
            else:
                self.lbl_bio.configure(text="Biodegradability: - %")
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
        row += [fmt(co2_values[i]) for i in sel_co2 if i < len(co2_values)]
        row += [fmt(temp_values[j]) for j in sel_th if j < len(temp_values)]
        row += [fmt(hum_values[j]) for j in sel_th if j < len(hum_values)]
        try:
            self.log_writer.writerow(row)
            self.log_file.flush()
        except Exception as e:
            print(f"Failed writing log row: {e}")

    def _update_stats(self):
        # Helper to compute current/avg/max ignoring None
        def stats(series):
            curr = series[-1] if series else None
            vals = [v for v in series if v is not None]
            avg = sum(vals)/len(vals) if vals else None
            mx = max(vals) if vals else None
            return curr, avg, mx
        def fmt(v, prec=1):
            return '-' if v is None else (f"{v:.{prec}f}" if isinstance(v, (int, float)) else str(v))

        for i in range(6):
            c, a, m = stats(self.co2_series[i])
            for lbl, val in zip(self.stat_labels['co2'][i], (fmt(c, 0), fmt(a, 1), fmt(m, 0))):
                lbl.configure(text=val)
        for j in range(2):
            c, a, m = stats(self.temp_series[j])
            for lbl, val in zip(self.stat_labels['temp'][j], (fmt(c, 1), fmt(a, 1), fmt(m, 1))):
                lbl.configure(text=val)
        for j in range(2):
            c, a, m = stats(self.hum_series[j])
            for lbl, val in zip(self.stat_labels['hum'][j], (fmt(c, 1), fmt(a, 1), fmt(m, 1))):
                lbl.configure(text=val)

    def _discover_ports(self):
        """Classify ports into CO2 and Temp/Humidity using USB identifiers.

        Rules:
          - CP210x / Silicon Labs -> CO2 sensors
          - WCH / CH340 family -> Temp/Humidity sensors
        Fallback keeps discovery stable but tries not to mix roles.
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
            # Explicitly ignore unwanted macOS virtual ports
            if 'cu.usbserial' in dev.lower() or 'cu.bluetooth' in dev.lower() or 'bluetooth' in blob:
                continue
            # Explicit path-based routing first (macOS): cu.SLAB* -> CO2, cu.wch* -> TH
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

        # Fallback: put remaining unknowns into CO2 first, then TH (exclude usbserial/bluetooth)
        for d in unknown:
            if ('cu.usbserial' in d.lower()) or ('cu.bluetooth' in d.lower()):
                continue
            if len(co2) < 6:
                co2.append(d)
            elif len(th) < 2:
                th.append(d)

        # Deduplicate, filter out cu.usbserial*/cu.Bluetooth*, sort and cap
        co2 = [d for d in dict.fromkeys(co2) if ('cu.usbserial' not in d.lower() and 'cu.bluetooth' not in d.lower())]
        th = [d for d in dict.fromkeys(th) if ('cu.usbserial' not in d.lower() and 'cu.bluetooth' not in d.lower())]
        co2 = self._sort_ports(co2)[:6]
        th = self._sort_ports(th)[:2]
        return co2, th

    def _setup_ports(self):
        self.co2_serials = [None] * 6
        self.th_serials = [None] * 2

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

        for j in range(min(2, len(th_ports))):
            port = th_ports[j]
            try:
                self.th_serials[j] = serial.Serial(port, baudrate=self.thr_baud, timeout=1)
                print(f"TH {j+1} -> {port}")
            except Exception as e:
                print(f"Temp/Humidity sensor {j+1} open failed on {port}: {e}")
                self.th_serials[j] = None

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
        co2, th = self._discover_ports()
        self._save_ports_to_config(co2, th)
        # Update in-memory mapping used for color/offset mapping
        try:
            self.co2_ports_used = (co2 + [None]*6)[:6]
        except Exception:
            pass

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
