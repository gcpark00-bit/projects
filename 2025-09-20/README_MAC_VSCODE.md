# macOS + VS Code quick start (USB multi-jar CO₂ logger)

## 1) Open this folder in VS Code
File → Open Folder… → select this directory.

## 2) Create venv & install deps
VS Code: Terminal → Run Task… → **Install deps**
or:
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## 3) Find your ports (macOS)
```bash
ls -l /dev/tty.usb* /dev/cu.usb* 2>/dev/null
```
Update `.vscode/launch.json` or `ports_config.json` with your actual device names.

## 4) Run
- Run and Debug ▶ **Run logger (config file)** (recommended)
- or **Run logger (args)**

## 5) Live plot & CSV
A Matplotlib window opens; data saved to `usb_multi_log.csv`.

## 6) GUI backend tip (macOS)
If the plot window doesn't appear:
```bash
MPLBACKEND=TkAgg python multichannel_usb_logger.py --config ports_config.json
```

## Notes
- Warm up MH-Z19C 5–10 min; disable ABC if needed.
- Keep tubing short and airtight.
