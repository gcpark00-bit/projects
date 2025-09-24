# multichannel_usb_logger_gui.py
# Adds Start, Stop, Pause, and Restart buttons to control the simulation.

import time
import random
from datetime import datetime
import matplotlib.pyplot as plt
from tkinter import Tk, Button, Label
from threading import Thread, Event

class DataSimulator:
    def __init__(self):
        self.running = False
        self.paused = False
        self.stop_event = Event()
        self.pause_event = Event()
        self.interval = 3.0  # Data generation interval in seconds
        self.xs = []
        self.ys = [[] for _ in range(3)]  # Simulate 3 CO2 channels
        self.ts, self.rhs = [], []
        self.start_time = None

    def generate_fake_data(self):
        """Generate fake data for CO2, temperature, and humidity."""
        co2_values = [random.randint(400, 5000) for _ in range(3)]  # Simulate 3 CO2 sensors
        temp = round(random.uniform(20.0, 30.0), 1)  # Simulate temperature in °C
        rh = round(random.uniform(30.0, 70.0), 1)  # Simulate relative humidity in %
        return co2_values, temp, rh

    def run_simulation(self):
        """Run the simulation loop."""
        self.start_time = time.time() if self.start_time is None else self.start_time
        plt.ion()
        plt.figure()
        plt.title("Simulated Multi-jar CO2 / T / RH")
        plt.xlabel("Elapsed (s)")
        plt.grid(True)

        while not self.stop_event.is_set():
            if self.paused:
                self.pause_event.wait()  # Wait until resumed
                self.pause_event.clear()

            elapsed = time.time() - self.start_time
            co2_values, temp, rh = self.generate_fake_data()

            # Print generated data to console
            print(f"{datetime.now().isoformat(timespec='seconds')} | Elapsed: {elapsed:.1f}s | "
                  f"CO2: {co2_values} ppm | Temp: {temp} °C | RH: {rh} %")

            # Append data for plotting
            self.xs.append(elapsed)
            for i, v in enumerate(co2_values):
                self.ys[i].append(v)
            self.ts.append(temp)
            self.rhs.append(rh)

            # Plot
            plt.clf()
            plt.title("Simulated Multi-jar CO2 / T / RH")
            plt.xlabel("Elapsed (s)")
            plt.grid(True)
            for i in range(3):
                plt.plot(self.xs, self.ys[i], label=f"CO2 ch{i+1} (ppm)")
            plt.plot(self.xs, self.ts, label="Temp (°C)")
            plt.plot(self.xs, self.rhs, label="RH (%)")
            plt.legend()
            plt.pause(0.001)

            time.sleep(self.interval)

        plt.ioff()
        plt.show()

    def start(self):
        """Start the simulation."""
        if not self.running:
            self.running = True
            self.stop_event.clear()
            self.simulation_thread = Thread(target=self.run_simulation)
            self.simulation_thread.start()

    def stop(self):
        """Stop the simulation."""
        self.stop_event.set()
        self.running = False
        self.start_time = None
for _ in range(int(self.interval / 0.1)):
    if self.stop_event.is_set():
        return
    time.sleep(0.1)
    def pause(self):
        """Pause the simulation."""
        if self.running and not self.paused:
            self.paused = True

    def resume(self):
        """Resume the simulation."""
        if self.paused:
            self.paused = False
            self.pause_event.set()

def create_gui(simulator):
    """Create the GUI for controlling the simulation."""
    root = Tk()
    root.title("Data Simulator Control")

    Label(root, text="Data Simulator Control", font=("Arial", 16)).pack(pady=10)

    start_button = Button(root, text="Start", font=("Arial", 14), command=simulator.start)
    start_button.pack(pady=5)

    stop_button = Button(root, text="Stop", font=("Arial", 14), command=simulator.stop)
    stop_button.pack(pady=5)

    pause_button = Button(root, text="Pause", font=("Arial", 14), command=simulator.pause)
    pause_button.pack(pady=5)

    restart_button = Button(root, text="Restart", font=("Arial", 14), command=lambda: [simulator.stop(), simulator.start()])
    restart_button.pack(pady=5)

    root.protocol("WM_DELETE_WINDOW", lambda: [simulator.stop(), root.destroy()])
    root.mainloop()

if __name__ == "__main__":
    simulator = DataSimulator()
    create_gui(simulator)