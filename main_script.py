# -*- coding: utf-8 -*-
"""
WEMS Dashboard – Super-Control Refresh Engine
Boundary-aligned dispatch refresh (boundary + 10 seconds)
"""

import sys
import pandas as pd
from datetime import datetime, timedelta

from PySide6.QtCore import QTimer, QRunnable, QThreadPool, Slot, Signal, QObject
from PySide6.QtWidgets import QApplication

# --- IMPORT YOUR MODULES ---
from dispatch_solution import main as dispatch_solution_main
from merge_data_frames import merge_data_frames
from outages import fetch_and_process_network_outages, fetch_and_process_generator_outages
from conditions import get_current_values, PI_TAGS
from gui.data_frame_viewer import DataFrameViewer
from tts_engine import TTSWorker

tts = TTSWorker(default_voice_substring="Australia")  # tries to pick an AU voice


# ============================================================
# FORCE INSTANT CONSOLE LOGGING
# ============================================================

try:
    sys.stdout.reconfigure(line_buffering=True)
except AttributeError:
    pass  # Spyder console doesn't support reconfigure


# ============================================================
# SIGNALS FOR EACH FEATURE
# ============================================================
class ViewerUpdateSignal(QObject):
    alarms = Signal(pd.DataFrame)
    dashboard = Signal(pd.DataFrame)
    generator_outages = Signal(pd.DataFrame)
    network_outages = Signal(pd.DataFrame)

    contingency_raise = Signal(pd.DataFrame)
    contingency_lower = Signal(pd.DataFrame)
    reg_raise = Signal(pd.DataFrame)
    reg_lower = Signal(pd.DataFrame)
    energy = Signal(pd.DataFrame)
    congestion = Signal(pd.DataFrame)


# ============================================================
# GENERIC WORKER FOR NON-DISPATCH FEATURES
# ============================================================
class FeatureWorker(QRunnable):
    def __init__(self, feature, signal):
        super().__init__()
        self.feature = feature
        self.signal = signal

    @Slot()
    def run(self):
        try:
            if self.feature == "alarms":
                _, df = get_current_values(list(PI_TAGS.values()))
                self.signal.emit(df)

            elif self.feature == "dashboard":
                df = merge_data_frames()
                self.signal.emit(df)

            elif self.feature == "generator_outages":
                df = fetch_and_process_generator_outages()
                self.signal.emit(df)

            elif self.feature == "network_outages":
                df = fetch_and_process_network_outages()
                self.signal.emit(df)

            print(f"{self.feature.capitalize()} updated at {datetime.now()}", flush=True)

        except Exception as e:
            print(f"[{self.feature.upper()} ERROR] {e}", flush=True)
            self.signal.emit(pd.DataFrame())


# ============================================================
# DISPATCH WORKER – ONE CALL → ALL DISPATCH TABS
# ============================================================
class DispatchWorker(QRunnable):
    def __init__(self, signals):
        super().__init__()
        self.signals = signals

    @Slot()
    def run(self):
        try:
            (df_CR,
             df_CL,
             df_RR,
             df_RL,
             df_energy,
             df_rocof,
             df_faststart,
             df_congestion) = dispatch_solution_main()

            self.signals.contingency_raise.emit(df_CR)
            self.signals.contingency_lower.emit(df_CL)
            self.signals.reg_raise.emit(df_RR)
            self.signals.reg_lower.emit(df_RL)
            self.signals.energy.emit(df_energy)
            self.signals.congestion.emit(df_congestion)

            print(f"[DISPATCH] Updated at {datetime.now()}", flush=True)

        except Exception as e:
            print(f"[DISPATCH ERROR] {e}", flush=True)


# ============================================================
# MAIN GUI FUNCTION – SUPER CONTROL VERSION
# ============================================================
def main_function_gui():

    print(f"Starting GUI at {datetime.now()}", flush=True)

    # --- INITIAL LOAD ---
    (df_CR,
     df_CL,
     df_RR,
     df_RL,
     df_energy,
     df_rocof,
     df_faststart,
     df_congestion) = dispatch_solution_main()

    df_dashboard = merge_data_frames()
    df_generator_outages = fetch_and_process_generator_outages()
    df_network_outages = fetch_and_process_network_outages()
    _, df_alarms = get_current_values(list(PI_TAGS.values()))

    # --- START QT APP ---
    app = QApplication(sys.argv)

    # --- TTS + VIEWER ---
    tts = TTSWorker()

    viewer = DataFrameViewer(
        df_alarms,
        df_dashboard,
        df_generator_outages,
        df_network_outages,
        df_CR,
        df_CL,
        df_RR,
        df_RL,
        df_energy,
        df_congestion,
        window_width=3600,
        window_height=1800,
        tts=tts,
    )

    viewer.show()

    # --- THREAD POOL ---
    thread_pool = QThreadPool()
    signals = ViewerUpdateSignal()
    timers = []

    # ============================================================
    # CONNECT SIGNALS → UI TABS (DISPLAY)
    # ============================================================
    signals.alarms.connect(lambda df: viewer.alarms_tab.load_data_frame(df))
    signals.dashboard.connect(lambda df: viewer.dashboard_tab.load_data_frame(df))
    signals.generator_outages.connect(lambda df: viewer.generator_outages_tab.load_data_frame(df))
    signals.network_outages.connect(lambda df: viewer.network_outages_tab.load_data_frame(df))

    signals.contingency_raise.connect(lambda df: viewer.contingency_raise_tab.load_data_frame(df))
    signals.contingency_lower.connect(lambda df: viewer.contingency_lower_tab.load_data_frame(df))
    signals.reg_raise.connect(lambda df: viewer.reg_raise_tab.load_data_frame(df))
    signals.reg_lower.connect(lambda df: viewer.reg_lower_tab.load_data_frame(df))
    signals.energy.connect(lambda df: viewer.energy_tab.load_data_frame(df))
    signals.congestion.connect(lambda df: viewer.congestion_tab.load_data_frame(df))

    # ============================================================
    # CONNECT SIGNALS → ALARM EVALUATION (VOICE)
    # ============================================================
    signals.dashboard.connect(viewer.dashboard_tab.evaluate_alarms)
    signals.alarms.connect(viewer.alarms_tab.evaluate_alarms)

    # ============================================================
    # TIMER HELPER
    # ============================================================
    def start_timer(interval_ms, fn):
        t = QTimer()
        t.setInterval(interval_ms)
        t.timeout.connect(fn)
        t.start()
        timers.append(t)

    # ============================================================
    # SUPER-CONTROL REFRESH RATES
    # ============================================================
    start_timer(5_000, lambda: thread_pool.start(FeatureWorker("alarms", signals.alarms)))
    start_timer(30_000, lambda: thread_pool.start(FeatureWorker("dashboard", signals.dashboard)))
    start_timer(1800_000, lambda: thread_pool.start(FeatureWorker("generator_outages", signals.generator_outages)))
    start_timer(1800_000, lambda: thread_pool.start(FeatureWorker("network_outages", signals.network_outages)))

    # ============================================================
    # DISPATCH – BOUNDARY + 10 SECONDS
    # ============================================================
    def schedule_boundary_dispatch():
        now = datetime.now()

        # Round DOWN to nearest 5-minute block
        minute_block = (now.minute // 5) * 5
        boundary = now.replace(minute=minute_block, second=10, microsecond=0)

        # If boundary already passed, move to next one
        if boundary <= now:
            boundary += timedelta(minutes=5)

        delay_ms = int((boundary - now).total_seconds() * 1000)

        print(f"[DISPATCH] Next boundary refresh at {boundary}", flush=True)

        one_shot = QTimer()
        one_shot.setSingleShot(True)

        def start_repeating_dispatch():
            # Run dispatch immediately at boundary
            thread_pool.start(DispatchWorker(signals))

            # Then every 5 minutes
            repeating = QTimer()
            repeating.setInterval(300_000)
            repeating.timeout.connect(lambda: thread_pool.start(DispatchWorker(signals)))
            repeating.start()
            timers.append(repeating)

        one_shot.timeout.connect(start_repeating_dispatch)
        one_shot.start(delay_ms)
        timers.append(one_shot)

    schedule_boundary_dispatch()

    sys.exit(app.exec())


# ============================================================
# ENTRY POINT
# ============================================================
if __name__ == "__main__":
    main_function_gui()
