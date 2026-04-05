# tts_engine.py
import multiprocessing
import pyttsx3
import time
import os
import signal


def _speak_process(text, voice_id):
    """Runs inside a separate process."""
    engine = pyttsx3.init()
    if voice_id:
        engine.setProperty("voice", voice_id)
    engine.say(text)
    engine.runAndWait()


class TTSWorker:
    """
    Multiprocessing-based TTS:
    - Each utterance runs in its own process
    - stop() kills the process instantly
    - No COM deadlocks
    - No freezes
    """

    def __init__(self, default_voice_substring="Australia"):
        self.current_voice_id = self._select_default_voice(default_voice_substring)
        self.current_process = None

    # ---------------------------------------------------------
    # Voice selection
    # ---------------------------------------------------------
    def _select_default_voice(self, substring):
        engine = pyttsx3.init()
        voices = engine.getProperty("voices")
        for v in voices:
            if substring.lower() in (v.name + " " + v.id).lower():
                return v.id
        return voices[0].id if voices else None

    # ---------------------------------------------------------
    # Public API
    # ---------------------------------------------------------
    def speak(self, text):
        """Start a new TTS process."""
        self.stop()  # kill any existing speech

        if not text:
            return

        self.current_process = multiprocessing.Process(
            target=_speak_process,
            args=(text, self.current_voice_id)
        )
        self.current_process.start()

    def stop(self):
        """Kill the current TTS process instantly."""
        if self.current_process and self.current_process.is_alive():
            try:
                os.kill(self.current_process.pid, signal.SIGTERM)
            except Exception:
                pass
        self.current_process = None

    def shutdown(self):
        """Clean shutdown."""
        self.stop()
