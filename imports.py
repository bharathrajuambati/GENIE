# imports.py

# Standard Library Imports
import sys
import os  # Added this line
import datetime
from functools import partial
import aemo_pi
# Third-Party Library Imports
import pandas as pd
import requests
import pytz
import pyttsx3  # Uncommented this line
from PySide6.QtCore import QTimer, QUrl
from PySide6.QtGui import QColor
from PySide6.QtWidgets import (
    QApplication, QMainWindow, QTableWidget, QTableWidgetItem,
    QVBoxLayout, QWidget, QTabWidget, QPushButton, QHBoxLayout
)
from PySide6.QtMultimedia import QMediaPlayer

import matplotlib.pyplot as plt
from pandas import json_normalize
import seaborn as sns
from matplotlib.patches import Patch

