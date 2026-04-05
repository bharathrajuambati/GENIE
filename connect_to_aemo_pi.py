# connect_to_aemo_pi.py

import aemo_pi

def connect_to_aemo_pi():
    """
    Connect to the AEMO PI system.
    Returns:
        pi: The connected AEMO PI object.
    """
    pi = aemo_pi.connect('PIProd')
    return pi
