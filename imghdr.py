"""
Temporary imghdr compatibility layer for Python 3.13+
Required because Streamlit still imports imghdr.
"""

def what(file, h=None):
    return None