import threading
import time
import pyperclip
import keyboard
import tkinter as tk
import tkinter.font as tkFont
from ctypes import Structure, c_long, c_uint, sizeof, byref, windll

# ─── Win32 structures for caret position ───────────────────────────
class POINT(Structure):
    _fields_ = [("x", c_long), ("y", c_long)]

class RECT(Structure):
    _fields_ = [
        ("left", c_long),
        ("top", c_long),
        ("right", c_long),
        ("bottom", c_long),
    ]

class GUITHREADINFO(Structure):
    _fields_ = [
        ("cbSize", c_uint),
        ("flags", c_uint),
        ("hwndActive", c_long),
        ("hwndFocus", c_long),
        ("hwndCapture", c_long),
        ("hwndMenuOwner", c_long),
        ("hwndMoveSize", c_long),
        ("hwndCaret", c_long),
        ("rcCaret", RECT),
    ]

def get_caret_pos():
    """Return (x,y) in screen coords of the caret (text cursor) in the active window."""
    user32 = windll.user32
    gti = GUITHREADINFO()
    gti.cbSize = sizeof(GUITHREADINFO)
    user32.GetGUIThreadInfo(0, byref(gti))

    # rcCaret is relative to the window identified by hwndCaret
    pt = POINT(gti.rcCaret.left, gti.rcCaret.bottom)
    user32.ClientToScreen(gti.hwndCaret, byref(pt))
    return pt.x, pt.y

# ─── Clipboard history buffer ──────────────────────────────────────
history = []

def monitor_clipboard():
    last = None
    while True:
        try:
            current = pyperclip.paste()
        except Exception:
            current = None
        if current and current != last:
            history.insert(0, current)
            if len(history) > 50:
                history.pop()
            last = current
        time.sleep(0.5)

# ─── Show history popup ────────────────────────────────────────────
def show_history():
    if not history:
        return

    # position at the text-cursor (caret) of the active window
    x, y = get_caret_pos()

    max_items   = min(len(history), 10)
    item_h      = 22
    width       = 400
    height      = max_items * item_h

    root = tk.Tk()
    root.overrideredirect(True)
    root.attributes("-topmost", True)
    # close when focus is lost
    root.bind("<FocusOut>", lambda e: root.destroy())
    # force focus so FocusOut works when clicking elsewhere
    root.focus_force()

    root.configure(
        bg="white",
        bd=1,
        highlightthickness=1,
        highlightbackground="#ccc"
    )
    root.geometry(f"{width}x{height}+{x}+{y}")

    font = tkFont.Font(family="Segoe UI", size=10)
    frame = tk.Frame(root, bg="white")
    frame.pack(fill="both", expand=True)

    sb = tk.Scrollbar(frame, orient="vertical")
    sb.pack(side="right", fill="y")

    lb = tk.Listbox(
        frame,
        font=font,
        bg="white",
        bd=0,
        highlightthickness=0,
        activestyle="none",
        selectbackground="#e6f0ff",
        yscrollcommand=sb.set,
    )
    lb.pack(side="left", fill="both", expand=True)
    sb.config(command=lb.yview)

    for item in history:
        txt = item.replace("\n", " ")
        lb.insert("end", txt if len(txt) < 80 else txt[:77] + "…")

    # Hover highlight
    def on_motion(event):
        idx = lb.nearest(event.y)
        lb.selection_clear(0, "end")
        lb.selection_set(idx)
        lb.activate(idx)
    lb.bind("<Motion>", on_motion)

    # Click: copy, close, then paste
    def on_click(event):
        idx = lb.nearest(event.y)
        if 0 <= idx < len(history):
            pyperclip.copy(history[idx])
            root.destroy()
            time.sleep(0.05)
            keyboard.send("ctrl+v")
        else:
            root.destroy()
    lb.bind("<ButtonRelease-1>", on_click)

    # also close on Escape
    root.bind("<Escape>", lambda e: root.destroy())

    root.mainloop()

# ─── Bootstrap ────────────────────────────────────────────────────
if __name__ == "__main__":
    threading.Thread(target=monitor_clipboard, daemon=True).start()
    keyboard.add_hotkey("windows+n", show_history)
    keyboard.wait()
