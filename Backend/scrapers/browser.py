import os
from typing import Dict, Any, Optional


def _candidates_windows() -> list:
    prog = os.environ.get("PROGRAMFILES", r"C:\Program Files")
    prog_x86 = os.environ.get("PROGRAMFILES(X86)", r"C:\Program Files (x86)")
    local_app = os.environ.get("LOCALAPPDATA", r"C:\Users\%USERNAME%\AppData\Local")

    candidates = [
        os.path.join(prog, "Google", "Chrome", "Application", "chrome.exe"),
        os.path.join(prog_x86, "Google", "Chrome", "Application", "chrome.exe"),
        os.path.join(prog, "Chromium", "Application", "chrome.exe"),
        os.path.join(prog_x86, "Chromium", "Application", "chrome.exe"),
        os.path.join(local_app, "Google", "Chrome", "Application", "chrome.exe"),
    ]

    return candidates


def detect_chrome_executable() -> Optional[str]:
    """Detecta un ejecutable de Chrome/Chromium instalado globalmente.

    - Primero revisa variables de entorno: `PLAYWRIGHT_CHROMIUM_PATH`, `CHROME_PATH`, `CHROME_BIN`.
    - Luego prueba rutas comunes en Windows.
    Devuelve la ruta del ejecutable o `None` si no se encuentra.
    """
    env_vars = ["PLAYWRIGHT_CHROMIUM_PATH", "CHROME_PATH", "CHROME_BIN"]
    for v in env_vars:
        p = os.environ.get(v)
        if p and os.path.exists(p):
            return p

    # Rutas comunes para Windows
    if os.name == "nt":
        for cand in _candidates_windows():
            if os.path.exists(cand):
                return cand

    # No encontrado
    return None


def get_launch_kwargs(headless: bool = True, extra_args: Optional[list] = None) -> Dict[str, Any]:
    """Construye kwargs para BrowserType.launch acomodando un Chromium global.

    - Si se detecta un ejecutable global, lo pasa como `executable_path`.
    - Incluye args por defecto compatibles con el proyecto.
    """
    args = ["--no-sandbox", "--disable-setuid-sandbox"]
    if extra_args:
        args.extend(extra_args)

    kwargs: Dict[str, Any] = {"headless": headless, "args": args}

    exe = detect_chrome_executable()
    if exe:
        kwargs["executable_path"] = exe

    return kwargs
