from __future__ import annotations

import os
import subprocess
from pathlib import Path
from typing import List

import streamlit as st


def run_cmd(cmd: List[str], cwd: Path) -> tuple[int, str, str]:
    result = subprocess.run(cmd, cwd=cwd, capture_output=True, text=True)
    return result.returncode, result.stdout or "", result.stderr or ""


def run_cmd_streaming(
    cmd: List[str],
    cwd: Path,
    placeholder: st.delta_generator.DeltaGenerator | None = None,
    height: int = 400,
) -> tuple[int, str, str]:
    env = os.environ.copy()
    env["PYTHONUNBUFFERED"] = "1"
    process = subprocess.Popen(
        cmd,
        cwd=cwd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
        env=env,
    )
    accumulated: List[str] = []
    if process.stdout is None:
        process.wait()
        return process.returncode or 0, "", ""
    for line in iter(process.stdout.readline, ""):
        accumulated.append(line)
        if placeholder is not None:
            placeholder.text_area(
                "Output",
                value="".join(accumulated),
                height=height,
                disabled=True,
                label_visibility="collapsed",
            )
    process.wait()
    full = "".join(accumulated)
    return process.returncode or 0, full, ""
