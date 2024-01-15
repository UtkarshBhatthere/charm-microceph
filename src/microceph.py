#!/usr/bin/env python3

"""Handle Ceph commands."""

import logging
import subprocess

logger = logging.getLogger(__name__)


def _run_cmd(cmd: list) -> None:
    """Execute provided command via subprocess."""
    process = subprocess.run(cmd, capture_output=True, text=True, check=True, timeout=180)
    logger.debug(
        f"Command {' '.join(cmd)} finished; stdout: {process.stdout}, stderr: {process.stderr}"
    )


def add_osd_cmd(spec: str) -> None:
    """Executes MicroCeph add osd cmd with provided spec."""
    cmd = ["sudo", "microceph", "disk", "add", spec]
    _run_cmd(cmd)
