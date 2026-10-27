"""
Local system resource metrics collector for MeshWeaver nodes.

Gathers CPU, memory, and task execution counts.
Uses psutil when installed; falls back to standard /proc and os functions
so nodes can run on basic edge devices without extra pip packages.
"""

from __future__ import annotations

import logging
import os
import sys
import time
try:
    import resource
except ImportError:
    resource = None
from typing import Any, Callable, Dict, Optional, Tuple

from .models import NodeMetrics

logger = logging.getLogger("MeshWeaver.MetricsCollector")

try:
    import psutil
    _HAS_PSUTIL = True
except ImportError:
    psutil = None
    _HAS_PSUTIL = False


class SystemMetricsCollector:
    """Samples CPU, memory, load average, and active worker tasks."""

    def __init__(
        self,
        node_id: str,
        task_counter_provider: Optional[Callable[[], Tuple[int, int, int]]] = None,
    ) -> None:
        self.node_id = node_id
        self.task_counter_provider = task_counter_provider
        self._start_time = time.time()
        self._metric_sequence = 0

        # Used for delta calculations when psutil isn't present
        self._last_cpu_time = time.process_time()
        self._last_wall_time = time.perf_counter()
        self._last_proc_stat: Optional[Tuple[int, int]] = None

        # Overrides for testing and demo setups
        self._simulated_cpu: Optional[float] = None
        self._simulated_ram: Optional[float] = None
        self._simulated_active_tasks: Optional[int] = None

    @classmethod
    def has_psutil(cls) -> bool:
        """Returns True if psutil is available."""
        return _HAS_PSUTIL

    def set_simulated_metrics(
        self,
        cpu_percent: Optional[float] = None,
        ram_percent: Optional[float] = None,
        active_tasks: Optional[int] = None,
    ) -> None:
        """Sets fixed metric values for demos or tests."""
        self._simulated_cpu = None if cpu_percent is None else max(0.0, min(100.0, float(cpu_percent)))
        self._simulated_ram = None if ram_percent is None else max(0.0, min(100.0, float(ram_percent)))
        self._simulated_active_tasks = None if active_tasks is None else max(0, int(active_tasks))

    def clear_simulated_metrics(self) -> None:
        """Clears test overrides and returns to real system readings."""
        self._simulated_cpu = None
        self._simulated_ram = None
        self._simulated_active_tasks = None

    def _get_cpu_percent(self) -> float:
        """Measures CPU usage percentage."""
        if self._simulated_cpu is not None:
            return self._simulated_cpu

        if _HAS_PSUTIL and psutil is not None:
            try:
                return float(psutil.cpu_percent(interval=None))
            except Exception as e:
                logger.debug(f"psutil CPU read failed: {e}")

        # Linux: read /proc/stat
        if os.path.exists("/proc/stat"):
            try:
                with open("/proc/stat", "r") as f:
                    fields = f.readline().strip().split()[1:]
                    cpu_numbers = [int(x) for x in fields]
                    idle_time = cpu_numbers[3] + (cpu_numbers[4] if len(cpu_numbers) > 4 else 0)
                    total_time = sum(cpu_numbers)
                    if self._last_proc_stat is not None:
                        prev_idle, prev_total = self._last_proc_stat
                        delta_idle = idle_time - prev_idle
                        delta_total = total_time - prev_total
                        self._last_proc_stat = (idle_time, total_time)
                        if delta_total > 0:
                            usage = (1.0 - (delta_idle / delta_total)) * 100.0
                            return max(0.0, min(100.0, round(usage, 1)))
                    self._last_proc_stat = (idle_time, total_time)
            except Exception as e:
                logger.debug(f"/proc/stat read error: {e}")

        # macOS / Unix load average fallback
        try:
            if hasattr(os, "getloadavg"):
                load_1m, _, _ = os.getloadavg()
                cpu_count = os.cpu_count() or 1
                normalized_load = (load_1m / cpu_count) * 100.0
                return max(0.0, min(100.0, round(normalized_load, 1)))
        except Exception:
            pass

        # Process time delta fallback
        now_cpu = time.process_time()
        now_wall = time.perf_counter()
        delta_cpu = now_cpu - self._last_cpu_time
        delta_wall = now_wall - self._last_wall_time
        self._last_cpu_time = now_cpu
        self._last_wall_time = now_wall
        if delta_wall > 0:
            cpu_count = os.cpu_count() or 1
            usage = (delta_cpu / (delta_wall * cpu_count)) * 100.0
            return max(0.0, min(100.0, round(usage, 1)))

        return 10.0

    def _get_ram_metrics(self) -> Tuple[float, float, float]:
        """Returns (ram_percent, used_mb, total_mb)."""
        if self._simulated_ram is not None:
            pct = self._simulated_ram
            total_mb = 8192.0
            used_mb = (pct / 100.0) * total_mb
            return (pct, used_mb, total_mb)

        if _HAS_PSUTIL and psutil is not None:
            try:
                mem = psutil.virtual_memory()
                used_mb = mem.used / (1024 * 1024)
                total_mb = mem.total / (1024 * 1024)
                return (float(mem.percent), round(used_mb, 1), round(total_mb, 1))
            except Exception as e:
                logger.debug(f"psutil memory read failed: {e}")

        # Linux: read /proc/meminfo
        if os.path.exists("/proc/meminfo"):
            try:
                meminfo: Dict[str, float] = {}
                with open("/proc/meminfo", "r") as f:
                    for line in f:
                        parts = line.split(":")
                        if len(parts) == 2:
                            val = parts[1].strip().split()[0]
                            meminfo[parts[0].strip()] = float(val)
                total_kb = meminfo.get("MemTotal", 0.0)
                avail_kb = meminfo.get("MemAvailable", meminfo.get("MemFree", 0.0))
                if total_kb > 0:
                    used_kb = total_kb - avail_kb
                    pct = (used_kb / total_kb) * 100.0
                    return (
                        max(0.0, min(100.0, round(pct, 1))),
                        round(used_kb / 1024.0, 1),
                        round(total_kb / 1024.0, 1),
                    )
            except Exception as e:
                logger.debug(f"/proc/meminfo read error: {e}")

        # Fallback via resource.getrusage
        try:
            usage = resource.getrusage(resource.RUSAGE_SELF)
            rss_mb = (usage.ru_maxrss / (1024 * 1024)) if sys.platform == "darwin" else (usage.ru_maxrss / 1024)
            estimated_total_mb = 4096.0
            estimated_pct = min(100.0, (rss_mb / estimated_total_mb) * 100.0)
            return (round(max(15.0, estimated_pct), 1), round(rss_mb, 1), estimated_total_mb)
        except Exception:
            return (25.0, 1024.0, 4096.0)

    def _get_load_average(self) -> float:
        """Returns 1-minute system load average."""
        try:
            if hasattr(os, "getloadavg"):
                return round(float(os.getloadavg()[0]), 2)
        except Exception:
            pass
        return 0.0

    def collect(self) -> NodeMetrics:
        """Samples current system state and returns a NodeMetrics snapshot."""
        cpu_pct = self._get_cpu_percent()
        ram_pct, used_mb, total_mb = self._get_ram_metrics()
        load_1m = self._get_load_average()
        uptime = round(time.time() - self._start_time, 1)

        active = 0
        completed = 0
        failed = 0
        if self._simulated_active_tasks is not None:
            active = self._simulated_active_tasks
        elif self.task_counter_provider:
            try:
                active, completed, failed = self.task_counter_provider()
            except Exception as e:
                logger.warning(f"Could not read task counters: {e}")

        self._metric_sequence += 1

        return NodeMetrics(
            node_id=self.node_id,
            cpu_percent=cpu_pct,
            ram_percent=ram_pct,
            memory_used_mb=used_mb,
            memory_total_mb=total_mb,
            active_tasks=active,
            completed_tasks=completed,
            failed_tasks=failed,
            load_average_1m=load_1m,
            uptime_seconds=uptime,
            timestamp=time.time(),
            sequence_number=self._metric_sequence,
        )
