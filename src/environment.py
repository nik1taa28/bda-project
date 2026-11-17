"""T20/T21: detect the real machine and toolchain, write outputs/environment.json."""
import json
import os
import platform
import subprocess
import sys
from pathlib import Path


def _sh(cmd):
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=20)
        return (r.stdout + r.stderr).strip()
    except Exception:
        return None


def _sysctl(key):
    out = _sh(["sysctl", "-n", key]) if platform.system() == "Darwin" else None
    return out if out and "unknown" not in out else None


def detect(bench_cores=(1, 2, 4)) -> dict:
    logical = os.cpu_count()
    if platform.system() == "Darwin":
        ram = int(_sysctl("hw.memsize") or 0)
        cpu = _sysctl("machdep.cpu.brand_string")
        physical = _sysctl("hw.physicalcpu")
        perf = _sysctl("hw.perflevel0.logicalcpu")
        eff = _sysctl("hw.perflevel1.logicalcpu")
    else:
        ram = os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES")
        cpu, physical, perf, eff = platform.processor(), None, None, None
    java = _sh(["java", "-version"])
    try:
        import pyspark
        spark_v = pyspark.__version__
    except Exception:
        spark_v = None
    env = {
        "os": f"{platform.system()} {platform.release()} ({platform.machine()})",
        "cpu": cpu,
        "logical_cores": logical,
        "physical_cores": int(physical) if physical else None,
        "performance_cores": int(perf) if perf else None,
        "efficiency_cores": int(eff) if eff else None,
        "ram_gb": round(ram / 1024 ** 3, 1),
        "python": sys.version.split()[0],
        "java": java.splitlines()[0] if java else "NOT FOUND - install Java 11 or 17",
        "pyspark": spark_v,
        "benchmark_cores": list(bench_cores),
        "warnings": [],
    }
    avail = logical or 0
    for n in bench_cores:
        if n > avail:
            env["warnings"].append(f"local[{n}] is OVERSUBSCRIBED: only {avail} logical cores available")
    if avail < 4:
        env["warnings"].append("fewer than 4 cores: the 4-core result is oversubscribed")
    if env["java"].startswith("NOT FOUND"):
        env["warnings"].append("Java missing: PySpark 3.5 needs Java 8/11/17")
    return env


def main():
    env = detect()
    Path("outputs").mkdir(exist_ok=True)
    Path("outputs/environment.json").write_text(json.dumps(env, indent=2))
    print(json.dumps(env, indent=2))


if __name__ == "__main__":
    main()
