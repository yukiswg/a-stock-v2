#!/usr/bin/env python3
"""
监控 Codex Worker 的实时输出，并自动打开 Codex App（如果可用）。
"""
import argparse
import json
import subprocess
import sys
import time
from pathlib import Path

def find_latest_run(project_root: Path) -> Path | None:
    """找到最新的 codex_delegate 运行目录"""
    base = project_root / "data" / "output" / "codex_delegate"
    if not base.exists():
        return None
    dirs = sorted(base.iterdir(), key=lambda p: p.stat().st_mtime, reverse=True)
    return dirs[0] if dirs else None

def monitor_worker(run_dir: Path, worker_id: str) -> None:
    """实时监控 worker 的 stderr 输出"""
    worker_dir = run_dir / "workers" / "iteration_1" / worker_id
    if not worker_dir.exists():
        print(f"❌ Worker 目录不存在: {worker_dir}")
        return

    stderr_path = worker_dir / "stderr.log"
    if not stderr_path.exists():
        print(f"❌ stderr.log 不存在，Worker 可能还没启动")
        return

    print(f"=== 监控 Worker: {worker_id} ===")
    print(f"Run dir: {run_dir}")
    print(f"=== 实时输出（Ctrl+C 退出） ===\n")

    # 尝试打开 Codex App（ macOS）
    try:
        subprocess.run(["open", "-a", "Codex"], check=False)
        print("\n✓ 已尝试打开 Codex App（如果已打开会切换到前台）")
    except Exception as e:
        print(f"\n⚠ 无法自动打开 Codex App: {e}")

    # tail -f 监控
    cmd = ["tail", "-f", str(stderr_path)]
    try:
        subprocess.run(cmd, execcmd="tail")
    except KeyboardInterrupt:
        print("\n\n=== 停止监控 ===")

def list_workers(run_dir: Path) -> None:
    """列出所有 Worker 及其状态"""
    workers_dir = run_dir / "workers" / "iteration_1"
    if not workers_dir.exists():
        print("❌ Workers 目录不存在")
        return

    print(f"=== Workers in {run_dir.name} ===")
    for worker_dir in sorted(workers_dir.iterdir()):
        worker_id = worker_dir.name
        response_file = worker_dir / "response.json"

        if response_file.exists():
            try:
                data = json.loads(response_file.read_text())
                status = data.get("status", "unknown")
                print(f"  {worker_id}: {status}")
            except:
                print(f"  {worker_id}: (无法读取状态)")
        else:
            print(f"  {worker_id}: 运行中...")

def main() -> None:
    parser = argparse.ArgumentParser(description="监控 Codex Worker 输出")
    parser.add_argument("--run-dir", help="指定运行目录（默认用最新的）")
    parser.add_argument("--worker", default="wp1", help="Worker ID（默认 wp1）")
    parser.add_argument("--list", action="store_true", help="列出所有 Worker 状态")

    args = parser.parse_args()

    # 自动检测项目根目录
    cwd = Path.cwd()
    while cwd != cwd.parent and not (cwd / "src" / "ashare_harness_v2").exists():
        cwd = cwd.parent

    project_root = cwd / "src" / "ashare_harness_v2" if (cwd / "src" / "ashare_harness_v2").exists() else cwd
    # 找到真正的项目根（向上找）
    for parent in [project_root, project_root.parent, project_root.parent.parent]:
        if (parent / "src" / "ashare_harness_v2").exists():
            project_root = parent
            break

    if args.run_dir:
        run_dir = Path(args.run_dir)
    else:
        run_dir = find_latest_run(project_root)

    if not run_dir or not run_dir.exists():
        print("❌ 找不到运行目录")
        print("提示：可以用 --run-dir 指定完整路径")
        sys.exit(1)

    if args.list:
        list_workers(run_dir)
    else:
        monitor_worker(run_dir, args.worker)

if __name__ == "__main__":
    main()
