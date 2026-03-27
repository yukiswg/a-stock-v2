from __future__ import annotations

import json
import os
import shutil
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any


# ---------------------------------------------------------------------------
# Schemas
# ---------------------------------------------------------------------------

# Leader 的输出结构：定义 outcome、done_means 和分给 Worker 的任务包。
PLAN_SCHEMA: dict[str, Any] = {
    "type": "object",
    "additionalProperties": False,
    "required": ["outcome", "done_means", "work_packets", "risks"],
    "properties": {
        "outcome": {
            "type": "string",
            "description": "一句话描述最终目标。必须是可观测的结果，不是实现步骤。",
        },
        "done_means": {
            "type": "array",
            "description": "验收条件列表。每条必须是可判定的（通过/不通过），不能含糊。",
            "items": {"type": "string"},
        },
        "risks": {
            "type": "array",
            "description": "已知风险和边界条件，供 Worker 参考，不写入 task。",
            "items": {"type": "string"},
        },
        "work_packets": {
            "type": "array",
            "minItems": 1,
            "maxItems": 6,
            "description": "每个 packet 由一个 Codex Worker 独立完成。",
            "items": {
                "type": "object",
                "additionalProperties": False,
                "required": ["packet_id", "title", "objective", "scope", "test_file", "acceptance_checks"],
                "properties": {
                    "packet_id": {
                        "type": "string",
                        "description": "唯一标识，如 wp1, wp2。",
                    },
                    "title": {
                        "type": "string",
                        "description": "简短标题，描述这个包做了什么。",
                    },
                    "objective": {
                        "type": "string",
                        "description": "详细描述 Worker 要做什么。用业务语言，不是技术步骤。",
                    },
                    "scope": {
                        "type": "array",
                        "description": "Worker 可以修改的文件路径列表（可含通配符）。",
                        "items": {"type": "string"},
                    },
                    "test_file": {
                        "type": "string",
                        "description": (
                            "Worker 需要创建或更新的 acceptance test 文件路径（相对于项目根目录）。"
                            "必须是可独立运行的 unittest / pytest 文件。"
                            "运行结果（PASS/FAIL）是 Leader 验收的唯一依据。"
                        ),
                    },
                    "acceptance_checks": {
                        "type": "array",
                        "description": "该 packet 必须转成测试断言的 2-6 条具体检查项。",
                        "minItems": 1,
                        "items": {"type": "string"},
                    },
                },
            },
        },
    },
}


# Worker 的输出结构：原始执行证据，不做任何主观总结。
WORKER_SCHEMA: dict[str, Any] = {
    "type": "object",
    "additionalProperties": False,
    "required": ["status", "changed_files", "test_results", "evidence"],
    "properties": {
        "status": {
            "type": "string",
            "enum": ["completed", "blocked", "failed"],
            "description": "completed=任务完成且测试通过；blocked=有障碍；failed=执行失败。",
        },
        "changed_files": {
            "type": "array",
            "description": "实际修改的文件列表（相对于项目根目录）。",
            "items": {"type": "string"},
        },
        "test_results": {
            "type": "array",
            "description": "每条 acceptance test 的原始运行结果。Leader 用这个判定 PASS/FAIL。",
            "items": {
                "type": "object",
                "additionalProperties": False,
                "required": ["name", "passed", "output"],
                "properties": {
                    "name": {"type": "string"},
                    "passed": {"type": "boolean"},
                    "output": {"type": "string", "description": "测试的标准输出或错误信息（截取最后 500 字符）。"},
                },
            },
        },
        "evidence": {
            "type": "array",
            "description": "阻塞或失败时提供诊断信息。正常时为空列表。",
            "items": {"type": "string"},
        },
    },
}


# ---------------------------------------------------------------------------
# Dataclass
# ---------------------------------------------------------------------------

@dataclass
class CodexRunResult:
    phase: str
    name: str
    returncode: int
    session_id: str | None
    response: dict[str, Any]
    output_path: Path
    stdout_path: Path
    stderr_path: Path


# ---------------------------------------------------------------------------
# Codex binary resolution
# ---------------------------------------------------------------------------

def _resolve_codex_bin() -> str:
    explicit = os.environ.get("CODEX_BIN")
    if explicit:
        path = Path(explicit).expanduser()
        if path.is_file() and os.access(path, os.X_OK):
            return str(path)

    discovered = shutil.which("codex")
    if discovered:
        return discovered

    candidates = [
        Path("/opt/homebrew/bin/codex"),
        Path("/usr/local/bin/codex"),
        Path.home() / ".local" / "bin" / "codex",
    ]
    candidates.extend(
        Path.home().glob(".vscode/extensions/openai.chatgpt-*/bin/macos-aarch64/codex")
    )
    for candidate in candidates:
        if candidate.is_file() and os.access(candidate, os.X_OK):
            return str(candidate)

    searched = [str(path) for path in candidates]
    raise RuntimeError(
        "Codex CLI not found. Set CODEX_BIN or add codex to PATH. "
        f"Searched PATH={os.environ.get('PATH', '')} and fallback locations={searched}."
    )


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def delegate_codex(
    *,
    task: str,
    project_root: Path,
    workers: int = 4,
    planner_model: str | None = None,
    worker_model: str | None = None,
    dry_run: bool = False,
    stage_timeout_seconds: int = 1800,
) -> dict[str, Any]:
    """
    老板模式委托入口。

    Boss（用户）只需传入意图字符串 task，后续全由 Leader（Claude）和
    Worker（Codex）完成。Leader 负责组织和验收，不亲自写代码。

    返回结构包含：
      - run_dir: 这次运行的工件目录
      - status: "all_pass" | "needs_retry" | "blocked"
      - decision: Leader 的最终 PASS/FAIL 判定及理由
      - 如果 needs_retry: leader_retry_task 是下一步要发下去的文字
    """
    workers = max(1, min(3, workers))
    planner_model = planner_model or "gpt-5.4-mini"
    run_dir = _build_run_dir(project_root)
    schemas_dir = run_dir / "schemas"
    planner_dir = run_dir / "planner"
    workers_dir = run_dir / "workers"
    for path in (schemas_dir, planner_dir, workers_dir):
        path.mkdir(parents=True, exist_ok=True)

    task_path = run_dir / "task.txt"
    task_path.write_text(task, encoding="utf-8")

    plan_schema_path = schemas_dir / "plan.schema.json"
    worker_schema_path = schemas_dir / "worker.schema.json"
    _write_json(plan_schema_path, PLAN_SCHEMA)
    _write_json(worker_schema_path, WORKER_SCHEMA)

    _write_json(
        run_dir / "metadata.json",
        {
            "task": task,
            "project_root": str(project_root),
            "workers_requested": workers,
            "created_at": datetime.now().isoformat(timespec="seconds"),
            "boss": "用户（只写意图）",
            "leader": "Claude（组织策划 + 验收）",
            "workers": "Codex（执行 + 写测试）",
        },
    )

    if dry_run:
        # 纯离线：只生成 prompt 文件，不调用 Codex
        prompts = {
            "leader": _build_leader_prompt(
                task=task, project_root=project_root, max_packets=workers
            ),
            "worker_template": _build_worker_prompt(
                plan={"outcome": "", "done_means": [], "risks": []},
                packet={
                    "packet_id": "wp1",
                    "title": "",
                    "objective": "",
                    "scope": [],
                    "test_file": "tests/test_<packet_id>.py",
                    "acceptance_checks": ["<observable assertion>"],
                },
                project_root=project_root,
                run_dir=run_dir,
                iteration=1,
            ),
        }
        _write_json(run_dir / "dry_run_prompts.json", prompts)
        return {
            "run_dir": str(run_dir),
            "dry_run": True,
            "prompts_file": str(run_dir / "dry_run_prompts.json"),
        }

    # ── 步骤 1：Leader 把 Boss 的意图转成执行计划 ──────────────────────────
    planner = _run_codex_exec(
        phase="leader",
        name="leader",
        project_root=project_root,
        work_dir=planner_dir,
        prompt=_build_leader_prompt(task=task, project_root=project_root, max_packets=workers),
        schema_path=plan_schema_path,
        sandbox="read-only",
        model=planner_model,
        timeout_seconds=stage_timeout_seconds,
    )
    plan = planner.response
    _write_json(run_dir / "plan.json", plan)

    _validate_plan_schema(plan, plan_schema_path)

    # ── 步骤 2：Worker 并行执行各自的任务包 ──────────────────────────────────
    iteration_dir = workers_dir / "iteration_1"
    iteration_dir.mkdir(parents=True, exist_ok=True)
    worker_results = _run_worker_batch(
        plan=plan,
        project_root=project_root,
        run_dir=run_dir,
        work_dir=iteration_dir,
        schema_path=worker_schema_path,
        model=worker_model,
        iteration=1,
        timeout_seconds=stage_timeout_seconds,
    )
    _write_json(run_dir / "worker_results.json", worker_results)

    # ── 步骤 3：Leader 读测试结果，判定 PASS / FAIL ─────────────────────────
    decision = _leader_decide(
        plan=plan,
        worker_results=worker_results,
        run_dir=run_dir,
    )
    _write_json(run_dir / "decision.json", decision)

    all_pass = decision["status"] == "all_pass"
    summary = {
        "run_dir": str(run_dir),
        "status": decision["status"],
        "decision": decision,
        "plan_path": str(run_dir / "plan.json"),
        "worker_results_path": str(run_dir / "worker_results.json"),
        "decision_path": str(run_dir / "decision.json"),
        "worker_status_counts": _count_worker_statuses(worker_results),
    }
    _write_json(run_dir / "summary.json", summary)
    return summary


# ---------------------------------------------------------------------------
# Leader 决策逻辑（纯读取，不自己跑验证）
# ---------------------------------------------------------------------------

def _leader_decide(
    plan: dict[str, Any],
    worker_results: list[dict[str, Any]],
    run_dir: Path,
) -> dict[str, Any]:
    """
    Leader 验收的核心规则：
      - 遍历所有 Worker 的 test_results
      - 全部 passed=True → all_pass
      - 任一 passed=False → needs_retry（自动生成 retry task）
      - 任一 status=blocked → blocked
      - 任一 status=failed → blocked
    不自己读文件、不自己运行 Python snippet。
    """
    failures: list[str] = []
    blocked_reasons: list[str] = []

    for item in worker_results:
        packet_id = item["packet_id"]
        result = item["result"]

        if result["status"] == "failed":
            blocked_reasons.append(f"[{packet_id}] 执行失败：{' '.join(result.get('evidence', []))}")
            continue
        if result["status"] == "blocked":
            blocked_reasons.append(f"[{packet_id}] 被阻塞：{' '.join(result.get('evidence', []))}")
            continue

        # status == completed：检查 test_results
        for test in result.get("test_results", []):
            if not test["passed"]:
                failures.append(
                    f"[{packet_id}] {test['name']} 未通过：{test['output'][-300:]}"
                )

    if blocked_reasons:
        return {
            "status": "blocked",
            "pass": False,
            "reason": "Worker 遇到阻塞",
            "details": blocked_reasons,
            "leader_retry_task": None,
        }

    if failures:
        retry_task = _build_retry_task(plan, worker_results, failures)
        return {
            "status": "needs_retry",
            "pass": False,
            "reason": f"{len(failures)} 条 acceptance test 未通过",
            "failures": failures,
            "leader_retry_task": retry_task,
        }

    return {
        "status": "all_pass",
        "pass": True,
        "reason": "所有 Worker 全部 test passed",
        "done_means": plan.get("done_means", []),
        "leader_retry_task": None,
    }


def _build_retry_task(
    plan: dict[str, Any],
    worker_results: list[dict[str, Any]],
    failures: list[str],
) -> str:
    """把 test failures 自动转成一条精简的 retry task 文字。"""
    lines = [
        f"上一轮未通过，需要修复。",
        f"失败原因：",
    ]
    for f in failures:
        lines.append(f"  - {f}")
    lines.append("")
    lines.append("验收条件（不变）：")
    for d in plan.get("done_means", []):
        lines.append(f"  - {d}")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Worker batch
# ---------------------------------------------------------------------------

def _run_worker_batch(
    *,
    plan: dict[str, Any],
    project_root: Path,
    run_dir: Path,
    work_dir: Path,
    schema_path: Path,
    model: str | None,
    iteration: int,
    timeout_seconds: int,
) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    futures = {}
    packets = list(plan["work_packets"])
    worker_count = max(1, len(packets))

    with ThreadPoolExecutor(max_workers=worker_count) as executor:
        for packet in packets:
            packet_dir = work_dir / packet["packet_id"]
            packet_dir.mkdir(parents=True, exist_ok=True)
            future = executor.submit(
                _run_codex_exec,
                phase="worker",
                name=packet["packet_id"],
                project_root=project_root,
                work_dir=packet_dir,
                prompt=_build_worker_prompt(
                    plan=plan,
                    packet=packet,
                    project_root=project_root,
                    run_dir=run_dir,
                    iteration=iteration,
                ),
                schema_path=schema_path,
                sandbox="workspace-write",
                model=model,
                timeout_seconds=timeout_seconds,
            )
            futures[future] = packet

        for future in as_completed(futures):
            packet = futures[future]
            result = future.result()
            results.append(
                {
                    "packet_id": packet["packet_id"],
                    "title": packet["title"],
                    "result": result.response,
                    "artifact": str(result.output_path),
                    "stdout": str(result.stdout_path),
                    "stderr": str(result.stderr_path),
                    "session_id": result.session_id,
                }
            )

    results.sort(key=lambda item: item["packet_id"])
    return results


# ---------------------------------------------------------------------------
# Codex exec
# ---------------------------------------------------------------------------

def _run_codex_exec(
    *,
    phase: str,
    name: str,
    project_root: Path,
    work_dir: Path,
    prompt: str,
    schema_path: Path,
    sandbox: str,
    model: str | None,
    timeout_seconds: int,
) -> CodexRunResult:
    work_dir.mkdir(parents=True, exist_ok=True)
    prompt_path = work_dir / "prompt.txt"
    output_path = work_dir / "response.json"
    stdout_path = work_dir / "stdout.log"
    stderr_path = work_dir / "stderr.log"
    command_path = work_dir / "command.json"

    prompt_path.write_text(prompt, encoding="utf-8")
    codex_bin = _resolve_codex_bin()

    command = [
        codex_bin,
        "exec",
        "--skip-git-repo-check",
        "-C",
        str(project_root),
        "--sandbox",
        sandbox,
        "--color",
        "never",
        "--output-schema",
        str(schema_path),
        "--output-last-message",
        str(output_path),
        prompt,
    ]
    if model:
        command[2:2] = ["--model", model]

    _write_json(command_path, {"command": command, "phase": phase, "name": name})
    try:
        with stdout_path.open("w", encoding="utf-8") as stdout_handle, stderr_path.open(
            "w", encoding="utf-8"
        ) as stderr_handle:
            completed = subprocess.run(
                command,
                cwd=project_root,
                stdout=stdout_handle,
                stderr=stderr_handle,
                text=True,
                timeout=timeout_seconds,
            )
    except subprocess.TimeoutExpired as exc:
        raise RuntimeError(
            f"Codex {phase} '{name}' 超时（{timeout_seconds}s）。"
            f"见 {stdout_path} 和 {stderr_path}。"
        ) from exc

    if completed.returncode != 0:
        raise RuntimeError(
            f"Codex {phase} '{name}' 退出码 {completed.returncode}。"
            f"见 {stdout_path} 和 {stderr_path}。"
        )

    response = json.loads(output_path.read_text(encoding="utf-8"))
    stdout_text = stdout_path.read_text(encoding="utf-8")
    return CodexRunResult(
        phase=phase,
        name=name,
        returncode=completed.returncode,
        session_id=_extract_session_id(stdout_text),
        response=response,
        output_path=output_path,
        stdout_path=stdout_path,
        stderr_path=stderr_path,
    )


# ---------------------------------------------------------------------------
# Prompt 构建
# ---------------------------------------------------------------------------

def _build_leader_prompt(
    *,
    task: str,
    project_root: Path,
    max_packets: int,
) -> str:
    """
    Leader（Claude）的 prompt。
    职责：把 Boss（用户）的意图转成可执行计划。

    Leader 要做的事：
      1. 理解 task 中的业务意图
      2. 定义验收条件（done_means）
      3. 拆成 1~max_packets 个 Worker 包
      4. 每个包必须包含：要创建的 acceptance test 文件路径 + 该测试要覆盖的 acceptance_checks
    Leader 不写代码，不改文件，不运行测试。
    """
    return f"""
你是这个交付循环的 Leader（组织者），不是执行者。

你的 Boss（用户）给了你这个任务：

---
{task}
---

## 你的职责（只有以下三件事）

1. **定义验收条件（done_means）**
   - 每条必须是可判定的（测得出 PASS/FAIL），不是含糊的方向描述
   - 用结果语言，不写实现步骤
   - 例如："trend_score_from_parts(ret_5=0.12) 的得分 < trend_score_from_parts(ret_5=0.08)"
   - 例如："build_prediction_bundle() 返回的 PredictionBundle 包含 intraday/dayend/nextday/longterm 四个字段"

2. **拆成 {max_packets} 个以内的工作包（work_packets）**
   - 每个包 = 一个 Codex Worker 的独立任务
   - **优先拆细**：如果任务涉及多个文件或多步逻辑，拆成 3-5 个小包，而不是塞给 1-2 个大包
   - scope 写明 Worker 可以改哪些文件
   - 同一文件原则上不跨包，防止冲突
   - **每个包的预计完成时间应控制在 10-15 分钟内**，避免超时

3. **给每个包定义 acceptance test 规格**
   - test_file 是 Worker 需要创建或更新的 unittest / pytest 文件路径
   - acceptance_checks 写 2-6 条该包自己的可观测断言，Worker 会据此落成测试文件
   - 测试必须覆盖该包负责的 done_means 子集
   - 测试文件放在 tests/ 目录下，用有意义的命名
   - 优先设计可单独运行的 targeted test，不要依赖全量测试发现
   - 不要写 mock 测试；写真正能验证行为的测试

## 约束

- 输出必须是纯 JSON，符合提供的 schema
- 不要加 markdown fence
- 不要写代码实现，只写计划
- outcome 用一句话描述最终目标（可观测结果）
- risks 可选，写给 Worker 参考的边界条件，不写入 Boss 的 task

## 拆包指导

**关键原则**：每个 Worker 只能改 1-2 个相关文件，不能超过 3 个。如果 scope 需要改 3 个以上的文件，必须拆成多个包。

**Worker 有 30 分钟超时限制。** 如果任务涉及以下情况，必须拆成多个小包：

- 新增多个独立文件（如 dataclass、engine、CLI 命令）
- 修改多个独立模块（如 advice_harness、evaluation_harness、skill_harness）
- 需要写多个测试文件

**推荐的拆包维度**：
- 按模块拆：advice_harness 改动 / evaluation_harness 改动 / CLI + reporting
- 按功能拆：数据层 / 业务逻辑层 / 展示层
- 按文件拆：每个 Worker 只改 1-2 个相关文件

**示例**：
- ❌ 错误：一个包同时改 advice_harness、evaluation_harness、orchestrator 三个模块
- ✅ 正确：拆成 3 个包，每个包只改一个模块

## 项目上下文

Project root: {project_root}
Worker 会在 sandbox=workspace-write 模式下执行，可以创建和修改 scope 中的文件，以及自己负责的 test_file。
Worker 返回的 test_results 是 Leader 验收的唯一依据。

请先快速浏览项目结构（README.md 等），再产出计划。
""".strip()


def _build_worker_prompt(
    *,
    plan: dict[str, Any],
    packet: dict[str, Any],
    project_root: Path,
    run_dir: Path,
    iteration: int,
) -> str:
    """
    Worker（Codex）的 prompt。
    职责：执行 + 写/运行测试 + 返回原始证据。

    重要：Worker 要做两件事
      1. 按 objective + scope 改代码
      2. 按 test_file 写测试（如果文件还不存在）并运行它
    """
    test_file = packet.get("test_file", "")
    test_module = test_file[:-3].replace("/", ".") if test_file.endswith(".py") else ""
    packet_json = json.dumps(packet, ensure_ascii=False, indent=2)
    plan_frag = json.dumps(
        {
            "outcome": plan.get("outcome", ""),
            "done_means": plan.get("done_means", []),
            "risks": plan.get("risks", []),
        },
        ensure_ascii=False,
        indent=2,
    )
    return f"""
你是这个交付循环的 Worker（执行者），负责完成分配给你的任务包。

Project root: {project_root}
Run directory: {run_dir}
Iteration: {iteration}

## Leader 制定的大目标
{plan_frag}

## Leader 分配给你的包
{packet_json}

## 你要做的事（按顺序执行）

### 第一步：执行 objective
- 严格在 scope 范围内改文件
- 不要动其他 Worker 负责的文件
- 不要做 scope 以外的改动
- **例外**：你可以创建或修改自己的 acceptance test 文件 `{test_file}`

### 第二步：先写 acceptance test，再运行
- test_file: {test_file or "（未指定，需自己写）"}
- 你必须根据 `acceptance_checks` 创建或更新这个 test_file；不要假设它已经存在
- 优先写 **可单独运行的 targeted test**，避免依赖整个测试套件
- 优先运行命令（选最合适的）：
    PYTHONPATH=src python3 -m unittest {test_module} -v
    PYTHONPATH=src python3 {test_file}
    PYTHONPATH=src python3 -m pytest {test_file} -v
- **不要默认跑 `unittest discover`**；只有在 targeted test 无法覆盖 acceptance_checks 时才作为补充
- 把每条你实际执行的 targeted test 的 passed/output 填入 test_results

### 第三步：返回原始结果
- status: completed（全部通过）/ blocked（有障碍）/ failed（执行失败）
- 只要你自己创建/运行的 targeted acceptance tests 全部通过，就应该返回 completed
- 如果全量测试或环境里有与本 packet 无关的错误，把它们写进 evidence，不要因此把 targeted-pass 的任务降级成 blocked
- 不要写主观总结，只列 changed_files + test_results + evidence
- 返回纯 JSON，符合 schema

## 约束
- 不要加 markdown fence
- 只动 scope 里的文件，以及 `{test_file}`
- test_results 里每个条目：{{"name": "...", "passed": bool, "output": "..."}}
- output 截取测试运行输出最后 500 字符即可
""".strip()


# ---------------------------------------------------------------------------
# 工具
# ---------------------------------------------------------------------------

def _validate_plan_schema(plan: dict[str, Any], schema_path: Path) -> None:
    """如果 Leader 输出的 plan 不符合 schema，立��抛出。"""
    import jsonschema

    try:
        jsonschema.validate(plan, json.loads(schema_path.read_text(encoding="utf-8")))
    except jsonschema.ValidationError as exc:
        raise RuntimeError(
            f"Leader 输出的 plan 不符合 schema，delegate-codex 无法继续：\n{exc.message}"
        ) from exc


def _build_run_dir(project_root: Path) -> Path:
    stamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    return project_root / "data" / "output" / "codex_delegate" / stamp


def _extract_session_id(stdout: str) -> str | None:
    marker = "session id:"
    for line in stdout.splitlines():
        if marker in line.lower():
            return line.split(":", 1)[1].strip()
    return None


def _write_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _count_worker_statuses(worker_results: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {"completed": 0, "blocked": 0, "failed": 0}
    for item in worker_results:
        counts[item["result"]["status"]] = counts.get(item["result"]["status"], 0) + 1
    return counts
