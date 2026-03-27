from __future__ import annotations

from pathlib import Path
from typing import Any

from ..models import SessionManifest
from ..utils import append_jsonl, ensure_dir, iso_now, repo_relative_env_path, timestamp_slug, write_json


class RuntimeSession:
    def __init__(self, *, runtime_dir: str | Path, as_of: str, session_type: str) -> None:
        self.run_id = f"{session_type}_{timestamp_slug()}"
        self.root = ensure_dir(Path(runtime_dir) / as_of / self.run_id)
        self.manifest = SessionManifest(
            run_id=self.run_id,
            as_of=as_of,
            session_type=session_type,
            created_at=iso_now(),
            cwd=repo_relative_env_path(),
            status="running",
            artifacts={},
        )
        self.log_path = self.root / "logs.jsonl"
        self.manifest_path = self.root / "manifest.json"
        write_json(self.manifest_path, self.manifest.to_dict())

    def log(self, event: str, **payload: Any) -> None:
        append_jsonl(self.log_path, [{"ts": iso_now(), "event": event, **payload}])

    def add_artifact(self, key: str, path: str | Path) -> None:
        self.manifest.artifacts[key] = str(path)
        write_json(self.manifest_path, self.manifest.to_dict())

    def finish(self, *, status: str = "completed", **metadata: Any) -> None:
        self.manifest.status = status
        self.manifest.metadata.update(metadata)
        write_json(self.manifest_path, self.manifest.to_dict())
