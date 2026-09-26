"""统计分析准入服务的领域用例。"""

from __future__ import annotations

import json
import sqlite3
from datetime import timedelta
from decimal import Decimal
from typing import Any, Iterable, Mapping

from .analysis import ALGORITHM_VERSION, analyze
from .clock import SystemClock, isoformat
from .contracts import Observation, Protocol, ValidationError
from .errors import Conflict, Forbidden, InvalidState, LeaseConflict, NotFound, ValidationFailed
from .jsonio import canonical_json, content_digest
from .storage import initialize, transaction


ROLE_PERMISSIONS = {
    "operator": {
        "catalog.write", "batch.create", "batch.start", "observation.import",
        "exclusion.request", "exclusion.revoke",
    },
    "statistician": {"protocol.publish", "batch.seal", "exclusion.review", "analysis.run"},
    "approver": {"decision.write"},
    "auditor": {"report.read", "audit.read"},
}


class TrialService:
    """在单个 SQLite 连接上提供全部业务操作。"""

    def __init__(self, connection: sqlite3.Connection, clock=None) -> None:
        self.connection = connection
        self.clock = clock or SystemClock()
        initialize(connection)

    def _now(self) -> str:
        return isoformat(self.clock.now())

    def _user(self, user_id: str) -> sqlite3.Row:
        row = self.connection.execute(
            "SELECT user_id, display_name, role, active FROM users WHERE user_id=?", (user_id,)
        ).fetchone()
        if row is None:
            raise NotFound(f"用户不存在: {user_id}")
        if not row["active"]:
            raise Forbidden("用户已停用")
        return row

    def _require(self, user_id: str, permission: str) -> sqlite3.Row:
        user = self._user(user_id)
        if permission not in ROLE_PERMISSIONS[user["role"]]:
            raise Forbidden(f"角色 {user['role']} 无权执行 {permission}")
        return user

    def _audit(
        self, entity_type: str, entity_id: str, event_type: str, actor_id: str, payload: Mapping[str, Any]
    ) -> None:
        self.connection.execute(
            "INSERT INTO audit_events(entity_type,entity_id,event_type,actor_id,payload_json,created_at) "
            "VALUES(?,?,?,?,?,?)",
            (entity_type, entity_id, event_type, actor_id, canonical_json(payload), self._now()),
        )

    def create_user(self, user_id: str, display_name: str, role: str) -> dict[str, Any]:
        if role not in ROLE_PERMISSIONS:
            raise ValidationFailed(f"未知角色: {role}")
        if not user_id.strip() or not display_name.strip():
            raise ValidationFailed("用户编号和名称不能为空")
        try:
            with transaction(self.connection, immediate=True):
                self.connection.execute(
                    "INSERT INTO users(user_id,display_name,role) VALUES(?,?,?)",
                    (user_id.strip(), display_name.strip(), role),
                )
        except sqlite3.IntegrityError as exc:
            raise Conflict(f"用户已存在: {user_id}") from exc
        return {"user_id": user_id.strip(), "role": role}

    def register_robot(
        self, actor_id: str, robot_id: str, model_name: str, vendor: str
    ) -> dict[str, Any]:
        self._require(actor_id, "catalog.write")
        try:
            with transaction(self.connection, immediate=True):
                self.connection.execute(
                    "INSERT INTO robots(robot_id,model_name,vendor,created_at) VALUES(?,?,?,?)",
                    (robot_id, model_name, vendor, self._now()),
                )
                self._audit("robot", robot_id, "robot.registered", actor_id, {"model_name": model_name})
        except sqlite3.IntegrityError as exc:
            raise Conflict(f"传感器已存在: {robot_id}") from exc
        return {"robot_id": robot_id, "model_name": model_name, "vendor": vendor}

    def register_build(
        self, actor_id: str, build_id: str, robot_id: str, version: str, content_sha256: str
    ) -> dict[str, Any]:
        self._require(actor_id, "catalog.write")
        if len(content_sha256) != 64:
            raise ValidationFailed("构建摘要必须是 64 位 SHA-256")
        try:
            with transaction(self.connection, immediate=True):
                self.connection.execute(
                    "INSERT INTO builds(build_id,robot_id,version,content_sha256,created_at) VALUES(?,?,?,?,?)",
                    (build_id, robot_id, version, content_sha256.lower(), self._now()),
                )
                self._audit("build", build_id, "build.registered", actor_id, {"robot_id": robot_id, "version": version})
        except sqlite3.IntegrityError as exc:
            raise Conflict("构建编号、版本或摘要冲突") from exc
        return {"build_id": build_id, "robot_id": robot_id, "version": version}

    def publish_protocol(self, actor_id: str, raw: Mapping[str, Any]) -> dict[str, Any]:
        self._require(actor_id, "protocol.publish")
        try:
            protocol = Protocol.from_dict(raw)
        except ValidationError as exc:
            raise ValidationFailed(str(exc)) from exc
        text = canonical_json(raw)
        digest = content_digest([raw])
        try:
            with transaction(self.connection, immediate=True):
                self.connection.execute(
                    "INSERT INTO protocol_catalog(protocol_id,version,title,task_family,canonical_json,content_sha256,created_at) "
                    "VALUES(?,?,?,?,?,?,?)",
                    (
                        protocol.protocol_id,
                        protocol.version,
                        protocol.title,
                        protocol.task_family,
                        text,
                        digest,
                        self._now(),
                    ),
                )
                identity = f"{protocol.protocol_id}@{protocol.version}"
                self._audit("protocol", identity, "protocol.published", actor_id, {"sha256": digest})
        except sqlite3.IntegrityError as exc:
            raise Conflict("协议版本或内容摘要已经存在") from exc
        return {"protocol_id": protocol.protocol_id, "version": protocol.version, "sha256": digest}

    def _protocol(self, protocol_id: str, version: int) -> tuple[Protocol, str]:
        row = self.connection.execute(
            "SELECT canonical_json,content_sha256 FROM protocol_catalog WHERE protocol_id=? AND version=?",
            (protocol_id, version),
        ).fetchone()
        if row is None:
            raise NotFound("协议版本不存在")
        return Protocol.from_dict(json.loads(row["canonical_json"])), row["content_sha256"]

    def create_batch(
        self,
        actor_id: str,
        batch_id: str,
        protocol_id: str,
        protocol_version: int,
        build_id: str,
    ) -> dict[str, Any]:
        self._require(actor_id, "batch.create")
        self._protocol(protocol_id, protocol_version)
        try:
            with transaction(self.connection, immediate=True):
                self.connection.execute(
                    "INSERT INTO batches(batch_id,protocol_id,protocol_version,build_id,state,created_by,created_at) "
                    "VALUES(?,?,?,?,?,?,?)",
                    (batch_id, protocol_id, protocol_version, build_id, "draft", actor_id, self._now()),
                )
                self._audit("batch", batch_id, "batch.created", actor_id, {"build_id": build_id})
        except sqlite3.IntegrityError as exc:
            raise Conflict("批次编号冲突或构建不存在") from exc
        return self.get_batch(batch_id)

    def get_batch(self, batch_id: str) -> dict[str, Any]:
        row = self.connection.execute("SELECT * FROM batches WHERE batch_id=?", (batch_id,)).fetchone()
        if row is None:
            raise NotFound("批次不存在")
        return dict(row)

    def start_batch(self, actor_id: str, batch_id: str, expected_revision: int) -> dict[str, Any]:
        self._require(actor_id, "batch.start")
        with transaction(self.connection, immediate=True):
            cursor = self.connection.execute(
                "UPDATE batches SET state='running',revision=revision+1,started_at=? "
                "WHERE batch_id=? AND state='draft' AND revision=?",
                (self._now(), batch_id, expected_revision),
            )
            if cursor.rowcount != 1:
                raise InvalidState("批次不是当前草稿版本")
            self._audit("batch", batch_id, "batch.started", actor_id, {"from_revision": expected_revision})
        return self.get_batch(batch_id)

    def _idempotent_response(self, scope: str, key: str, request_digest: str) -> dict[str, Any] | None:
        row = self.connection.execute(
            "SELECT request_sha256,response_json FROM idempotency_keys WHERE scope=? AND key=?",
            (scope, key),
        ).fetchone()
        if row is None:
            return None
        if row["request_sha256"] != request_digest:
            raise Conflict("同一幂等键对应了不同请求内容")
        return json.loads(row["response_json"])

    def import_observations(
        self,
        actor_id: str,
        batch_id: str,
        idempotency_key: str,
        raw_rows: Iterable[Mapping[str, Any]],
    ) -> dict[str, Any]:
        self._require(actor_id, "observation.import")
        rows = tuple(raw_rows)
        if not rows:
            raise ValidationFailed("测点数组不能为空")
        request_digest = content_digest(rows)
        scope = f"observations:{batch_id}"
        existing = self._idempotent_response(scope, idempotency_key, request_digest)
        if existing is not None:
            return existing
        batch = self.get_batch(batch_id)
        if batch["state"] != "running":
            raise InvalidState("只有运行中的批次可以导入测点")
        protocol, _ = self._protocol(batch["protocol_id"], batch["protocol_version"])
        parsed: list[Observation] = []
        for raw in rows:
            try:
                item = Observation.from_dict(raw, protocol)
            except ValidationError as exc:
                raise ValidationFailed(str(exc)) from exc
            if item.robot_id != self.connection.execute(
                "SELECT robot_id FROM builds WHERE build_id=?", (batch["build_id"],)
            ).fetchone()["robot_id"]:
                raise ValidationFailed("测点传感器与批次构建不一致")
            parsed.append(item)
        response = {"batch_id": batch_id, "inserted": len(parsed), "request_sha256": request_digest}
        try:
            with transaction(self.connection, immediate=True):
                for item, raw in zip(parsed, rows):
                    self.connection.execute(
                        "INSERT INTO observations(batch_id,source_batch,source_row,robot_id,stratum_key,observed_at," 
                        "metrics_json,content_sha256,imported_by,imported_at) VALUES(?,?,?,?,?,?,?,?,?,?)",
                        (
                            batch_id,
                            item.source_batch,
                            item.source_row,
                            item.robot_id,
                            item.stratum_key,
                            item.observed_at,
                            canonical_json({key: format(value, "f") for key, value in item.metrics.items()}),
                            content_digest([raw]),
                            actor_id,
                            self._now(),
                        ),
                    )
                self.connection.execute(
                    "INSERT INTO idempotency_keys(scope,key,request_sha256,response_json,created_at) VALUES(?,?,?,?,?)",
                    (scope, idempotency_key, request_digest, canonical_json(response), self._now()),
                )
                self._audit("batch", batch_id, "observations.imported", actor_id, response)
        except sqlite3.IntegrityError as exc:
            raise Conflict("来源行重复或幂等键并发冲突") from exc
        return response

    def request_exclusion(self, actor_id: str, observation_id: int, reason: str) -> dict[str, Any]:
        self._require(actor_id, "exclusion.request")
        observation = self.connection.execute(
            "SELECT observation_id,batch_id FROM observations WHERE observation_id=?", (observation_id,)
        ).fetchone()
        if observation is None:
            raise NotFound("测点不存在")
        try:
            with transaction(self.connection, immediate=True):
                cursor = self.connection.execute(
                    "INSERT INTO exclusion_requests(observation_id,status,reason,requested_by,requested_at) "
                    "VALUES(?,?,?,?,?)",
                    (observation_id, "pending", reason, actor_id, self._now()),
                )
                exclusion_id = cursor.lastrowid
                self._audit("observation", str(observation_id), "exclusion.requested", actor_id, {"reason": reason})
        except sqlite3.IntegrityError as exc:
            raise Conflict("该测点已有待处理或生效排除") from exc
        return {"exclusion_id": exclusion_id, "status": "pending"}

    def review_exclusion(
        self, actor_id: str, exclusion_id: int, approve: bool, note: str
    ) -> dict[str, Any]:
        self._require(actor_id, "exclusion.review")
        row = self.connection.execute(
            "SELECT * FROM exclusion_requests WHERE exclusion_id=?", (exclusion_id,)
        ).fetchone()
        if row is None:
            raise NotFound("排除申请不存在")
        if row["status"] != "pending":
            raise InvalidState("排除申请已经处理")
        if row["requested_by"] == actor_id:
            raise Forbidden("申请人不能复核自己的排除申请")
        status = "approved" if approve else "rejected"
        with transaction(self.connection, immediate=True):
            self.connection.execute(
                "UPDATE exclusion_requests SET status=?,reviewed_by=?,reviewed_at=?,review_note=? "
                "WHERE exclusion_id=? AND status='pending'",
                (status, actor_id, self._now(), note, exclusion_id),
            )
            self._audit("exclusion", str(exclusion_id), f"exclusion.{status}", actor_id, {"note": note})
        return {"exclusion_id": exclusion_id, "status": status}

    def revoke_exclusion(self, actor_id: str, exclusion_id: int, reason: str) -> dict[str, Any]:
        self._require(actor_id, "exclusion.revoke")
        row = self.connection.execute(
            "SELECT e.*,o.batch_id FROM exclusion_requests e "
            "JOIN observations o ON o.observation_id=e.observation_id WHERE e.exclusion_id=?",
            (exclusion_id,),
        ).fetchone()
        if row is None:
            raise NotFound("排除记录不存在")
        if row["status"] != "approved":
            raise InvalidState("只有已批准的排除可以撤销")
        if row["requested_by"] != actor_id:
            raise Forbidden("只有原申请人可以撤销排除")
        batch = self.get_batch(row["batch_id"])
        if batch["state"] != "running":
            raise InvalidState("批次封存后不能改变排除状态")
        with transaction(self.connection, immediate=True):
            cursor = self.connection.execute(
                "UPDATE exclusion_requests SET status='revoked',review_note=?,reviewed_at=? "
                "WHERE exclusion_id=? AND status='approved'",
                (reason, self._now(), exclusion_id),
            )
            if cursor.rowcount != 1:
                raise InvalidState("排除状态已变化")
            self._audit(
                "observation",
                str(row["observation_id"]),
                "exclusion.revoked",
                actor_id,
                {"exclusion_id": exclusion_id, "reason": reason},
            )
        return {"exclusion_id": exclusion_id, "status": "revoked"}

    def seal_batch(self, actor_id: str, batch_id: str, expected_revision: int) -> dict[str, Any]:
        self._require(actor_id, "batch.seal")
        with transaction(self.connection, immediate=True):
            pending = self.connection.execute(
                "SELECT count(*) FROM exclusion_requests e JOIN observations o ON o.observation_id=e.observation_id "
                "WHERE o.batch_id=? AND e.status='pending'", (batch_id,)
            ).fetchone()[0]
            if pending:
                raise InvalidState("仍有待复核的排除申请")
            cursor = self.connection.execute(
                "UPDATE batches SET state='sealed',revision=revision+1,sealed_at=? "
                "WHERE batch_id=? AND state='running' AND revision=?",
                (self._now(), batch_id, expected_revision),
            )
            if cursor.rowcount != 1:
                raise InvalidState("批次状态或版本已变化")
            new_revision = expected_revision + 1
            now = self._now()
            self.connection.execute(
                "INSERT INTO analysis_jobs(batch_id,batch_revision,state,available_at,created_at,updated_at) "
                "VALUES(?,?, 'queued', ?,?,?)",
                (batch_id, new_revision, now, now, now),
            )
            self._audit("batch", batch_id, "batch.sealed", actor_id, {"revision": new_revision})
        return self.get_batch(batch_id)

    def _input_snapshot(
        self, batch: Mapping[str, Any], protocol: Protocol
    ) -> tuple[tuple[Observation, ...], str]:
        """汇总批次当前测点快照与其内容摘要，作为租约栅栏的输入指纹。"""

        observations = self._analysis_observations(batch["batch_id"], protocol)
        snapshot_rows = [
            {
                "source_batch": item.source_batch,
                "source_row": item.source_row,
                "stratum": item.stratum_key,
                "metrics": {key: format(value, "f") for key, value in item.metrics.items()},
                "excluded_reason": item.excluded_reason,
            }
            for item in observations
        ]
        return observations, content_digest(snapshot_rows)

    def claim_job(self, worker_id: str, lease_seconds: int = 60) -> dict[str, Any] | None:
        if lease_seconds <= 0:
            raise ValidationFailed("租约时长必须大于零")
        now = self._now()
        expires = isoformat(self.clock.now() + timedelta(seconds=lease_seconds))
        with transaction(self.connection, immediate=True):
            row = self.connection.execute(
                "SELECT * FROM analysis_jobs WHERE "
                "(state='queued' AND available_at<=?) OR (state='leased' AND lease_expires_at<=?) "
                "ORDER BY available_at,job_id LIMIT 1",
                (now, now),
            ).fetchone()
            if row is None:
                return None
            batch = self.get_batch(row["batch_id"])
            protocol, _ = self._protocol(batch["protocol_id"], batch["protocol_version"])
            _, input_digest = self._input_snapshot(batch, protocol)
            takeover = row["state"] == "leased"
            if takeover:
                self._audit(
                    "analysis_job",
                    str(row["job_id"]),
                    "analysis_job.lease_expired",
                    worker_id,
                    {
                        "job_id": row["job_id"],
                        "batch_id": row["batch_id"],
                        "previous_owner": row["lease_owner"],
                        "previous_lease_epoch": row["lease_epoch"],
                        "lease_expires_at": row["lease_expires_at"],
                        "observed_at": now,
                        "input_sha256": input_digest,
                    },
                )
            cursor = self.connection.execute(
                "UPDATE analysis_jobs SET state='leased',attempts=attempts+1,lease_epoch=lease_epoch+1,"
                "lease_owner=?,lease_expires_at=?,input_sha256=?,updated_at=? "
                "WHERE job_id=? AND ((state='queued' AND available_at<=?) OR (state='leased' AND lease_expires_at<=?))",
                (worker_id, expires, input_digest, now, row["job_id"], now, now),
            )
            if cursor.rowcount != 1:
                raise Conflict("分析任务领取竞争失败，请重试")
            claimed = self.connection.execute(
                "SELECT * FROM analysis_jobs WHERE job_id=?", (row["job_id"],)
            ).fetchone()
            event_type = "analysis_job.taken_over" if takeover else "analysis_job.claimed"
            payload: dict[str, Any] = {
                "job_id": claimed["job_id"],
                "batch_id": claimed["batch_id"],
                "batch_revision": claimed["batch_revision"],
                "worker_id": worker_id,
                "attempts": claimed["attempts"],
                "lease_epoch": claimed["lease_epoch"],
                "lease_expires_at": expires,
                "input_sha256": input_digest,
            }
            if takeover:
                payload["previous_owner"] = row["lease_owner"]
                payload["previous_lease_epoch"] = row["lease_epoch"]
            self._audit("analysis_job", str(claimed["job_id"]), event_type, worker_id, payload)
        return dict(claimed)

    def _analysis_observations(self, batch_id: str, protocol: Protocol) -> tuple[Observation, ...]:
        rows = self.connection.execute(
            "SELECT o.*,e.reason AS excluded_reason FROM observations o "
            "LEFT JOIN exclusion_requests e ON e.observation_id=o.observation_id AND e.status='approved' "
            "WHERE o.batch_id=? ORDER BY o.observation_id",
            (batch_id,),
        ).fetchall()
        items: list[Observation] = []
        for row in rows:
            metrics = json.loads(row["metrics_json"])
            items.append(Observation(
                source_batch=row["source_batch"],
                source_row=row["source_row"],
                robot_id=row["robot_id"],
                protocol_id=protocol.protocol_id,
                protocol_version=protocol.version,
                stratum_key=row["stratum_key"],
                observed_at=row["observed_at"],
                metrics={key: Decimal(str(value)) for key, value in metrics.items()},
                excluded_reason=row["excluded_reason"],
            ))
        return tuple(items)

    def _fence_mismatches(
        self,
        job: sqlite3.Row,
        worker_id: str,
        lease_epoch: int,
        input_sha256: str,
        batch: Mapping[str, Any],
        current_digest: str,
        now: str,
    ) -> list[dict[str, Any]]:
        """逐项核对提交栅栏：领取者、租约代次、有效期限、任务修订号、输入摘要。"""

        mismatches: list[dict[str, Any]] = []
        if job["state"] != "leased":
            mismatches.append({"term": "state", "expected": "leased", "actual": job["state"]})
        if job["lease_owner"] != worker_id:
            mismatches.append({"term": "lease_owner", "expected": worker_id, "actual": job["lease_owner"]})
        if job["lease_epoch"] != lease_epoch:
            mismatches.append({"term": "lease_epoch", "expected": lease_epoch, "actual": job["lease_epoch"]})
        if job["lease_expires_at"] is None or job["lease_expires_at"] <= now:
            mismatches.append(
                {"term": "lease_expires_at", "expected": f"> {now}", "actual": job["lease_expires_at"]}
            )
        if job["batch_revision"] != batch["revision"]:
            mismatches.append(
                {"term": "batch_revision", "expected": batch["revision"], "actual": job["batch_revision"]}
            )
        if job["input_sha256"] != input_sha256 or current_digest != input_sha256:
            mismatches.append({
                "term": "input_sha256",
                "expected": current_digest,
                "actual": input_sha256,
                "claimed": job["input_sha256"],
            })
        return mismatches

    def _completion_replay(
        self, job: sqlite3.Row, worker_id: str, lease_epoch: int, input_sha256: str
    ) -> dict[str, Any]:
        """任务已成功：同一持有者重复相同提交时取回原响应，其余一律冲突。"""

        mismatches: list[dict[str, Any]] = []
        if job["lease_owner"] != worker_id:
            mismatches.append({"term": "lease_owner", "expected": worker_id, "actual": job["lease_owner"]})
        if job["lease_epoch"] != lease_epoch:
            mismatches.append({"term": "lease_epoch", "expected": lease_epoch, "actual": job["lease_epoch"]})
        if job["input_sha256"] != input_sha256:
            mismatches.append(
                {"term": "input_sha256", "expected": job["input_sha256"], "actual": input_sha256}
            )
        if mismatches:
            raise LeaseConflict(
                "分析任务已由其他租约完成，当前提交身份不一致",
                details={"job_id": job["job_id"], "state": "succeeded", "mismatches": mismatches},
            )
        row = self.connection.execute(
            "SELECT analysis_id,result_json FROM analyses WHERE batch_id=? AND batch_revision=? AND input_sha256=?",
            (job["batch_id"], job["batch_revision"], input_sha256),
        ).fetchone()
        if row is None:
            raise InvalidState("任务已成功但缺少对应分析记录")
        return {
            "analysis_id": row["analysis_id"],
            "input_sha256": input_sha256,
            "result": json.loads(row["result_json"]),
        }

    def complete_job(
        self, worker_id: str, job_id: int, statistician_id: str, lease_epoch: int, input_sha256: str
    ) -> dict[str, Any]:
        """提交分析结果；全部栅栏条件在同一个事务内核对并守卫写入。"""

        self._require(statistician_id, "analysis.run")
        now = self._now()
        with transaction(self.connection, immediate=True):
            job = self.connection.execute(
                "SELECT * FROM analysis_jobs WHERE job_id=?", (job_id,)
            ).fetchone()
            if job is None:
                raise NotFound("分析任务不存在")
            if job["state"] == "succeeded":
                return self._completion_replay(job, worker_id, lease_epoch, input_sha256)
            batch = self.get_batch(job["batch_id"])
            protocol, protocol_digest = self._protocol(batch["protocol_id"], batch["protocol_version"])
            observations, current_digest = self._input_snapshot(batch, protocol)
            mismatches = self._fence_mismatches(
                job, worker_id, lease_epoch, input_sha256, batch, current_digest, now
            )
            if mismatches:
                raise LeaseConflict(
                    "分析结果提交被拒绝：租约栅栏已落后",
                    details={"job_id": job_id, "mismatches": mismatches},
                )
            result = analyze(protocol, observations)
            commit_now = self._now()
            cursor = self.connection.execute(
                "UPDATE analysis_jobs SET state='succeeded',updated_at=? "
                "WHERE job_id=? AND state='leased' AND lease_owner=? AND lease_epoch=? "
                "AND lease_expires_at>? AND batch_revision=? AND input_sha256=?",
                (commit_now, job_id, worker_id, lease_epoch, commit_now, batch["revision"], input_sha256),
            )
            if cursor.rowcount != 1:
                raise LeaseConflict(
                    "分析结果提交被拒绝：租约栅栏已落后",
                    details={
                        "job_id": job_id,
                        "mismatches": [{"term": "lease_fence", "expected": "守卫更新命中 1 行", "actual": "命中 0 行"}],
                    },
                )
            existing = self.connection.execute(
                "SELECT analysis_id,result_json FROM analyses WHERE batch_id=? AND batch_revision=? AND input_sha256=?",
                (batch["batch_id"], job["batch_revision"], input_sha256),
            ).fetchone()
            if existing is None:
                cursor = self.connection.execute(
                    "INSERT INTO analyses(batch_id,batch_revision,protocol_sha256,input_sha256,algorithm_version,seed,"
                    "result_json,created_by,created_at) VALUES(?,?,?,?,?,?,?,?,?)",
                    (
                        batch["batch_id"], job["batch_revision"], protocol_digest, input_sha256,
                        ALGORITHM_VERSION, protocol.seed, canonical_json(result), statistician_id, commit_now,
                    ),
                )
                analysis_id = cursor.lastrowid
            else:
                analysis_id = existing["analysis_id"]
                result = json.loads(existing["result_json"])
            self.connection.execute(
                "UPDATE batches SET state='analyzed' WHERE batch_id=? AND state IN ('sealed','analyzing')",
                (batch["batch_id"],),
            )
            self._audit(
                "analysis_job",
                str(job_id),
                "analysis_job.completed",
                statistician_id,
                {
                    "job_id": job_id,
                    "batch_id": batch["batch_id"],
                    "batch_revision": job["batch_revision"],
                    "worker_id": worker_id,
                    "lease_epoch": lease_epoch,
                    "attempts": job["attempts"],
                    "analysis_id": analysis_id,
                    "input_sha256": input_sha256,
                },
            )
            self._audit(
                "batch",
                batch["batch_id"],
                "analysis.completed",
                statistician_id,
                {"analysis_id": analysis_id, "input_sha256": input_sha256},
            )
        return {"analysis_id": analysis_id, "input_sha256": input_sha256, "result": result}

    def fail_job(
        self, worker_id: str, job_id: int, lease_epoch: int, error: str, retry_seconds: int = 0
    ) -> dict[str, Any]:
        now = self._now()
        available = isoformat(self.clock.now() + timedelta(seconds=retry_seconds))
        with transaction(self.connection, immediate=True):
            job = self.connection.execute(
                "SELECT * FROM analysis_jobs WHERE job_id=?", (job_id,)
            ).fetchone()
            if job is None:
                raise NotFound("分析任务不存在")
            mismatches: list[dict[str, Any]] = []
            if job["state"] != "leased":
                mismatches.append({"term": "state", "expected": "leased", "actual": job["state"]})
            if job["lease_owner"] != worker_id:
                mismatches.append({"term": "lease_owner", "expected": worker_id, "actual": job["lease_owner"]})
            if job["lease_epoch"] != lease_epoch:
                mismatches.append({"term": "lease_epoch", "expected": lease_epoch, "actual": job["lease_epoch"]})
            if job["lease_expires_at"] is None or job["lease_expires_at"] <= now:
                mismatches.append(
                    {"term": "lease_expires_at", "expected": f"> {now}", "actual": job["lease_expires_at"]}
                )
            if mismatches:
                raise LeaseConflict(
                    "失败回报被拒绝：租约栅栏已落后",
                    details={"job_id": job_id, "mismatches": mismatches},
                )
            cursor = self.connection.execute(
                "UPDATE analysis_jobs SET state='queued',available_at=?,lease_owner=NULL,lease_expires_at=NULL,"
                "last_error=?,updated_at=? "
                "WHERE job_id=? AND state='leased' AND lease_owner=? AND lease_epoch=? AND lease_expires_at>?",
                (available, error[:1000], now, job_id, worker_id, lease_epoch, now),
            )
            if cursor.rowcount != 1:
                raise LeaseConflict(
                    "失败回报被拒绝：租约栅栏已落后",
                    details={
                        "job_id": job_id,
                        "mismatches": [{"term": "lease_fence", "expected": "守卫更新命中 1 行", "actual": "命中 0 行"}],
                    },
                )
            self._audit(
                "analysis_job",
                str(job_id),
                "analysis_job.failed",
                worker_id,
                {
                    "job_id": job_id,
                    "batch_id": job["batch_id"],
                    "batch_revision": job["batch_revision"],
                    "worker_id": worker_id,
                    "lease_epoch": lease_epoch,
                    "attempts": job["attempts"],
                    "error": error[:1000],
                    "available_at": available,
                    "input_sha256": job["input_sha256"],
                },
            )
        return {"job_id": job_id, "state": "queued", "available_at": available}

    def job_history(self, actor_id: str, job_id: int) -> dict[str, Any]:
        """运维时间线：各次领取、失效、接管、失败与最终落库及其输入摘要。"""

        self._require(actor_id, "audit.read")
        job = self.connection.execute(
            "SELECT * FROM analysis_jobs WHERE job_id=?", (job_id,)
        ).fetchone()
        if job is None:
            raise NotFound("分析任务不存在")
        events = self.connection.execute(
            "SELECT event_type,actor_id,payload_json,created_at FROM audit_events "
            "WHERE entity_type='analysis_job' AND entity_id=? ORDER BY event_id",
            (str(job_id),),
        ).fetchall()
        return {
            "job": dict(job),
            "events": [dict(row) | {"payload": json.loads(row["payload_json"])} for row in events],
        }

    def decide(
        self, actor_id: str, batch_id: str, analysis_id: int, decision: str, reason: str
    ) -> dict[str, Any]:
        self._require(actor_id, "decision.write")
        if decision not in {"needs_more_data", "approved", "rejected"}:
            raise ValidationFailed("未知分析准入决定")
        analysis_row = self.connection.execute(
            "SELECT * FROM analyses WHERE analysis_id=? AND batch_id=?", (analysis_id, batch_id)
        ).fetchone()
        if analysis_row is None:
            raise NotFound("分析版本不存在")
        if analysis_row["created_by"] == actor_id:
            raise Forbidden("统计负责人不能批准自己的分析")
        batch = self.get_batch(batch_id)
        if batch["state"] != "analyzed" or batch["revision"] != analysis_row["batch_revision"]:
            raise InvalidState("分析不是批次当前可审批版本")
        try:
            with transaction(self.connection, immediate=True):
                cursor = self.connection.execute(
                    "INSERT INTO decisions(batch_id,analysis_id,decision,reason,decided_by,decided_at) "
                    "VALUES(?,?,?,?,?,?)",
                    (batch_id, analysis_id, decision, reason, actor_id, self._now()),
                )
                self.connection.execute("UPDATE batches SET state='decided' WHERE batch_id=?", (batch_id,))
                self._audit(
                    "batch",
                    batch_id,
                    "decision.recorded",
                    actor_id,
                    {"decision_id": cursor.lastrowid, "analysis_id": analysis_id, "decision": decision},
                )
        except sqlite3.IntegrityError as exc:
            raise Conflict("该分析版本已经形成决定") from exc
        return {"batch_id": batch_id, "analysis_id": analysis_id, "decision": decision}

    def report(self, actor_id: str, batch_id: str) -> dict[str, Any]:
        user = self._user(actor_id)
        if user["role"] not in {"statistician", "approver", "auditor"}:
            raise Forbidden("当前角色不能读取完整报告")
        batch = self.get_batch(batch_id)
        protocol, protocol_digest = self._protocol(batch["protocol_id"], batch["protocol_version"])
        analysis_row = self.connection.execute(
            "SELECT * FROM analyses WHERE batch_id=? ORDER BY analysis_id DESC LIMIT 1", (batch_id,)
        ).fetchone()
        decision_row = None
        if analysis_row is not None:
            decision_row = self.connection.execute(
                "SELECT * FROM decisions WHERE analysis_id=?", (analysis_row["analysis_id"],)
            ).fetchone()
        exclusions = self.connection.execute(
            "SELECT e.exclusion_id,e.observation_id,e.status,e.reason,e.requested_by,e.reviewed_by "
            "FROM exclusion_requests e JOIN observations o ON o.observation_id=e.observation_id "
            "WHERE o.batch_id=? ORDER BY e.exclusion_id", (batch_id,)
        ).fetchall()
        events = self.connection.execute(
            "SELECT event_type,actor_id,payload_json,created_at FROM audit_events "
            "WHERE entity_type='batch' AND entity_id=? "
            "ORDER BY event_id", (batch_id,)
        ).fetchall()
        return {
            "batch": batch,
            "protocol": {
                "protocol_id": protocol.protocol_id,
                "version": protocol.version,
                "sha256": protocol_digest,
                "seed": protocol.seed,
                "bootstrap_samples": protocol.bootstrap_samples,
            },
            "analysis": None if analysis_row is None else {
                "analysis_id": analysis_row["analysis_id"],
                "input_sha256": analysis_row["input_sha256"],
                "algorithm_version": analysis_row["algorithm_version"],
                "created_by": analysis_row["created_by"],
                "result": json.loads(analysis_row["result_json"]),
            },
            "decision": None if decision_row is None else dict(decision_row),
            "exclusions": [dict(row) for row in exclusions],
            "events": [dict(row) | {"payload": json.loads(row["payload_json"])} for row in events],
        }
