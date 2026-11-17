"""共享构建：同一共享键 + 同一构建定义只执行一次，按订阅者维度治理取消。

语义：
- 同键同定义：多个订阅者挂在同一个共享组上，底层构建只执行一次。
- 同一订阅者重试：返回原订阅（幂等，不新建、不报错）；已撤销的订阅
  不复活，重试仍返回那条已撤销的订阅。
- 同键不同定义：409 冲突，组与既有订阅不受影响。
- 每个订阅者可单独撤销；仅当组内最后一个有效订阅被撤销时才取消实际
  构建（复用调度器整次取消：终止进程组、未启动步骤不再启动）。
- 构建已到终态后撤销只翻订阅状态，绝不改写构建结果。
- 共享关系全部落 SQLite，API 重启后仍可查询；运行中的共享构建与普通
  构建一样按原规则标记 interrupted。

并发模型：
- 所有共享关系变更（建组、加订阅、撤销、按构建号取消的共享检查）都在
  同一把 asyncio.Lock 内完成；单 worker 事件循环下即全局串行。
- 新组的落库顺序：构建/步骤记录 -> 共享组 -> 订阅 -> 启动构建任务。
  共享关系确认前任务不启动，因此不存在"构建在跑但共享关系查不到"的窗口。
- 撤销触发的取消走 落库 cancel_requested + per-build event 双通道：
  构建任务尚未启动时由 _run_build 启动检查兜底，已启动时 event 即时生效。
"""
from __future__ import annotations

import asyncio
import hashlib
import json
from datetime import datetime, timezone
from typing import Any, Optional

from .db import Database
from .models import (
    BUILD_TERMINAL,
    SUBSCRIPTION_ACTIVE,
    SharedBuildCreate,
    validate_dag,
)
from .scheduler import Scheduler


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def canonical_definition(payload: SharedBuildCreate) -> str:
    """构建定义的规范化 JSON：键排序、无空白，保证同一定义哈希稳定一致。"""
    return json.dumps(
        payload.build.model_dump(),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )


def definition_hash(payload: SharedBuildCreate) -> str:
    return hashlib.sha256(canonical_definition(payload).encode("utf-8")).hexdigest()


class SharedConflictError(Exception):
    """同键不同定义。"""


class SharedNotFoundError(Exception):
    """共享组或订阅不存在。"""


class SharedBuildService:
    def __init__(self, db: Database, scheduler: Scheduler) -> None:
        self.db = db
        self.scheduler = scheduler
        # 共享关系的全局串行锁：建组/加订阅/撤销/按构建号取消的共享检查
        self.lock = asyncio.Lock()

    # ------------------------------------------------------------- 提交

    async def submit(self, payload: SharedBuildCreate) -> tuple[dict[str, Any], bool]:
        """返回 (订阅视图, 是否新建订阅)。ValueError 由路由层转 400。"""
        validate_dag(payload.build.steps)
        digest = definition_hash(payload)
        canonical = canonical_definition(payload)
        now = _now()
        async with self.lock:
            group = await self.db.get_shared_group_by_key(payload.share_key)
            if group is None:
                return await self._create_group(payload, digest, canonical, now), True
            if group["definition_hash"] != digest:
                raise SharedConflictError(
                    f"共享键 {payload.share_key!r} 已绑定不同的构建定义，拒绝覆盖"
                )
            sub = await self.db.get_shared_subscription(
                group["id"], payload.subscriber_key
            )
            if sub is not None:
                # 同一订阅者重试：原样返回原订阅（含已撤销状态），不做任何修改
                view = await self._subscription_view(group, sub, "订阅已存在，返回原订阅")
                return view, False
            await self.db.create_shared_subscription(
                group["id"], payload.subscriber_key, now
            )
            sub = await self.db.get_shared_subscription(
                group["id"], payload.subscriber_key
            )
            view = await self._subscription_view(group, sub, "已加入既有共享组")
            return view, True

    async def _create_group(
        self,
        payload: SharedBuildCreate,
        digest: str,
        canonical: str,
        now: str,
    ) -> dict[str, Any]:
        """新共享组：先落构建记录与共享关系，全部确认后才启动构建任务。"""
        build_id = await self.scheduler.prepare_build(payload.build)
        group_id: Optional[int] = None
        try:
            group_id = await self.db.create_shared_group(
                payload.share_key, digest, canonical, build_id, now
            )
            await self.db.create_shared_subscription(
                group_id, payload.subscriber_key, now
            )
        except Exception:
            # 共享关系落库失败：构建不得作为普通构建继续存在/运行，
            # 标记失败终态并清理可能已写入的组行，避免污染共享键
            await self.db.set_build_status(
                build_id,
                "failed",
                _now(),
                detail="共享关系登记失败，构建未启动",
                finished=True,
            )
            if group_id is not None:
                await self.db.delete_shared_group(group_id)
            raise
        # 共享关系已确认，此刻才启动构建任务
        self.scheduler.launch_build(build_id)
        group = await self.db.get_shared_group_by_key(payload.share_key)
        sub = await self.db.get_shared_subscription(group_id, payload.subscriber_key)
        return await self._subscription_view(group, sub, "共享组已创建，构建只执行一次")

    # ------------------------------------------------------------- 查询

    async def get_group(self, share_key: str) -> dict[str, Any]:
        group = await self.db.get_shared_group_by_key(share_key)
        if group is None:
            raise SharedNotFoundError(f"共享键 {share_key!r} 不存在")
        return await self._group_view(group)

    async def list_groups(self, limit: int = 100) -> list[dict[str, Any]]:
        groups = await self.db.list_shared_groups(limit)
        return [await self._group_view(group) for group in groups]

    # ------------------------------------------------------------- 撤销

    async def revoke(self, share_key: str, subscriber_key: str) -> dict[str, Any]:
        """撤销单个订阅；最后一个有效订阅撤销时取消实际构建。

        幂等：重复撤销返回当前状态，不重复触发取消。
        """
        async with self.lock:
            group = await self.db.get_shared_group_by_key(share_key)
            if group is None:
                raise SharedNotFoundError(f"共享键 {share_key!r} 不存在")
            sub = await self.db.get_shared_subscription(group["id"], subscriber_key)
            if sub is None:
                raise SharedNotFoundError(
                    f"共享键 {share_key!r} 下不存在订阅者 {subscriber_key!r}"
                )
            build = await self.db.get_build(group["build_id"])
            if sub["status"] != SUBSCRIPTION_ACTIVE:
                return self._revoke_view(
                    group, sub, build, False, "订阅已撤销，重复撤销幂等忽略"
                )

            await self.db.revoke_shared_subscription(sub["id"], _now())
            triggered = False
            if build["status"] in BUILD_TERMINAL:
                detail = "构建已结束，仅撤销订阅，不改写构建结果"
            else:
                active = await self.db.count_active_subscriptions(group["id"])
                if active == 0:
                    # 最后一个有效订阅：取消实际构建（落库标志 + event 双通道）
                    await self.scheduler.cancel(build["id"])
                    triggered = True
                    detail = "最后一个有效订阅已撤销，实际构建取消中"
                else:
                    detail = f"订阅已撤销，组内仍有 {active} 个有效订阅，构建继续"
            fresh_sub = await self.db.get_shared_subscription(
                group["id"], subscriber_key
            )
            fresh_build = await self.db.get_build(group["build_id"])
            return self._revoke_view(group, fresh_sub, fresh_build, triggered, detail)

    # ------------------------------------------------------------- 视图

    async def _subscription_view(
        self, group: Any, sub: Any, detail: str
    ) -> dict[str, Any]:
        build = await self.db.get_build(group["build_id"])
        return {
            "share_key": group["share_key"],
            "subscriber_key": sub["subscriber_key"],
            "subscription_status": sub["status"],
            "build_id": group["build_id"],
            "build_status": build["status"],
            "subscribed_at": sub["created_at"],
            "revoked_at": sub["revoked_at"],
            "detail": detail,
        }

    async def _group_view(self, group: Any) -> dict[str, Any]:
        build = await self.db.get_build(group["build_id"])
        subs = await self.db.list_shared_subscriptions(group["id"])
        active = sum(1 for s in subs if s["status"] == SUBSCRIPTION_ACTIVE)
        return {
            "share_key": group["share_key"],
            "definition_hash": group["definition_hash"],
            "build_id": group["build_id"],
            "build_status": build["status"],
            "build_cancel_requested": bool(build["cancel_requested"]),
            "created_at": group["created_at"],
            "active_subscriptions": active,
            "subscriptions": [
                {
                    "share_key": group["share_key"],
                    "subscriber_key": s["subscriber_key"],
                    "subscription_status": s["status"],
                    "build_id": group["build_id"],
                    "build_status": build["status"],
                    "subscribed_at": s["created_at"],
                    "revoked_at": s["revoked_at"],
                    "detail": "",
                }
                for s in subs
            ],
        }

    @staticmethod
    def _revoke_view(
        group: Any,
        sub: Any,
        build: Any,
        triggered: bool,
        detail: str,
    ) -> dict[str, Any]:
        return {
            "share_key": group["share_key"],
            "subscriber_key": sub["subscriber_key"],
            "subscription_status": sub["status"],
            "build_id": group["build_id"],
            "build_status": build["status"],
            "build_cancel_triggered": triggered,
            "detail": detail,
        }
