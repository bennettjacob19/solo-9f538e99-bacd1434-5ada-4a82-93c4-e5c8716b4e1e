"""FastAPI 应用：路由、序列化与启动恢复。"""
from __future__ import annotations

import asyncio
import contextlib
import json
from pathlib import Path

from fastapi import FastAPI, HTTPException, Query, Response

from . import __version__, config
from .db import Database
from .models import (
    BuildCreate,
    BuildOut,
    CancelOut,
    LogOut,
    SharedBuildCreate,
    SharedGroupOut,
    SharedRevokeOut,
    SharedSubscriptionOut,
    StepOut,
)
from .scheduler import Scheduler
from .shared import SharedBuildService, SharedConflictError, SharedNotFoundError

db = Database()
scheduler = Scheduler(db)
shared_service = SharedBuildService(db, scheduler)


def _step_out(row) -> StepOut:
    return StepOut(
        id=row["step_uid"],
        command=row["command"],
        depends_on=json.loads(row["depends_on"]),
        timeout_seconds=row["timeout_seconds"],
        status=row["status"],
        exit_code=row["exit_code"],
        detail=row["detail"],
        started_at=row["started_at"],
        finished_at=row["finished_at"],
    )


async def _build_out(build_id: int) -> BuildOut:
    row = await db.get_build(build_id)
    if row is None:
        raise HTTPException(status_code=404, detail="构建不存在")
    steps = [_step_out(r) for r in await db.get_steps(build_id)]
    return BuildOut(
        id=row["id"],
        name=row["name"],
        status=row["status"],
        cancel_requested=bool(row["cancel_requested"]),
        created_at=row["created_at"],
        started_at=row["started_at"],
        finished_at=row["finished_at"],
        detail=row["detail"],
        steps=steps,
    )


@contextlib.asynccontextmanager
async def lifespan(app: FastAPI):
    # 顺序不可颠倒：先落库连接，再清空旧构建残留并标记中断，最后才对外服务
    await db.connect()
    app.state.recovery = await scheduler.recover()
    try:
        yield
    finally:
        await db.close()


app = FastAPI(
    title="固件构建流水线 API",
    version=__version__,
    description=(
        "接收最多 12 步的 JSON DAG，在挂载工作目录中执行真实命令；"
        "全局最多两步并行，依赖全成功才启动，失败后代跳过，"
        "超时/取消终止整个进程组，支持逐步状态与日志查询。"
        "共享构建：同一共享键 + 同一构建定义只执行一次，"
        "按订阅者维度加入/撤销，最后一个有效订阅撤销才取消实际构建。"
    ),
    lifespan=lifespan,
)


@app.get("/health")
async def health() -> dict:
    return {
        "status": "ok",
        "version": __version__,
        "max_steps": config.MAX_STEPS,
        "max_parallel": config.MAX_PARALLEL,
        "recovery": getattr(app.state, "recovery", None),
    }


@app.post("/builds", response_model=BuildOut, status_code=201)
async def create_build(payload: BuildCreate) -> BuildOut:
    try:
        build_id = await scheduler.submit(payload)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return await _build_out(build_id)


@app.get("/builds", response_model=list[BuildOut])
async def list_builds(limit: int = Query(100, ge=1, le=500)) -> list[BuildOut]:
    rows = await db.list_builds(limit)
    return [await _build_out(r["id"]) for r in rows]


@app.get("/builds/{build_id}", response_model=BuildOut)
async def get_build(build_id: int) -> BuildOut:
    return await _build_out(build_id)


@app.post("/builds/{build_id}/cancel", response_model=CancelOut)
async def cancel_build(build_id: int) -> CancelOut:
    # 共享构建由订阅关系治理：按构建号直接取消会绕过共享关系，一律拒绝。
    # 检查与取消在同一把共享锁内完成，避免与建组/撤销竞态。
    async with shared_service.lock:
        group = await db.get_shared_group_by_build_id(build_id)
        if group is not None:
            raise HTTPException(
                status_code=409,
                detail=(
                    "该构建由共享订阅托管，不能按构建号取消；"
                    f"请撤销共享键 {group['share_key']!r} 下的订阅"
                ),
            )
        row, message = await scheduler.cancel(build_id)
    if row is None:
        raise HTTPException(status_code=404, detail="构建不存在")
    # 重新读取以拿到 cancel_requested 的真实落库值（终态上的重复取消不会置位）
    fresh = await db.get_build(build_id)
    return CancelOut(
        id=row["id"],
        status=fresh["status"],
        cancel_requested=bool(fresh["cancel_requested"]),
        detail=message,
    )


# ---------------------------------------------------------------- 共享构建


@app.post("/shared-builds", response_model=SharedSubscriptionOut, status_code=201)
async def create_shared_build(
    payload: SharedBuildCreate, response: Response
) -> SharedSubscriptionOut:
    """提交共享构建：同键同定义只执行一次；同订阅者重试返回原订阅（200）。"""
    try:
        view, created = await shared_service.submit(payload)
    except SharedConflictError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    if not created:
        response.status_code = 200
    return SharedSubscriptionOut(**view)


@app.get("/shared-builds", response_model=list[SharedGroupOut])
async def list_shared_builds(
    limit: int = Query(100, ge=1, le=500)
) -> list[SharedGroupOut]:
    return [SharedGroupOut(**g) for g in await shared_service.list_groups(limit)]


@app.get("/shared-builds/{share_key}", response_model=SharedGroupOut)
async def get_shared_build(share_key: str) -> SharedGroupOut:
    try:
        view = await shared_service.get_group(share_key)
    except SharedNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return SharedGroupOut(**view)


@app.post(
    "/shared-builds/{share_key}/subscriptions/{subscriber_key}/revoke",
    response_model=SharedRevokeOut,
)
async def revoke_shared_subscription(
    share_key: str, subscriber_key: str
) -> SharedRevokeOut:
    """撤销单个订阅；最后一个有效订阅撤销时取消实际构建（幂等）。"""
    try:
        view = await shared_service.revoke(share_key, subscriber_key)
    except SharedNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return SharedRevokeOut(**view)


@app.get("/builds/{build_id}/steps/{step_id}/logs", response_model=LogOut)
async def get_step_log(
    build_id: int,
    step_id: str,
    tail: int = Query(0, ge=0, description="只返回最后 N 行，0 表示返回全部（受字节上限约束）"),
) -> LogOut:
    step = await db.get_step(build_id, step_id)
    if step is None:
        raise HTTPException(status_code=404, detail="构建或步骤不存在")
    path = Path(step["log_path"])
    if not path.exists():
        return LogOut(build_id=build_id, step_id=step_id, status=step["status"],
                      truncated=False, content="")
    # 在线程池读文件，避免大日志阻塞事件循环
    raw = await asyncio.to_thread(path.read_bytes)
    truncated = False
    if len(raw) > config.LOG_MAX_BYTES:
        raw = raw[-config.LOG_MAX_BYTES:]
        truncated = True
    content = raw.decode("utf-8", errors="replace")
    if tail > 0:
        lines = content.splitlines()[-tail:]
        content = "\n".join(lines)
    return LogOut(
        build_id=build_id,
        step_id=step_id,
        status=step["status"],
        truncated=truncated,
        content=content,
    )
