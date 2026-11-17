"""共享构建端到端与并发交错测试。

分两部分：
1. 以真实 uvicorn 子进程验证 HTTP 语义（去重、重试、冲突、撤销治理、
   按构建号取消守卫、重启后可查询）。
2. 进程内直接驱动 Scheduler/SharedBuildService，确定性验证关键交错：
   取消先于构建任务落达、并发创建不双跑、并发撤销只触发一次取消、
   提交与撤销交错结果自洽、全部撤销后不再启动命令。

运行:
  .venv/bin/pytest tests/test_shared.py -v
"""
from __future__ import annotations

import asyncio
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import httpx
import pytest

from app import config
from app.db import Database
from app.models import BUILD_TERMINAL, BuildCreate, SharedBuildCreate, StepIn
from app.scheduler import Scheduler
from app.shared import SharedBuildService

from conftest import (
    ServerHarness,
    assert_pid_dead,
    command_output,
    free_port,
    make_step,
    shared_payload,
    step_map,
    step_pid,
    submit_shared,
    wait_build,
)


# ===================================================================
# 1. HTTP 集成测试
# ===================================================================

def test_shared_dedup_executes_once(shared_server):
    srv = shared_server
    key = "fw-team:rev-dedup"
    counter = srv.data_dir / "dedup-counter.txt"
    steps = [make_step("compile", f'echo ran >> "{counter}" && echo done')]

    r1 = submit_shared(srv.client, key, "ci-1", "dedup", steps)
    assert r1.status_code == 201, r1.text
    r2 = submit_shared(srv.client, key, "ci-2", "dedup", steps)
    assert r2.status_code == 201  # 新订阅者加入既有组
    assert r1.json()["build_id"] == r2.json()["build_id"]
    bid = r1.json()["build_id"]

    detail = wait_build(srv.client, bid)
    assert detail["status"] == "succeeded"
    # 同键同定义只执行一次：副作用文件只有一行
    assert counter.read_text().count("ran") == 1

    group = srv.client.get(f"/shared-builds/{key}").json()
    assert group["build_id"] == bid
    assert group["build_status"] == "succeeded"
    assert group["active_subscriptions"] == 2
    assert len(group["definition_hash"]) == 64

    # 构建结束后新订阅者加入：仍指向同一构建，绝不重跑
    r3 = submit_shared(srv.client, key, "ci-3", "dedup", steps)
    assert r3.status_code == 201
    assert r3.json()["build_id"] == bid
    assert r3.json()["build_status"] == "succeeded"
    time.sleep(0.3)
    assert counter.read_text().count("ran") == 1


def test_same_subscriber_retry_returns_original(shared_server):
    srv = shared_server
    key = "fw-team:rev-retry"
    steps = [make_step("a", "echo ok")]
    r1 = submit_shared(srv.client, key, "ci-1", "retry", steps)
    assert r1.status_code == 201
    r2 = submit_shared(srv.client, key, "ci-1", "retry", steps)
    assert r2.status_code == 200  # 重试拿回原订阅，不新建
    assert r2.json()["build_id"] == r1.json()["build_id"]
    assert r2.json()["subscribed_at"] == r1.json()["subscribed_at"]
    group = srv.client.get(f"/shared-builds/{key}").json()
    assert len(group["subscriptions"]) == 1
    assert wait_build(srv.client, r1.json()["build_id"])["status"] == "succeeded"


def test_conflicting_definition_rejected(shared_server):
    srv = shared_server
    key = "fw-team:rev-conflict"
    r1 = submit_shared(srv.client, key, "ci-1", "conflict", [make_step("a", "echo v1")])
    assert r1.status_code == 201
    bid = r1.json()["build_id"]

    # 步骤不同 -> 冲突
    r2 = submit_shared(srv.client, key, "ci-2", "conflict",
                       [make_step("a", "echo v2-different")])
    assert r2.status_code == 409
    assert "不同的构建定义" in r2.json()["detail"]
    # 仅名称不同也算不同定义 -> 冲突
    r3 = submit_shared(srv.client, key, "ci-2", "conflict-renamed",
                       [make_step("a", "echo v1")])
    assert r3.status_code == 409

    # 组与既有订阅不受影响，原构建按原定义跑完
    group = srv.client.get(f"/shared-builds/{key}").json()
    assert len(group["subscriptions"]) == 1
    assert wait_build(srv.client, bid)["status"] == "succeeded"
    log = srv.client.get(f"/builds/{bid}/steps/a/logs").json()
    assert "v1" in log["content"]
    assert "v2-different" not in log["content"]


def test_revoke_last_cancels_build_and_kills_processes(shared_server):
    srv = shared_server
    key = "fw-team:rev-revoke"
    steps = [make_step("slow", "echo start; sleep 300")]
    r1 = submit_shared(srv.client, key, "ci-1", "revoke", steps)
    r2 = submit_shared(srv.client, key, "ci-2", "revoke", steps)
    assert r1.status_code == r2.status_code == 201
    bid = r1.json()["build_id"]

    deadline = time.time() + 10
    while time.time() < deadline:
        sm = step_map(srv.client.get(f"/builds/{bid}").json())
        if sm["slow"]["status"] == "running":
            break
        time.sleep(0.1)
    else:
        raise AssertionError("步骤未进入 running")
    pid = step_pid(srv.data_dir, bid, "slow")

    # 撤销第一个订阅：构建继续
    d1 = srv.client.post(f"/shared-builds/{key}/subscriptions/ci-1/revoke")
    assert d1.status_code == 200
    assert d1.json()["build_cancel_triggered"] is False
    assert d1.json()["subscription_status"] == "revoked"
    assert srv.client.get(f"/builds/{bid}").json()["status"] == "running"

    # 撤销最后一个有效订阅：取消实际构建并清空进程
    d2 = srv.client.post(f"/shared-builds/{key}/subscriptions/ci-2/revoke")
    assert d2.status_code == 200
    assert d2.json()["build_cancel_triggered"] is True
    detail = wait_build(srv.client, bid, timeout=20)
    assert detail["status"] == "cancelled"
    assert step_map(detail)["slow"]["status"] == "cancelled"
    assert_pid_dead(pid)

    # 重复撤销幂等，不重复触发取消
    d3 = srv.client.post(f"/shared-builds/{key}/subscriptions/ci-2/revoke")
    assert d3.status_code == 200
    assert d3.json()["build_cancel_triggered"] is False
    assert "幂等" in d3.json()["detail"]


def test_revoke_after_finish_does_not_rewrite_result(shared_server):
    srv = shared_server
    key = "fw-team:rev-late-revoke"
    r1 = submit_shared(srv.client, key, "ci-1", "late-revoke",
                       [make_step("a", "echo done")])
    bid = r1.json()["build_id"]
    detail = wait_build(srv.client, bid)
    assert detail["status"] == "succeeded"

    d = srv.client.post(f"/shared-builds/{key}/subscriptions/ci-1/revoke")
    assert d.status_code == 200
    body = d.json()
    assert body["build_cancel_triggered"] is False
    assert body["subscription_status"] == "revoked"
    assert body["build_status"] == "succeeded"

    after = srv.client.get(f"/builds/{bid}").json()
    assert after["status"] == "succeeded"
    assert after["finished_at"] == detail["finished_at"]  # 结果未被改写
    assert after["cancel_requested"] is False


def test_cancel_by_build_id_rejected_for_shared(shared_server):
    srv = shared_server
    key = "fw-team:rev-guard"
    r1 = submit_shared(srv.client, key, "ci-1", "guard",
                       [make_step("slow", "sleep 300")])
    bid = r1.json()["build_id"]
    deadline = time.time() + 10
    while time.time() < deadline:
        if srv.client.get(f"/builds/{bid}").json()["status"] == "running":
            break
        time.sleep(0.1)

    # 按构建号取消不能绕过共享关系
    r = srv.client.post(f"/builds/{bid}/cancel")
    assert r.status_code == 409
    assert "共享订阅" in r.json()["detail"]
    fresh = srv.client.get(f"/builds/{bid}").json()
    assert fresh["cancel_requested"] is False
    assert fresh["status"] == "running"

    # 共享路径（撤销全部订阅）才能取消
    d = srv.client.post(f"/shared-builds/{key}/subscriptions/ci-1/revoke")
    assert d.json()["build_cancel_triggered"] is True
    assert wait_build(srv.client, bid, timeout=20)["status"] == "cancelled"


def test_concurrent_submits_single_execution(shared_server):
    srv = shared_server
    key = "fw-team:rev-storm"
    counter = srv.data_dir / "storm-counter.txt"
    steps = [make_step("compile", f'echo ran >> "{counter}"')]
    base = str(srv.client.base_url)

    def submit_one(i: int) -> httpx.Response:
        # 每线程独立 client，模拟多个 CI 回调同时打到 API
        with httpx.Client(base_url=base, timeout=30) as c:
            return c.post("/shared-builds",
                          json=shared_payload(key, f"ci-{i}", "storm", steps))

    with ThreadPoolExecutor(max_workers=8) as ex:
        responses = list(ex.map(submit_one, range(8)))

    assert all(r.status_code in (200, 201) for r in responses)
    bids = {r.json()["build_id"] for r in responses}
    assert len(bids) == 1  # 并发创建不双跑
    bid = bids.pop()
    assert wait_build(srv.client, bid)["status"] == "succeeded"
    assert counter.read_text().count("ran") == 1
    group = srv.client.get(f"/shared-builds/{key}").json()
    assert group["active_subscriptions"] == 8


def test_revoke_all_prevents_pending_step_start(shared_server):
    srv = shared_server
    key = "fw-team:rev-no-restart"
    steps = [
        make_step("a", "echo a-start; sleep 300"),
        make_step("b", "echo SHOULD-NOT-RUN", ["a"]),
    ]
    r1 = submit_shared(srv.client, key, "ci-1", "no-restart", steps)
    bid = r1.json()["build_id"]
    deadline = time.time() + 10
    while time.time() < deadline:
        sm = step_map(srv.client.get(f"/builds/{bid}").json())
        if sm["a"]["status"] == "running":
            break
        time.sleep(0.1)

    d = srv.client.post(f"/shared-builds/{key}/subscriptions/ci-1/revoke")
    assert d.json()["build_cancel_triggered"] is True
    detail = wait_build(srv.client, bid, timeout=20)
    sm = step_map(detail)
    assert detail["status"] == "cancelled"
    assert sm["a"]["status"] == "cancelled"
    # 全部撤销后，依赖中的后续步骤不得再启动
    assert sm["b"]["status"] == "cancelled"
    log_b = srv.client.get(f"/builds/{bid}/steps/b/logs").json()
    assert "SHOULD-NOT-RUN" not in command_output(log_b)


def test_restart_preserves_shared_relations(tmp_path_factory):
    data_dir = tmp_path_factory.mktemp("shared-restart")
    port = free_port()
    key = "fw-team:rev-restart"
    srv = ServerHarness(data_dir, port)
    srv.start()
    try:
        r1 = submit_shared(srv.client, key, "ci-1", "restart",
                           [make_step("slow", "sleep 300")])
        assert r1.status_code == 201
        bid = r1.json()["build_id"]
        deadline = time.time() + 10
        while time.time() < deadline:
            if srv.client.get(f"/builds/{bid}").json()["status"] == "running":
                break
            time.sleep(0.1)
    finally:
        srv.stop()

    srv2 = ServerHarness(data_dir, port)
    srv2.start()
    try:
        # 已确认的共享关系重启后仍可查询；运行中的构建按原规则中断
        group = srv2.client.get(f"/shared-builds/{key}")
        assert group.status_code == 200
        g = group.json()
        assert g["build_id"] == bid
        assert g["build_status"] == "interrupted"
        assert g["active_subscriptions"] == 1
        assert g["subscriptions"][0]["subscriber_key"] == "ci-1"

        # 同一订阅者重试：拿回原订阅（指向已中断的构建）
        r2 = submit_shared(srv2.client, key, "ci-1", "restart",
                           [make_step("slow", "sleep 300")])
        assert r2.status_code == 200
        assert r2.json()["build_id"] == bid
        assert r2.json()["build_status"] == "interrupted"

        # 同键不同定义仍然冲突
        r3 = submit_shared(srv2.client, key, "ci-2", "restart",
                           [make_step("slow", "sleep 301")])
        assert r3.status_code == 409

        # 按构建号取消的守卫重启后依然生效
        assert srv2.client.post(f"/builds/{bid}/cancel").status_code == 409

        # 普通构建照旧
        r4 = srv2.client.post("/builds", json={
            "name": "normal-after-restart",
            "steps": [make_step("z", "echo ok")],
        })
        assert r4.status_code == 201
        assert wait_build(srv2.client, r4.json()["id"])["status"] == "succeeded"
    finally:
        srv2.stop()


def test_shared_404_and_validation(shared_server):
    srv = shared_server
    assert srv.client.get("/shared-builds/no-such-key").status_code == 404
    assert srv.client.post(
        "/shared-builds/no-such-key/subscriptions/ci-1/revoke"
    ).status_code == 404

    key = "fw-team:rev-404"
    r1 = submit_shared(srv.client, key, "ci-1", "404", [make_step("a", "echo ok")])
    assert r1.status_code == 201
    # 组存在但订阅者不存在
    assert srv.client.post(
        f"/shared-builds/{key}/subscriptions/ghost/revoke"
    ).status_code == 404

    # 非法键：空白、路径保留字符
    bad1 = shared_payload("bad key", "ci-1", "x", [make_step("a", "true")])
    assert srv.client.post("/shared-builds", json=bad1).status_code == 422
    bad2 = shared_payload("ok-key", "ci/1", "x", [make_step("a", "true")])
    assert srv.client.post("/shared-builds", json=bad2).status_code == 422

    # 非法 DAG（循环依赖）-> 400
    cyc = shared_payload("fw-team:rev-bad-dag", "ci-1", "x", [
        make_step("a", "true", ["b"]), make_step("b", "true", ["a"]),
    ])
    r = srv.client.post("/shared-builds", json=cyc)
    assert r.status_code == 400 and "循环" in r.json()["detail"]

    # 列表接口可用且包含刚创建的组
    listing = srv.client.get("/shared-builds").json()
    assert any(g["share_key"] == key for g in listing)
    wait_build(srv.client, r1.json()["build_id"])


def test_shared_endpoints_in_openapi(shared_server):
    spec = shared_server.client.get("/openapi.json").json()
    assert "/shared-builds" in spec["paths"]
    assert "/shared-builds/{share_key}" in spec["paths"]
    revoke_path = "/shared-builds/{share_key}/subscriptions/{subscriber_key}/revoke"
    assert revoke_path in spec["paths"]
    assert shared_server.client.get("/docs").status_code == 200


# ===================================================================
# 2. 进程内确定性交错测试
# ===================================================================

@pytest.fixture
def pipe(tmp_path, monkeypatch):
    """把数据目录与清理时序收窄到测试可控范围。"""
    monkeypatch.setattr(config, "DATA_DIR", tmp_path)
    monkeypatch.setattr(config, "DB_PATH", tmp_path / "pipeline.db")
    monkeypatch.setattr(config, "WORKDIR_ROOT", tmp_path / "workdir")
    monkeypatch.setattr(config, "LOG_ROOT", tmp_path / "logs")
    monkeypatch.setattr(config, "TERM_GRACE_SECONDS", 0.2)
    monkeypatch.setattr(config, "KILL_GRACE_SECONDS", 5.0)
    monkeypatch.setattr(config, "PROC_POLL_INTERVAL", 0.02)
    monkeypatch.setattr(config, "SCHEDULER_TICK", 0.02)
    return tmp_path


async def _wait_build_terminal(db: Database, build_id: int, timeout: float = 15.0):
    deadline = asyncio.get_running_loop().time() + timeout
    while True:
        row = await db.get_build(build_id)
        if row["status"] in BUILD_TERMINAL:
            return row
        if asyncio.get_running_loop().time() > deadline:
            raise AssertionError(f"构建 {build_id} 未在 {timeout}s 内进入终态")
        await asyncio.sleep(0.02)


async def _wait_step_status(db: Database, build_id: int, uid: str,
                            want: str, timeout: float = 10.0):
    deadline = asyncio.get_running_loop().time() + timeout
    while True:
        row = await db.get_step(build_id, uid)
        if row is not None and row["status"] == want:
            return row
        if asyncio.get_running_loop().time() > deadline:
            raise AssertionError(f"步骤 {uid} 未在 {timeout}s 内进入 {want}")
        await asyncio.sleep(0.02)


async def _wait_pid_dead(pid: int, timeout: float = 10.0) -> None:
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        if not Path(f"/proc/{pid}").exists():
            return
        await asyncio.sleep(0.05)
    raise AssertionError(f"进程 {pid} 在清理后仍然存活")


def _payload(share_key: str, subscriber_key: str, steps: list[StepIn],
             name: str = "shared") -> SharedBuildCreate:
    return SharedBuildCreate(
        share_key=share_key,
        subscriber_key=subscriber_key,
        build=BuildCreate(name=name, steps=steps),
    )


def test_cancel_before_build_task_start_is_honored(pipe):
    """取消先于构建任务落达：落库标志必须被尊重，任何命令都不得启动。

    共享场景：最后一个订阅在构建任务启动前被撤销，cancel 时
    _build_events 里还没有 event，只能靠落库的 cancel_requested。
    """
    async def main() -> None:
        db = Database()
        await db.connect()
        try:
            scheduler = Scheduler(db)
            payload = BuildCreate(
                name="pre-cancelled",
                steps=[StepIn(id="a", command="echo SHOULD-NOT-RUN",
                              timeout_seconds=5)],
            )
            build_id = await scheduler.prepare_build(payload)
            await db.request_cancel(build_id)  # 取消先于 launch 落库
            scheduler.launch_build(build_id)
            row = await _wait_build_terminal(db, build_id)
            assert row["status"] == "cancelled"
            steps = await db.get_steps(build_id)
            assert [s["status"] for s in steps] == ["cancelled"]
            log = Path(steps[0]["log_path"]).read_text()
            assert "SHOULD-NOT-RUN" not in log
        finally:
            await db.close()

    asyncio.run(main())


def test_service_concurrent_submit_single_execution(pipe):
    """并发创建同一共享键：只落一个组、构建只执行一次。"""
    counter = pipe / "counter.txt"

    async def main() -> None:
        db = Database()
        await db.connect()
        try:
            scheduler = Scheduler(db)
            service = SharedBuildService(db, scheduler)
            steps = [StepIn(id="a", command=f'echo ran >> "{counter}"',
                            timeout_seconds=5)]
            results = await asyncio.gather(*(
                service.submit(_payload("fw:rev-concurrent", f"ci-{i}", steps))
                for i in range(10)
            ))
            build_ids = {view["build_id"] for view, _ in results}
            assert len(build_ids) == 1
            assert all(created for _, created in results)  # 10 个新订阅
            build_id = build_ids.pop()
            row = await _wait_build_terminal(db, build_id)
            assert row["status"] == "succeeded"
            assert counter.read_text().count("ran") == 1  # 没有双跑
            group = await service.get_group("fw:rev-concurrent")
            assert group["active_subscriptions"] == 10
        finally:
            await db.close()

    asyncio.run(main())


def test_service_concurrent_last_revokes_cancel_once(pipe):
    """最后两个订阅并发撤销：恰好触发一次构建取消，进程组被清空。"""
    async def main() -> None:
        db = Database()
        await db.connect()
        try:
            scheduler = Scheduler(db)
            service = SharedBuildService(db, scheduler)
            key = "fw:rev-revoke"
            steps = [StepIn(id="a", command="sleep 300", timeout_seconds=60)]
            await service.submit(_payload(key, "ci-1", steps))
            await service.submit(_payload(key, "ci-2", steps))
            group = await service.get_group(key)
            build_id = group["build_id"]
            running = await _wait_step_status(db, build_id, "a", "running")
            pid = running["pid"]

            r1, r2 = await asyncio.gather(
                service.revoke(key, "ci-1"),
                service.revoke(key, "ci-2"),
            )
            triggered = [r1["build_cancel_triggered"], r2["build_cancel_triggered"]]
            assert triggered.count(True) == 1  # 只触发一次取消
            row = await _wait_build_terminal(db, build_id)
            assert row["status"] == "cancelled"
            await _wait_pid_dead(pid)
        finally:
            await db.close()

    asyncio.run(main())


def test_service_submit_revoke_interleaving_consistency(pipe):
    """新订阅与最后一撤销并发：无论谁先，结果必须自洽且不双跑。"""
    async def main() -> None:
        db = Database()
        await db.connect()
        try:
            scheduler = Scheduler(db)
            service = SharedBuildService(db, scheduler)
            key = "fw:rev-interleave"
            steps = [StepIn(id="a", command="sleep 300", timeout_seconds=60)]
            await service.submit(_payload(key, "ci-1", steps))
            group = await service.get_group(key)
            build_id = group["build_id"]
            running = await _wait_step_status(db, build_id, "a", "running")
            pid = running["pid"]

            (sub_view, _), revoke_view = await asyncio.gather(
                service.submit(_payload(key, "ci-2", steps)),
                service.revoke(key, "ci-1"),
            )
            # 不双跑：新订阅者必然挂在同一个构建上
            assert sub_view["build_id"] == build_id
            build = await db.get_build(build_id)
            # 自洽：撤销时 ci-1 是最后一个有效订阅 <=> 取消已触发并落库
            assert bool(build["cancel_requested"]) == revoke_view["build_cancel_triggered"]

            # 收尾：撤销剩余订阅，构建必须走向取消且进程被清空
            await service.revoke(key, "ci-2")
            row = await _wait_build_terminal(db, build_id)
            assert row["status"] == "cancelled"
            await _wait_pid_dead(pid)
        finally:
            await db.close()

    asyncio.run(main())


def test_service_revoke_all_stops_pending_steps(pipe):
    """全部撤销后，排队中的后续步骤不得再启动任何命令。"""
    async def main() -> None:
        db = Database()
        await db.connect()
        try:
            scheduler = Scheduler(db)
            service = SharedBuildService(db, scheduler)
            key = "fw:rev-no-restart"
            steps = [
                StepIn(id="a", command="sleep 300", timeout_seconds=60),
                StepIn(id="b", command="echo SHOULD-NOT-RUN",
                       depends_on=["a"], timeout_seconds=5),
            ]
            await service.submit(_payload(key, "ci-1", steps))
            group = await service.get_group(key)
            build_id = group["build_id"]
            await _wait_step_status(db, build_id, "a", "running")

            view = await service.revoke(key, "ci-1")
            assert view["build_cancel_triggered"] is True
            row = await _wait_build_terminal(db, build_id)
            assert row["status"] == "cancelled"
            final_steps = {s["step_uid"]: s for s in await db.get_steps(build_id)}
            assert final_steps["a"]["status"] == "cancelled"
            assert final_steps["b"]["status"] == "cancelled"
            log_b = Path(final_steps["b"]["log_path"]).read_text()
            assert "SHOULD-NOT-RUN" not in log_b
        finally:
            await db.close()

    asyncio.run(main())
