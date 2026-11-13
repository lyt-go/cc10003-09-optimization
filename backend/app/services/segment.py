"""分群分析业务规则：状态流转、字段校验与周期刷新编排都收在这里。

刷新结果分两层管理，对应"共享结果 vs 逐分群结果"的取舍：

- 标签版本快照（共享层）：以（标签编号、标签版本、刷新周期）为键，只存覆盖对象
  集合与覆盖对象数。多个分群共享同一标签版本时复用同一份快照，周期任务只计算
  一次标签集合；快照必须"已确认"才允许复用，覆盖对象数按批次赋值、不重复累计。
- 分群刷新任务（逐分群层）：以（分群编号、标签版本组合、筛选规则摘要、刷新周期）
  为幂等键，记录规则明细、进度、最终规模。最终规模按分群各自计算、各自落库，
  共享覆盖集合但不把某个分群的规模写回其他分群。
"""
from __future__ import annotations

import hashlib
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from typing import Any

from app.store import store

MODULE = "segment"
TAG_MODULE = "tagmgr"
SNAPSHOT_TABLE = "tag_snapshot"  # 标签版本覆盖结果集（共享层）
RUN_TABLE = "segment_refresh"  # 分群刷新任务（逐分群层）

REQUIRED_FIELDS = ["分群编号", "分群名称", "筛选规则"]
STATUS_ORDER = ["草稿", "规则校验中", "已生效", "已失效"]
ACTION_RULES = {"提交规则校验": "规则校验中", "生效分群": "已生效", "失效分群": "已失效"}
NEGATIVE_ACTIONS: list[str] = []
REFRESH_ACTION = "周期刷新"

TAG_REF_PATTERN = re.compile(r"TAGM-\d{4}")

SNAPSHOT_COMPUTING = "计算中"
SNAPSHOT_CONFIRMED = "已确认"
SNAPSHOT_FAILED = "失败"

RUN_RUNNING = "进行中"
RUN_DONE = "已完成"
RUN_PENDING = "待处理"  # 失败/中断都停留在这里，不伪装成已完成

STAT_KEYS = ["命中", "重算", "续做", "冲突"]


def _now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _digest(text: str, length: int = 10) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:length]


def _extract_tag_codes(rule_text: str) -> list[str]:
    """从筛选规则里解析标签引用（TAGM-xxxx），去重并保持出现顺序。"""
    codes: list[str] = []
    for match in TAG_REF_PATTERN.finditer(rule_text or ""):
        code = match.group(0)
        if code not in codes:
            codes.append(code)
    return codes


def _tag_version(tag: dict[str, Any]) -> str:
    """标签版本 = 计算逻辑的内容指纹：逻辑变更即产生新版本，无需人工维护版本号。"""
    return _digest(str(tag.get("计算逻辑") or ""), 8)


def _compute_tag_objects(tag_code: str, version: str) -> list[str]:
    """计算标签版本的覆盖对象集合。

    同一标签版本的结果确定（幂等），任何时候重算都得到同一集合，复用才安全。
    真实项目里这里是对标签引擎/数仓的调用。
    """
    seed = int(_digest(f"{tag_code}@{version}", 16), 16)
    count = 60 + seed % 241  # 60~300 个对象
    start = seed % 10000
    objects = {f"OBJ-{(start + i * 37) % 100000:05d}" for i in range(count)}
    time.sleep(0.05)  # 模拟计算耗时，让并发共享/冲突路径真实可达
    return sorted(objects)


def _segment_final_size(seg_code: str, rule_digest: str, pool_size: int) -> int:
    """分群最终规模：由分群编号+规则摘要决定过滤比例，同一批覆盖集合下结果确定。"""
    seed = int(_digest(f"{seg_code}|{rule_digest}", 16), 16)
    permille = 300 + seed % 500  # 30.0% ~ 79.9%
    return pool_size * permille // 1000


class SegmentService:
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._snapshot_events: dict[int, threading.Event] = {}
        self._batch_seq = 0

    # ------------------------------------------------------------------
    # 基础 CRUD 与状态流转
    # ------------------------------------------------------------------
    def list_entries(
        self,
        *,
        keyword: str | None = None,
        status: str | None = None,
        page: int = 1,
        size: int = 20,
    ) -> tuple[list[dict[str, Any]], int]:
        rows = store.rows(MODULE)
        if keyword:
            rows = [row for row in rows if keyword in str(row.get("分群编号", ""))]
        if status:
            rows = [row for row in rows if row.get("status") == status]
        total = len(rows)
        start = max(page - 1, 0) * size
        return rows[start:start + size], total

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        return store.find(MODULE, entry_id)

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        missing = [field for field in REQUIRED_FIELDS if not str(values.get(field) or "").strip()]
        if missing:
            return None, missing
        rows = store.rows(MODULE)
        entry = {"id": max((int(row.get("id", 0)) for row in rows), default=0) + 1}
        entry.update({field: values.get(field) for field in REQUIRED_FIELDS})
        for field in ("最小规模", "最大规模", "刷新周期"):
            if str(values.get(field) or "").strip():
                entry[field] = values.get(field)
        entry["status"] = STATUS_ORDER[0]
        entry["pending"] = True
        entry["abnormal"] = False
        rows.append(entry)
        return entry, []

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str, dict[str, Any] | None, bool]:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"人群分群 {entry_id} 不存在或已归档", None, False
        if action == REFRESH_ACTION:
            run, message, stats, mode = self.refresh_entry(entry_id, trigger="手动")
            detail = {"任务": run, "统计": stats, "执行方式": mode} if run is not None else None
            # 任务未落地或停留在待处理都属于未完成，不能按成功返回
            ok = run is not None and run["状态"] != RUN_PENDING
            return (entry if run is not None else None), message, detail, ok
        if action not in ACTION_RULES:
            return None, f"动作「{action}」不属于分群分析可执行范围", None, False
        if action == "生效分群":
            problems = self._check_effective(entry)
            if problems:
                return None, "；".join(problems), None, False
        target = ACTION_RULES[action]
        if target not in STATUS_ORDER:
            return None, f"目标状态「{target}」不在允许的状态序列里", None, False
        entry["status"] = target
        entry["pending"] = target != STATUS_ORDER[-1]
        entry["abnormal"] = action in NEGATIVE_ACTIONS
        return entry, f"人群分群已{action}", None, True

    # ------------------------------------------------------------------
    # 周期刷新：单个分群
    # ------------------------------------------------------------------
    def refresh_entry(
        self, entry_id: int, *, trigger: str = "手动"
    ) -> tuple[dict[str, Any] | None, str, dict[str, int], str]:
        """刷新单个分群。幂等：同一（分群、标签版本、筛选规则、刷新周期）只生效一次。

        返回（任务，说明，统计，执行方式）；执行方式：新建/续做/复用/进行中/未执行。
        """
        empty_stats = {key: 0 for key in STAT_KEYS}
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"人群分群 {entry_id} 不存在或已归档", empty_stats, "未执行"
        seg_code = str(entry.get("分群编号") or "")
        rule_text = str(entry.get("筛选规则") or "")
        tag_codes = _extract_tag_codes(rule_text)
        if not tag_codes:
            return None, (
                f"分群 {seg_code} 的筛选规则未引用任何标签（TAGM-xxxx），"
                "无法周期刷新，请先在筛选规则中引用标签"
            ), empty_stats, "未执行"
        cycle = str(entry.get("刷新周期") or "").strip() or "未配置"
        rule_digest = _digest(rule_text, 10)
        version_parts = []
        for code in tag_codes:
            tag = self._find_tag(code)
            version_parts.append(f"{code}@{_tag_version(tag) if tag else '缺失'}")
        version_key = "|".join(sorted(version_parts))

        with self._lock:
            run = self._find_run(seg_code, version_key, rule_digest, cycle)
            if run is not None and run["状态"] == RUN_DONE:
                return run, (
                    f"刷新任务 {run['任务编号']} 已完成（批次 {run['批次号']}）：标签版本、筛选规则、"
                    "刷新周期均未变化，本次直接复用已完成结果，不重复计算"
                ), dict(run["统计"]), "复用"
            if run is not None and run["状态"] == RUN_RUNNING:
                return run, (
                    f"刷新任务 {run['任务编号']} 正在执行，本次重复启动已忽略，同一任务只生效一次"
                ), dict(run["统计"]), "进行中"
            resumed = run is not None
            if run is None:
                run = self._new_run(entry, version_key, rule_digest, cycle, tag_codes, trigger)
            else:
                run["触发方式"] = trigger
                run["状态"] = RUN_RUNNING
                run["开始时间"] = _now()

        try:
            self._execute_run(run, entry, cycle, resumed)
        except Exception as exc:  # 中断：停留在待处理，不伪装成已完成
            with self._lock:
                run["状态"] = RUN_PENDING
                run["说明"] = f"刷新中断：{exc}；任务停留在待处理，可重试从失败规则续做"
            entry["abnormal"] = True
            return run, str(run["说明"]), dict(run["统计"]), "续做" if resumed else "新建"
        return run, str(run["说明"]), dict(run["统计"]), "续做" if resumed else "新建"

    def _execute_run(self, run: dict[str, Any], entry: dict[str, Any], cycle: str, resumed: bool) -> None:
        """执行刷新任务：续做时只跑失败规则，成功规则沿用上次结果。"""
        stats = run["统计"]
        batch = self._next_batch()
        pool: set[str] | None = None
        failed: list[str] = []
        for rule in run["规则明细"]:
            if rule["状态"] == "成功":
                if rule["类型"] == "标签":
                    objects = self._confirmed_objects(rule["标签编号"], cycle)
                    pool = set(objects) if pool is None else pool & set(objects)
                continue
            if resumed:
                stats["续做"] += 1
            if rule["类型"] == "标签":
                objects, error = self._run_tag_rule(run, rule, cycle, batch, stats)
                if error:
                    failed.append(error)
                    continue
                pool = set(objects) if pool is None else pool & set(objects)
            else:
                # 分群条件依赖全部标签规则先成功，否则保持待处理、留待续做
                blocking = [r for r in run["规则明细"] if r["类型"] == "标签" and r["状态"] != "成功"]
                if blocking:
                    rule["说明"] = "等待标签覆盖集合就绪后再过滤"
                    continue
                base = pool or set()
                final_size = _segment_final_size(str(run["分群编号"]), str(run["筛选规则摘要"]), len(base))
                rule["状态"] = "成功"
                rule["覆盖对象数"] = final_size
                rule["说明"] = "覆盖集合交集为空，最终规模为 0" if not base else "分群自身条件过滤完成"
                run["最终规模"] = final_size

        done_count = sum(1 for rule in run["规则明细"] if rule["状态"] == "成功")
        run["进度"] = f"{done_count}/{len(run['规则明细'])}"
        unfinished = [rule for rule in run["规则明细"] if rule["状态"] != "成功"]
        if unfinished:
            # 异常链路停留在待处理：分群保留上次成功规模，只标记受影响
            run["状态"] = RUN_PENDING
            run["批次号"] = ""
            run["完成时间"] = ""
            run["最终规模"] = ""
            reasons = failed or [f"{rule['规则']} 未执行完成" for rule in unfinished]
            run["说明"] = "；".join(reasons) + "；任务停留在待处理，修复后可重试续做，分群保留上次成功规模"
            entry["abnormal"] = True
            return
        final_size = int(run.get("最终规模") or 0)
        run["状态"] = RUN_DONE
        run["批次号"] = batch
        run["完成时间"] = _now()
        run["说明"] = (
            f"刷新完成（批次 {batch}）：最终规模 {final_size}，"
            "刷新进度、最终规模与标签覆盖对象数均来自本批结果"
        )
        # 最终规模按分群各自落库，共享覆盖集合但不串写其他分群
        entry["最终规模"] = str(final_size)
        entry["上次成功规模"] = str(final_size)
        entry["最近计算时间"] = run["完成时间"]
        entry["规模批次号"] = batch
        entry["abnormal"] = False

    def _run_tag_rule(
        self,
        run: dict[str, Any],
        rule: dict[str, Any],
        cycle: str,
        batch: str,
        stats: dict[str, int],
    ) -> tuple[list[str], str]:
        """执行一条标签规则，返回（覆盖对象集合，失败原因）。"""
        code = str(rule["标签编号"])
        tag = self._find_tag(code)
        if tag is None:
            reason = f"标签 {code} 不存在或已归档"
            rule.update({"状态": "失败", "原因": reason})
            return [], reason
        if tag.get("status") != "已发布":
            reason = f"标签 {code} 未发布（当前状态：{tag.get('status')}），无法计算覆盖集合"
            rule.update({"状态": "失败", "原因": reason})
            return [], reason
        version = _tag_version(tag)
        role, snap = self._acquire_snapshot(code, version, cycle, str(run["任务编号"]))
        if role == "hit":
            stats["命中"] += 1
            rule["说明"] = f"复用已确认快照（批次 {snap['批次号']}），未重复计算"
            objects = list(snap["覆盖对象"])
        elif role == "conflict":
            stats["冲突"] += 1
            if not self._wait_snapshot(snap):
                reason = (
                    f"标签 {code} 版本由任务 {snap['属主任务']} 计算但未确认，"
                    "本任务无法转成复用"
                )
                rule.update({"状态": "失败", "原因": reason})
                return [], reason
            rule["说明"] = f"与任务 {snap['属主任务']} 刷新同一标签版本，已转成复用其确认结果"
            objects = list(snap["覆盖对象"])
        else:
            stats["重算"] += 1
            objects = _compute_tag_objects(code, version)
            self._confirm_snapshot(snap, objects, batch)
            self._write_tag_coverage(tag, snap, batch)
            rule["说明"] = "本次新计算并确认快照"
        rule["状态"] = "成功"
        rule["覆盖对象数"] = len(objects)
        return objects, ""

    # ------------------------------------------------------------------
    # 周期刷新：批量（周期任务入口）
    # ------------------------------------------------------------------
    def refresh_batch(self) -> tuple[list[dict[str, Any]], dict[str, int], str]:
        """周期任务入口：并发刷新所有已生效分群，共享标签版本只计算一次。"""
        entries = [row for row in store.rows(MODULE) if row.get("status") == "已生效"]
        total_stats = {key: 0 for key in STAT_KEYS}
        if not entries:
            return [], total_stats, "暂无已生效分群需要周期刷新"
        runs: list[dict[str, Any]] = []
        modes: dict[str, int] = {}
        with ThreadPoolExecutor(max_workers=4) as pool:
            futures = [pool.submit(self.refresh_entry, int(entry["id"]), trigger="周期") for entry in entries]
            for future in futures:
                run, _message, stats, mode = future.result()
                if run is not None:
                    runs.append(run)
                modes[mode] = modes.get(mode, 0) + 1
                for key in STAT_KEYS:
                    total_stats[key] += stats.get(key, 0)
        pending = [str(run["分群编号"]) for run in runs if run["状态"] != RUN_DONE]
        mode_text = "、".join(f"{name} {count}" for name, count in sorted(modes.items()))
        message = (
            f"周期刷新完成：{len(runs)} 个分群（{mode_text}），"
            f"命中 {total_stats['命中']}、重算 {total_stats['重算']}、"
            f"续做 {total_stats['续做']}、冲突 {total_stats['冲突']}"
        )
        if pending:
            message += f"；{len(pending)} 个分群停留在待处理：{'、'.join(pending)}"
        return runs, total_stats, message

    # ------------------------------------------------------------------
    # 刷新任务查询与统计（验收入口）
    # ------------------------------------------------------------------
    def list_refresh_runs(self, *, page: int = 1, size: int = 20) -> tuple[list[dict[str, Any]], int]:
        runs = list(reversed(store.rows(RUN_TABLE)))
        total = len(runs)
        start = max(page - 1, 0) * size
        return runs[start:start + size], total

    def refresh_stats(self) -> dict[str, Any]:
        runs = store.rows(RUN_TABLE)
        stats = {key: 0 for key in STAT_KEYS}
        status_count: dict[str, int] = {}
        for run in runs:
            for key in STAT_KEYS:
                stats[key] += int(run["统计"].get(key, 0))
            status_count[str(run["状态"])] = status_count.get(str(run["状态"]), 0) + 1
        return {
            "统计": stats,
            "任务状态": status_count,
            "任务总数": len(runs),
            "快照总数": len(store.rows(SNAPSHOT_TABLE)),
        }

    # ------------------------------------------------------------------
    # 内部：快照协调（共享层）
    # ------------------------------------------------------------------
    def _acquire_snapshot(
        self, tag_code: str, version: str, cycle: str, owner: str
    ) -> tuple[str, dict[str, Any]]:
        """认领标签版本快照，返回（角色，快照）。

        hit：已有已确认快照，直接复用；
        conflict：另一任务正在计算同一标签版本，等待其确认后转成复用；
        owner：由本任务计算（无快照、失败快照或自己上次中断留下的快照）。
        """
        with self._lock:
            snap = self._find_snapshot(tag_code, version, cycle)
            if snap is not None and snap["状态"] == SNAPSHOT_CONFIRMED:
                return "hit", snap
            if snap is not None and snap["状态"] == SNAPSHOT_COMPUTING and snap["属主任务"] != owner:
                return "conflict", snap
            if snap is None:
                snap = {
                    "id": len(store.rows(SNAPSHOT_TABLE)) + 1,
                    "标签编号": tag_code,
                    "标签版本": version,
                    "刷新周期": cycle,
                }
                store.rows(SNAPSHOT_TABLE).append(snap)
            snap.update({
                "状态": SNAPSHOT_COMPUTING,
                "属主任务": owner,
                "覆盖对象": [],
                "覆盖对象数": 0,
                "批次号": "",
                "失败原因": "",
            })
            self._snapshot_events[int(snap["id"])] = threading.Event()
            return "owner", snap

    def _confirm_snapshot(self, snap: dict[str, Any], objects: list[str], batch: str) -> None:
        with self._lock:
            snap.update({
                "状态": SNAPSHOT_CONFIRMED,
                "覆盖对象": objects,
                "覆盖对象数": len(objects),
                "批次号": batch,
                "最近计算时间": _now(),
                "失败原因": "",
            })
            event = self._snapshot_events.get(int(snap["id"]))
        if event is not None:
            event.set()

    def _wait_snapshot(self, snap: dict[str, Any], timeout: float = 10.0) -> bool:
        """等待属主任务确认快照；只有已确认的结果才允许复用。"""
        event = self._snapshot_events.get(int(snap["id"]))
        if event is not None:
            event.wait(timeout)
        return snap.get("状态") == SNAPSHOT_CONFIRMED

    def _confirmed_objects(self, tag_code: str, cycle: str) -> list[str]:
        """续做时取回成功规则对应的已确认覆盖集合。"""
        tag = self._find_tag(tag_code)
        if tag is None:
            return []
        snap = self._find_snapshot(tag_code, _tag_version(tag), cycle)
        if snap is None or snap["状态"] != SNAPSHOT_CONFIRMED:
            return []
        return list(snap["覆盖对象"])

    def _write_tag_coverage(self, tag: dict[str, Any], snap: dict[str, Any], batch: str) -> None:
        """标签覆盖对象数 = 本批确认的对象数（赋值而非累加），与快照同批次、不重复累计。"""
        tag["覆盖对象数"] = str(snap["覆盖对象数"])
        tag["覆盖批次号"] = batch
        tag["最近计算时间"] = _now()

    # ------------------------------------------------------------------
    # 内部：生效校验与工具
    # ------------------------------------------------------------------
    def _check_effective(self, entry: dict[str, Any]) -> list[str]:
        """标签版本与人群最终规模共同决定分群能否生效，返回问题列表。"""
        problems: list[str] = []
        rule_text = str(entry.get("筛选规则") or "")
        tag_codes = _extract_tag_codes(rule_text)
        if not tag_codes:
            problems.append("筛选规则未引用任何标签，无法确认标签版本")
        cycle = str(entry.get("刷新周期") or "").strip() or "未配置"
        for code in tag_codes:
            tag = self._find_tag(code)
            if tag is None:
                problems.append(f"标签 {code} 不存在或已归档")
                continue
            version = _tag_version(tag)
            snap = self._find_snapshot(code, version, cycle)
            if snap is None or snap["状态"] != SNAPSHOT_CONFIRMED:
                problems.append(f"标签 {code} 当前版本（{version}）尚无已确认的覆盖结果，请先周期刷新")
        size_text = str(entry.get("最终规模") or "").strip()
        batch = str(entry.get("规模批次号") or "").strip()
        if not size_text.isdigit() or not batch:
            problems.append("最终规模缺失或不来自已完成的刷新批次，请先周期刷新")
        else:
            size = int(size_text)
            min_size = str(entry.get("最小规模") or "").strip()
            max_size = str(entry.get("最大规模") or "").strip()
            if min_size.isdigit() and size < int(min_size):
                problems.append(f"最终规模 {size} 低于最小规模 {min_size}")
            if max_size.isdigit() and size > int(max_size):
                problems.append(f"最终规模 {size} 超过最大规模 {max_size}")
        return problems

    def _find_tag(self, code: str) -> dict[str, Any] | None:
        for row in store.rows(TAG_MODULE):
            if row.get("标签编号") == code:
                return row
        return None

    def _find_snapshot(self, tag_code: str, version: str, cycle: str) -> dict[str, Any] | None:
        for row in store.rows(SNAPSHOT_TABLE):
            if (
                row.get("标签编号") == tag_code
                and row.get("标签版本") == version
                and row.get("刷新周期") == cycle
            ):
                return row
        return None

    def _find_run(
        self, seg_code: str, version_key: str, rule_digest: str, cycle: str
    ) -> dict[str, Any] | None:
        for row in store.rows(RUN_TABLE):
            if (
                row.get("分群编号") == seg_code
                and row.get("标签版本") == version_key
                and row.get("筛选规则摘要") == rule_digest
                and row.get("刷新周期") == cycle
            ):
                return row
        return None

    def _new_run(
        self,
        entry: dict[str, Any],
        version_key: str,
        rule_digest: str,
        cycle: str,
        tag_codes: list[str],
        trigger: str,
    ) -> dict[str, Any]:
        rules: list[dict[str, Any]] = [
            {"类型": "标签", "规则": f"标签 {code} 覆盖集合", "标签编号": code, "状态": "待处理", "原因": "", "说明": ""}
            for code in tag_codes
        ]
        rules.append({"类型": "分群条件", "规则": "分群自身筛选条件过滤", "标签编号": "", "状态": "待处理", "原因": "", "说明": ""})
        run = {
            "id": len(store.rows(RUN_TABLE)) + 1,
            "任务编号": f"SEGR-{len(store.rows(RUN_TABLE)) + 1:04d}",
            "分群编号": entry.get("分群编号"),
            "分群id": entry.get("id"),
            "标签版本": version_key,
            "筛选规则摘要": rule_digest,
            "刷新周期": cycle,
            "触发方式": trigger,
            "状态": RUN_RUNNING,
            "规则明细": rules,
            "统计": {key: 0 for key in STAT_KEYS},
            "批次号": "",
            "最终规模": "",
            "进度": f"0/{len(rules)}",
            "开始时间": _now(),
            "完成时间": "",
            "说明": "",
        }
        store.rows(RUN_TABLE).append(run)
        return run

    def _next_batch(self) -> str:
        with self._lock:
            self._batch_seq += 1
            return f"B{datetime.now():%Y%m%d}-{self._batch_seq:04d}"
