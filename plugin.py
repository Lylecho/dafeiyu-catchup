"""掉线补读 — MaiBot 版 v0.2.0

核心原则：只补"麦麦进程离线期间"到达的消息。

- EventHandler 监听所有消息，维护每个会话的 last_seen 高水位（持久化）。
- 后台探针周期检查：last_seen 之后若存在未跟进的消息，且时间差达到阈值，
  判定为经历了真实的离线窗口（开机/重启/掉线），触发补读。
- 麦麦在线期间它自己"选择不回复"的消息（群聊潜水是正常行为）永远不会被
  当成漏读——这是与错误版本 v0.1 的本质区别。
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from datetime import datetime
from pathlib import Path
from typing import Any

from maibot_sdk import API, Command, EventHandler, Field, MaiBotPlugin, PluginConfigBase
from maibot_sdk.types import EventType

logger = logging.getLogger("dafeiyu_catchup")

VERSION = "0.2.1"


class PluginSectionConfig(PluginConfigBase):
    """插件基础配置。"""

    __ui_label__ = "插件"

    enabled: bool = Field(default=True, description="是否启用插件")
    config_version: str = Field(default="2.0.0", description="配置版本")


class CatchupConfig(PluginConfigBase):
    """补读行为配置。"""

    __ui_label__ = "补读"

    enable: bool = Field(default=True, description="启用补读功能")
    bot_qq_account: str = Field(default="3895924298", description="机器人 QQ 号（用于排除自己的消息）")
    min_gap_minutes: int = Field(default=10, description="掉线短于该分钟数不补读")
    lookback_hours: int = Field(default=24, description="补读最多向前追溯多少小时")
    max_messages_per_chat: int = Field(default=20, description="单会话最多补读条数")
    max_chats_per_run: int = Field(default=3, description="单次最多补读会话数")
    chat_cooldown_minutes: int = Field(default=60, description="同会话两次补读的最小间隔")
    probe_interval_seconds: int = Field(default=90, description="探针轮询间隔秒数")
    min_new_messages: int = Field(default=2, description="错过的消息至少多少条才补读")
    dry_run: bool = Field(default=False, description="只记日志不真发")


class PersonaConfig(PluginConfigBase):
    """人设提示配置。"""

    __ui_label__ = "人设"

    persona_hint: str = Field(
        default=(
            "你是大肥鱼：一条圆滚滚的浅蓝色小鲸鱼，主人是须臾枕风。软萌傲娇、特别贪吃"
            "（最爱心心念念白饭），和熟人爱开玩笑、会接梗，把重要的事记在自己的长期记忆小本本里。"
        ),
        description="补聊生成时附加的人设提示（生成走宿主模型，但不含人设 system，需要在这里补充）",
    )
    reply_style_hint: str = Field(
        default=(
            "口语化短句，一两句话，最多一个括号小动作（比如（尾巴晃了晃））。"
            "不逐条复读，不@所有人，不出现'掉线/断网/系统/消息记录'这类词。"
        ),
        description="表达风格提示",
    )


class PluginConfig(PluginConfigBase):
    """插件总配置。"""

    __ui_label__ = "掉线补读"

    plugin: PluginSectionConfig = Field(default_factory=PluginSectionConfig)
    catchup: CatchupConfig = Field(default_factory=CatchupConfig)
    persona: PersonaConfig = Field(default_factory=PersonaConfig)


def _to_ts(value: Any) -> float:
    """把消息时间字段解析为 epoch 秒：兼容 float/int、"YYYY-MM-DD HH:MM:SS"、毫秒时间戳。"""
    if value is None:
        return 0.0
    if isinstance(value, (int, float)):
        try:
            return float(value)
        except (TypeError, ValueError):
            return 0.0
    s = str(value).strip()
    if not s:
        return 0.0
    if s.isdigit():
        v = float(s)
        return v / 1000.0 if v > 1e11 else v
    for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(s[:26], fmt).timestamp()
        except ValueError:
            continue
    try:
        return float(s)
    except (TypeError, ValueError):
        return 0.0


class DaFeiyuCatchupPlugin(MaiBotPlugin):
    """掉线补读：让麦麦上线后知道离线时发生了什么，并自然地接上话。"""

    config_model = PluginConfig

    def __init__(self) -> None:
        super().__init__()
        self._sessions: dict[str, dict[str, Any]] = {}
        self._task: asyncio.Task | None = None
        self._lock = asyncio.Lock()
        self._last_result = "尚未执行过补读"
        self._catchup_count = 0
        self._reg_path = Path("data") / "dafeiyu_catchup_registry.json"
        self._load_registry()

    # ---------- 注册表 ----------

    def _load_registry(self) -> None:
        try:
            if self._reg_path.exists():
                data = json.loads(self._reg_path.read_text(encoding="utf-8"))
                if isinstance(data, dict):
                    self._sessions = data
                    logger.info(f"[Catchup] 已恢复 {len(self._sessions)} 个会话的补读进度")
        except Exception as e:  # noqa: BLE001
            logger.warning(f"[Catchup] 读取补读进度失败: {e}")

    def _save_registry(self) -> None:
        try:
            self._reg_path.parent.mkdir(parents=True, exist_ok=True)
            self._reg_path.write_text(
                json.dumps(self._sessions, ensure_ascii=False, indent=1), encoding="utf-8"
            )
        except Exception as e:  # noqa: BLE001
            logger.warning(f"[Catchup] 保存补读进度失败: {e}")

    # ---------- 生命周期 ----------

    async def on_load(self) -> None:
        if self._task is None:
            self._task = asyncio.create_task(self._probe_loop())
        logger.info(f"[Catchup] 掉线补读 v{VERSION} 已加载（探针 {self.config.catchup.probe_interval_seconds}s）")

    async def on_unload(self) -> None:
        if self._task:
            self._task.cancel()
        self._save_registry()

    async def on_config_update(self, scope: str, config_data: dict[str, object], version: str) -> None:
        """处理配置热重载事件。"""

        del scope, config_data, version

    # ---------- 消息监听 ----------

    @EventHandler("catchup_message_watcher", description="登记会话活跃时间", event_type=EventType.ON_MESSAGE)
    async def watch_messages(self, message: Any = None, stream_id: str = "", **kwargs: Any):
        """推进每个会话的 last_seen 高水位（麦麦在线时收到的消息都已由正常流程处理）。"""
        try:
            if not stream_id:
                return
            sender = self._sender_of(message)
            if self.config.catchup.bot_qq_account and sender == self.config.catchup.bot_qq_account:
                return
            ts = time.time()
            session = self._sessions.setdefault(
                stream_id, {"last_seen": 0.0, "last_catchup": 0.0}
            )
            if ts > float(session.get("last_seen", 0)):
                session["last_seen"] = ts
                self._save_registry()
            return True, True, None, None, None
        except Exception as e:  # noqa: BLE001
            logger.warning(f"[Catchup] 观察消息异常（不影响主流程）: {e}")
            return True, True, None, None, None

    @staticmethod
    def _sender_of(message: Any) -> str:
        if not isinstance(message, dict):
            return ""
        return str(
            message.get("user_id")
            or message.get("sender_id")
            or (message.get("sender") or {}).get("user_id", "")
            or ""
        )

    # ---------- 探针 ----------

    async def _probe_loop(self) -> None:
        logger.info("[Catchup] 探针已启动")
        while True:
            try:
                await asyncio.sleep(max(30, int(self.config.catchup.probe_interval_seconds)))
                await self.run_catchup()
            except asyncio.CancelledError:
                break
            except Exception as e:  # noqa: BLE001
                logger.warning(f"[Catchup] 探针轮异常（继续运行）: {e}")

    # ---------- 补读主流程 ----------

    async def run_catchup(self, lookback_hours: float | None = None) -> str:
        """扫描所有会话：补读 last_seen 之后、进程离线期间到达的消息。"""
        if not self.config.plugin.enabled or not self.config.catchup.enable:
            return "补读未启用"
        results: list[str] = []
        async with self._lock:
            now = time.time()
            streams = await self._collect_streams()
            lookback = (lookback_hours or self.config.catchup.lookback_hours) * 3600
            min_gap = max(1, int(self.config.catchup.min_gap_minutes)) * 60
            cooldown = max(1, int(self.config.catchup.chat_cooldown_minutes)) * 60

            done = 0
            for stream_id in sorted(
                streams, key=lambda s: -float(self._sessions.get(s, {}).get("last_seen", 0))
            ):
                if done >= max(1, int(self.config.catchup.max_chats_per_run)):
                    results.append("……其余会话本轮跳过（达到单次上限）")
                    break
                session = self._sessions.get(stream_id)
                if not session:
                    continue
                last_seen = float(session.get("last_seen", 0))
                if last_seen <= 0:
                    continue
                gap = now - last_seen
                if gap < min_gap:
                    continue
                if now - float(session.get("last_catchup", 0)) < cooldown:
                    continue

                missed, newest = await self._fetch_missed(stream_id, last_seen, lookback, now)
                # 先推进高水位再生成，防止失败后重复补同一段
                session["last_seen"] = max(last_seen, newest or last_seen)
                session["last_catchup"] = now
                self._save_registry()
                if len(missed) < max(1, int(self.config.catchup.min_new_messages)):
                    continue

                is_group = "GroupMessage" in stream_id
                scene = f"群聊（群 {stream_id.rsplit(':', 1)[-1]}）" if is_group else "私聊"
                gap_min = max(1, int(gap // 60))
                transcript = await self._build_transcript(missed)
                if not transcript:
                    continue
                logger.info(
                    f"[Catchup] {stream_id} 离开约 {gap_min} 分钟，补读 {len(missed)} 条"
                    + ("（dry_run）" if self.config.catchup.dry_run else "")
                )
                reply = await self._generate_reply(scene, gap_min, transcript)
                if not reply:
                    results.append(f"{scene}: 生成失败")
                    continue
                if self.config.catchup.dry_run:
                    results.append(f"{scene}: [dry_run] {reply}")
                else:
                    if await self._send(stream_id, reply):
                        self._catchup_count += 1
                        results.append(f"{scene}: {reply}")
                        done += 1
                    else:
                        results.append(f"{scene}: 发送失败")
            if not results:
                results.append("没有检测到需要补读的离线消息")
            return "\n".join(results)

    @staticmethod
    def _sender_of_message(m: dict[str, Any]) -> str:
        return str(
            m.get("user_id")
            or m.get("sender_id")
            or (m.get("sender") or {}).get("user_id", "")
            or ""
        )

    async def _ensure_self_id(self) -> None:
        if self._self_id or self._bot is None:
            return
        try:
            info = await self._bot.get_login_info()
            self._self_id = str(info.get("user_id") or "")
        except Exception as e:
            logger.warning(f"[Catchup] 获取机器人自身 QQ 号失败: {e}")

    async def _fetch_missed(
        self, stream_id: str, last_seen: float, lookback: float, now: float
    ) -> tuple[list[dict[str, Any]], float]:
        """拉取 (last_seen, now-90s] 窗口内的消息；返回 (消息列表, 库内最新时间)。"""
        try:
            rows = await self.ctx.message.get_by_time_in_chat(
                stream_id,
                max(0.0, last_seen + 1),
                now,
            )
        except Exception as e:  # noqa: BLE001
            logger.warning(f"[Catchup] 拉取历史失败（{stream_id}）: {e}")
            return [], 0.0
        rows = self._as_list(rows)
        fresh_edge = now - 90  # 最近 90 秒内的消息视为正常流程已处理
        missed: list[dict[str, Any]] = []
        newest = 0.0
        last_self_ts = 0.0
        for m in rows:
            if not isinstance(m, dict):
                continue
            ts = _to_ts(m.get("time") or m.get("timestamp"))
            newest = max(newest, ts)
            sender = self._sender_of_message(m)
            if self.config.catchup.bot_qq_account and sender == self.config.catchup.bot_qq_account:
                last_self_ts = max(last_self_ts, ts)  # 我们已回复过的部分不算漏读
                continue
            if ts <= last_seen or ts > fresh_edge:
                continue
            who = str(
                m.get("user_nickname")
                or m.get("sender_nickname")
                or (m.get("sender") or {}).get("card", "")
                or (m.get("sender") or {}).get("nickname", "")
                or sender
            )
            missed.append({"time": ts, "sender": sender, "who": who, "msg": m})
        missed.sort(key=lambda x: x["time"])
        limit = max(1, int(self.config.catchup.max_messages_per_chat))
        if len(missed) > limit:
            missed = missed[-limit:]
        missed = [m for m in missed if m["time"] > last_self_ts]
        return missed, newest

    async def _build_transcript(self, missed: list[dict[str, Any]]) -> str:
        lines: list[str] = []
        for item in missed:
            m = item["msg"]
            ts = datetime.fromtimestamp(item["time"]).strftime("%H:%M")
            who = str(
                m.get("user_nickname")
                or m.get("sender_nickname")
                or (m.get("sender") or {}).get("card", "")
                or (m.get("sender") or {}).get("nickname", "")
                or item.get("who")
                or item.get("sender")
                or "某人"
            )
            text = str(m.get("plain_text") or "").strip()
            if not text:
                text = "[图片或特殊消息]"
            lines.append(f"[{ts}] {who}：{text[:160]}")
        return "\n".join(lines)

    # ---------- 生成与发送 ----------

    async def _generate_reply(self, scene: str, gap_min: int, transcript: str) -> str | None:
        try:
            h = datetime.now().hour
            if gap_min >= 2880:
                reason = "有好几天没露面了，像是出了趟远门"
            elif gap_min >= 1440:
                reason = "一整天没露面，像是出门野了一整天"
            elif 23 <= h or h < 6:
                reason = "半夜去睡觉了"
            elif 11 <= h < 14:
                reason = "吃午饭去了"
            elif 17 <= h < 20:
                reason = "吃晚饭去了"
            elif gap_min >= 180:
                reason = "游出去溜达了一圈，顺便巡视鱼缸"
            else:
                reason = "打了个盹、走神了一小会儿"

            prompt = (
                f"{self.config.persona.persona_hint}\n\n"
                f"{self.config.persona.reply_style_hint}\n\n"
                f"现在补一条“刚回来”的消息。情况：你离开了约 {gap_min} 分钟，{scene}里"
                f"错过了这些消息（按时间顺序，“你”是你自己）：\n"
                f"{transcript}\n\n"
                f"要求：\n"
                f"1. 先用一句话自然交代这段时间去干嘛了——合理的方向：{reason}。"
                f"用符合自己身份的方式说，不要真的写“掉线”。\n"
                f"2. 接住错过的消息里最值得回应的一两件事；有具体提问优先回答；"
                f"提到具体的人时用消息里的昵称称呼对方，别认错人。\n"
                f"3. 一两句话，最多一个括号小动作；不逐条复读，不出现“消息记录/补读/系统”。\n"
                f"\n只输出要发送的这一条消息本身。"
            )
            result = await self.ctx.llm.generate(prompt, temperature=0.8)
            reply = str(result.get("response") or "").strip()
            return reply[:500] or None
        except Exception as e:  # noqa: BLE001
            logger.error(f"[Catchup] 生成补聊失败: {e}")
            return None

    async def _send(self, stream_id: str, text: str) -> bool:
        try:
            return bool(await self.ctx.send.text(text, stream_id))
        except Exception as e:  # noqa: BLE001
            logger.error(f"[Catchup] 发送失败（{stream_id}）: {e}")
            return False

    # ---------- 指令 ----------

    @Command("补读状态", description="查看掉线补读插件的会话进度", pattern=r"^/补读状态$")
    async def cmd_status(self, stream_id: str = "", **kwargs: Any):
        if not self._sessions:
            await self.ctx.send.text("还没有登记任何会话进度～", stream_id)
            return True, "空", True
        now = time.time()
        lines = []
        for sid, s in sorted(
            self._sessions.items(), key=lambda kv: -float(kv[1].get("last_seen", 0))
        )[:12]:
            age = int((now - float(s.get("last_seen", 0))) // 60)
            kind = "群" if "GroupMessage" in sid else "私"
            lines.append(f"{kind} {sid.rsplit(':', 1)[-1]}：最后活跃 {age} 分钟前")
        await self.ctx.send.text("补读进度：\n" + "\n".join(lines), stream_id)
        return True, "ok", True

    @Command("补读", description="手动触发一次补读（默认回看 24 小时）", pattern=r"^/补读(?:\s+(\d{1,3}))?$")
    async def cmd_catchup(self, stream_id: str = "", **kwargs: Any):
        matched = (kwargs.get("matched_groups") or {}) if isinstance(kwargs, dict) else {}
        hours = int(matched.get("hours") or 24)
        summary = await self.run_catchup(lookback_hours=float(min(hours, 168)))
        await self.ctx.send.text(f"补读结果（回看 {hours} 小时）：\n{summary}", stream_id)
        return True, "ok", True

    # ---------- WebUI 控制台 ----------

    @API("webui_status", description="读取补读状态摘要", version="1")
    async def webui_status(self) -> dict[str, Any]:
        now = time.time()
        sessions = []
        for sid, s in sorted(
            self._sessions.items(), key=lambda kv: -float(kv[1].get("last_seen", 0))
        )[:10]:
            sessions.append(
                {
                    "id": sid.rsplit(":", 1)[-1],
                    "kind": "群聊" if "GroupMessage" in sid else "私聊",
                    "minutes_since_active": int((now - float(s.get("last_seen", 0))) // 60),
                    "minutes_since_catchup": int((now - float(s.get("last_catchup", 0))) // 60)
                    if s.get("last_catchup")
                    else None,
                }
            )
        return {
            "session_count": len(self._sessions),
            "catchup_count": self._catchup_count,
            "dry_run": "开启" if self.config.catchup.dry_run else "关闭",
            "enabled": self.config.plugin.enabled and self.config.catchup.enable,
            "last_result": self._last_result,
            "sessions": sessions,
        }

    @API("webui_run_3h", description="补读最近 3 小时错过的消息", version="1")
    async def webui_run_3h(self) -> dict[str, Any]:
        summary = await self.run_catchup(lookback_hours=3.0)
        self._last_result = summary
        return {"summary": summary}

    @API("webui_run_24h", description="补读最近 24 小时错过的消息", version="1")
    async def webui_run_24h(self) -> dict[str, Any]:
        summary = await self.run_catchup(lookback_hours=24.0)
        self._last_result = summary
        return {"summary": summary}


def create_plugin() -> DaFeiyuCatchupPlugin:
    """创建掉线补读插件实例。"""

    return DaFeiyuCatchupPlugin()
