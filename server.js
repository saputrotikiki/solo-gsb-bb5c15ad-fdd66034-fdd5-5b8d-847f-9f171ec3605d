/**
 * 多人发言席位 —— HTTP + WebSocket 服务
 *
 * 协议（均为 JSON 文本帧）：
 *
 * 客户端 -> 服务端
 *   { type: "join",         room, nick }
 *   { type: "join",         room, nick, participantId, secret, hostToken }  // 断线重连
 *   { type: "request_speak",  version }
 *   { type: "cancel_speak",   version }
 *   { type: "swap_invite",    version, participantId }  // 向另一名排队者发起换位邀请
 *   { type: "swap_accept",    version, swapId }         // 被邀请方接受
 *   { type: "swap_reject",    version, swapId }         // 被邀请方拒绝
 *   { type: "swap_cancel",    version, swapId }         // 发起方撤回
 *   { type: "leave" }
 *   { type: "start_speaking", version, participantId, durationMin? }  // 主持人；durationMin 为 1~30 整数时开启限时发言
 *   { type: "end_speaking",   version }                 // 主持人
 *   { type: "reorder",        version, order:[pid...] } // 主持人
 *   { type: "remove",         version, participantId }  // 主持人
 *   { type: "undo_host",      version }                 // 主持人：安全撤销最近一次主持人操作
 *   { type: "clear",          version }                 // 主持人：一键清空
 *   { type: "clear_logs",     version }                 // 主持人：仅清空操作记录
 *   { type: "rollcall_start",  version }                // 主持人：启动队首点名接力
 *   { type: "rollcall_confirm",version, callId }        // 当前点名对象确认（同时校验 version + callId）
 *   { type: "rollcall_skip",   version, callId }        // 当前点名对象主动跳过（同时校验 version + callId）
 *   { type: "rollcall_cancel", version }                // 主持人：取消点名
 *   { type: "topic_create", version, title, durationSec }  // 主持人：发起当前议题并设置截止秒数
 *   { type: "topic_end",    version }                      // 主持人：提前结束并结算当前议题
 *   { type: "topic_vote",   version, topicId, choice }     // choice=yes|no|abstain；截止前可改投
 *   { type: "quota_reset",  version, durationMin }         // 主持人：开启/重置发言配额周期（1~30 分钟）
 *   { type: "task_create",  version, participantId, title, deadline }  // 主持人：创建行动项（deadline 为 ms 时间戳）
 *   { type: "task_accept",  version, taskId }     // 负责人：接受（待接受 -> 处理中）
 *   { type: "task_reject",  version, taskId }     // 负责人：拒绝（待接受 -> 已拒绝）
 *   { type: "task_complete",version, taskId }     // 负责人：标记完成（处理中 -> 待确认）
 *   { type: "task_confirm", version, taskId }     // 主持人：确认完成（待确认 -> 已完成）
 *   { type: "task_return",  version, taskId }     // 主持人：退回处理中（待确认 -> 处理中）
 *   { type: "task_deadline_request", version, taskId, deadline, reason }  // 负责人：提交延期申请（待接受/处理中）
 *   { type: "task_deadline_approve", version, taskId }  // 主持人：批准延期（原子更新截止时间）
 *   { type: "task_deadline_reject",  version, taskId }  // 主持人：拒绝延期（保留理由）
 *   { type: "group_generate", version, memberIds:[pid...], groupCount, constraints:[{a,b,type}] }
 *                                                       // 主持人：生成分组讨论方案（type=together|apart）
 *   { type: "question_submit",  version, text }                       // 当前成员：提交现场问题（≤120 字）
 *   { type: "question_support", version, questionId }                 // 当前成员：对未合并问题切换支持状态
 *   { type: "question_merge",   version, sourceId, targetId }         // 主持人：把来源问题并入另一条未合并问题
 *   { type: "question_close",   version, questionId, answer }         // 主持人：填写 1~300 字答复并结案（仅未合并、未结案）
 *
 * 服务端 -> 客户端
 *   { type: "joined",  snapshot, you:{id,secret,host} }
 *   { type: "snapshot", snapshot }
 *   { type: "error",   error, code, snapshot? }
 *
 * 所有队列命令必须携带客户端所见 version；服务端原子校验，
 * 不匹配则拒绝执行并返回最新快照（code = STALE_VERSION）。
 *
 * 安全撤销：主持人的 reorder / remove / start_speaking / end_speaking
 * 四类操作会在快照 undo 字段留下结构化逆操作；之后只要发生任一版本化
 * 状态变更（version 增长），该逆操作立即标记为 invalid 并注明原因，
 * undo_host 同样经过版本守卫，冲突仍返回 STALE_VERSION + 最新快照。
 *
 * 限时发言：start_speaking 携带 durationMin（1~30 分钟）时，快照
 * speaking 下发 startedAt / endsAt / roundId（唯一轮次标识），各客户端
 * 按 serverTs 校准时钟显示同步倒计时。服务端为每轮安排定时任务，到期
 * 自动结束本轮（version+1 并写操作记录，按普通状态变更使挂起撤销失效）。
 * 手动结束、撤销开始、切换发言人、移出/离开/清空都会使旧定时任务失效；
 * 回调内再校验 roundId，旧回调不会结束后续轮次。断线重连后倒计时继续。
 *
 * 累计发言配额：主持人 quota_reset 开启/重置每人 1~30 分钟的累计发言配额，
 * 每次重置产生新的 periodId（周期标识）并将全员已用额度清零；当前发言者
 * 从重置时刻起计入新周期。服务端在每轮发言结束时按实际发言时长结算扣减；
 * 发言人余额耗尽时自动结束本轮发言，余额为 0 的成员禁止再次排队/被请发言
 * （QUOTA_EXHAUSTED）。手动结束与到期回调竞争时，roundId 守卫保证本轮
 * 配额只结算一次；旧周期的到期回调携带旧 periodId，一律作废，不得扣减
 * 新周期。快照 quota 字段下发 periodId、limitMs 与每人 usedMs/remainingMs
 * （当前发言者含实时累计），断线重连后凭快照恢复真实余额。
 * 开始发言与重置周期均走现有版本守卫。
 *
 * 队首点名接力：rollcall_start 启动后，服务端按队列顺序向成员发出
 * ROLLCALL_TIMEOUT_MS（默认 20 秒）确认。快照 rollcall 字段下发 callId、
 * 当前对象 currentId、截止时间 deadline、本轮已点名成员 called、本轮冻结的
 * 待点名单 pending 与总数 total。成员 rollcall_confirm 即从队列移除并开始发言；
 * rollcall_skip 或超时则把该成员移到队尾并自动点名下一人（pending 顺序不变，
 * 因此本轮每个人最多被点名一次）。确认/跳过命令同时校验 version 与 callId，
 * 错误 callId 返回 BAD_CALL；每个被点名成员各安排一个定时回调，回调内再校验
 * roundId（每次点名自增）与 callId，旧回调不会推进新一轮/后续点名。
 * 主持人可 rollcall_cancel；主持人直接请人发言、一键清空、被点名人离开/取消/
 * 被移出都会结束或推进本轮点名。断线不影响服务端计时，重连后凭快照恢复当前
 * 对象与剩余倒计时。点名期间不允许换位/重排，点名启动会使挂起撤销失效。
 *
 * 议题投票：主持人 topic_create 创建当前议题（标题 + 截止秒数），快照 topic
 * 字段下发唯一 topicId、createdAt、deadline、实时票数 choices 与每人选择
 * votes。参与者（含主持人）在截止前对赞成 yes / 反对 no / 弃权 abstain 投
 * 一票，重复投票以 topic_vote 改投为准（每人一票）。投票命令同时校验
 * version 与 topicId：旧议题返回 BAD_TOPIC，截止后返回 TOPIC_CLOSED。
 * 服务端为每个议题安排一个到期定时任务，回调内再校验 topicId 是否仍为
 * 当前议题——主持人发起新议题（旧议题立即提前结算）或提前结束都会先
 * clearTimeout 并替换 room.topic，旧回调直接忽略，绝不结算后续议题。
 * 结算结果（赞成/反对/弃权票数与通过/否决/平票结论）追加到 room.topicHistory
 * 并随快照下发，断线重连后实时票数、剩余时间与已结算结果均连续恢复。
 *
 * 行动项：主持人 task_create 从当前成员中指定负责人、填写事项与截止时间
 * （ms 时间戳）。行动项随快照 tasks 字段下发，状态机为 待接受(pending) ->
 * 处理中(active) -> 待确认(review) -> 已完成(done)；待接受可被负责人拒绝
 * （rejected），待确认可被主持人退回处理中。负责人接受/拒绝/标记完成、
 * 主持人确认/退回，全部经过版本守卫（过期返回 STALE_VERSION + 最新快照），
 * 并校验操作者身份（TASK_NOT_YOURS / FORBIDDEN）与当前状态
 * （BAD_TASK_STATE + 最新快照）——多端重复确认、越权跳转都会被拒绝且不
 * 改变版本。每个行动项保留最近 TASK_HISTORY_LIMIT 条状态变更记录（含操作者
 * 与时间）随快照下发；已完结行动项保留最近 TASK_RESOLVED_LIMIT 条滚动淘汰。
 *
 * 延期申请：负责人可对待接受(pending)/处理中(active)的行动项提交新的截止
 * 时间与理由（task_deadline_request），同一事项仅保留一条待审(pending)申请
 * ——已有待审申请返回 TASK_REQUEST_PENDING；事项离开待接受/处理中（如已
 * 完成/已拒绝）后不能再申请（BAD_TASK_STATE）。主持人可批准
 * （task_deadline_approve：原子更新 deadline 并标记已生效 approved）或拒绝
 * （task_deadline_reject：保留理由，状态置为 rejected）。负责人标记完成使
 * 事项进入待确认时，未审结的申请自动作废（voided）；进入终态（确认完成/
 * 拒绝事项）时同样自动作废。申请与审批命令均携带 version，过期返回
 * STALE_VERSION + 最新快照；非待审申请的审批返回 TASK_REQUEST_NONE。
 * 申请结构随快照每个行动项的 deadlineRequest 字段下发，含申请人、审批人
 * 及各自时间；操作记录（kind:'task'）写入完整申请/审批信息。
 * 行动项无定时任务，断线重连后凭快照完整恢复。
 *
 * 分组讨论：主持人 group_generate 从当前房间成员中选取 2～12 人、设置 2～4
 * 个小组（组数不超过人数），并可对所选成员对添加「必须同组(together)」/
 * 「必须分开(apart)」约束；同组关系按传递闭包生效（a~b、b~c 则 a 必与 c 同组）。
 * 服务端用完整回溯搜索求解：覆盖全部所选成员且每人恰好出现一次，各组人数
 * 之差不超过 1（floor/ceil 两种规模），同时满足全部约束——存在可行方案就
 * 必然找到一个，找不到时返回 GROUP_NO_SOLUTION 且不改变版本、保留上次方案。
 * 方案连同生成时参数（成员对、组数、约束）与每人当时的昵称存入 room.groupPlan
 * 并随快照 groupPlan 字段下发：成员进出不会自动重分或移除成员（方案中已离开
 * 成员标注 present:false 并按留存昵称展示），断线重连后凭快照完整恢复。
 * 生成命令复用主持人权限与版本守卫：过期返回 STALE_VERSION，包含已离开成员、
 * 参数非法（人数/组数/约束越界等）返回 GROUP_BAD_*——均附最新快照且不改版本。
 *
 * 现场问题墙：当前成员可提交不超过 120 字的问题（question_submit），并对未合并
 * 问题切换支持状态（question_support，同一成员对同一问题只计一次，重复命令即取消
 * 支持）。主持人可把一条问题并入另一条未合并问题（question_merge）：目标问题合并
 * 两边支持者（按成员去重，同一成员只计一次），来源问题标记 mergedInto 且不再接受
 * 支持（对已合并问题的支持/合并一律拒绝，计数不变）。
 *
 * 问题答复结案：主持人可对一条未合并、未结案的问题填写 1～300 字答复并结案
 * （question_close）。结案时固定该问题的支持人数与支持者、记录答复人与答复时间，
 * 并把沿合并链归入它的来源问题（直接来源 + 各级间接来源）一并快照入答复记录；
 * 结案后该问题不可再获支持或参与合并（支持/合并/重复结案分别返回
 * QUESTION_CLOSED 且不改变版本与记录），空白或超长答复返回 NO_QUESTION_ANSWER /
 * QUESTION_ANSWER_TOO_LONG。已结案问题连同答复在快照 questions.closed 中单独展示，
 * 已合并来源的最终去向解析到已结案问题时可直接展示最终答复。快照随房间状态保存，
 * 断线重连后凭快照完整恢复。
 *
 * 问题与支持记录随快照 questions 字段下发，断线重连后凭快照完整恢复。提交/支持/
 * 合并/结案全部沿用现有 WebSocket 身份与版本守卫：过期返回 STALE_VERSION + 最新
 * 快照，针对已合并问题的操作返回 QUESTION_MERGED，针对已结案问题的操作返回
 * QUESTION_CLOSED，均不改变计数。页面按支持人数降序展示未合并问题（同票按提交
 * 时间先后），已合并问题可查看合并去向（沿合并链解析到最终目标，目标结案时带最终
 * 答复），已结案问题按结案时间倒序单独展示。
 */

'use strict';

const http = require('http');
const path = require('path');
const crypto = require('crypto');
const express = require('express');
const { WebSocketServer, OPEN } = require('ws');

const PORT = parseInt(process.env.PORT, 10) || 3000;
const HOST = process.env.HOST || '0.0.0.0';
const WS_PATH = process.env.WS_PATH || '/ws';
const ROOM_TTL_MS = parseInt(process.env.ROOM_TTL_MS, 10) || 4 * 60 * 60 * 1000;
const LOG_LIMIT = parseInt(process.env.LOG_LIMIT, 10) || 500;
// 已结束的换位邀请（接受/拒绝/撤回/失效）在快照中保留多久，用于前端展示操作反馈
const SWAP_RESOLVED_TTL_MS = parseInt(process.env.SWAP_RESOLVED_TTL_MS, 10) || 30000;
// 限时发言：时长单位（默认 1 分钟）。测试可调小以快速验证到期自动结束
const SPEAK_TIME_UNIT_MS = parseInt(process.env.SPEAK_TIME_UNIT_MS, 10) || 60000;
const SPEAK_MIN_MINUTES = 1;
const SPEAK_MAX_MINUTES = 30;
// 累计发言配额：时长单位（默认 1 分钟）。测试可调小以快速验证额度耗尽自动结束
const QUOTA_TIME_UNIT_MS = parseInt(process.env.QUOTA_TIME_UNIT_MS, 10) || 60000;
const QUOTA_MIN_MINUTES = 1;
const QUOTA_MAX_MINUTES = 30;
// 队首点名接力：每人的确认时限（默认 20 秒）。测试可调小以快速验证超时接力
const ROLLCALL_TIMEOUT_MS = parseInt(process.env.ROLLCALL_TIMEOUT_MS, 10) || 20000;
// 议题投票：截止时长下限（默认 10 秒）。测试可调小（如 1 秒）以快速验证到期结算
const TOPIC_MIN_SECONDS = parseInt(process.env.TOPIC_MIN_SECONDS, 10) || 10;
// 议题投票：截止时长上限（默认 3600 秒 = 60 分钟）
const TOPIC_MAX_SECONDS = parseInt(process.env.TOPIC_MAX_SECONDS, 10) || 3600;
// 已结算议题在房间中保留的数量（仅保留最近 N 条）
const TOPIC_HISTORY_LIMIT = parseInt(process.env.TOPIC_HISTORY_LIMIT, 10) || 20;
const TOPIC_TITLE_MAX = 80;
const VOTE_CHOICES = ['yes', 'no', 'abstain'];
// 行动项：每个行动项保留的最近状态变更记录条数
const TASK_HISTORY_LIMIT = parseInt(process.env.TASK_HISTORY_LIMIT, 10) || 20;
// 行动项：已完结（已完成/已拒绝）在房间中保留的数量（超出滚动淘汰）
const TASK_RESOLVED_LIMIT = parseInt(process.env.TASK_RESOLVED_LIMIT, 10) || 50;
// 行动项：未完结（待接受/处理中/待确认）数量上限
const TASK_MAX_OPEN = parseInt(process.env.TASK_MAX_OPEN, 10) || 100;
const TASK_TITLE_MAX = 120;
// 延期申请理由的最大长度
const TASK_REASON_MAX = 300;
// 行动项截止时间：容忍客户端时钟偏差，过去 5 分钟内的截止时间仍视为有效
const TASK_DEADLINE_GRACE_MS = 5 * 60 * 1000;
// 行动项截止时间最远可设为 366 天后
const TASK_MAX_AHEAD_MS = 366 * 24 * 60 * 60 * 1000;
// 分组讨论：可选成员数 2～12
const GROUP_MIN_MEMBERS = 2;
const GROUP_MAX_MEMBERS = 12;
// 分组讨论：小组数 2～4
const GROUP_MIN_COUNT = 2;
const GROUP_MAX_COUNT = 4;
// 分组讨论：约束对数上限（12 人全部两两组合）
const GROUP_MAX_CONSTRAINTS = 66;
const GROUP_CONSTRAINT_TYPES = ['together', 'apart'];
// 现场问题墙：问题正文最大长度
const QUESTION_TEXT_MAX = 120;
// 现场问题墙：结案答复长度区间（去空白后按字符数计）
const QUESTION_ANSWER_MIN = 1;
const QUESTION_ANSWER_MAX = 300;
// 现场问题墙：未合并（且未结案）问题数量上限（防止内存无限增长；已合并/已结案问题随来源留存）
const QUESTION_MAX_OPEN = parseInt(process.env.QUESTION_MAX_OPEN, 10) || 300;

const app = express();
const server = http.createServer(app);
const wss = new WebSocketServer({ server, path: WS_PATH });

app.use(express.json({ limit: '32kb' }));
app.use(express.static(path.join(__dirname, 'public')));

// ---------------------------------------------------------------------------
// 内存数据（房间数据仅在事件循环中被同步读写，命令处理天然原子）
// ---------------------------------------------------------------------------

/**
 * room = {
 *   code, hostToken, hostId,
 *   version,                 // 单调递增的状态版本号
 *   participants: Map<id, {id, nick, joinedAt, online, ws?}>,
 *   order: [id],             // 排队顺序
 *   speaking: { id, startedAt, endsAt, roundId, quotaBase } | null,
 *                            // endsAt 为 null 表示不限时；roundId 是唯一轮次标识；
 *                            // quotaBase 是本轮计入配额的计费起点（重置周期/撤销恢复时重定）
 *   speakTimer,              // 当前轮次的到期定时器（Timeout | null）
 *   quota: null | {          // 累计发言配额（null 表示未开启）
 *     periodId,              // 本周期唯一标识（每次开启/重置都更换）
 *     limitMs,               // 每人配额毫秒数
 *     startedAt,             // 周期开启时间
 *     used: Map<pid, ms>     // 每人已结算的发言毫秒数（当前发言者的进行中时长在快照时实时计入）
 *   },
 *   quotaTimer,              // 当前发言人的配额到期定时器（Timeout | null）
 *   rollcall: null | {
 *     callId,                // 本轮点名接力的唯一标识（确认/跳过命令须携带并校验）
 *     roundNo,               // 本轮内第几次点名（每次点名 +1，用于旧定时回调守卫）
 *     currentId,             // 当前点名对象 participantId（null 表示本轮已结束/尚未开始点名）
 *     deadline,              // 当前点名对象的确认截止时间（ms）
 *     timer,                 // 当前点名的到期定时器（Timeout | null）
 *     pending: [id],         // 本轮冻结的待点名名单（启动时按队列顺序拷贝）
 *     called: Set<id>        // 本轮已点名成员（含已确认/已跳过/中途失效）
 *   },
 *   swaps: Map<id, {
 *     id, from, to,          // 发起人 / 目标方 participantId
 *     version, fromPos, toPos,  // 发起时的队列版本与位置（下标）
 *     status: 'pending'|'accepted'|'rejected'|'cancelled'|'invalid',
 *     reason, ts, resolvedAt
 *   }],
 *   undo: {                 // 最近一次可撤销主持人操作（安全撤销）
 *     op, status,           // reorder|remove|start_speaking|end_speaking；available|invalid
 *     version,              // 该操作提交后的版本（撤销锚点：必须等于当前 version）
 *     reason, reasonText,   // 失效原因（status=invalid 时）
 *     ts, summary, payload  // 结构化逆操作数据（仅 status=available 时存在）
 *   } | null,
 *   topic: null | {         // 当前议题投票（null 表示没有进行中的议题）
 *     id, title, createdAt, deadline, timer,
 *     votes: Map<participantId, 'yes'|'no'|'abstain'>
 *   },
 *   topicHistory: [{        // 最近已结算议题（最新在前，超过 TOPIC_HISTORY_LIMIT 滚动淘汰）
 *     id, title, createdAt, closedAt, endedBy,   // endedBy: 'deadline'|'host'|'replaced'
 *     counts: { yes, no, abstain }, total, result  // result: passed|rejected|tie
 *   }],
 *   tasks: Map<id, {        // 行动项（状态机：pending -> active -> review -> done / rejected）
 *     id, title,            // 事项内容
 *     assigneeId,           // 负责人 participantId
 *     assigneeNick,         // 创建时留存的负责人昵称（成员离开房间后仍可展示）
 *     createdBy,            // 创建者（主持人）participantId
 *     createdAt, deadline,  // 创建时间与截止时间（ms 时间戳）
 *     status,               // pending(待接受)|active(处理中)|review(待确认)|done(已完成)|rejected(已拒绝)
 *     updatedAt,            // 最近一次状态变更时间（状态流转或延期生效都会更新）
 *     history: [{ts, from, to, by, byNick}],  // 最近状态变更记录（上限 TASK_HISTORY_LIMIT）
 *     deadlineRequest: null | {  // 最近一次延期申请（同一事项仅保留一条；null 表示从未申请）
 *       id,                 // 申请唯一标识
 *       status,             // pending(待审批)|approved(已批准并生效)|rejected(已拒绝)|voided(已作废)
 *       newDeadline,        // 申请的新截止时间（ms）
 *       reason,             // 申请理由
 *       requestedBy, requestedByNick, requestedAt,  // 申请人与申请时间
 *       decidedBy, decidedByNick, decidedAt         // 审批人与审批时间（待审批时为 null）
 *     }
 *   }>,
 *   groupPlan: null | {     // 最近一次分组讨论方案（null 表示尚未生成；成员进出不影响已有方案）
 *     id, createdAt, groupCount,
 *     members: [{ id, nick, present }],   // 生成时留存的昵称；present=false 表示该成员已离开房间
 *     constraints: [{ a, b, type }],     // 生成时的约束（type: together|apart）
 *     groups: [[idx...]]                  // 分组结果：以 members 下标表示，每人恰好出现一次，组规模差 ≤1
 *   },
 *   questions: Map<id, {      // 现场问题墙（提交、支持、合并、结案；无定时任务）
 *     id, text,               // 问题正文（≤120 字）
 *     authorId, authorNick,   // 提交人（昵称按提交时留存，离开房间后仍可展示）
 *     createdAt,
 *     supporters: Set<pid>,   // 当前支持者（合并时按成员去重并入目标；结案后冻结不再变化）
 *     mergedInto: pid|null,   // 非 null 表示已并入该问题（直接去向；快照解析沿合并链到最终目标）
 *     mergedAt, mergedBy,     // 合并时间与操作者（主持人）
 *     closedAt: ts|null,      // 非 null 表示已结案（仅未合并问题可结案；结案后不可支持/合并/再结案）
 *     answer,                 // 结案答复正文（1~300 字）；结案前为 null
 *     answeredBy,             // 结案答复人 participantId（主持人）
 *     answeredByNick,         // 结案时留存的答复人昵称
 *     answerSources: [       // 结案瞬间沿合并链归入该问题的来源问题快照（直接 + 各级间接，按合并时间升序）
 *       { id, text, authorId, authorNick, createdAt, mergedAt, mergedByNick,
 *         supportCount, supporterIds, supporters: [{id,nick}] }
 *     ],
 *     answerSupporters: [    // 结案瞬间冻结的目标问题支持者快照 [{id,nick}]（支持人数以其长度固定）
 *   }>,
 *   logs: [{id, ts, kind, text}],
 *   createdAt, lastSeen
 * }
 */
const rooms = new Map();

function newRoom(code) {
  return {
    code,
    hostToken: crypto.randomBytes(16).toString('hex'),
    hostId: null,
    version: 1,
    participants: new Map(),
    order: [],
    speaking: null,
    speakTimer: null,
    quota: null,
    quotaTimer: null,
    rollcall: null,
    swaps: new Map(),
    undo: null,
    topic: null,
    topicHistory: [],
    tasks: new Map(),
    groupPlan: null,
    questions: new Map(),
    logs: [],
    createdAt: Date.now(),
    lastSeen: Date.now()
  };
}

function genId(prefix) {
  return prefix + crypto.randomBytes(6).toString('hex');
}

function genRoomCode() {
  for (let i = 0; i < 20; i++) {
    const code = String(crypto.randomInt(0, 1000000)).padStart(6, '0');
    if (!rooms.has(code)) return code;
  }
  // 理论上不会走到这里
  return crypto.randomBytes(3).toString('hex').toUpperCase();
}

function addLog(room, kind, text) {
  room.logs.push({ id: genId('log_'), ts: Date.now(), kind, text });
  if (room.logs.length > LOG_LIMIT) room.logs.splice(0, room.logs.length - LOG_LIMIT);
}

// ---------------------------------------------------------------------------
// 限时发言：服务端定时任务（轮次守卫）
// ---------------------------------------------------------------------------

/** 使当前轮次的到期定时任务失效（手动结束/撤销/切换/移出/清空时调用） */
function clearSpeakTimer(room) {
  if (room.speakTimer) {
    clearTimeout(room.speakTimer);
    room.speakTimer = null;
  }
}

/**
 * 为当前限时发言轮次安排服务端自动结束。
 * 回调内以 roundId 二次校验：旧轮次的回调不得结束后续轮次。
 */
function scheduleSpeakTimer(room) {
  clearSpeakTimer(room);
  const speaking = room.speaking;
  if (!speaking || !speaking.endsAt) return;
  const roundId = speaking.roundId;
  const delay = Math.max(speaking.endsAt - Date.now(), 0);
  room.speakTimer = setTimeout(() => {
    room.speakTimer = null;
    if (!room.speaking || room.speaking.roundId !== roundId) return; // 旧回调作废
    autoEndSpeaking(room, roundId);
  }, delay);
  if (room.speakTimer.unref) room.speakTimer.unref();
}

/**
 * 到时自动结束：与手动结束一样是一次普通版本化状态变更
 * （version+1、广播快照、写操作记录，并使挂起的撤销失效）。
 */
function autoEndSpeaking(room, roundId) {
  const speaking = finalizeSpeaking(room); // 结算配额；限时/配额定时任务一并失效
  const speaker = speaking ? room.participants.get(speaking.id) : null;
  const nick = speaker ? speaker.nick : '已离开';
  addLog(room, 'talk', `「${nick}」发言时间到，自动结束本轮（轮次 ${roundId}）`);
  commit(room, { undoReason: 'speaking_changed' });
}

// ---------------------------------------------------------------------------
// 累计发言配额：周期化额度 + 到期定时任务（periodId + roundId 双重守卫）
// ---------------------------------------------------------------------------

/** 使当前发言人的配额到期定时任务失效（结束/切换/重置/回收时调用） */
function clearQuotaTimer(room) {
  if (room.quotaTimer) {
    clearTimeout(room.quotaTimer);
    room.quotaTimer = null;
  }
}

/**
 * 把一轮发言的实际时长结算进本周期配额（以 limitMs 封顶）。
 * 计费起点 quotaBase：开始发言时等于 startedAt；重置周期 / 撤销恢复时
 * 重定为当时刻，保证旧周期（或已结算）的时长不会重复扣减。
 */
function settleSpeakingQuota(room, speaking) {
  const quota = room.quota;
  if (!quota || !speaking) return;
  const base = speaking.quotaBase ?? speaking.startedAt;
  const elapsed = Math.max(Date.now() - base, 0);
  if (elapsed <= 0) return;
  const prev = quota.used.get(speaking.id) || 0;
  quota.used.set(speaking.id, Math.min(prev + elapsed, quota.limitMs));
}

/**
 * 结束当前发言轮次：结算配额、使限时与配额定时任务失效并清空 speaking。
 * 手动结束与到期回调共用此入口——speaking 置空后，另一路的回调因
 * roundId 不匹配直接作废，本轮配额只会结算一次。
 */
function finalizeSpeaking(room) {
  const speaking = room.speaking;
  if (!speaking) return null;
  settleSpeakingQuota(room, speaking);
  clearSpeakTimer(room);
  clearQuotaTimer(room);
  room.speaking = null;
  return speaking;
}

/**
 * 为当前发言人安排配额到期自动结束。回调内以 periodId + roundId 双重校验：
 * 重置周期或轮次结束后，旧回调一律作废，不得扣减新周期/新轮次。
 */
function scheduleQuotaTimer(room) {
  clearQuotaTimer(room);
  const quota = room.quota;
  const speaking = room.speaking;
  if (!quota || !speaking) return;
  const base = speaking.quotaBase ?? speaking.startedAt;
  const usedBefore = quota.used.get(speaking.id) || 0;
  const exhaustAt = base + Math.max(quota.limitMs - usedBefore, 0);
  const periodId = quota.periodId;
  const roundId = speaking.roundId;
  const delay = Math.max(exhaustAt - Date.now(), 0);
  room.quotaTimer = setTimeout(() => {
    room.quotaTimer = null;
    if (!room.quota || room.quota.periodId !== periodId) return; // 旧周期回调作废
    if (!room.speaking || room.speaking.roundId !== roundId) return; // 旧轮次回调作废
    const speaker = room.participants.get(room.speaking.id);
    const nick = speaker ? speaker.nick : '已离开';
    finalizeSpeaking(room); // 结算本轮实际发言时长（与手动结束竞争时只此一次）
    addLog(room, 'quota', `「${nick}」的发言配额已用尽，自动结束本轮发言`);
    commit(room, { undoReason: 'speaking_changed' });
  }, delay);
  if (room.quotaTimer.unref) room.quotaTimer.unref();
}

/** 某成员本周期剩余配额（仅已结算部分；未开启配额时返回 Infinity） */
function quotaRemainingMs(room, pid) {
  const quota = room.quota;
  if (!quota) return Infinity;
  return quota.limitMs - (quota.used.get(pid) || 0);
}

/**
 * 配额快照视图：周期标识 + 每人已用/剩余毫秒。
 * 当前发言者的本轮进行时长实时计入（尚未结算），
 * 客户端按 serverTs 校准时钟后可继续推演实时余额。
 */
function buildQuotaView(room, now) {
  const quota = room.quota;
  if (!quota) return null;
  const speaking = room.speaking;
  const members = {};
  for (const p of room.participants.values()) {
    let used = quota.used.get(p.id) || 0;
    if (speaking && speaking.id === p.id) {
      used += Math.max(now - (speaking.quotaBase ?? speaking.startedAt), 0);
    }
    used = Math.min(Math.max(used, 0), quota.limitMs);
    members[p.id] = { usedMs: used, remainingMs: quota.limitMs - used };
  }
  return {
    periodId: quota.periodId,
    limitMs: quota.limitMs,
    startedAt: quota.startedAt,
    members
  };
}

// ---------------------------------------------------------------------------
// 队首点名接力
// ---------------------------------------------------------------------------

/** 取消当前点名对象的到期定时任务（保留 rollcall 本体） */
function clearRollcallTimer(room) {
  const rc = room.rollcall;
  if (rc && rc.timer) {
    clearTimeout(rc.timer);
    rc.timer = null;
  }
}

/** 结束本轮点名：清理定时任务并清空 rollcall（不写日志，由调用方负责记录） */
function finishRollcall(room) {
  clearRollcallTimer(room);
  room.rollcall = null;
}

/**
 * 点名进行中，目标因外部操作（取消/被移出/离开）失去队列位置时的统一推进：
 * 立即安排下一人，避免剩余确认时间被浪费。调用方负责最终的单次 commit。
 */
function rollcallTargetGone(room, targetId) {
  const rc = room.rollcall;
  if (!rc || rc.currentId !== targetId) return false;
  const nick = room.participants.get(targetId)?.nick ?? '已离开';
  addLog(room, 'rollcall', `「${nick}」无法继续接受点名，自动点名下一人`);
  callNextRollcall(room);
  return true;
}

/**
 * 启动一轮点名接力：冻结当前队列顺序作为本轮待点名名单，
 * 立即点名队首。调用前已完成主持人与版本守卫，且队列非空。
 */
function startRollcall(room) {
  room.rollcall = {
    callId: genId('cl_'),
    roundNo: 0,
    currentId: null,
    deadline: 0,
    timer: null,
    pending: room.order.slice(),
    called: new Set()
  };
  // 点名期间不允许换位：待处理邀请随点名启动全部失效
  invalidateSwaps(room, 'rollcall');
  addLog(room, 'rollcall', '主持人启动了队首点名接力');
  callNextRollcall(room);
}

/**
 * 点名下一人：取冻结名单中第一个「尚未点名且仍在队列中」的成员；
 * 名单耗尽（本轮每人都已被点过一次）则结束本轮。
 * 每次点名自增 roundNo，作为定时回调的二次守卫。
 */
function callNextRollcall(room) {
  const rc = room.rollcall;
  if (!rc) return;
  clearRollcallTimer(room);
  const nextIdx = rc.pending.findIndex((id) => !rc.called.has(id) && room.order.includes(id));
  if (nextIdx === -1) {
    finishRollcall(room);
    addLog(room, 'rollcall', '本轮点名结束：全部成员均已跳过，接力结束');
    return;
  }
  const targetId = rc.pending[nextIdx];
  rc.currentId = targetId;
  rc.called.add(targetId);
  rc.roundNo += 1;
  rc.deadline = Date.now() + ROLLCALL_TIMEOUT_MS;
  scheduleRollcallTimer(room, targetId, rc.roundNo, rc.deadline);
  const nick = room.participants.get(targetId)?.nick ?? '已离开';
  addLog(room, 'rollcall', `点名「${nick}」，请在 ${Math.round(ROLLCALL_TIMEOUT_MS / 1000)} 秒内确认或跳过`);
}

/**
 * 为当前点名安排超时回调。回调内以 callId + roundNo 双重校验：
 * 旧点名（含跨轮）的定时回调不得推进新一轮/后续点名。
 */
function scheduleRollcallTimer(room, targetId, roundNo, deadline) {
  const rc = room.rollcall;
  if (!rc) return;
  const callId = rc.callId;
  const delay = Math.max(deadline - Date.now(), 0);
  rc.timer = setTimeout(() => {
    if (room.rollcall?.callId !== callId) return; // 旧轮次回调作废
    if (room.rollcall.roundNo !== roundNo) return; // 同一轮内旧点名回调作废
    rollcallTimeout(room, targetId);
  }, delay);
  if (rc.timer.unref) rc.timer.unref();
}

/** 当前点名对象确认：从队列移除并开始发言（不限时），随后自动点名下一人 */
function rollcallConfirm(room, targetId) {
  const rc = room.rollcall;
  const nick = room.participants.get(targetId)?.nick ?? '已离开';
  finishRollcall(room);
  const idx = room.order.indexOf(targetId);
  if (idx !== -1) {
    room.order.splice(idx, 1);
    invalidateSwaps(room, 'started_speaking', (s) => s.from === targetId || s.to === targetId);
  }
  finalizeSpeaking(room); // 防御：正常情况下点名期间无人发言
  const now = Date.now();
  room.speaking = {
    id: targetId,
    startedAt: now,
    endsAt: null,
    roundId: genId('rd_'),
    quotaBase: now
  };
  scheduleQuotaTimer(room); // 配额启用时：余额耗尽自动结束
  addLog(room, 'rollcall', `「${nick}」已确认点名，开始发言`);
  // 点名接力在一人确认发言后结束（同一轮每人最多被点名一次）
  commit(room, { undoReason: 'speaking_changed' });
}

/**
 * 当前点名对象主动跳过或超时：从队首位置移到队尾（若仍在队列中），
 * 随后自动点名下一人。跳过者已在 called 集合中，本轮不会再次被点。
 */
function rollcallSkip(room, targetId, reason) {
  const rc = room.rollcall;
  if (!rc) return;
  const nick = room.participants.get(targetId)?.nick ?? '已离开';
  const idx = room.order.indexOf(targetId);
  if (idx !== -1) {
    room.order.splice(idx, 1);
    room.order.push(targetId);
  }
  addLog(
    room,
    'rollcall',
    reason === 'manual'
      ? `「${nick}」主动跳过，移到队尾`
      : `「${nick}」超时未确认，移到队尾并点名下一人`
  );
  callNextRollcall(room);
  commit(room, { undoReason: 'rollcall_changed' });
}

function rollcallTimeout(room, targetId) {
  // 定时回调内已完成 callId + roundNo 双重校验，这里按超时跳过处理
  rollcallSkip(room, targetId, 'timeout');
}

// ---------------------------------------------------------------------------
// 议题投票：当前议题 + 截止定时任务（topicId 守卫）
// ---------------------------------------------------------------------------

/** 统计当前议题的实时票数 */
function topicCounts(topic) {
  const counts = { yes: 0, no: 0, abstain: 0 };
  for (const choice of topic.votes.values()) {
    counts[choice] += 1;
  }
  return counts;
}

/** 结算结论：赞成多于反对为通过，反对多于赞成为否决，相等（含全弃权）为平票 */
function topicResult(counts) {
  if (counts.yes > counts.no) return 'passed';
  if (counts.no > counts.yes) return 'rejected';
  return 'tie';
}

/** 使当前议题的截止定时任务失效（发起新议题/提前结束/房间回收时调用） */
function clearTopicTimer(room) {
  if (room.topic && room.topic.timer) {
    clearTimeout(room.topic.timer);
    room.topic.timer = null;
  }
}

/**
 * 结算当前议题：统计票数、写入结论与操作记录，归档到 topicHistory。
 * 不做状态提交，由调用方负责最终的单次 commit（与点名推进的写法一致）。
 */
function settleTopic(room, endedBy) {
  const topic = room.topic;
  if (!topic) return null;
  clearTopicTimer(room);
  const counts = topicCounts(topic);
  const total = counts.yes + counts.no + counts.abstain;
  const result = topicResult(counts);
  const entry = {
    id: topic.id,
    title: topic.title,
    createdAt: topic.createdAt,
    closedAt: Date.now(),
    endedBy,
    counts,
    total,
    result
  };
  room.topicHistory.unshift(entry);
  if (room.topicHistory.length > TOPIC_HISTORY_LIMIT) {
    room.topicHistory.length = TOPIC_HISTORY_LIMIT;
  }
  room.topic = null;

  const resultText = { passed: '通过', rejected: '否决', tie: '平票' }[result];
  addLog(
    room,
    'topic',
    `议题「${topic.title}」已结算（${endedBy === 'host' ? '主持人提前结束' : '截止时间到'}）：` +
      `赞成 ${counts.yes} · 反对 ${counts.no} · 弃权 ${counts.abstain}，结论：${resultText}`
  );
  return entry;
}

/**
 * 为当前议题安排到期结算。回调内以 topicId 二次校验：
 * 主持人发起新议题或提前结束都会先 clearTimeout 并替换 room.topic，
 * 旧议题的定时回调触发时发现自己已不是当前议题，直接忽略。
 */
function scheduleTopicTimer(room) {
  clearTopicTimer(room);
  const topic = room.topic;
  if (!topic) return;
  const topicId = topic.id;
  const delay = Math.max(topic.deadline - Date.now(), 0);
  topic.timer = setTimeout(() => {
    topic.timer = null;
    if (!room.topic || room.topic.id !== topicId) return; // 旧回调作废
    settleTopic(room, 'deadline');
    commit(room, { undoReason: 'topic_changed' });
  }, delay);
  if (topic.timer.unref) topic.timer.unref();
}

/**
 * 主持人发起当前议题：若已有进行中的议题，先按「被新议题替换」提前结算，
 * 再创建新议题并安排截止任务。调用前已完成主持人与版本守卫。
 */
function createTopic(room, title, durationSec) {
  if (room.topic) {
    settleTopic(room, 'replaced');
  }
  const now = Date.now();
  const topic = {
    id: genId('tp_'),
    title,
    createdAt: now,
    deadline: now + durationSec * 1000,
    timer: null,
    votes: new Map()
  };
  room.topic = topic;
  addLog(
    room,
    'topic',
    `主持人发起议题「${title}」，投票截止时间 ${durationSec} 秒`
  );
  scheduleTopicTimer(room);
}

/** 当前议题的快照视图：实时票数 + 每人选择 */
function buildTopicView(topic) {
  if (!topic) return null;
  return {
    id: topic.id,
    title: topic.title,
    createdAt: topic.createdAt,
    deadline: topic.deadline,
    counts: topicCounts(topic),
    votedCount: topic.votes.size,
    votes: [...topic.votes.entries()].map(([participantId, choice]) => ({ participantId, choice }))
  };
}

// ---------------------------------------------------------------------------
// 行动项：状态机（待接受 -> 处理中 -> 待确认 -> 已完成 / 已拒绝）
// ---------------------------------------------------------------------------

const TASK_STATUS_TEXT = {
  pending: '待接受',
  active: '处理中',
  review: '待确认',
  done: '已完成',
  rejected: '已拒绝'
};

/**
 * 行动项状态流转：更新状态并追加一条状态变更记录
 * （每个行动项保留最近 TASK_HISTORY_LIMIT 条，超出滚动淘汰）。
 * 进入终态（已完成/已拒绝）时，若仍有未审结的延期申请则一并自动作废，
 * 并在操作记录中注明。不做版本提交，由调用方负责最终的单次 commit
 * （与议题结算的写法一致）。
 */
function transitionTask(room, task, toStatus, by) {
  const now = Date.now();
  task.history.push({ ts: now, from: task.status, to: toStatus, by: by.id, byNick: by.nick });
  if (task.history.length > TASK_HISTORY_LIMIT) {
    task.history.splice(0, task.history.length - TASK_HISTORY_LIMIT);
  }
  task.status = toStatus;
  task.updatedAt = now;
  // 终态：待审延期申请不再可能被处理，随事项一并作废（保留申请理由与记录）
  const req = task.deadlineRequest;
  if ((toStatus === 'done' || toStatus === 'rejected') && req && req.status === 'pending') {
    req.status = 'voided';
    req.decidedBy = by.id;
    req.decidedByNick = by.nick;
    req.decidedAt = now;
    addLog(
      room,
      'task',
      `行动项「${task.title}」已${TASK_STATUS_TEXT[toStatus]}，其待审延期申请自动作废`
    );
  }
}

/** 校验截止时间（ms 时间戳）：容忍 5 分钟客户端时钟偏差，最远 366 天 */
function validTaskDeadline(deadline, now) {
  return (
    Number.isFinite(deadline) &&
    deadline >= now - TASK_DEADLINE_GRACE_MS &&
    deadline <= now + TASK_MAX_AHEAD_MS
  );
}

/** 毫秒时间戳 -> MM-DD HH:mm（操作记录中展示截止时间用） */
function formatDeadlineShort(ts) {
  const d = new Date(ts);
  const p = (n) => String(n).padStart(2, '0');
  return `${p(d.getMonth() + 1)}-${p(d.getDate())} ${p(d.getHours())}:${p(d.getMinutes())}`;
}

/** 已完结行动项滚动淘汰：仅保留最近 TASK_RESOLVED_LIMIT 条（不触碰未完结项，不改变版本） */
function pruneTasks(room) {
  const resolved = [];
  for (const t of room.tasks.values()) {
    if (t.status === 'done' || t.status === 'rejected') resolved.push(t);
  }
  if (resolved.length <= TASK_RESOLVED_LIMIT) return;
  resolved.sort((a, b) => b.updatedAt - a.updatedAt);
  for (const t of resolved.slice(TASK_RESOLVED_LIMIT)) {
    room.tasks.delete(t.id);
  }
}

/** 延期申请快照视图：完整下发状态、理由、申请人/审批人及时间 */
function buildDeadlineRequestView(req) {
  if (!req) return null;
  return {
    id: req.id,
    status: req.status,
    newDeadline: req.newDeadline,
    reason: req.reason,
    requestedBy: req.requestedBy,
    requestedByNick: req.requestedByNick,
    requestedAt: req.requestedAt,
    decidedBy: req.decidedBy ?? null,
    decidedByNick: req.decidedByNick ?? null,
    decidedAt: req.decidedAt ?? null
  };
}

/** 行动项快照视图：负责人昵称实时解析（已离开房间则用创建时留存的昵称） */
function buildTaskView(room, task) {
  const assignee = room.participants.get(task.assigneeId);
  return {
    id: task.id,
    title: task.title,
    assigneeId: task.assigneeId,
    assigneeNick: assignee ? assignee.nick : task.assigneeNick,
    createdBy: task.createdBy,
    createdAt: task.createdAt,
    deadline: task.deadline,
    status: task.status,
    updatedAt: task.updatedAt,
    deadlineRequest: buildDeadlineRequestView(task.deadlineRequest),
    history: task.history.map((h) => ({ ts: h.ts, from: h.from, to: h.to, by: h.by, byNick: h.byNick }))
  };
}

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// 分组讨论：约束求解（并查集传递同组 + 完整回溯分组）
// ---------------------------------------------------------------------------

/**
 * 依据成员与约束生成分组方案。
 *
 * @param {string[]} memberIds  所选成员 id（调用方已保证均为当前房间成员、无重复）
 * @param {number} groupCount  小组数（2～4 且不超过人数）
 * @param {{a:string,b:string,type:'together'|'apart'}[]} constraints 成员对约束
 * @returns {{ok:true, groups: number[][]} | {ok:false, reason:'direct_conflict'|'no_solution'}}
 *   groups 以成员下标数组表示，每个下标 0..n-1 恰好出现一次；组规模差不超过 1。
 *
 * 同组约束按传递关系生效：先用并查集把 together 边连成「同组块」，块作为
 * 不可分拆的整体再做回溯分组。规模 n ≤ 12，块数 ≤ 12；空组只能按固定顺序
 * 使用（新块只能放进第一个空组），仅此一条即完整打破组间排列对称——
 * 每个块只放置一次，任意两个非空组的块集合必然不同，天然可区分。
 * 存在可行解时完整遍历必然找到；遍历完毕仍无解则确认为无解（不忽略任何约束）。
 */
function solveGroups(memberIds, groupCount, constraints) {
  const n = memberIds.length;

  // 1) together 边求传递闭包（并查集）
  const parent = memberIds.map((_, i) => i);
  const find = (x) => {
    while (parent[x] !== x) {
      parent[x] = parent[parent[x]];
      x = parent[x];
    }
    return x;
  };
  const union = (x, y) => {
    const rx = find(x);
    const ry = find(y);
    if (rx !== ry) parent[rx] = ry;
  };
  const idxOf = new Map(memberIds.map((id, i) => [id, i]));
  for (const c of constraints) {
    if (c.type === 'together') union(idxOf.get(c.a), idxOf.get(c.b));
  }

  // 2) 同组块：根 -> 成员下标集合；大块优先放置（更早暴露容量冲突）
  const blockMap = new Map();
  for (let i = 0; i < n; i++) {
    const r = find(i);
    if (!blockMap.has(r)) blockMap.set(r, []);
    blockMap.get(r).push(i);
  }
  const blocks = [...blockMap.values()].sort((a, b) => b.length - a.length);
  // 组规模：k 个组中 cap 个为 base+1 人，其余为 base 人（差恰好不超过 1）
  const base = Math.floor(n / groupCount);
  const cap = n - base * groupCount;
  const maxSize = base + 1;
  if (blocks[0].length > maxSize) {
    return { ok: false, reason: 'no_solution' }; // 同组块超过最大组规模，必无解
  }

  // 3) apart 约束提升为「块对块」冲突矩阵（a~b 同组又必须分开 = 直接矛盾）
  const m = blocks.length;
  const blockOf = new Array(n);
  blocks.forEach((blk, bi) => blk.forEach((idx) => (blockOf[idx] = bi)));
  const apart = Array.from({ length: m }, () => new Set());
  for (const c of constraints) {
    if (c.type !== 'apart') continue;
    const ba = blockOf[idxOf.get(c.a)];
    const bb = blockOf[idxOf.get(c.b)];
    if (ba === bb) return { ok: false, reason: 'direct_conflict' };
    apart[ba].add(bb);
    apart[bb].add(ba);
  }

  // 4) 回溯：按 blocks 顺序逐块放入 0..k-1 组；组内记录已放入的块编号
  const groups = Array.from({ length: groupCount }, () => []);
  const sizes = new Array(groupCount).fill(0);
  let solution = null;

  function dfs(bi, usedCap) {
    if (bi === m) {
      if (usedCap === cap) solution = groups.map((g) => g.slice());
      return;
    }
    const size = blocks[bi].length;
    const remainingBlocks = m - bi - 1;
    // 第一个空组的位置（无空组时为 -1）
    let firstEmpty = -1;
    for (let g = 0; g < groupCount; g++) {
      if (sizes[g] === 0) {
        firstEmpty = g;
        break;
      }
    }
    // 剩余块（不含当前块）必须足够填满当前块填一个空组后仍空着的组（每组至少 1 块）
    if (firstEmpty !== -1 && remainingBlocks < groupCount - firstEmpty - 1) return;

    for (let g = 0; g < groupCount; g++) {
      // 对称剪枝：空组只能按固定顺序使用——只允许进入第一个空组
      if (firstEmpty !== -1 && g > firstEmpty && sizes[g] === 0) break;
      if (sizes[g] + size > maxSize) continue; // 不超最大组规模
      // 放入后超过 base 人的组即消耗一个扩容名额，名额总数为 cap
      const capUsed = sizes[g] + size > base && sizes[g] < base + 1 ? 1 : 0;
      if (usedCap + capUsed > cap) continue;
      // apart：本组已含任一与当前块冲突的块则不可放入
      let conflict = false;
      for (const placedBlock of groups[g]) {
        if (apart[bi].has(placedBlock)) {
          conflict = true;
          break;
        }
      }
      if (conflict) continue;

      groups[g].push(bi);
      sizes[g] += size;
      dfs(bi + 1, usedCap + capUsed);
      groups[g].pop();
      sizes[g] -= size;
      if (solution) return;
    }
  }

  dfs(0, 0);
  if (!solution) return { ok: false, reason: 'no_solution' };
  // 块编号还原为成员下标；组内按所选名单顺序，组间按首成员位置排序，输出稳定
  const result = solution.map((grp) => grp.flatMap((bi) => blocks[bi]).sort((a, b) => a - b));
  result.sort((a, b) => a[0] - b[0]);
  return { ok: true, groups: result };
}

/**
 * 分组方案快照视图：昵称一律使用方案生成时留存的昵称（成员之后改名也不改变方案
 * 展示），仅以 present 标注该成员是否仍在房间；离开成员同样按留存昵称展示。
 */
function buildGroupPlanView(room) {
  const plan = room.groupPlan;
  if (!plan) return null;
  const nickOfPlan = new Map(plan.members.map((m) => [m.id, m.nick]));
  const viewOf = (m) => ({
    id: m.id,
    nick: m.nick, // 生成时昵称：成员进出/改名都不影响本方案展示
    present: room.participants.has(m.id)
  });
  return {
    id: plan.id,
    createdAt: plan.createdAt,
    groupCount: plan.groupCount,
    members: plan.members.map(viewOf),
    constraints: plan.constraints.map((c) => ({
      a: c.a,
      b: c.b,
      type: c.type,
      aNick: nickOfPlan.get(c.a) ?? '已离开',
      bNick: nickOfPlan.get(c.b) ?? '已离开'
    })),
    groups: plan.groups.map((grp) => grp.map((idx) => viewOf(plan.members[idx])))
  };
}

// ---------------------------------------------------------------------------
// 现场问题墙：提交 / 支持切换 / 合并
// ---------------------------------------------------------------------------

/**
 * 沿 mergedInto 链解析一条问题的最终去向。合并只允许目标为未合并问题，
 * 因此链必然终止于某条未合并问题（房间生命周期内问题记录不删除）。
 * @returns {{ rootId: string, hops: number }} rootId 为最终并入的问题 id；
 *          hops 为跳转次数（直接合并为 1）。问题不存在时 rootId 为其自身 id。
 */
function resolveQuestionRoot(room, questionId) {
  let cur = questionId;
  let hops = 0;
  const seen = new Set([cur]); // 防御：理论上无环，加 visited 杜绝意外死循环
  for (;;) {
    const q = room.questions.get(cur);
    if (!q || !q.mergedInto) return { rootId: cur, hops };
    cur = q.mergedInto;
    hops += 1;
    if (seen.has(cur)) {
      // 不应出现：一旦成环，回退到环入口的上一个问题作为去向
      return { rootId: cur, hops };
    }
    seen.add(cur);
  }
}

/**
 * 结案时冻结的支持者/来源问题昵称解析：优先取房间内当前昵称，
 * 成员已离开时回退到留存昵称（再兜底「已离开」），保证快照可追溯。
 */
function resolveQuestionNick(room, pid, fallbackNick) {
  const p = room.participants.get(pid);
  if (p) return p.nick;
  return fallbackNick || '已离开';
}

/** 结案瞬间冻结一名支持者（昵称一并留存） */
function freezeQuestionSupporter(room, pid) {
  return { id: pid, nick: resolveQuestionNick(room, pid, null) };
}

/** 结案瞬间沿合并链收集全部来源问题快照（直接来源 + 各级间接来源，按合并时间升序） */
function freezeQuestionSources(room, targetId) {
  const sources = [];
  for (const q of room.questions.values()) {
    if (!q.mergedInto) continue;
    const root = resolveQuestionRoot(room, q.id);
    if (root.rootId !== targetId || root.hops === 0) continue;
    sources.push({
      id: q.id,
      text: q.text,
      authorId: q.authorId,
      authorNick: q.authorNick,
      createdAt: q.createdAt,
      mergedInto: q.mergedInto,
      mergedRootId: root.rootId,
      mergedAt: q.mergedAt,
      mergedByNick: q.mergedBy ? resolveQuestionNick(room, q.mergedBy, '主持人') : '主持人',
      supportCount: q.supporters.size,
      supporterIds: [...q.supporters],
      supporters: [...q.supporters].map((sp) => ({ id: sp, nick: resolveQuestionNick(room, sp, null) }))
    });
  }
  sources.sort((a, b) => (a.mergedAt || 0) - (b.mergedAt || 0) || (a.id < b.id ? -1 : 1));
  return sources;
}

/** 未合并、未结案问题快照视图：支持者实时昵称解析（已离开房间则标注，但支持计数仍保留） */
function buildOpenQuestionView(room, q) {
  const supporters = [...q.supporters].map((pid) => {
    const p = room.participants.get(pid);
    return {
      id: pid,
      nick: p ? p.nick : '已离开',
      present: room.participants.has(pid)
    };
  });
  return {
    id: q.id,
    text: q.text,
    authorId: q.authorId,
    authorNick: room.participants.get(q.authorId)?.nick ?? q.authorNick,
    authorPresent: room.participants.has(q.authorId),
    createdAt: q.createdAt,
    supportCount: q.supporters.size,
    supporterIds: [...q.supporters],
    supporters,
    merged: false,
    closed: false
  };
}

/**
 * 已合并问题快照视图：直接去向 + 沿合并链解析的最终目标。
 * 最终目标若已结案，额外带上最终答复（rootClosed/rootAnswer/rootAnsweredByNick/rootClosedAt），
 * 保证来源问题仍能找到最终答复。
 */
function buildMergedQuestionView(room, q) {
  const root = resolveQuestionRoot(room, q.id);
  const rootQ = room.questions.get(root.rootId);
  const supporters = [...q.supporters].map((pid) => {
    const p = room.participants.get(pid);
    return {
      id: pid,
      nick: p ? p.nick : '已离开',
      present: room.participants.has(pid)
    };
  });
  const view = {
    id: q.id,
    text: q.text,
    authorId: q.authorId,
    authorNick: room.participants.get(q.authorId)?.nick ?? q.authorNick,
    authorPresent: room.participants.has(q.authorId),
    createdAt: q.createdAt,
    supportCount: q.supporters.size,
    supporterIds: [...q.supporters],
    supporters,
    merged: true,
    mergedInto: q.mergedInto,
    mergedIntoText: rootQ ? rootQ.text : null,
    mergedRootId: root.rootId,
    mergedAt: q.mergedAt ?? null,
    mergedByNick: q.mergedBy ? room.participants.get(q.mergedBy)?.nick ?? '主持人' : null,
    rootClosed: rootQ ? rootQ.closedAt !== null : false,
    rootAnswer: rootQ && rootQ.closedAt !== null ? rootQ.answer : null,
    rootAnsweredByNick: rootQ && rootQ.closedAt !== null ? rootQ.answeredByNick : null,
    rootClosedAt: rootQ && rootQ.closedAt !== null ? rootQ.closedAt : null
  };
  return view;
}

/**
 * 已结案问题快照视图：答复与结案时固定的支持人数/支持者/沿链来源全部取冻结快照，
 * 昵称按结案时留存（成员事后离开/改名不影响可追溯记录）。
 */
function buildClosedQuestionView(q) {
  return {
    id: q.id,
    text: q.text,
    authorId: q.authorId,
    authorNick: q.authorNick,
    createdAt: q.createdAt,
    closedAt: q.closedAt,
    answer: q.answer,
    answeredBy: q.answeredBy,
    answeredByNick: q.answeredByNick,
    supportCount: q.answerSupporters.length,
    supporterIds: q.answerSupporters.map((s) => s.id),
    supporters: q.answerSupporters.map((s) => ({ id: s.id, nick: s.nick })),
    sources: q.answerSources
  };
}

/**
 * 问题墙快照视图：
 * - open：未合并、未结案问题，按支持人数降序（同票按提交时间升序，再按 id 稳定兜底）；
 * - merged：已合并问题（无论最终目标是否结案），按合并时间降序（最新合并在前）；
 * - closed：已结案问题（连同答复与冻结记录），按结案时间降序。
 */
function buildQuestionsView(room) {
  const open = [];
  const merged = [];
  const closed = [];
  for (const q of room.questions.values()) {
    if (q.mergedInto) {
      merged.push(buildMergedQuestionView(room, q));
    } else if (q.closedAt !== null) {
      closed.push(buildClosedQuestionView(q));
    } else {
      open.push(buildOpenQuestionView(room, q));
    }
  }
  open.sort((a, b) => {
    if (b.supportCount !== a.supportCount) return b.supportCount - a.supportCount;
    if (a.createdAt !== b.createdAt) return a.createdAt - b.createdAt;
    return a.id < b.id ? -1 : a.id > b.id ? 1 : 0;
  });
  merged.sort((a, b) => (b.mergedAt || 0) - (a.mergedAt || 0));
  closed.sort((a, b) => (b.closedAt || 0) - (a.closedAt || 0));
  return { open, merged, closed };
}

// ---------------------------------------------------------------------------
// 安全撤销
// ---------------------------------------------------------------------------
/** 介入变更原因 -> 快照/日志中展示的失效说明 */
const UNDO_REASON_TEXT = {
  queue_changed: '队列出现新的变更',
  speaking_changed: '当前发言状态出现新的变更',
  presence_changed: '房间成员发生变化',
  swap_changed: '换位状态出现新的变更',
  rollcall_changed: '队首点名接力状态出现新的变更',
  topic_changed: '议题投票状态出现新的变更',
  quota_changed: '发言配额周期出现新的变更',
  task_changed: '行动项状态出现新的变更',
  group_changed: '分组讨论方案出现新的变更',
  question_changed: '现场问题墙出现新的变更',
  cleared: '主持人清空了队列',
  logs_cleared: '主持人清空了操作记录',
  state_changed: '房间状态出现新的变更'
};

/**
 * 为一次可撤销的主持人操作（reorder/remove/start_speaking/end_speaking）
 * 武装结构化逆操作。必须在该操作 commit() 之前调用：
 * 提交后的版本号作为撤销锚点，之后任一版本化变更都会使其失效。
 */
function armUndo(room, op, summary, payload) {
  room.undo = {
    op,
    status: 'available',
    version: room.version + 1, // commit 会先把 version +1
    reason: null,
    reasonText: null,
    ts: Date.now(),
    summary,
    payload
  };
}

/**
 * 在一次非撤销提交前，使仍挂起的撤销失效。由 commit 统一调用，
 * 保证「其后出现任一版本化状态变更，撤销立即失效」。
 */
function invalidateUndo(room, reason) {
  const undo = room.undo;
  if (!undo || undo.status !== 'available') return;
  undo.status = 'invalid';
  undo.reason = reason;
  undo.reasonText = UNDO_REASON_TEXT[reason] || UNDO_REASON_TEXT.state_changed;
  delete undo.payload;
}

function buildUndoSnapshot(room) {
  const undo = room.undo;
  if (!undo) return null;
  return {
    op: undo.op,
    status: undo.status,
    version: undo.version,
    ts: undo.ts,
    summary: undo.summary || null,
    reason: undo.reason || null,
    reasonText: undo.reasonText || null
  };
}

// ---------------------------------------------------------------------------
// 协商换位
// ---------------------------------------------------------------------------

/** 邀请失效/结束原因 -> 操作记录文案 */
const SWAP_REASON_TEXT = {
  rejected: '对方拒绝了换位',
  cancelled: '邀请已撤回',
  accepted: '双方已交换位置',
  canceled_speak: '一方取消了发言申请',
  started_speaking: '一方开始发言',
  removed: '一方被移出队列',
  left: '一方离开了房间',
  reordered: '主持人调整了发言顺序',
  cleared: '主持人清空了队列',
  position_changed: '双方位置已变化',
  rollcall: '主持人启动了队首点名接力'
};

/** 找到某参与者当前挂起的换位邀请（作为发起方或目标方均可） */
function pendingSwapOf(room, participantId) {
  for (const s of room.swaps.values()) {
    if (s.status === 'pending' && (s.from === participantId || s.to === participantId)) {
      return s;
    }
  }
  return null;
}

/**
 * 使满足条件的待处理邀请失效并写操作记录。
 * - filter 省略：失效房间内全部待处理邀请
 * - filter(id) 返回 true 的邀请才失效
 * - nickOf(side, swap) 用于在参与者可能已被移除时仍能写出昵称
 * 返回是否有邀请被失效（调用方据此决定是否 commit）。
 */
function invalidateSwaps(room, reason, filter, nickOf) {
  const dead = [];
  for (const s of room.swaps.values()) {
    if (s.status !== 'pending') continue;
    if (filter && !filter(s)) continue;
    s.status = 'invalid';
    s.reason = reason;
    s.resolvedAt = Date.now();
    dead.push(s);
  }
  for (const s of dead) {
    const fromNick = nickOf ? nickOf('from', s) : room.participants.get(s.from)?.nick ?? '已离开';
    const toNick = nickOf ? nickOf('to', s) : room.participants.get(s.to)?.nick ?? '已离开';
    addLog(
      room,
      'swap',
      `「${fromNick}」与「${toNick}」的换位邀请失效：${SWAP_REASON_TEXT[reason] || reason}`
    );
  }
  return dead.length > 0;
}

/** 清理过期的已结束邀请（不触碰 pending，不改变版本） */
function pruneSwaps(room) {
  const now = Date.now();
  for (const [id, s] of room.swaps) {
    if (s.status !== 'pending' && s.resolvedAt && now - s.resolvedAt > SWAP_RESOLVED_TTL_MS) {
      room.swaps.delete(id);
    }
  }
}

function uniqueNick(room, nick) {
  let name = nick.trim().slice(0, 20);
  if (!name) name = '参与者';
  const used = new Set([...room.participants.values()].map((p) => p.nick));
  if (!used.has(name)) return name;
  for (let i = 2; i < 100; i++) {
    const candidate = `${name}(${i})`;
    if (!used.has(candidate)) return candidate;
  }
  return name + crypto.randomBytes(2).toString('hex');
}

/**
 * 构建快照。服务端时钟随快照下发（serverTs），
 * 客户端据此修正时钟偏差计算发言时长。
 */
function buildSnapshot(room) {
  const serverTs = Date.now();
  pruneSwaps(room);
  pruneTasks(room);
  return {
    room: room.code,
    version: room.version,
    hostId: room.hostId,
    serverTs,
    order: room.order.slice(),
    speaking: room.speaking
      ? {
          id: room.speaking.id,
          nick: room.participants.get(room.speaking.id)?.nick ?? '已离开',
          startedAt: room.speaking.startedAt,
          endsAt: room.speaking.endsAt ?? null,
          roundId: room.speaking.roundId
        }
      : null,
    rollcall: room.rollcall
      ? {
          callId: room.rollcall.callId,
          currentId: room.rollcall.currentId, // 本轮当前点名对象（null 表示本轮已结束）
          deadline: room.rollcall.deadline,
          roundNo: room.rollcall.roundNo,
          total: room.rollcall.pending.length,
          pending: room.rollcall.pending.filter((id) => !room.rollcall.called.has(id)),
          called: [...room.rollcall.called]
        }
      : null,
    quota: buildQuotaView(room, serverTs),
    participants: [...room.participants.values()].map((p) => ({
      id: p.id,
      nick: p.nick,
      online: p.online,
      joinedAt: p.joinedAt
    })),
    swaps: [...room.swaps.values()].map((s) => ({
      id: s.id,
      from: s.from,
      to: s.to,
      version: s.version,
      fromPos: s.fromPos,
      toPos: s.toPos,
      status: s.status,
      reason: s.reason || null,
      ts: s.ts,
      resolvedAt: s.resolvedAt || null
    })),
    undo: buildUndoSnapshot(room),
    topic: buildTopicView(room.topic),
    tasks: [...room.tasks.values()].map((t) => buildTaskView(room, t)),
    groupPlan: buildGroupPlanView(room),
    questions: buildQuestionsView(room),
    topicHistory: room.topicHistory.map((t) => ({
      id: t.id,
      title: t.title,
      createdAt: t.createdAt,
      closedAt: t.closedAt,
      endedBy: t.endedBy,
      counts: { yes: t.counts.yes, no: t.counts.no, abstain: t.counts.abstain },
      total: t.total,
      result: t.result
    })),
    logs: room.logs.slice(-200)
  };
}

function send(ws, obj) {
  if (ws && ws.readyState === OPEN) {
    ws.send(JSON.stringify(obj));
  }
}

/**
 * 版本 +1 并向房间内所有在线连接广播最新快照。
 * opts.undoArm:    本次提交是一次可撤销的主持人操作（room.undo 已由 armUndo 武装）。
 * opts.undoConsume:本次提交就是撤销本身（清除逆操作，不再失效/重新武装）。
 * opts.undoReason: 其他版本化变更使挂起的撤销失效时展示的原因。
 */
function commit(room, opts = {}) {
  if (opts.undoConsume) {
    room.undo = null;
  } else if (!opts.undoArm && room.undo && room.undo.status === 'available') {
    invalidateUndo(room, opts.undoReason || 'state_changed');
  }
  room.version += 1;
  room.lastSeen = Date.now();
  const snap = { type: 'snapshot', snapshot: buildSnapshot(room) };
  const data = JSON.stringify(snap);
  for (const p of room.participants.values()) {
    if (p.ws && p.ws.readyState === OPEN) p.ws.send(data);
  }
}

function sendError(ws, message, code, room) {
  const msg = { type: 'error', error: message, code };
  if (room) msg.snapshot = buildSnapshot(room);
  send(ws, msg);
}

// ---------------------------------------------------------------------------
// HTTP：创建房间
// ---------------------------------------------------------------------------

app.post('/api/rooms', (req, res) => {
  const nick = String(req.body?.nick ?? '').trim().slice(0, 20) || '主持人';
  const code = genRoomCode();
  const room = newRoom(code);
  rooms.set(code, room);

  const pid = genId('p_');
  room.participants.set(pid, {
    id: pid,
    nick: uniqueNick(room, nick),
    joinedAt: Date.now(),
    online: false,
    ws: null
  });
  addLog(room, 'system', `房间已创建，主持人「${room.participants.get(pid).nick}」`);
  room.hostId = pid;

  res.json({
    room: code,
    version: room.version,
    participantId: pid,
    hostToken: room.hostToken
  });
});

app.get('/healthz', (_req, res) => res.json({ ok: true, rooms: rooms.size }));

// ---------------------------------------------------------------------------
// WebSocket
// ---------------------------------------------------------------------------

wss.on('connection', (ws) => {
  let room = null;
  let pid = null;
  let isHost = false;
  ws.isAlive = true;

  ws.on('pong', () => {
    ws.isAlive = true;
  });

  ws.on('message', (raw) => {
    let msg;
    try {
      msg = JSON.parse(raw.toString());
    } catch {
      return sendError(ws, '消息不是合法的 JSON', 'BAD_MESSAGE');
    }
    if (!msg || typeof msg.type !== 'string') {
      return sendError(ws, '缺少 type 字段', 'BAD_MESSAGE');
    }

    try {
      switch (msg.type) {
        case 'join':
          return handleJoin(ws, msg, (ctx) => {
            room = ctx.room;
            pid = ctx.pid;
            isHost = ctx.isHost;
          });

        case 'leave':
          if (!room) return;
          return handleLeave(room, pid, 'leave');

        case 'request_speak':
        case 'cancel_speak':
        case 'swap_invite':
        case 'swap_accept':
        case 'swap_reject':
        case 'swap_cancel':
        case 'start_speaking':
        case 'end_speaking':
        case 'reorder':
        case 'remove':
        case 'undo_host':
        case 'clear':
        case 'clear_logs':
        case 'rollcall_start':
        case 'rollcall_confirm':
        case 'rollcall_skip':
        case 'rollcall_cancel':
        case 'topic_create':
        case 'topic_end':
        case 'topic_vote':
        case 'quota_reset':
        case 'task_create':
        case 'task_accept':
        case 'task_reject':
        case 'task_complete':
        case 'task_confirm':
        case 'task_return':
        case 'task_deadline_request':
        case 'task_deadline_approve':
        case 'task_deadline_reject':
        case 'group_generate':
        case 'question_submit':
        case 'question_support':
        case 'question_merge':
        case 'question_close':
          return handleCommand(ws, msg, room, pid, isHost);

        default:
          return sendError(ws, `未知消息类型：${msg.type}`, 'UNKNOWN_TYPE', room);
      }
    } catch (err) {
      console.error('处理消息出错：', err);
      sendError(ws, '服务端内部错误', 'INTERNAL', room);
    }
  });

  ws.on('close', () => {
    if (!room || !pid) return;
    // 若该连接已被重连的新连接顶替，旧连接关闭时不做清理
    const current = room.participants.get(pid);
    if (!current || current.ws !== ws) return;
    handleLeave(room, pid, 'disconnect');
  });

  ws.on('error', () => {
    // close 事件会负责清理
  });
});

// ---------------------------------------------------------------------------
// 加入 / 重连
// ---------------------------------------------------------------------------

function handleJoin(ws, msg, bind) {
  const code = String(msg.room ?? '').trim().toUpperCase();
  const target = rooms.get(code);
  if (!target) {
    return sendError(ws, '房间不存在或已过期', 'NO_ROOM');
  }

  const hostToken = typeof msg.hostToken === 'string' ? msg.hostToken : null;
  const asHost = hostToken && hostToken === target.hostToken;

  // 携带 hostToken：恢复/接管主持人身份
  if (asHost) {
    const existingId = typeof msg.participantId === 'string' ? msg.participantId : null;
    const p = existingId ? target.participants.get(existingId) : null;

    if (p) {
      // 旧连接让位
      if (p.ws && p.ws !== ws && p.ws.readyState === OPEN) {
        send(p.ws, { type: 'kicked', reason: '同一身份在其他标签页/设备打开' });
        p.ws.close(1000, 'replaced');
      }
      if (typeof msg.nick === 'string' && msg.nick.trim()) {
        const nick = uniqueNickExcept(target, msg.nick, p.id);
        if (nick !== p.nick) {
          p.nick = nick;
          addLog(target, 'presence', `主持人改名为「${nick}」`);
        }
      }
      p.online = true;
      p.ws = ws;
      target.hostId = p.id;
      target.lastSeen = Date.now();
      bind({ room: target, pid: p.id, isHost: true });
      send(ws, {
        type: 'joined',
        you: { id: p.id, nick: p.nick, host: true },
        snapshot: buildSnapshot(target)
      });
      return commit(target, { undoReason: 'presence_changed' });
    }

    // id 对不上（例如本地身份丢失）：以新成员身份进入并接管为主持人
    const nick = String(msg.nick ?? '').trim().slice(0, 20) || '主持人';
    const id = genId('p_');
    const participant = {
      id,
      nick: uniqueNick(target, nick),
      joinedAt: Date.now(),
      online: true,
      ws
    };
    target.participants.set(id, participant);
    target.hostId = id;
    addLog(target, 'presence', `主持人「${participant.nick}」已接入`);
    bind({ room: target, pid: id, isHost: true });
    send(ws, {
      type: 'joined',
      you: { id, nick: participant.nick, host: true },
      snapshot: buildSnapshot(target)
    });
    return commit(target, { undoReason: 'presence_changed' });
  }

  // 普通参与者断线重连：id + secret
  const reconnectId = typeof msg.participantId === 'string' ? msg.participantId : null;
  const secret = typeof msg.secret === 'string' ? msg.secret : null;
  const existing = reconnectId ? target.participants.get(reconnectId) : null;
  if (existing && secret && existing.secret === secret) {
    if (existing.ws && existing.ws !== ws && existing.ws.readyState === OPEN) {
      send(existing.ws, { type: 'kicked', reason: '同一身份在其他标签页/设备打开' });
      existing.ws.close(1000, 'replaced');
    }
    existing.online = true;
    existing.ws = ws;
    target.lastSeen = Date.now();
    bind({ room: target, pid: existing.id, isHost: false });
    send(ws, {
      type: 'joined',
      you: { id: existing.id, nick: existing.nick, host: false },
      snapshot: buildSnapshot(target)
    });
    return commit(target, { undoReason: 'presence_changed' });
  }

  // 全新加入
  const nick = String(msg.nick ?? '').trim().slice(0, 20);
  if (!nick) {
    return sendError(ws, '请填写昵称', 'NO_NICK', target);
  }
  const id = genId('p_');
  const participant = {
    id,
    nick: uniqueNick(target, nick),
    secret: crypto.randomBytes(12).toString('hex'),
    joinedAt: Date.now(),
    online: true,
    ws
  };
  target.participants.set(id, participant);
  target.lastSeen = Date.now();
  addLog(target, 'presence', `「${participant.nick}」加入房间`);
  bind({ room: target, pid: id, isHost: false });
  send(ws, {
    type: 'joined',
    you: { id, nick: participant.nick, host: false, secret: participant.secret },
    snapshot: buildSnapshot(target)
  });
  commit(target, { undoReason: 'presence_changed' });
}

function uniqueNickExcept(room, nick, exceptId) {
  const name = nick.trim().slice(0, 20) || '参与者';
  const used = new Set(
    [...room.participants.values()].filter((p) => p.id !== exceptId).map((p) => p.nick)
  );
  if (!used.has(name)) return name;
  for (let i = 2; i < 100; i++) {
    const candidate = `${name}(${i})`;
    if (!used.has(candidate)) return candidate;
  }
  return name + crypto.randomBytes(2).toString('hex');
}

function handleLeave(room, id, reason) {
  const p = room.participants.get(id);
  if (!p) return;

  if (reason === 'disconnect') {
    // 断线：保留席位与队列位置，标记离线，等待重连
    // 注意：这仍是一次版本化状态变更，按要求会立即使挂起的撤销失效，
    // 但撤销本身不会回滚在线状态。
    p.online = false;
    p.ws = null;
    return commit(room, { undoReason: 'presence_changed' });
  }

  // 主动离开：移出房间；若正在发言则结束发言（保留计时记录）
  const nick = p.nick;
  const queuedIdx = room.order.indexOf(id);
  const wasSpeaking = room.speaking && room.speaking.id === id;
  const wasRollcallTarget = room.rollcall && room.rollcall.currentId === id;

  if (wasSpeaking) {
    finalizeSpeaking(room); // 结算本轮配额并使限时/配额定时任务失效
  }
  // 主动离开：该方参与的待处理换位邀请随之失效（断线保留，不清理）
  invalidateSwaps(room, 'left', (s) => s.from === id || s.to === id, (side, s) =>
    s[side] === id ? nick : room.participants.get(s[side])?.nick ?? '已离开'
  );
  if (queuedIdx !== -1) room.order.splice(queuedIdx, 1);
  room.participants.delete(id);
  if (room.quota) room.quota.used.delete(id); // 主动离开：清除其本周期已用额度记录
  // 点名进行中：离开者若为当前对象，立即点名下一人；否则其待点名位置自然跳过
  let rollcallMoved = false;
  if (room.rollcall) {
    rollcallMoved = rollcallTargetGone(room, id);
  }
  addLog(room, 'presence', `「${nick}」离开房间`);
  if (wasRollcallTarget || rollcallMoved) {
    return commit(room, { undoReason: 'rollcall_changed' });
  }
  commit(room, { undoReason: 'presence_changed' });
}

// ---------------------------------------------------------------------------
// 队列命令：统一版本校验
// ---------------------------------------------------------------------------

function guardVersion(ws, msg, room) {
  const ver = Number(msg.version);
  if (!Number.isInteger(ver)) {
    sendError(ws, '命令缺少 version', 'NO_VERSION', room);
    return false;
  }
  if (ver !== room.version) {
    // 过期命令：拒绝执行，返回最新快照
    sendError(
      ws,
      `版本已过期（你的 v${ver}，当前 v${room.version}）`,
      'STALE_VERSION',
      room
    );
    return false;
  }
  return true;
}

/**
 * 换位邀请的接受/拒绝/撤回不强制版本相等：真正的冲突以双方位置重新校验为准，
 * 只要求携带 version 字段，避免队尾无关变动导致操作被版本守卫拦截。
 */
function guardVersionPresence(ws, msg, room) {
  if (!Number.isInteger(Number(msg.version))) {
    sendError(ws, '命令缺少 version', 'NO_VERSION', room);
    return false;
  }
  return true;
}

function requireHost(ws, room, isHost) {
  if (!isHost) {
    sendError(ws, '只有主持人可以执行此操作', 'FORBIDDEN', room);
    return false;
  }
  return true;
}

function handleCommand(ws, msg, room, pid, isHost) {
  if (!room) return sendError(ws, '尚未加入房间', 'NOT_JOINED');
  const me = room.participants.get(pid);
  if (!me) return sendError(ws, '参与者不存在，请重新加入', 'NOT_JOINED');

  switch (msg.type) {
    case 'request_speak': {
      if (!guardVersion(ws, msg, room)) return;
      if (room.speaking?.id === pid) {
        return sendError(ws, '你正在发言中', 'ALREADY_SPEAKING', room);
      }
      if (room.order.includes(pid)) {
        return sendError(ws, '你已在发言队列中', 'ALREADY_QUEUED', room);
      }
      // 配额开启时：余额耗尽的成员禁止再次排队
      if (quotaRemainingMs(room, pid) <= 0) {
        return sendError(ws, '你的发言配额已用尽，本周期内无法再次排队', 'QUOTA_EXHAUSTED', room);
      }
      room.order.push(pid);
      addLog(room, 'queue', `「${me.nick}」申请发言`);
      return commit(room, { undoReason: 'queue_changed' });
    }

    case 'cancel_speak': {
      if (!guardVersion(ws, msg, room)) return;
      const idx = room.order.indexOf(pid);
      if (idx === -1) {
        return sendError(ws, '你不在发言队列中', 'NOT_QUEUED', room);
      }
      room.order.splice(idx, 1);
      invalidateSwaps(room, 'canceled_speak', (s) => s.from === pid || s.to === pid);
      // 点名进行中：若取消者是当前对象，立即推进到下一人（其已计入本轮已点名）
      if (room.rollcall) {
        rollcallTargetGone(room, pid);
        addLog(room, 'queue', `「${me.nick}」取消发言`);
        return commit(room, { undoReason: 'rollcall_changed' });
      }
      addLog(room, 'queue', `「${me.nick}」取消发言`);
      return commit(room, { undoReason: 'queue_changed' });
    }

    case 'swap_invite': {
      if (!guardVersion(ws, msg, room)) return;
      if (room.rollcall) {
        return sendError(ws, '队首点名接力进行中，暂不能换位', 'ROLLCALL_ACTIVE', room);
      }
      const fromPos = room.order.indexOf(pid);
      if (fromPos === -1) {
        return sendError(ws, '你不在发言队列中，无法换位', 'NOT_QUEUED', room);
      }
      const targetId = String(msg.participantId || '');
      if (targetId === pid) {
        return sendError(ws, '不能与自己换位', 'BAD_TARGET', room);
      }
      const toPos = room.order.indexOf(targetId);
      if (toPos === -1) {
        return sendError(ws, '对方不在发言队列中', 'TARGET_NOT_QUEUED', room);
      }
      if (!room.participants.has(targetId)) {
        return sendError(ws, '参与者不存在', 'NO_PARTICIPANT', room);
      }
      // 每人同一时刻最多参与一条待处理邀请
      if (pendingSwapOf(room, pid)) {
        return sendError(ws, '你已有一条待处理的换位邀请', 'SWAP_BUSY', room);
      }
      if (pendingSwapOf(room, targetId)) {
        return sendError(ws, '对方已有一条待处理的换位邀请', 'SWAP_BUSY', room);
      }
      const swap = {
        id: genId('sw_'),
        from: pid,
        to: targetId,
        version: room.version,          // 发起时的队列版本
        fromPos,                        // 发起时双方的位置（下标）
        toPos,
        status: 'pending',
        reason: null,
        ts: Date.now(),
        resolvedAt: null
      };
      room.swaps.set(swap.id, swap);
      addLog(
        room,
        'swap',
        `「${me.nick}」向「${room.participants.get(targetId).nick}」发起换位邀请（第 ${fromPos + 1} 位 ↔ 第 ${toPos + 1} 位）`
      );
      return commit(room, { undoReason: 'swap_changed' });
    }

    case 'swap_accept': {
      if (!guardVersionPresence(ws, msg, room)) return;
      const swap = room.swaps.get(String(msg.swapId || ''));
      if (!swap) return sendError(ws, '换位邀请不存在', 'NO_SWAP', room);
      if (swap.status !== 'pending') {
        return sendError(ws, '该换位邀请已结束', 'SWAP_RESOLVED', room);
      }
      if (swap.to !== pid) {
        return sendError(ws, '只有被邀请方可以接受', 'SWAP_NOT_YOURS', room);
      }
      // 重新校验双方仍在发起邀请时的原位置，否则邀请失效且队列保持不变
      const stillInPlace =
        room.order[swap.fromPos] === swap.from && room.order[swap.toPos] === swap.to;
      const fromNick = room.participants.get(swap.from)?.nick ?? '已离开';
      const toNick = room.participants.get(swap.to)?.nick ?? '已离开';
      if (!stillInPlace) {
        swap.status = 'invalid';
        swap.reason = 'position_changed';
        swap.resolvedAt = Date.now();
        addLog(room, 'swap', `「${fromNick}」与「${toNick}」的换位邀请失效：双方位置已变化`);
        return commit(room, { undoReason: 'swap_changed' });
      }
      // 原子交换（同一次同步状态变更内完成，无命令可插入）
      room.order[swap.fromPos] = swap.to;
      room.order[swap.toPos] = swap.from;
      swap.status = 'accepted';
      swap.reason = 'accepted';
      swap.resolvedAt = Date.now();
      addLog(room, 'swap', `「${fromNick}」与「${toNick}」已交换发言位置`);
      return commit(room, { undoReason: 'swap_changed' });
    }

    case 'swap_reject': {
      if (!guardVersionPresence(ws, msg, room)) return;
      const swap = room.swaps.get(String(msg.swapId || ''));
      if (!swap) return sendError(ws, '换位邀请不存在', 'NO_SWAP', room);
      if (swap.status !== 'pending') {
        return sendError(ws, '该换位邀请已结束', 'SWAP_RESOLVED', room);
      }
      if (swap.to !== pid) {
        return sendError(ws, '只有被邀请方可以拒绝', 'SWAP_NOT_YOURS', room);
      }
      swap.status = 'rejected';
      swap.reason = 'rejected';
      swap.resolvedAt = Date.now();
      addLog(
        room,
        'swap',
        `「${room.participants.get(swap.from)?.nick ?? '已离开'}」与「${me.nick}」的换位邀请已被拒绝`
      );
      return commit(room, { undoReason: 'swap_changed' });
    }

    case 'swap_cancel': {
      if (!guardVersionPresence(ws, msg, room)) return;
      const swap = room.swaps.get(String(msg.swapId || ''));
      if (!swap) return sendError(ws, '换位邀请不存在', 'NO_SWAP', room);
      if (swap.status !== 'pending') {
        return sendError(ws, '该换位邀请已结束', 'SWAP_RESOLVED', room);
      }
      if (swap.from !== pid) {
        return sendError(ws, '只有发起方可以撤回', 'SWAP_NOT_YOURS', room);
      }
      swap.status = 'cancelled';
      swap.reason = 'cancelled';
      swap.resolvedAt = Date.now();
      addLog(
        room,
        'swap',
        `「${me.nick}」撤回了与「${room.participants.get(swap.to)?.nick ?? '已离开'}」的换位邀请`
      );
      return commit(room, { undoReason: 'swap_changed' });
    }

    case 'start_speaking': {
      if (!guardVersion(ws, msg, room) || !requireHost(ws, room, isHost)) return;
      const targetId = String(msg.participantId || room.order[0] || '');
      const target = room.participants.get(targetId);
      if (!target) return sendError(ws, '参与者不存在', 'NO_PARTICIPANT', room);
      if (room.speaking?.id === targetId && room.order.indexOf(targetId) === -1) {
        return sendError(ws, '该成员正在发言中', 'ALREADY_SPEAKING', room);
      }
      // 可选限时：1~30 分钟的整数；缺省表示不限时
      let durationMin = null;
      if (msg.durationMin !== undefined && msg.durationMin !== null) {
        const d = Number(msg.durationMin);
        if (!Number.isInteger(d) || d < SPEAK_MIN_MINUTES || d > SPEAK_MAX_MINUTES) {
          return sendError(
            ws,
            `发言时长须为 ${SPEAK_MIN_MINUTES}～${SPEAK_MAX_MINUTES} 分钟的整数`,
            'BAD_DURATION',
            room
          );
        }
        durationMin = d;
      }
      // 配额开启时：余额耗尽的成员不能再开始发言
      if (quotaRemainingMs(room, targetId) <= 0) {
        return sendError(ws, `「${target.nick}」本周期的发言配额已用尽`, 'QUOTA_EXHAUSTED', room);
      }

      // 逆操作：结束目标发言，必要时把其放回原队列位置，并恢复之前的发言人
      const prevSpeaking = room.speaking
        ? {
            id: room.speaking.id,
            startedAt: room.speaking.startedAt,
            endsAt: room.speaking.endsAt ?? null,
            roundId: room.speaking.roundId
          }
        : null;
      const idx = room.order.indexOf(targetId);
      const prevNick = prevSpeaking
        ? room.participants.get(prevSpeaking.id)?.nick ?? '已离开'
        : null;
      const undoSummary = prevNick
        ? `请「${target.nick}」开始发言（原发言人「${prevNick}」）`
        : `请「${target.nick}」开始发言`;
      // 点名进行中主持人直接请人发言：本轮点名结束（旧定时任务随之失效）
      if (room.rollcall) {
        finishRollcall(room);
        addLog(room, 'rollcall', '主持人直接请成员发言，本轮点名接力结束');
      }
      armUndo(room, 'start_speaking', undoSummary, {
        targetId,
        targetNick: target.nick,
        queueIndex: idx,               // -1 表示原来不在队列中
        prevSpeaking,
        prevSpeakingNick: prevNick
      });
      if (room.speaking) {
        const prev = room.participants.get(room.speaking.id);
        finalizeSpeaking(room); // 结算前一轮配额并使其定时任务失效
        addLog(room, 'talk', `「${prev ? prev.nick : '已离开'}」发言结束`);
      }
      if (idx !== -1) {
        room.order.splice(idx, 1);
        invalidateSwaps(room, 'started_speaking', (s) => s.from === targetId || s.to === targetId);
      }
      const now = Date.now();
      room.speaking = {
        id: targetId,
        startedAt: now,
        endsAt: durationMin ? now + durationMin * SPEAK_TIME_UNIT_MS : null,
        roundId: genId('rd_'),
        quotaBase: now // 本轮发言的配额计费起点
      };
      scheduleSpeakTimer(room);
      scheduleQuotaTimer(room); // 配额启用时：余额耗尽自动结束
      addLog(
        room,
        'talk',
        durationMin
          ? `主持人请「${target.nick}」开始发言（限时 ${durationMin} 分钟）`
          : `主持人请「${target.nick}」开始发言`
      );
      return commit(room, { undoArm: true });
    }

    case 'end_speaking': {
      if (!guardVersion(ws, msg, room) || !requireHost(ws, room, isHost)) return;
      if (!room.speaking) return sendError(ws, '当前没有人发言', 'NO_SPEAKER', room);
      const speaker = room.participants.get(room.speaking.id);
      const speakerNick = speaker ? speaker.nick : '已离开';
      // 逆操作：以原 startedAt/endsAt/roundId 恢复当前发言（计时连续，不产生新的计时起点）
      armUndo(room, 'end_speaking', `结束「${speakerNick}」的发言`, {
        id: room.speaking.id,
        nick: speakerNick,
        startedAt: room.speaking.startedAt,
        endsAt: room.speaking.endsAt ?? null,
        roundId: room.speaking.roundId
      });
      finalizeSpeaking(room); // 手动结束：结算本轮配额，限时/配额定时任务一并失效
      addLog(room, 'talk', `主持人结束「${speakerNick}」的发言`);
      return commit(room, { undoArm: true });
    }

    case 'reorder': {
      if (!guardVersion(ws, msg, room) || !requireHost(ws, room, isHost)) return;
      if (room.rollcall) {
        return sendError(ws, '队首点名接力进行中，暂不能调整顺序', 'ROLLCALL_ACTIVE', room);
      }
      if (!Array.isArray(msg.order)) {
        return sendError(ws, 'order 必须是数组', 'BAD_ORDER', room);
      }
      const incoming = msg.order.map(String);
      // 必须与当前队列是同一批人，仅允许改变顺序
      const current = room.order.slice().sort();
      const proposed = incoming.slice().sort();
      if (current.length !== proposed.length || current.some((id, i) => id !== proposed[i])) {
        return sendError(ws, '排序内容与当前队列不一致，请刷新', 'ORDER_MISMATCH', room);
      }
      if (incoming.some((id) => !room.participants.has(id))) {
        return sendError(ws, '队列中包含不存在的参与者', 'NO_PARTICIPANT', room);
      }
      const same = incoming.length === room.order.length && incoming.every((id, i) => id === room.order[i]);
      if (same) return sendError(ws, '顺序没有变化', 'NO_CHANGE', room);
      // 逆操作：恢复拖动前的完整队列顺序
      const prevOrder = room.order.slice();
      armUndo(room, 'reorder', '调整发言顺序', { prevOrder });
      room.order = incoming;
      // 主持人重排后，所有待处理换位邀请均作废，需重新发起
      invalidateSwaps(room, 'reordered');
      addLog(room, 'queue', '主持人调整了发言顺序');
      return commit(room, { undoArm: true });
    }

    case 'remove': {
      if (!guardVersion(ws, msg, room) || !requireHost(ws, room, isHost)) return;
      const targetId = String(msg.participantId || '');
      const target = room.participants.get(targetId);
      if (!target) return sendError(ws, '参与者不存在', 'NO_PARTICIPANT', room);
      // 逆操作：席位始终保留（remove 只移出队列/发言），按原位恢复
      const queueIndex = room.order.indexOf(targetId);
      const wasSpeaking = room.speaking?.id === targetId;
      const speakingStartedAt = wasSpeaking ? room.speaking.startedAt : null;
      const speakingEndsAt = wasSpeaking ? room.speaking.endsAt ?? null : null;
      const speakingRoundId = wasSpeaking ? room.speaking.roundId : null;
      armUndo(room, 'remove', `将「${target.nick}」移出队列`, {
        targetId,
        nick: target.nick,
        queueIndex,                     // -1 表示原来就不在队列
        wasSpeaking,
        speakingStartedAt,
        speakingEndsAt,
        speakingRoundId
      });
      if (queueIndex !== -1) room.order.splice(queueIndex, 1);
      if (wasSpeaking) {
        finalizeSpeaking(room); // 结算本轮配额并使限时/配额定时任务失效
      }
      invalidateSwaps(room, 'removed', (s) => s.from === targetId || s.to === targetId);
      // 点名进行中：被移出者若为当前对象，立即点名下一人
      const rollcallMoved = room.rollcall && rollcallTargetGone(room, targetId);
      addLog(room, 'queue', `主持人将「${target.nick}」移出队列`);
      if (rollcallMoved) return commit(room, { undoReason: 'rollcall_changed' });
      return commit(room, { undoArm: true });
    }

    case 'undo_host':
      if (!guardVersion(ws, msg, room) || !requireHost(ws, room, isHost)) return;
      return handleUndoHost(ws, room);

    case 'clear': {
      if (!guardVersion(ws, msg, room) || !requireHost(ws, room, isHost)) return;
      room.order = [];
      finalizeSpeaking(room); // 结算本轮配额并使限时/配额定时任务失效
      finishRollcall(room);
      room.logs = [];
      invalidateSwaps(room, 'cleared');
      addLog(room, 'system', '主持人执行了一键清空（队列、当前发言、操作记录已重置）');
      return commit(room, { undoReason: 'cleared' });
    }

    case 'clear_logs': {
      if (!guardVersion(ws, msg, room) || !requireHost(ws, room, isHost)) return;
      room.logs = [];
      addLog(room, 'system', '主持人清空了操作记录');
      return commit(room, { undoReason: 'logs_cleared' });
    }

    case 'rollcall_start': {
      if (!guardVersion(ws, msg, room) || !requireHost(ws, room, isHost)) return;
      if (room.rollcall) {
        return sendError(ws, '队首点名接力已在进行中', 'ROLLCALL_ACTIVE', room);
      }
      if (room.speaking) {
        return sendError(ws, '当前有人发言，无法启动点名', 'ROLLCALL_SPEAKING', room);
      }
      if (room.order.length === 0) {
        return sendError(ws, '发言队列为空，无人可点名', 'ROLLCALL_EMPTY', room);
      }
      startRollcall(room);
      return commit(room, { undoReason: 'rollcall_changed' });
    }

    case 'rollcall_confirm':
    case 'rollcall_skip': {
      if (!guardVersion(ws, msg, room)) return;
      const rc = room.rollcall;
      if (!rc) {
        return sendError(ws, '当前没有进行中的点名', 'NO_ROLLCALL', room);
      }
      // callId 与 version 同时校验：错误/过期轮次的命令一律拒绝
      if (typeof msg.callId !== 'string' || msg.callId !== rc.callId) {
        return sendError(ws, '点名已更新（callId 不匹配），请刷新状态', 'BAD_CALL', room);
      }
      if (rc.currentId !== pid) {
        return sendError(ws, '当前点名的不是你', 'ROLLCALL_NOT_YOURS', room);
      }
      if (msg.type === 'rollcall_confirm') {
        if (!room.participants.has(pid) || !room.order.includes(pid)) {
          return sendError(ws, '你已不在发言队列中，无法确认', 'NOT_QUEUED', room);
        }
        if (quotaRemainingMs(room, pid) <= 0) {
          return sendError(ws, '你的发言配额已用尽，无法确认发言', 'QUOTA_EXHAUSTED', room);
        }
        return rollcallConfirm(room, pid);
      }
      return rollcallSkip(room, pid, 'manual');
    }

    case 'rollcall_cancel': {
      if (!guardVersion(ws, msg, room) || !requireHost(ws, room, isHost)) return;
      if (!room.rollcall) {
        return sendError(ws, '当前没有进行中的点名', 'NO_ROLLCALL', room);
      }
      finishRollcall(room);
      addLog(room, 'rollcall', '主持人取消了队首点名接力');
      return commit(room, { undoReason: 'rollcall_changed' });
    }

    case 'topic_create': {
      if (!guardVersion(ws, msg, room) || !requireHost(ws, room, isHost)) return;
      const title = String(msg.title ?? '').trim().slice(0, TOPIC_TITLE_MAX);
      if (!title) {
        return sendError(ws, '请填写议题标题', 'NO_TOPIC_TITLE', room);
      }
      const durationSec = Number(msg.durationSec);
      if (
        !Number.isInteger(durationSec) ||
        durationSec < TOPIC_MIN_SECONDS ||
        durationSec > TOPIC_MAX_SECONDS
      ) {
        return sendError(
          ws,
          `投票时长须为 ${TOPIC_MIN_SECONDS}～${TOPIC_MAX_SECONDS} 秒的整数`,
          'BAD_DURATION',
          room
        );
      }
      // 已有进行中的议题时先结算旧议题（旧定时任务随之失效），再创建新议题
      createTopic(room, title, durationSec);
      return commit(room, { undoReason: 'topic_changed' });
    }

    case 'topic_end': {
      if (!guardVersion(ws, msg, room) || !requireHost(ws, room, isHost)) return;
      if (!room.topic) {
        return sendError(ws, '当前没有进行中的议题', 'NO_TOPIC', room);
      }
      settleTopic(room, 'host');
      return commit(room, { undoReason: 'topic_changed' });
    }

    case 'topic_vote': {
      if (!guardVersion(ws, msg, room)) return;
      const topic = room.topic;
      if (!topic) {
        return sendError(ws, '当前没有进行中的议题', 'NO_TOPIC', room);
      }
      // topicId 与 version 同时校验：对旧议题投票一律拒绝
      if (typeof msg.topicId !== 'string' || msg.topicId !== topic.id) {
        return sendError(ws, '议题已更新（topicId 不匹配），请刷新状态', 'BAD_TOPIC', room);
      }
      if (serverTimeAfter(topic.deadline)) {
        // 防御性校验：极端定时偏差下，截止后的投票不写入（正常由到期回调先结算）
        return sendError(ws, '投票已截止', 'TOPIC_CLOSED', room);
      }
      const choice = String(msg.choice || '');
      if (!VOTE_CHOICES.includes(choice)) {
        return sendError(ws, '投票选项无效（赞成 / 反对 / 弃权）', 'BAD_CHOICE', room);
      }
      topic.votes.set(pid, choice); // 每人一票：重复投票以最新选择为准
      return commit(room, { undoReason: 'topic_changed' });
    }

    case 'quota_reset': {
      if (!guardVersion(ws, msg, room) || !requireHost(ws, room, isHost)) return;
      const d = Number(msg.durationMin);
      if (!Number.isInteger(d) || d < QUOTA_MIN_MINUTES || d > QUOTA_MAX_MINUTES) {
        return sendError(
          ws,
          `配额时长须为 ${QUOTA_MIN_MINUTES}～${QUOTA_MAX_MINUTES} 分钟的整数`,
          'BAD_DURATION',
          room
        );
      }
      const now = Date.now();
      // 开启/重置都是同一个动作：新周期标识 + 全员已用额度清零
      room.quota = {
        periodId: genId('qp_'),
        limitMs: d * QUOTA_TIME_UNIT_MS,
        startedAt: now,
        used: new Map()
      };
      // 当前发言从重置时刻起计入新周期；旧周期的到期回调因 periodId 不符一律作废
      if (room.speaking) {
        room.speaking.quotaBase = now;
      }
      scheduleQuotaTimer(room);
      addLog(
        room,
        'quota',
        `主持人开启新的发言配额周期：每人 ${d} 分钟（周期 ${room.quota.periodId}）`
      );
      return commit(room, { undoReason: 'quota_changed' });
    }

    case 'task_create': {
      if (!guardVersion(ws, msg, room) || !requireHost(ws, room, isHost)) return;
      const title = String(msg.title ?? '').trim().slice(0, TASK_TITLE_MAX);
      if (!title) {
        return sendError(ws, '请填写行动项内容', 'NO_TASK_TITLE', room);
      }
      // 负责人必须是当前房间成员
      const assigneeId = String(msg.participantId || '');
      const assignee = room.participants.get(assigneeId);
      if (!assignee) {
        return sendError(ws, '负责人不在当前房间成员中', 'NO_PARTICIPANT', room);
      }
      const deadline = Number(msg.deadline);
      const now = Date.now();
      if (!validTaskDeadline(deadline, now)) {
        return sendError(ws, '截止时间无效（须为将来一年内的时间）', 'BAD_DEADLINE', room);
      }
      let open = 0;
      for (const t of room.tasks.values()) {
        if (t.status === 'pending' || t.status === 'active' || t.status === 'review') open += 1;
      }
      if (open >= TASK_MAX_OPEN) {
        return sendError(ws, `未完结行动项已达上限（${TASK_MAX_OPEN} 条）`, 'TASK_LIMIT', room);
      }
      const task = {
        id: genId('tk_'),
        title,
        assigneeId,
        assigneeNick: assignee.nick, // 成员离开房间后仍按创建时昵称展示
        createdBy: pid,
        createdAt: now,
        deadline: Math.round(deadline),
        status: 'pending',
        updatedAt: now,
        history: [{ ts: now, from: null, to: 'pending', by: pid, byNick: me.nick }],
        deadlineRequest: null // 最近一次延期申请（同一事项仅保留一条）
      };
      room.tasks.set(task.id, task);
      addLog(room, 'task', `主持人创建行动项「${title}」，负责人「${assignee.nick}」`);
      return commit(room, { undoReason: 'task_changed' });
    }

    case 'task_accept':
    case 'task_reject':
    case 'task_complete': {
      if (!guardVersion(ws, msg, room)) return;
      const task = room.tasks.get(String(msg.taskId || ''));
      if (!task) return sendError(ws, '行动项不存在', 'NO_TASK', room);
      // 身份校验：仅该行动项的负责人可执行
      if (task.assigneeId !== pid) {
        return sendError(ws, '只有该行动项的负责人可以执行此操作', 'TASK_NOT_YOURS', room);
      }
      // 状态校验：接受/拒绝要求「待接受」，标记完成要求「处理中」
      const need = msg.type === 'task_complete' ? 'active' : 'pending';
      if (task.status !== need) {
        return sendError(
          ws,
          `行动项当前状态为「${TASK_STATUS_TEXT[task.status]}」，无法执行此操作`,
          'BAD_TASK_STATE',
          room
        );
      }
      const to =
        msg.type === 'task_accept' ? 'active' : msg.type === 'task_reject' ? 'rejected' : 'review';
      transitionTask(room, task, to, me);
      let text;
      if (msg.type === 'task_accept') {
        text = `「${me.nick}」接受了行动项「${task.title}」，开始处理`;
      } else if (msg.type === 'task_reject') {
        text = `「${me.nick}」拒绝了行动项「${task.title}」`;
      } else {
        text = `「${me.nick}」将行动项「${task.title}」标记为完成，待主持人确认`;
      }
      addLog(room, 'task', text);
      return commit(room, { undoReason: 'task_changed' });
    }

    case 'task_confirm':
    case 'task_return': {
      if (!guardVersion(ws, msg, room) || !requireHost(ws, room, isHost)) return;
      const task = room.tasks.get(String(msg.taskId || ''));
      if (!task) return sendError(ws, '行动项不存在', 'NO_TASK', room);
      // 状态校验：确认完成 / 退回处理中都仅对「待确认」的行动项有效
      if (task.status !== 'review') {
        return sendError(
          ws,
          `行动项当前状态为「${TASK_STATUS_TEXT[task.status]}」，无法执行此操作`,
          'BAD_TASK_STATE',
          room
        );
      }
      const to = msg.type === 'task_confirm' ? 'done' : 'active';
      transitionTask(room, task, to, me);
      addLog(
        room,
        'task',
        msg.type === 'task_confirm'
          ? `主持人确认行动项「${task.title}」已完成`
          : `主持人将行动项「${task.title}」退回处理中`
      );
      return commit(room, { undoReason: 'task_changed' });
    }

    case 'task_deadline_request': {
      if (!guardVersion(ws, msg, room)) return;
      const task = room.tasks.get(String(msg.taskId || ''));
      if (!task) return sendError(ws, '行动项不存在', 'NO_TASK', room);
      // 身份校验：仅负责人本人可申请延期
      if (task.assigneeId !== pid) {
        return sendError(ws, '只有该行动项的负责人可以申请延期', 'TASK_NOT_YOURS', room);
      }
      // 仅待接受 / 处理中可申请（待确认、已完成、已拒绝均拒绝）
      if (task.status !== 'pending' && task.status !== 'active') {
        return sendError(
          ws,
          `行动项当前状态为「${TASK_STATUS_TEXT[task.status]}」，无法申请延期`,
          'BAD_TASK_STATE',
          room
        );
      }
      // 同一事项仅保留一条待审申请
      if (task.deadlineRequest && task.deadlineRequest.status === 'pending') {
        return sendError(ws, '该行动项已有一条待审批的延期申请', 'TASK_REQUEST_PENDING', room);
      }
      const reason = String(msg.reason ?? '').trim().slice(0, TASK_REASON_MAX);
      if (!reason) {
        return sendError(ws, '请填写延期理由', 'NO_REQUEST_REASON', room);
      }
      const newDeadline = Number(msg.deadline);
      const now = Date.now();
      if (!validTaskDeadline(newDeadline, now)) {
        return sendError(ws, '新的截止时间无效（须为将来一年内的时间）', 'BAD_DEADLINE', room);
      }
      if (newDeadline <= task.deadline) {
        return sendError(ws, '新的截止时间必须晚于当前截止时间', 'BAD_DEADLINE', room);
      }
      // 覆盖上一条已审结申请：同一事项始终只保留最近一条
      task.deadlineRequest = {
        id: genId('dr_'),
        status: 'pending',
        newDeadline: Math.round(newDeadline),
        reason,
        requestedBy: pid,
        requestedByNick: me.nick,
        requestedAt: now,
        decidedBy: null,
        decidedByNick: null,
        decidedAt: null
      };
      task.updatedAt = now;
      addLog(
        room,
        'task',
        `「${me.nick}」就行动项「${task.title}」申请延期至 ${formatDeadlineShort(newDeadline)}，理由：${reason}`
      );
      return commit(room, { undoReason: 'task_changed' });
    }

    case 'task_deadline_approve':
    case 'task_deadline_reject': {
      if (!guardVersion(ws, msg, room) || !requireHost(ws, room, isHost)) return;
      const task = room.tasks.get(String(msg.taskId || ''));
      if (!task) return sendError(ws, '行动项不存在', 'NO_TASK', room);
      const req = task.deadlineRequest;
      if (!req || req.status !== 'pending') {
        return sendError(ws, '该行动项没有待审批的延期申请', 'TASK_REQUEST_NONE', room);
      }
      const now = Date.now();
      req.status = msg.type === 'task_deadline_approve' ? 'approved' : 'rejected';
      req.decidedBy = pid;
      req.decidedByNick = me.nick;
      req.decidedAt = now;
      task.updatedAt = now;
      if (msg.type === 'task_deadline_approve') {
        // 批准：截止时间与审批标记在同一次同步变更中原子生效
        task.deadline = req.newDeadline;
        addLog(
          room,
          'task',
          `主持人「${me.nick}」于 ${formatDeadlineShort(now)} 批准「${req.requestedByNick}」对行动项「${task.title}」的延期申请，` +
            `截止时间调整为 ${formatDeadlineShort(req.newDeadline)}`
        );
      } else {
        // 拒绝：保留理由与申请内容，截止时间不变
        addLog(
          room,
          'task',
          `主持人「${me.nick}」于 ${formatDeadlineShort(now)} 拒绝了「${req.requestedByNick}」对行动项「${task.title}」的延期申请（理由：${req.reason}），截止时间不变`
        );
      }
      return commit(room, { undoReason: 'task_changed' });
    }

    case 'group_generate': {
      if (!guardVersion(ws, msg, room) || !requireHost(ws, room, isHost)) return;
      // 成员名单：2～12 人、无重复，且必须全部是当前房间成员（含已离开成员的请求拒绝）
      if (!Array.isArray(msg.memberIds)) {
        return sendError(ws, 'memberIds 必须是数组', 'GROUP_BAD_MEMBERS', room);
      }
      const memberIds = msg.memberIds.map(String);
      if (new Set(memberIds).size !== memberIds.length) {
        return sendError(ws, '成员名单包含重复成员', 'GROUP_BAD_MEMBERS', room);
      }
      if (memberIds.length < GROUP_MIN_MEMBERS || memberIds.length > GROUP_MAX_MEMBERS) {
        return sendError(
          ws,
          `分组人数须为 ${GROUP_MIN_MEMBERS}～${GROUP_MAX_MEMBERS} 人`,
          'GROUP_BAD_MEMBERS',
          room
        );
      }
      if (memberIds.some((id) => !room.participants.has(id))) {
        return sendError(
          ws,
          '所选成员中包含已离开房间的成员，请刷新名单后重新生成',
          'GROUP_MEMBER_LEFT',
          room
        );
      }
      // 组数：2～4 且不超过人数
      const groupCount = Number(msg.groupCount);
      if (
        !Number.isInteger(groupCount) ||
        groupCount < GROUP_MIN_COUNT ||
        groupCount > GROUP_MAX_COUNT ||
        groupCount > memberIds.length
      ) {
        return sendError(
          ws,
          `小组数须为 ${GROUP_MIN_COUNT}～${GROUP_MAX_COUNT} 且不超过所选人数`,
          'GROUP_BAD_COUNT',
          room
        );
      }
      // 约束：所选成员对的 together/apart 列表；类型非法、成员不在名单、
      // 两端相同或重复（含反向重复）一律拒绝
      if (msg.constraints !== undefined && msg.constraints !== null && !Array.isArray(msg.constraints)) {
        return sendError(ws, 'constraints 必须是数组', 'GROUP_BAD_CONSTRAINT', room);
      }
      const rawConstraints = Array.isArray(msg.constraints) ? msg.constraints : [];
      if (rawConstraints.length > GROUP_MAX_CONSTRAINTS) {
        return sendError(ws, `约束数量过多（最多 ${GROUP_MAX_CONSTRAINTS} 条）`, 'GROUP_BAD_CONSTRAINT', room);
      }
      const memberSet = new Set(memberIds);
      const seenPairs = new Set();
      const constraints = [];
      for (const raw of rawConstraints) {
        const a = String(raw?.a ?? '');
        const b = String(raw?.b ?? '');
        const type = String(raw?.type ?? '');
        if (!GROUP_CONSTRAINT_TYPES.includes(type)) {
          return sendError(ws, '约束类型无效（须为必须同组或必须分开）', 'GROUP_BAD_CONSTRAINT', room);
        }
        if (!memberSet.has(a) || !memberSet.has(b)) {
          return sendError(ws, '约束中包含不在所选名单中的成员', 'GROUP_BAD_CONSTRAINT', room);
        }
        if (a === b) {
          return sendError(ws, '约束的两端不能是同一成员', 'GROUP_BAD_CONSTRAINT', room);
        }
        const key = `${type}:${a < b ? a : b}:${a < b ? b : a}`;
        if (seenPairs.has(key)) {
          return sendError(ws, '存在重复的约束，请移除后重试', 'GROUP_BAD_CONSTRAINT', room);
        }
        seenPairs.add(key);
        constraints.push({ a, b, type });
      }
      // 求解：存在可行方案就必然找到；无解时拒绝且不改变版本、保留上次方案
      const result = solveGroups(memberIds, groupCount, constraints);
      if (!result.ok) {
        if (result.reason === 'direct_conflict') {
          return sendError(
            ws,
            '约束相互矛盾：存在「必须同组」与「必须分开」指向同一对成员（含同组传递后冲突），请调整约束',
            'GROUP_NO_SOLUTION',
            room
          );
        }
        return sendError(
          ws,
          '当前约束下不存在满足条件的分组方案，请调整约束、人数或小组数（上次方案已保留）',
          'GROUP_NO_SOLUTION',
          room
        );
      }
      // 生成时留存昵称：之后成员改名/进出都不影响本方案的展示
      const regenerate = !!room.groupPlan;
      room.groupPlan = {
        id: genId('gp_'),
        createdAt: Date.now(),
        groupCount,
        members: memberIds.map((id) => ({ id, nick: room.participants.get(id).nick })),
        constraints,
        groups: result.groups
      };
      const sizes = result.groups.map((g) => g.length).join('/');
      addLog(
        room,
        'group',
        `主持人${regenerate ? '重新' : ''}生成分组讨论方案：${memberIds.length} 人分为 ${groupCount} 组（${sizes} 人），约束 ${constraints.length} 条`
      );
      return commit(room, { undoReason: 'group_changed' });
    }

    case 'question_submit': {
      if (!guardVersion(ws, msg, room)) return;
      const text = String(msg.text ?? '').trim().slice(0, QUESTION_TEXT_MAX);
      if (!text) {
        return sendError(ws, '请填写问题内容', 'NO_QUESTION_TEXT', room);
      }
      let open = 0;
      for (const q of room.questions.values()) {
        if (!q.mergedInto && q.closedAt === null) open += 1;
      }
      if (open >= QUESTION_MAX_OPEN) {
        return sendError(ws, `未合并问题已达上限（${QUESTION_MAX_OPEN} 条）`, 'QUESTION_LIMIT', room);
      }
      const now = Date.now();
      const question = {
        id: genId('q_'),
        text,
        authorId: pid,
        authorNick: me.nick, // 成员离开房间后仍按提交时昵称展示
        createdAt: now,
        supporters: new Set(),
        mergedInto: null,
        mergedAt: null,
        mergedBy: null,
        closedAt: null,
        answer: null,
        answeredBy: null,
        answeredByNick: null,
        answerSources: [],
        answerSupporters: []
      };
      room.questions.set(question.id, question);
      addLog(room, 'question', `「${me.nick}」提交了现场问题：${text.length > 40 ? text.slice(0, 40) + '…' : text}`);
      return commit(room, { undoReason: 'question_changed' });
    }

    case 'question_support': {
      if (!guardVersion(ws, msg, room)) return;
      const question = room.questions.get(String(msg.questionId || ''));
      if (!question) {
        return sendError(ws, '问题不存在', 'NO_QUESTION', room);
      }
      // 针对已合并问题的支持操作一律拒绝，计数不变
      if (question.mergedInto) {
        return sendError(ws, '该问题已合并，不能再支持', 'QUESTION_MERGED', room);
      }
      // 结案后的问题不可再获支持（支持人数已固定），拒绝且不改变记录
      if (question.closedAt !== null) {
        return sendError(ws, '该问题已结案，不能再支持', 'QUESTION_CLOSED', room);
      }
      // 切换支持状态：同一成员只计一次，重复命令即取消支持
      if (question.supporters.has(pid)) {
        question.supporters.delete(pid);
      } else {
        question.supporters.add(pid);
      }
      return commit(room, { undoReason: 'question_changed' });
    }

    case 'question_merge': {
      if (!guardVersion(ws, msg, room) || !requireHost(ws, room, isHost)) return;
      const sourceId = String(msg.sourceId || '');
      const targetId = String(msg.targetId || '');
      if (sourceId === targetId) {
        return sendError(ws, '不能将问题合并到自身', 'QUESTION_BAD_TARGET', room);
      }
      const source = room.questions.get(sourceId);
      if (!source) {
        return sendError(ws, '来源问题不存在', 'NO_QUESTION', room);
      }
      const target = room.questions.get(targetId);
      if (!target) {
        return sendError(ws, '目标问题不存在', 'NO_QUESTION', room);
      }
      // 来源必须是未合并问题（已标明去向的问题不能再次合并）
      if (source.mergedInto) {
        return sendError(ws, '来源问题已合并，不能重复合并', 'QUESTION_MERGED', room);
      }
      // 目标必须仍是未合并问题（并入已合并问题等于针对已合并问题操作，拒绝）
      if (target.mergedInto) {
        return sendError(ws, '目标问题已合并，请选择未合并问题作为目标', 'QUESTION_MERGED', room);
      }
      // 已结案问题不参与合并：来源/目标结案都明确拒绝且不改变记录
      if (source.closedAt !== null) {
        return sendError(ws, '来源问题已结案，不能再参与合并', 'QUESTION_CLOSED', room);
      }
      if (target.closedAt !== null) {
        return sendError(ws, '目标问题已结案，不能再并入', 'QUESTION_CLOSED', room);
      }
      // 合并两边支持者：同一成员只计一次（Set 天然去重，原子完成）
      let added = 0;
      for (const sup of source.supporters) {
        if (!target.supporters.has(sup)) {
          target.supporters.add(sup);
          added += 1;
        }
      }
      const now = Date.now();
      source.mergedInto = targetId;
      source.mergedAt = now;
      source.mergedBy = pid;
      addLog(
        room,
        'question',
        `主持人将一条问题并入「${target.text.length > 30 ? target.text.slice(0, 30) + '…' : target.text}」` +
          `（合并 ${source.supporters.size} 名支持者，新增 ${added} 人，目标共 ${target.supporters.size} 人支持）`
      );
      return commit(room, { undoReason: 'question_changed' });
    }

    case 'question_close': {
      if (!guardVersion(ws, msg, room) || !requireHost(ws, room, isHost)) return;
      const question = room.questions.get(String(msg.questionId || ''));
      if (!question) {
        return sendError(ws, '问题不存在', 'NO_QUESTION', room);
      }
      // 已合并来源不能单独结案（其最终答复随合并链终点一并给出）
      if (question.mergedInto) {
        return sendError(ws, '该问题已合并，不能单独结案', 'QUESTION_MERGED', room);
      }
      // 重复结案明确拒绝，且不改变版本与记录
      if (question.closedAt !== null) {
        return sendError(ws, '该问题已结案，不能重复结案', 'QUESTION_CLOSED', room);
      }
      // 答复去空白后必须为 1～300 字；空白拒绝、超长拒绝（不截断）
      const answer = String(msg.answer ?? '').trim();
      if (!answer) {
        return sendError(ws, '请填写答复内容', 'NO_QUESTION_ANSWER', room);
      }
      if (answer.length > QUESTION_ANSWER_MAX) {
        return sendError(
          ws,
          `答复不能超过 ${QUESTION_ANSWER_MAX} 字（当前 ${answer.length} 字）`,
          'QUESTION_ANSWER_TOO_LONG',
          room
        );
      }
      const now = Date.now();
      // 结案：固定支持人数与支持者、答复人与时间，并快照沿合并链归入它的全部来源
      question.closedAt = now;
      question.answer = answer;
      question.answeredBy = pid;
      question.answeredByNick = me.nick;
      question.answerSupporters = [...question.supporters].map((sp) => freezeQuestionSupporter(room, sp));
      question.answerSources = freezeQuestionSources(room, question.id);
      addLog(
        room,
        'question',
        `主持人答复并结案了问题：「${question.text.length > 30 ? question.text.slice(0, 30) + '…' : question.text}」` +
          `（固定 ${question.answerSupporters.length} 名支持者，含 ${question.answerSources.length} 条沿链来源）`
      );
      return commit(room, { undoReason: 'question_changed' });
    }
  }
}

/** 服务端当前时间是否已超过给定时间点 */
function serverTimeAfter(ts) {
  return Date.now() >= ts;
}

/**
 * 安全撤销：原子恢复最近一次主持人操作影响的队列位置与当前发言。
 * 不触碰成员在线状态、身份凭证以及无关的操作记录；
 * 仅追加一条 kind='audit' 的可见审计记录。
 */
function handleUndoHost(ws, room) {
  const undo = room.undo;
  if (!undo) {
    return sendError(ws, '暂无可撤销的主持人操作', 'UNDO_NONE', room);
  }
  if (undo.status === 'invalid') {
    return sendError(
      ws,
      `撤销已失效：${undo.reasonText || UNDO_REASON_TEXT.state_changed}`,
      'UNDO_INVALIDATED',
      room
    );
  }
  // 版本守卫保证 version 相等；此处再做一次防御性校验
  if (undo.version !== room.version) {
    undo.status = 'invalid';
    undo.reason = 'state_changed';
    undo.reasonText = UNDO_REASON_TEXT.state_changed;
    delete undo.payload;
    return sendError(
      ws,
      `撤销已失效：${UNDO_REASON_TEXT.state_changed}`,
      'UNDO_INVALIDATED',
      room
    );
  }

  const { op, payload } = undo;

  if (op === 'reorder') {
    // 仅恢复队列位置；成员集合若被介入变更改动，撤销理应早已失效，
    // 这里仍防御性地只恢复当前仍在房间内的成员的相对顺序。
    room.order = payload.prevOrder.filter((id) => room.participants.has(id));
    addLog(room, 'audit', '安全撤销：已恢复拖动前的发言顺序');
  } else if (op === 'remove') {
    if (!room.participants.has(payload.targetId)) {
      // 成员已离开房间——属于介入的成员变更，撤销不可能仍有效
      undo.status = 'invalid';
      undo.reason = 'presence_changed';
      undo.reasonText = UNDO_REASON_TEXT.presence_changed;
      delete undo.payload;
      return sendError(ws, '撤销已失效：成员已离开房间', 'UNDO_INVALIDATED', room);
    }
    if (payload.queueIndex !== -1 && room.order.indexOf(payload.targetId) === -1) {
      const at = Math.min(payload.queueIndex, room.order.length);
      room.order.splice(at, 0, payload.targetId);
    }
    if (payload.wasSpeaking && !room.speaking) {
      room.speaking = {
        id: payload.targetId,
        startedAt: payload.speakingStartedAt,
        endsAt: payload.speakingEndsAt ?? null,
        roundId: payload.speakingRoundId || genId('rd_'),
        quotaBase: Date.now() // 已结算部分保留在配额中，恢复后从当前时刻重新计费
      };
      scheduleSpeakTimer(room);
      scheduleQuotaTimer(room);
    }
    if (payload.queueIndex !== -1 || payload.wasSpeaking) {
      addLog(room, 'audit', `安全撤销：已恢复「${payload.nick}」的队列位置`);
    } else {
      addLog(room, 'audit', `安全撤销：已撤回将「${payload.nick}」移出队列的操作`);
    }
  } else if (op === 'start_speaking') {
    if (!room.participants.has(payload.targetId)) {
      undo.status = 'invalid';
      undo.reason = 'presence_changed';
      undo.reasonText = UNDO_REASON_TEXT.presence_changed;
      delete undo.payload;
      return sendError(ws, '撤销已失效：成员已离开房间', 'UNDO_INVALIDATED', room);
    }
    // 撤销开始：当前轮次定时任务失效并结算其配额；若恢复原发言人则按原 endsAt 重新排程
    finalizeSpeaking(room);
    room.speaking = payload.prevSpeaking && room.participants.has(payload.prevSpeaking.id)
      ? {
          id: payload.prevSpeaking.id,
          startedAt: payload.prevSpeaking.startedAt,
          endsAt: payload.prevSpeaking.endsAt ?? null,
          roundId: payload.prevSpeaking.roundId || genId('rd_'),
          quotaBase: Date.now() // 已结算部分保留在配额中，恢复后从当前时刻重新计费
        }
      : null;
    scheduleSpeakTimer(room);
    scheduleQuotaTimer(room);
    if (payload.queueIndex !== -1 && room.order.indexOf(payload.targetId) === -1) {
      const at = Math.min(payload.queueIndex, room.order.length);
      room.order.splice(at, 0, payload.targetId);
    }
    addLog(room, 'audit', `安全撤销：已撤回请「${payload.targetNick}」开始发言的操作`);
  } else if (op === 'end_speaking') {
    if (!room.participants.has(payload.id)) {
      undo.status = 'invalid';
      undo.reason = 'presence_changed';
      undo.reasonText = UNDO_REASON_TEXT.presence_changed;
      delete undo.payload;
      return sendError(ws, '撤销已失效：成员已离开房间', 'UNDO_INVALIDATED', room);
    }
    if (room.speaking) {
      undo.status = 'invalid';
      undo.reason = 'speaking_changed';
      undo.reasonText = UNDO_REASON_TEXT.speaking_changed;
      delete undo.payload;
      return sendError(ws, '撤销已失效：当前发言状态出现新的变更', 'UNDO_INVALIDATED', room);
    }
    room.speaking = {
      id: payload.id,
      startedAt: payload.startedAt,
      endsAt: payload.endsAt ?? null,
      roundId: payload.roundId || genId('rd_'),
      quotaBase: Date.now() // 已结算部分保留在配额中，恢复后从当前时刻重新计费
    };
    scheduleSpeakTimer(room); // 限时轮次恢复后倒计时继续（已到期则立即自动结束）
    scheduleQuotaTimer(room); // 配额轮次恢复后余额继续扣减（耗尽则立即自动结束）
    addLog(room, 'audit', `安全撤销：已恢复「${payload.nick}」的发言，计时连续`);
  } else {
    return sendError(ws, '未知的撤销操作', 'UNDO_NONE', room);
  }

  // 撤销本身是一次版本化提交：清除逆操作（不提供 redo），原子广播恢复后的快照
  commit(room, { undoConsume: true });
}

// ---------------------------------------------------------------------------
// 心跳：清理死连接；空闲房间回收
// ---------------------------------------------------------------------------

const heartbeat = setInterval(() => {
  for (const client of wss.clients) {
    if (client.isAlive === false) {
      client.terminate();
      continue;
    }
    client.isAlive = false;
    try {
      client.ping();
    } catch {
      // ignore
    }
  }

  const now = Date.now();
  for (const [code, room] of rooms) {
    const anyOnline = [...room.participants.values()].some(
      (p) => p.ws && p.ws.readyState === OPEN
    );
    if (!anyOnline && now - room.lastSeen > ROOM_TTL_MS) {
      clearSpeakTimer(room);
      clearQuotaTimer(room);
      clearRollcallTimer(room);
      clearTopicTimer(room);
      rooms.delete(code);
    }
  }
}, 30000);

wss.on('close', () => clearInterval(heartbeat));

server.listen(PORT, HOST, () => {
  console.log(`发言席位服务已启动: http://localhost:${PORT}  (WebSocket: ${WS_PATH})`);
});
