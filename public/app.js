/* global */
'use strict';

// ---------------------------------------------------------------------------
// 全局状态
// ---------------------------------------------------------------------------

const state = {
  snapshot: null, // 最近一次服务端快照
  version: 0,
  me: null,       // { id, nick, host, secret? }
  creds: null,    // localStorage 中持久化的身份
  ws: null,
  connected: false,
  intentionalClose: false,
  reconnectDelay: 500,
  reconnectTimer: null,
  clockOffset: 0, // 服务端时钟 - 本地时钟
  timerHandle: null,
  // 换位邀请：swapId -> 最近已知状态（用于检测状态变化并弹出反馈）
  swapSeeded: false,
  swapStatus: new Map(),
  // 行动项：taskId -> 最近已知状态（用于检测状态变化并弹出反馈）
  taskSeeded: false,
  taskStatus: new Map(),
  // 当前打开延期申请表单的行动项 id（同一时刻只展开一个）
  taskReqFormId: null,
  // 安全撤销：最近一次已展示的撤销状态（用于检测可用/失效/已撤销的跳变）
  undoSeen: null,
  // 分组讨论：生成分组表单是否展开；已展示过的方案 id（用于生成成功提示）
  groupFormOpen: false,
  groupSeenId: null,
  groupExpect: false,
  // 现场问题墙：已展示过的问题 id -> {merged, supportCount, mergedInto}（用于变化提示）
  questionSeeded: false,
  questionSeen: new Map()
};

const $ = (sel) => document.querySelector(sel);
const $$ = (sel) => [...document.querySelectorAll(sel)];

// ---------------------------------------------------------------------------
// DOM
// ---------------------------------------------------------------------------

const els = {
  home: $('#view-home'),
  room: $('#view-room'),
  formCreate: $('#form-create'),
  createNick: $('#create-nick'),
  formJoin: $('#form-join'),
  joinRoom: $('#join-room'),
  joinNick: $('#join-nick'),
  roomCode: $('#room-code'),
  hostBadge: $('#host-badge'),
  versionBadge: $('#version-badge'),
  connDot: $('#conn-dot'),
  connText: $('#conn-text'),
  btnCopyLink: $('#btn-copy-link'),
  btnExit: $('#btn-exit'),
  stageIdle: $('#stage-idle'),
  stageActive: $('#stage-active'),
  stageNick: $('#stage-nick'),
  stageTimer: $('#stage-timer'),
  stageCountdown: $('#stage-countdown'),
  stageQuota: $('#stage-quota'),
  speakDuration: $('#speak-duration'),
  quotaDuration: $('#quota-duration'),
  btnQuotaReset: $('#btn-quota-reset'),
  quotaBar: $('#quota-bar'),
  quotaBarText: $('#quota-bar-text'),
  quotaMine: $('#quota-mine'),
  btnEnd: $('#btn-end'),
  queueList: $('#queue-list'),
  queueEmpty: $('#queue-empty'),
  queueCount: $('#queue-count'),
  undoBar: $('#undo-bar'),
  swapBar: $('#swap-bar'),
  btnRequest: $('#btn-request'),
  btnCancel: $('#btn-cancel'),
  logList: $('#log-list'),
  peopleList: $('#people-list'),
  peopleCount: $('#people-count'),
  btnClearAll: $('#btn-clear-all'),
  btnClearLogs: $('#btn-clear-logs'),
  btnRollcallStart: $('#btn-rollcall-start'),
  rollcallBar: $('#rollcall-bar'),
  rollcallTargetName: $('#rollcall-target-name'),
  rollcallCountdown: $('#rollcall-countdown'),
  rollcallProgress: $('#rollcall-progress'),
  rollcallActionsMe: $('#rollcall-actions-me'),
  rollcallActionsHost: $('#rollcall-actions-host'),
  btnRollcallConfirm: $('#btn-rollcall-confirm'),
  btnRollcallSkip: $('#btn-rollcall-skip'),
  btnRollcallCancel: $('#btn-rollcall-cancel'),
  // 议题投票
  btnTopicNew: $('#btn-topic-new'),
  topicForm: $('#topic-create-form'),
  topicTitleInput: $('#topic-title-input'),
  topicDuration: $('#topic-duration'),
  btnTopicFormCancel: $('#btn-topic-cancel-form'),
  topicCard: $('#topic-card'),
  topicHistory: $('#topic-history'),
  // 行动项
  btnTaskNew: $('#btn-task-new'),
  taskForm: $('#task-create-form'),
  taskTitleInput: $('#task-title-input'),
  taskAssignee: $('#task-assignee'),
  taskDeadline: $('#task-deadline'),
  btnTaskFormCancel: $('#btn-task-cancel-form'),
  taskCount: $('#task-count'),
  taskGroups: $('#task-groups'),
  // 分组讨论
  btnGroupNew: $('#btn-group-new'),
  groupForm: $('#group-create-form'),
  groupCount: $('#group-count'),
  groupMembers: $('#group-members'),
  groupSelectedCount: $('#group-selected-count'),
  groupFormHint: $('#group-form-hint'),
  groupCa: $('#group-ca'),
  groupCb: $('#group-cb'),
  groupCtype: $('#group-ctype'),
  btnGroupAddConstraint: $('#btn-group-add-constraint'),
  groupConstraintChips: $('#group-constraint-chips'),
  btnGroupFormCancel: $('#btn-group-cancel-form'),
  groupResult: $('#group-result'),
  // 现场问题墙
  questionForm: $('#question-create-form'),
  questionTextInput: $('#question-text-input'),
  questionCharCount: $('#question-char-count'),
  questionList: $('#question-list'),
  questionEmpty: $('#question-empty'),
  questionCount: $('#question-count'),
  questionClosed: $('#question-closed'),
  questionMerged: $('#question-merged'),
  toast: $('#toast-container')
};

// ---------------------------------------------------------------------------
// 工具
// ---------------------------------------------------------------------------

function toast(message, kind = '') {
  const div = document.createElement('div');
  div.className = `toast ${kind}`;
  div.textContent = message;
  els.toast.appendChild(div);
  setTimeout(() => {
    div.style.opacity = '0';
    div.style.transition = 'opacity 0.3s';
    setTimeout(() => div.remove(), 300);
  }, 3200);
}

function storageKey(code) {
  return `speakqueue_room_${code}`;
}

function saveCreds() {
  if (!state.creds) return;
  localStorage.setItem(storageKey(state.creds.room), JSON.stringify(state.creds));
}

function clearCreds() {
  if (state.creds) localStorage.removeItem(storageKey(state.creds.room));
  state.creds = null;
}

function wsUrl() {
  const proto = location.protocol === 'https:' ? 'wss' : 'ws';
  return `${proto}://${location.host}/ws`;
}

function pad2(n) {
  return String(n).padStart(2, '0');
}

function formatDuration(ms) {
  if (ms < 0) ms = 0;
  const total = Math.floor(ms / 1000);
  const h = Math.floor(total / 3600);
  const m = Math.floor((total % 3600) / 60);
  const s = total % 60;
  return h > 0 ? `${h}:${pad2(m)}:${pad2(s)}` : `${pad2(m)}:${pad2(s)}`;
}

function formatTime(ts) {
  const d = new Date(ts);
  return `${pad2(d.getHours())}:${pad2(d.getMinutes())}:${pad2(d.getSeconds())}`;
}

function escapeHtml(s) {
  return String(s).replace(/[&<>"']/g, (c) => ({
    '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;'
  }[c]));
}

function serverNow() {
  return Date.now() + state.clockOffset;
}

/** datetime-local 输入框所需的本地时间格式（YYYY-MM-DDTHH:mm） */
function toLocalInputValue(ts) {
  const d = new Date(ts);
  return `${d.getFullYear()}-${pad2(d.getMonth() + 1)}-${pad2(d.getDate())}T${pad2(d.getHours())}:${pad2(d.getMinutes())}`;
}

// 限时时长选项：1～30 分钟（「不限时」为默认项，见 index.html）
for (let m = 1; m <= 30; m++) {
  const opt = document.createElement('option');
  opt.value = String(m);
  opt.textContent = `${m} 分钟`;
  els.speakDuration.appendChild(opt);
}

/** 主持人当前选择的限时时长（分钟）；null 表示不限时 */
function chosenDurationMin() {
  const v = parseInt(els.speakDuration.value, 10);
  return Number.isInteger(v) && v >= 1 && v <= 30 ? v : null;
}

// 发言配额周期时长选项：1～30 分钟（默认 10 分钟）
for (let m = 1; m <= 30; m++) {
  const opt = document.createElement('option');
  opt.value = String(m);
  opt.textContent = `${m} 分钟`;
  if (m === 10) opt.selected = true;
  els.quotaDuration.appendChild(opt);
}

// 议题投票时长选项（秒）：15/30 秒、1/2/3/5/10 分钟
const TOPIC_DURATION_OPTIONS = [15, 30, 60, 120, 180, 300, 600];
for (const sec of TOPIC_DURATION_OPTIONS) {
  const opt = document.createElement('option');
  opt.value = String(sec);
  opt.textContent = sec < 60 ? `${sec} 秒` : `${sec / 60} 分钟`;
  if (sec === 60) opt.selected = true;
  els.topicDuration.appendChild(opt);
}

// ---------------------------------------------------------------------------
// 视图切换
// ---------------------------------------------------------------------------

function showHome() {
  els.room.classList.add('hidden');
  els.home.classList.remove('hidden');
  document.body.classList.remove('role-host', 'role-participant');
}

function showRoom() {
  els.home.classList.add('hidden');
  els.room.classList.remove('hidden');
}

// ---------------------------------------------------------------------------
// 创建 / 加入（首页）
// ---------------------------------------------------------------------------

function prefillFromUrl() {
  const params = new URLSearchParams(location.search);
  const code = params.get('room');
  if (code) els.joinRoom.value = code.replace(/\D/g, '').slice(0, 6);
}
prefillFromUrl();

els.formCreate.addEventListener('submit', async (e) => {
  e.preventDefault();
  const nick = els.createNick.value.trim();
  try {
    const res = await fetch('/api/rooms', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ nick })
    });
    if (!res.ok) throw new Error(`创建失败（${res.status}）`);
    const data = await res.json();
    const creds = {
      room: data.room,
      id: data.participantId,
      nick: nick || '主持人',
      host: true,
      hostToken: data.hostToken
    };
    localStorage.setItem(storageKey(data.room), JSON.stringify(creds));
    enterRoom(creds);
  } catch (err) {
    toast(err.message || '创建房间失败', 'error');
  }
});

els.formJoin.addEventListener('submit', (e) => {
  e.preventDefault();
  const code = els.joinRoom.value.trim().replace(/\D/g, '').padStart(6, '0');
  const nick = els.joinNick.value.trim();
  if (code.length !== 6) return toast('请输入 6 位房间码', 'error');
  if (!nick) return toast('请输入昵称', 'error');

  // 若此浏览器存有该房间身份（含创建者 hostToken），直接恢复
  const stored = JSON.parse(localStorage.getItem(storageKey(code)) || 'null');
  if (stored) {
    stored.nick = nick;
    enterRoom(stored);
  } else {
    enterRoom({ room: code, nick, host: false });
  }
});

function enterRoom(creds) {
  state.creds = creds;
  state.me = null;
  state.snapshot = null;
  state.version = 0;
  state.swapSeeded = false;
  state.swapStatus = new Map();
  state.taskSeeded = false;
  state.taskStatus = new Map();
  state.taskReqFormId = null;
  state.undoSeen = null;
  state.groupFormOpen = false;
  state.groupSeenId = null;
  state.groupExpect = false;
  state.questionSeeded = false;
  state.questionSeen = new Map();
  state.intentionalClose = false;
  state.reconnectDelay = 500;

  els.roomCode.textContent = creds.room;
  showRoom();
  connect();
}

// ---------------------------------------------------------------------------
// WebSocket 连接与自动重连
// ---------------------------------------------------------------------------

function connect() {
  setConn('connecting');
  const ws = new WebSocket(wsUrl());
  state.ws = ws;

  ws.addEventListener('open', () => {
    state.connected = true;
    state.reconnectDelay = 500;
    setConn('online');
    sendJoin();
  });

  ws.addEventListener('message', (ev) => {
    let msg;
    try {
      msg = JSON.parse(ev.data);
    } catch {
      return;
    }
    handleMessage(msg);
  });

  ws.addEventListener('close', () => {
    state.connected = false;
    if (state.intentionalClose) return;
    setConn('offline');
    scheduleReconnect();
  });

  ws.addEventListener('error', () => {
    try { ws.close(); } catch { /* ignore */ }
  });
}

function scheduleReconnect() {
  clearTimeout(state.reconnectTimer);
  els.connText.textContent = `连接断开，${Math.round(state.reconnectDelay / 100) / 10} 秒后重连…`;
  state.reconnectTimer = setTimeout(() => {
    connect();
    state.reconnectDelay = Math.min(state.reconnectDelay * 1.5, 10000);
  }, state.reconnectDelay);
}

function sendJoin() {
  const c = state.creds;
  const msg = { type: 'join', room: c.room, nick: c.nick };
  if (c.id) msg.participantId = c.id;
  if (c.host && c.hostToken) msg.hostToken = c.hostToken;
  if (!c.host && c.secret) msg.secret = c.secret;
  wsSend(msg);
}

function wsSend(obj) {
  if (state.ws && state.ws.readyState === WebSocket.OPEN) {
    state.ws.send(JSON.stringify(obj));
    return true;
  }
  toast('当前未连接服务器', 'error');
  return false;
}

function sendCmd(obj) {
  obj.version = state.version;
  wsSend(obj);
}

function setConn(status) {
  els.connDot.classList.remove('online', 'offline');
  if (status === 'online') {
    els.connDot.classList.add('online');
    els.connText.textContent = '已连接';
  } else if (status === 'offline') {
    els.connDot.classList.add('offline');
    els.connText.textContent = '已断开';
  } else {
    els.connText.textContent = '连接中…';
  }
}

// ---------------------------------------------------------------------------
// 服务端消息
// ---------------------------------------------------------------------------

function handleMessage(msg) {
  switch (msg.type) {
    case 'joined': {
      state.me = msg.you;
      // 首次快照只播种状态，不把重连前已存在的邀请/撤销状态当成新变化弹提示
      state.swapSeeded = false;
      state.swapStatus = new Map();
      state.taskSeeded = false;
      state.taskStatus = new Map();
      state.undoSeen = null;
      state.groupSeenId = null;
      state.groupExpect = false;
      state.questionSeeded = false;
      state.questionSeen = new Map();
      // 持久化身份，供断线/刷新后恢复
      const creds = state.creds;
      creds.id = msg.you.id;
      creds.nick = msg.you.nick;
      creds.host = !!msg.you.host;
      if (msg.you.secret) creds.secret = msg.you.secret;
      saveCreds();

      document.body.classList.toggle('role-host', creds.host);
      document.body.classList.toggle('role-participant', !creds.host);
      els.hostBadge.classList.toggle('hidden', !creds.host);
      applySnapshot(msg.snapshot);
      break;
    }
    case 'snapshot':
      applySnapshot(msg.snapshot);
      break;
    case 'error':
      // 分组生成失败（无解/成员已离开/参数非法/过期）：清除等待标记，保留表单与上次方案
      if (msg.code === 'GROUP_NO_SOLUTION' || msg.code === 'GROUP_BAD_MEMBERS' ||
          msg.code === 'GROUP_BAD_COUNT' || msg.code === 'GROUP_BAD_CONSTRAINT' ||
          msg.code === 'GROUP_MEMBER_LEFT' || msg.code === 'STALE_VERSION') {
        state.groupExpect = false;
      }
      if (msg.code === 'STALE_VERSION') {
        toast(`状态已过期，已同步到最新版本`, 'error');
      } else if (msg.code === 'NO_ROOM') {
        toast('房间不存在或已被回收', 'error');
        state.intentionalClose = true;
        try { state.ws.close(); } catch { /* ignore */ }
        clearCreds();
        setTimeout(showHome, 800);
      } else {
        toast(msg.error || '操作失败', 'error');
      }
      if (msg.snapshot) applySnapshot(msg.snapshot);
      break;
    case 'kicked':
      toast(msg.reason || '你已在其他页面打开此房间', 'error');
      state.intentionalClose = true;
      try { state.ws.close(); } catch { /* ignore */ }
      clearCreds();
      setTimeout(showHome, 1000);
      break;
  }
}

function applySnapshot(snap) {
  state.snapshot = snap;
  state.version = snap.version;
  state.clockOffset = snap.serverTs - Date.now();
  els.versionBadge.textContent = `v${snap.version}`;
  processSwaps(snap);
  processUndo(snap);
  processTasks(snap);
  processGroups(snap);
  processQuestions(snap);
  render();
}

// ---------------------------------------------------------------------------
// 安全撤销：按快照展示可撤销操作摘要或失效原因
// ---------------------------------------------------------------------------

const UNDO_OP_TEXT = {
  reorder: '调整发言顺序',
  remove: '移出队列',
  start_speaking: '请其开始发言',
  end_speaking: '结束发言'
};

/** 生成撤销状态的本地签名，用于检测跳变，避免重复弹提示 */
function undoSignature(u) {
  if (!u) return 'none';
  return `${u.op}:${u.status}:${u.version}:${u.reason || ''}`;
}

function processUndo(snap) {
  const u = snap.undo || null;
  const sig = undoSignature(u);
  if (state.undoSeen === null) {
    // 首次快照（含重连）只播种，不提示
    state.undoSeen = sig;
    return;
  }
  if (sig === state.undoSeen) return;
  const prev = state.undoSeen;
  state.undoSeen = sig;

  if (u && u.status === 'invalid' && prev !== 'none' && !prev.endsWith(':invalid:' + (u.reason || ''))) {
    // 可撤销操作因介入变更而失效
    toast(`撤销已失效：${u.reasonText || '房间状态出现新的变更'}`, 'error');
  } else if (!u && prev !== 'none') {
    // 撤销被成功消费（或被一键清空等整体重置）
    toast('已撤销最近一次主持人操作', 'success');
  } else if (u && u.status === 'available' && prev === 'none') {
    toast(`可撤销：${u.summary || UNDO_OP_TEXT[u.op] || '主持人操作'}`, 'info');
  }
}

// ---------------------------------------------------------------------------
// 协商换位：状态变化反馈
// ---------------------------------------------------------------------------

function nickOf(snap, id) {
  const p = snap.participants.find((x) => x.id === id);
  return p ? p.nick : '已离开';
}

function swapOutcomeToast(snap, swap, prevStatus) {
  if (swap.status === 'pending') {
    if (state.me && swap.to === state.me.id && !prevStatus) {
      return [`「${nickOf(snap, swap.from)}」邀请你交换发言位置（第 ${swap.fromPos + 1} ↔ 第 ${swap.toPos + 1} 位）`, 'info'];
    }
    return null;
  }
  if (swap.status === 'accepted') {
    return [`换位成功：「${nickOf(snap, swap.from)}」与「${nickOf(snap, swap.to)}」已交换位置`, 'success'];
  }
  if (swap.status === 'rejected') {
    if (state.me && swap.from === state.me.id) {
      return [`「${nickOf(snap, swap.to)}」拒绝了你的换位邀请`, 'error'];
    }
    return ['换位邀请已拒绝', 'info'];
  }
  if (swap.status === 'cancelled') {
    if (state.me && swap.to === state.me.id) {
      return [`「${nickOf(snap, swap.from)}」撤回了换位邀请`, 'info'];
    }
    return ['换位邀请已撤回', 'info'];
  }
  if (swap.status === 'invalid') {
    const reasonText = {
      canceled_speak: '一方取消了发言申请',
      started_speaking: '一方开始发言',
      removed: '一方被移出队列',
      left: '一方离开了房间',
      reordered: '主持人调整了发言顺序',
      cleared: '主持人清空了队列',
      position_changed: '双方位置已变化'
    }[swap.reason] || '邀请已失效';
    return [`换位邀请已失效：${reasonText}`, 'error'];
  }
  return null;
}

function processSwaps(snap) {
  const seen = new Set();
  for (const swap of snap.swaps || []) {
    seen.add(swap.id);
    const prev = state.swapStatus.get(swap.id);
    if (state.swapSeeded && (!prev || prev.status !== swap.status)) {
      const outcome = swapOutcomeToast(snap, swap, prev ? prev.status : null);
      if (outcome) toast(outcome[0], outcome[1]);
    }
    state.swapStatus.set(swap.id, { status: swap.status, reason: swap.reason });
  }
  // 清理已从快照消失（过期回收）的本地记录
  for (const id of [...state.swapStatus.keys()]) {
    if (!seen.has(id)) state.swapStatus.delete(id);
  }
  state.swapSeeded = true;
}

// ---------------------------------------------------------------------------
// 渲染
// ---------------------------------------------------------------------------

function render() {
  const snap = state.snapshot;
  if (!snap) return;
  renderStage(snap);
  renderRollcall(snap);
  renderQuota(snap);
  renderQueue(snap);
  renderUndoBar(snap);
  renderSwapBar(snap);
  renderTopic(snap);
  renderTasks(snap);
  renderGroups(snap);
  renderQuestions(snap);
  renderLogs(snap);
  renderPeople(snap);
}

function participantMap(snap) {
  const m = new Map();
  for (const p of snap.participants) m.set(p.id, p);
  return m;
}

function renderStage(snap) {
  if (snap.speaking) {
    els.stageIdle.classList.add('hidden');
    els.stageActive.classList.remove('hidden');
    els.stageNick.innerHTML =
      `<span class="pulse"></span> ${escapeHtml(snap.speaking.nick)}`;
    els.stageQuota.classList.toggle('hidden', !snap.quota);
    updateTimer();
  } else {
    els.stageIdle.classList.remove('hidden');
    els.stageActive.classList.add('hidden');
    els.stageCountdown.classList.add('hidden');
    els.stageCountdown.classList.remove('timer-warning');
    els.stageQuota.classList.add('hidden');
    els.stageQuota.classList.remove('timer-warning');
  }
}

// 最后 30 秒醒目提示的阈值
const COUNTDOWN_WARN_MS = 30000;
// 点名接力倒计时最后 5 秒醒目提示
const ROLLCALL_WARN_MS = 5000;

/**
 * 队首点名接力状态条：当前对象、同步倒计时、本轮进度、本人确认/跳过操作。
 * 倒计时基于服务端下发的 deadline 与 serverTs 校准后的时钟，断线重连后自然恢复。
 */
function renderRollcall(snap) {
  const rc = snap.rollcall;
  if (!rc) {
    els.rollcallBar.hidden = true;
    els.btnRollcallStart.disabled = false;
    els.btnRollcallStart.classList.remove('is-disabled');
    return;
  }
  els.rollcallBar.hidden = false;
  els.btnRollcallStart.disabled = true;
  els.btnRollcallStart.classList.add('is-disabled');

  els.rollcallTargetName.textContent = nickOf(snap, rc.currentId);
  const done = (rc.called || []).length;
  els.rollcallProgress.textContent = `本轮已点名 ${done}/${rc.total} 人`;

  const myId = state.me ? state.me.id : null;
  const isMine = rc.currentId === myId;
  els.rollcallActionsMe.hidden = !isMine;
  // 主持人本人不会在队列中；主持人操作条由 CSS（.host-only）控制显隐
  els.rollcallActionsHost.hidden = !(state.me && state.me.host);

  updateRollcallCountdown();
}

function updateRollcallCountdown() {
  const rc = state.snapshot && state.snapshot.rollcall;
  if (!rc) return;
  const remainMs = Math.max(rc.deadline - serverNow(), 0);
  const remain = Math.ceil(remainMs / 1000);
  els.rollcallCountdown.textContent = `${remain} 秒`;
  els.rollcallCountdown.classList.toggle('timer-warning', remainMs <= ROLLCALL_WARN_MS);
}

function updateTimer() {
  const snap = state.snapshot;
  if (!snap || !snap.speaking) return;
  const speaking = snap.speaking;
  const elapsed = serverNow() - speaking.startedAt;
  els.stageTimer.textContent = formatDuration(elapsed);
  if (speaking.endsAt) {
    // 同步倒计时：基于服务端下发的 endsAt 与 serverTs 校准后的时钟
    const remain = Math.max(speaking.endsAt - serverNow(), 0);
    els.stageCountdown.classList.remove('hidden');
    els.stageCountdown.textContent = `剩余 ${formatDuration(remain)}`;
    els.stageCountdown.classList.toggle('timer-warning', remain <= COUNTDOWN_WARN_MS);
  } else {
    els.stageCountdown.classList.add('hidden');
    els.stageCountdown.classList.remove('timer-warning');
  }
}

// ---------------------------------------------------------------------------
// 累计发言配额：周期条、实时余额与耗尽状态
// ---------------------------------------------------------------------------

/**
 * 配额条：周期标识、每人额度与我的实时余额。
 * 余额随快照下发（当前发言者含实时累计），此处按 serverTs 校准继续推演，
 * 断线重连后凭最新快照恢复真实余额。
 */
function renderQuota(snap) {
  const q = snap.quota;
  els.btnQuotaReset.textContent = q ? '🔁 重置配额周期' : '⏱ 开启发言配额';
  els.btnQuotaReset.title = q
    ? '重置发言配额周期：全员已用额度清零，开始新的计费周期'
    : '开启发言配额：本周期内每人累计发言时长不得超过所选额度';
  if (!q) {
    els.quotaBar.hidden = true;
    return;
  }
  els.quotaBar.hidden = false;
  const limitText = q.limitMs % 60000 === 0 ? `${q.limitMs / 60000} 分钟` : formatDuration(q.limitMs);
  els.quotaBarText.innerHTML =
    `每人 <b>${escapeHtml(limitText)}</b> · 周期 <span class="quota-period">${escapeHtml(q.periodId)}</span>`;
  updateQuotaTick();
}

/** 某成员的实时剩余配额：当前发言者的余额按 serverTs 校准时钟持续递减 */
function quotaLiveRemainingMs(snap, pid) {
  const q = snap && snap.quota;
  if (!q || !q.members) return null;
  const m = q.members[pid];
  if (!m) return null;
  let remain = m.remainingMs;
  if (snap.speaking && snap.speaking.id === pid) {
    remain -= serverNow() - snap.serverTs;
  }
  return Math.max(remain, 0);
}

function updateQuotaTick() {
  const snap = state.snapshot;
  if (!snap || !snap.quota) return;
  // 舞台：当前发言人的实时余额
  if (snap.speaking) {
    const remain = quotaLiveRemainingMs(snap, snap.speaking.id);
    els.stageQuota.textContent = remain > 0 ? `配额剩余 ${formatDuration(remain)}` : '配额已用尽';
    els.stageQuota.classList.toggle('timer-warning', remain <= COUNTDOWN_WARN_MS);
  }
  // 配额条：我的余额
  if (state.me) {
    const mine = quotaLiveRemainingMs(snap, state.me.id);
    if (mine !== null) {
      els.quotaMine.textContent = mine <= 0 ? '我的余额：已用尽' : `我的余额 ${formatDuration(mine)}`;
      els.quotaMine.classList.toggle('quota-exhausted', mine <= 0);
    }
  }
  // 成员列表中当前发言者的余额同步跳动
  const liveEl = els.peopleList.querySelector('[data-quota-speaking]');
  if (liveEl && snap.speaking) {
    const remain = quotaLiveRemainingMs(snap, snap.speaking.id);
    liveEl.textContent = remain > 0 ? `余 ${formatDuration(remain)}` : '已用尽';
    liveEl.classList.toggle('quota-exhausted', remain <= 0);
  }
}

if (!state.timerHandle) {
  state.timerHandle = setInterval(() => {
    updateTimer();
    updateRollcallCountdown();
    updateTopicCountdown();
    updateQuotaTick();
    updateTaskTick();
  }, 250);
}

function renderQueue(snap) {
  const pmap = participantMap(snap);
  els.queueList.textContent = '';
  els.queueCount.textContent = snap.order.length;
  els.queueEmpty.classList.toggle('hidden', snap.order.length > 0);
  els.queueList.classList.toggle('hidden', snap.order.length === 0);

  const myId = state.me ? state.me.id : null;
  const isHost = !!(state.me && state.me.host);
  const myPending = myId ? (snap.swaps || []).find((s) => s.status === 'pending' && (s.from === myId || s.to === myId)) : null;
  const rc = snap.rollcall;
  const calledSet = rc ? new Set(rc.called) : null;
  const pendingSet = rc ? new Set(rc.pending) : null;

  snap.order.forEach((pid, i) => {
    const p = pmap.get(pid);
    const nick = p ? p.nick : '已离开';
    const li = document.createElement('li');
    li.className = 'queue-item';
    li.dataset.id = pid;
    if (state.me && pid === state.me.id) li.classList.add('is-me');
    if (drag && drag.item.dataset.id === pid) li.classList.add('dragging');
    if (rc && rc.currentId === pid) li.classList.add('is-rollcall-current');

    const pendingHere = (snap.swaps || []).find(
      (s) => s.status === 'pending' && (s.from === pid || s.to === pid)
    );
    let swapTag = '';
    if (pendingHere) {
      const otherId = pendingHere.from === pid ? pendingHere.to : pendingHere.from;
      const role = pendingHere.from === pid ? '邀请' : '被邀';
      swapTag = `<span class="swap-tag" title="${role}换位：与「${escapeHtml(nickOf(snap, otherId))}」">⇄ 换位${role}中</span>`;
    }

    // 参与者：对队列中其他人可发起换位邀请；自己有待处理邀请或对方已有邀请时禁用；
    // 点名接力进行中冻结换位入口。
    let swapInviteBtn = '';
    if (!isHost && pid !== myId && !rc) {
      const busy = !!myPending || !!pendingHere;
      swapInviteBtn = `
        <button class="btn btn-ghost btn-mini swap-invite-btn ${busy ? 'is-disabled' : ''}"
                data-act="swap-invite" ${busy ? 'disabled' : ''}
                title="${busy ? '存在待处理的换位邀请' : `邀请「${escapeHtml(nick)}」交换位置`}">⇄ 换位</button>`;
    }

    let rollcallTag = '';
    if (rc) {
      if (rc.currentId === pid) {
        rollcallTag = '<span class="rollcall-now-tag">📢 正在点名</span>';
      } else if (calledSet.has(pid)) {
        rollcallTag = '<span class="rollcall-done-tag">已跳过</span>';
      } else if (pendingSet.has(pid)) {
        rollcallTag = '<span class="rollcall-wait-tag">待点名</span>';
      }
    }

    // 配额余额标签（排队成员未在发言，余额为快照结算值）
    let quotaTag = '';
    if (snap.quota && snap.quota.members) {
      const qm = snap.quota.members[pid];
      if (qm) {
        quotaTag = qm.remainingMs <= 0
          ? '<span class="quota-mini-tag quota-exhausted">配额已用尽</span>'
          : `<span class="quota-mini-tag">余额 ${formatDuration(qm.remainingMs)}</span>`;
      }
    }

    li.draggable = false; // 使用自定义 pointer 拖拽，同时兼容触屏
    li.innerHTML = `
      <span class="drag-handle ${isHost ? '' : 'hidden'}" title="拖动排序">⠿</span>
      <span class="queue-index">${i + 1}</span>
      <span class="queue-nick">${escapeHtml(nick)}</span>
      ${state.me && pid === state.me.id ? '<span class="me-tag">我</span>' : ''}
      ${rollcallTag}
      ${quotaTag}
      ${swapTag}
      ${p && !p.online ? '<span class="offline-tag">离线</span>' : ''}
      ${swapInviteBtn}
      ${isHost ? `
        <button class="btn btn-primary btn-mini" data-act="start" title="请其发言">▶ 开始</button>
        <button class="btn btn-ghost btn-mini" data-act="remove" title="移出队列">✕</button>
      ` : ''}
    `;
    els.queueList.appendChild(li);
  });

  // 拖拽过程中若收到快照重建了 DOM，需要把拖拽引用绑到新元素，
  // 并保持用户已拖出的视觉位置
  if (drag) {
    const fresh = els.queueList.querySelector(`.queue-item[data-id="${CSS.escape(drag.item.dataset.id)}"]`);
    if (fresh && fresh !== drag.item) {
      drag.item = fresh;
    }
  }

  // 参与者的申请/取消按钮状态（配额耗尽时禁止再次排队）
  if (state.me && !state.me.host) {
    const myId = state.me.id;
    const inQueue = snap.order.includes(myId);
    const speaking = snap.speaking && snap.speaking.id === myId;
    const myQuota = snap.quota && snap.quota.members ? snap.quota.members[myId] : null;
    const exhausted = !!(myQuota && myQuota.remainingMs <= 0);
    els.btnRequest.classList.toggle('hidden', inQueue || speaking);
    els.btnCancel.classList.toggle('hidden', !inQueue);
    els.btnRequest.disabled = speaking || exhausted;
    els.btnRequest.textContent = speaking ? '你正在发言中' : exhausted ? '配额已用尽' : '🙋 申请发言';
  }
}

/**
 * 安全撤销条：展示最近一次主持人操作的结构化摘要，
 * 可一键撤销；若已被介入变更作废则展示失效原因（按钮禁用）。
 */
function renderUndoBar(snap) {
  const bar = els.undoBar;
  const u = snap.undo;
  bar.textContent = '';
  if (!state.me || !state.me.host || !u) {
    bar.hidden = true;
    return;
  }
  bar.hidden = false;
  bar.classList.toggle('undo-invalid', u.status === 'invalid');

  const main = document.createElement('span');
  main.className = 'undo-main';
  const opText = UNDO_OP_TEXT[u.op] || '主持人操作';
  if (u.status === 'available') {
    const time = formatTime(u.ts);
    main.innerHTML =
      `<span class="undo-tag">可撤销</span>` +
      `<span class="undo-summary">${escapeHtml(u.summary || opText)}</span>` +
      `<span class="undo-meta">${time} · v${u.version} 之后的任何变更都会使其失效</span>`;
    const btn = document.createElement('button');
    btn.className = 'btn btn-mini';
    btn.type = 'button';
    btn.dataset.act = 'undo-host';
    btn.textContent = '↩ 撤销';
    bar.appendChild(main);
    bar.appendChild(btn);
  } else {
    main.innerHTML =
      `<span class="undo-tag undo-tag-dead">已失效</span>` +
      `<span class="undo-summary">${escapeHtml(opText)}：${escapeHtml(u.reasonText || '房间状态出现新的变更')}</span>`;
    bar.appendChild(main);
  }
}

/**
 * 协商换位状态卡：分别展示「我发起的」与「我收到的」待处理邀请，
 * 提供接受 / 拒绝 / 撤回操作。普通参与者视图可见。
 */
function renderSwapBar(snap) {
  els.swapBar.textContent = '';
  if (!state.me || state.me.host) return;

  const myId = state.me.id;
  const pending = (snap.swaps || []).filter((s) => s.status === 'pending');
  const outgoing = pending.filter((s) => s.from === myId);
  const incoming = pending.filter((s) => s.to === myId);

  for (const swap of outgoing) {
    const card = document.createElement('div');
    card.className = 'swap-card swap-out';
    card.innerHTML = `
      <div class="swap-card-main">
        <span class="swap-card-title">⇄ 换位邀请已发出</span>
        <span class="swap-card-desc">等待「${escapeHtml(nickOf(snap, swap.to))}」回应
          （第 ${swap.fromPos + 1} 位 ↔ 第 ${swap.toPos + 1} 位）</span>
      </div>
      <button class="btn btn-ghost btn-mini" data-swap-act="cancel" data-swap-id="${swap.id}">撤回邀请</button>
    `;
    els.swapBar.appendChild(card);
  }

  for (const swap of incoming) {
    const card = document.createElement('div');
    card.className = 'swap-card swap-in';
    card.innerHTML = `
      <div class="swap-card-main">
        <span class="swap-card-title">⇄ 收到换位邀请</span>
        <span class="swap-card-desc">「${escapeHtml(nickOf(snap, swap.from))}」想与你交换位置
          （第 ${swap.fromPos + 1} 位 ↔ 第 ${swap.toPos + 1} 位）</span>
      </div>
      <div class="swap-card-actions">
        <button class="btn btn-primary btn-mini" data-swap-act="accept" data-swap-id="${swap.id}">接受</button>
        <button class="btn btn-ghost btn-mini" data-swap-act="reject" data-swap-id="${swap.id}">拒绝</button>
      </div>
    `;
    els.swapBar.appendChild(card);
  }
}

// ---------------------------------------------------------------------------
// 议题投票：当前议题卡（实时票数 / 剩余时间 / 投票）与已结算结果
// ---------------------------------------------------------------------------

const TOPIC_WARN_MS = 10000;

const TOPIC_CHOICE_TEXT = { yes: '赞成', no: '反对', abstain: '弃权' };
const TOPIC_RESULT_TEXT = { passed: '✅ 通过', rejected: '❌ 否决', tie: '⚖️ 平票' };

/** 主持人的「发起议题」表单：有进行中议题时隐藏（发起新议题会先结算旧议题） */
function syncTopicFormVisibility() {
  const hasOpen = !!(state.snapshot && state.snapshot.topic);
  els.topicForm.classList.toggle('is-hidden', hasOpen);
}

function myTopicChoice(topic) {
  if (!state.me) return null;
  const mine = (topic.votes || []).find((v) => v.participantId === state.me.id);
  return mine ? mine.choice : null;
}

/**
 * 当前议题卡：
 * - 进行中：实时票数（赞成/反对/弃权 + 占已投票比例）、服务端校准的剩余时间、
 *   本人投票按钮（截止前可反复改投，高亮当前选择）、主持人「提前结束」；
 * - 无议题：空状态。已结算议题在下方 history 列表展示。
 */
function renderTopic(snap) {
  syncTopicFormVisibility();
  const topic = snap.topic;
  const card = els.topicCard;
  card.textContent = '';

  if (!topic) {
    const empty = document.createElement('div');
    empty.className = 'topic-empty';
    empty.textContent = '暂无进行中的议题';
    card.appendChild(empty);
  } else {
    const myChoice = myTopicChoice(topic);
    const totalVotes = topic.votedCount || 0;
    const remainMs = Math.max(topic.deadline - serverNow(), 0);
    const closed = remainMs <= 0;

    const head = document.createElement('div');
    head.className = 'topic-head';
    head.innerHTML = `
      <span class="topic-status-tag ${closed ? 'closed' : 'open'}">${closed ? '已截止' : '投票中'}</span>
      <span class="topic-title">${escapeHtml(topic.title)}</span>
      <span class="topic-countdown" id="topic-countdown"></span>
    `;
    card.appendChild(head);

    const tally = document.createElement('div');
    tally.className = 'topic-tally';
    for (const choice of ['yes', 'no', 'abstain']) {
      const n = topic.counts[choice] || 0;
      const pct = totalVotes > 0 ? Math.round((n / totalVotes) * 100) : 0;
      const row = document.createElement('div');
      row.className = `tally-row ${choice}${myChoice === choice ? ' is-picked' : ''}`;
      row.innerHTML = `
        <span class="tally-label"><span class="dot"></span>${TOPIC_CHOICE_TEXT[choice]}</span>
        <span class="tally-bar"><span class="tally-fill" style="width:${totalVotes > 0 ? pct : 0}%"></span></span>
        <span class="tally-meta">${n} 票 · ${pct}%</span>
      `;
      tally.appendChild(row);
    }
    card.appendChild(tally);

    const meta = document.createElement('div');
    meta.className = 'topic-result-meta';
    meta.style.marginTop = '8px';
    meta.textContent = `已投票 ${totalVotes} / ${snap.participants.length} 人${
      myChoice ? `　·　我的选择：${TOPIC_CHOICE_TEXT[myChoice]}（截止前可改投）` : ''
    }`;
    card.appendChild(meta);

    // 投票操作（主持人也可投票；截止后禁用，等待服务端结算快照）
    const actions = document.createElement('div');
    actions.className = 'topic-vote-actions';
    for (const choice of ['yes', 'no', 'abstain']) {
      const btn = document.createElement('button');
      btn.type = 'button';
      btn.className =
        `btn topic-vote-btn ${choice === 'yes' ? 'btn-primary' : choice === 'no' ? 'btn-danger' : 'btn-secondary'}` +
        (myChoice === choice ? ' picked' : '');
      btn.dataset.topicChoice = choice;
      btn.disabled = closed;
      btn.textContent = `${TOPIC_CHOICE_TEXT[choice]}${myChoice === choice ? ' ✓' : ''}`;
      actions.appendChild(btn);
    }
    card.appendChild(actions);

    if (state.me && state.me.host) {
      const hostActions = document.createElement('div');
      hostActions.className = 'topic-host-actions host-only';
      const endBtn = document.createElement('button');
      endBtn.type = 'button';
      endBtn.className = 'btn btn-ghost btn-mini';
      endBtn.id = 'btn-topic-end';
      endBtn.textContent = '⏹ 提前结束并结算';
      hostActions.appendChild(endBtn);
      card.appendChild(hostActions);
    }
  }

  updateTopicCountdown();
  renderTopicHistory(snap);
}

function updateTopicCountdown() {
  const topic = state.snapshot && state.snapshot.topic;
  const el = document.getElementById('topic-countdown');
  if (!topic || !el) return;
  const remainMs = Math.max(topic.deadline - serverNow(), 0);
  el.textContent = remainMs > 0 ? `剩余 ${formatDuration(remainMs)}` : '已截止，等待结算…';
  el.classList.toggle('timer-warning', remainMs > 0 && remainMs <= TOPIC_WARN_MS);
}

/** 已结算议题列表（最近 N 条，最新在前） */
function renderTopicHistory(snap) {
  const box = els.topicHistory;
  box.textContent = '';
  const history = snap.topicHistory || [];
  if (history.length === 0) return;

  const title = document.createElement('div');
  title.className = 'topic-history-title';
  title.textContent = '已结算议题';
  box.appendChild(title);

  for (const t of history) {
    const item = document.createElement('div');
    item.className = 'topic-history-item';
    const endedText = { deadline: '截止结算', host: '主持人结束', replaced: '被新议题替换' }[t.endedBy] || '';
    item.innerHTML = `
      <span class="th-result ${t.result}">${TOPIC_RESULT_TEXT[t.result] || t.result}</span>
      <span class="th-title">${escapeHtml(t.title)}</span>
      <span class="th-counts">赞成 ${t.counts.yes} · 反对 ${t.counts.no} · 弃权 ${t.counts.abstain}
        （共 ${t.total} 票）· ${endedText} · ${formatTime(t.closedAt)}</span>
    `;
    box.appendChild(item);
  }
}

// ---------------------------------------------------------------------------
// 行动项：分组看板（待接受 / 处理中 / 待确认 / 已完成·已拒绝）与状态流转
// ---------------------------------------------------------------------------

const TASK_STATUS_TEXT = {
  pending: '待接受',
  active: '处理中',
  review: '待确认',
  done: '已完成',
  rejected: '已拒绝'
};

// 延期申请状态文案
const TASK_REQ_STATUS_TEXT = {
  pending: '延期待审批',
  approved: '延期已批准',
  rejected: '延期已拒绝',
  voided: '延期已作废'
};

// 看板分组：key 对应 index.html 中的 data-group / data-task-list
const TASK_GROUPS = [
  { key: 'pending', statuses: ['pending'] },
  { key: 'active', statuses: ['active'] },
  { key: 'review', statuses: ['review'] },
  { key: 'closed', statuses: ['done', 'rejected'] }
];

/** 状态变更记录 -> 可读文案（按 from -> to 区分「接受」与「退回」） */
function taskHistoryText(h) {
  if (h.to === 'pending') return '创建了行动项';
  if (h.to === 'active') return h.from === 'review' ? '退回处理中' : '接受了，开始处理';
  if (h.to === 'review') return '标记完成，待确认';
  if (h.to === 'done') return '确认完成';
  if (h.to === 'rejected') return '拒绝了';
  return '状态变更';
}

function formatDeadline(ts) {
  const d = new Date(ts);
  return `${pad2(d.getMonth() + 1)}-${pad2(d.getDate())} ${pad2(d.getHours())}:${pad2(d.getMinutes())}`;
}

/** 剩余 / 逾期时长的人性化文本（按服务端校准时钟计算） */
function taskRemainText(deadline) {
  const diff = deadline - serverNow();
  const abs = Math.abs(diff);
  const days = Math.floor(abs / 86400000);
  const hours = Math.floor((abs % 86400000) / 3600000);
  const mins = Math.floor((abs % 3600000) / 60000);
  let txt;
  if (days > 0) txt = `${days} 天 ${hours} 小时`;
  else if (hours > 0) txt = `${hours} 小时 ${mins} 分`;
  else txt = `${Math.max(mins, 1)} 分钟`;
  return diff >= 0 ? `剩 ${txt}` : `已逾期 ${txt}`;
}

/** 状态变化反馈：与本人相关（我是负责人或主持人）的流转弹提示 */
function taskTransitionToast(snap, task, prevStatus) {
  const me = state.me;
  if (!me) return null;
  const isAssignee = task.assigneeId === me.id;
  if (!prevStatus && task.status === 'pending' && isAssignee) {
    return [`主持人给你指派了行动项「${task.title}」`, 'info'];
  }
  if (task.status === 'active' && prevStatus === 'pending' && me.host) {
    return [`「${task.assigneeNick}」接受了行动项「${task.title}」`, 'success'];
  }
  if (task.status === 'rejected' && me.host) {
    return [`「${task.assigneeNick}」拒绝了行动项「${task.title}」`, 'error'];
  }
  if (task.status === 'review' && me.host) {
    return [`「${task.assigneeNick}」标记完成「${task.title}」，待你确认`, 'info'];
  }
  if (task.status === 'done' && isAssignee) {
    return [`主持人已确认「${task.title}」完成`, 'success'];
  }
  if (task.status === 'active' && prevStatus === 'review' && isAssignee) {
    return [`主持人将「${task.title}」退回处理中`, 'error'];
  }
  return null;
}

/** 延期申请状态 -> 提示（负责人收到审批结果；主持人收到新申请 / 作废提示） */
function taskRequestToast(snap, task, req, prevSig) {
  const me = state.me;
  if (!me || !req) return null;
  const isAssignee = task.assigneeId === me.id;
  // 首次见到该申请：只提示「新申请」，不把重连前的申请当新消息
  const isNew = !prevSig;
  if (req.status === 'pending') {
    if (isNew && me.host && !isAssignee) {
      return [`「${req.requestedByNick}」就行动项「${task.title}」申请延期，待你审批`, 'info'];
    }
    return null;
  }
  // 已审结：从「待审批」跳变到结果时提示（避免重连后重复弹）
  if (prevSig && prevSig.startsWith('pending:')) {
    if (req.status === 'approved') {
      if (isAssignee) {
        return [`主持人已批准「${task.title}」的延期申请，新截止时间已生效`, 'success'];
      }
    } else if (req.status === 'rejected') {
      if (isAssignee) {
        return [`主持人拒绝了「${task.title}」的延期申请`, 'error'];
      }
    } else if (req.status === 'voided') {
      if (isAssignee) {
        return [`行动项「${task.title}」状态已变更，延期申请自动作废`, 'error'];
      }
    }
  }
  return null;
}

/** 延期申请的本地签名：id:状态，用于检测新申请与审批结果跳变 */
function taskReqSignature(req) {
  return req ? `${req.id}:${req.status}` : null;
}

function processTasks(snap) {
  const seen = new Set();
  for (const task of snap.tasks || []) {
    seen.add(task.id);
    const prev = state.taskStatus.get(task.id);
    if (state.taskSeeded && (!prev || prev.status !== task.status)) {
      const outcome = taskTransitionToast(snap, task, prev ? prev.status : null);
      if (outcome) toast(outcome[0], outcome[1]);
    }
    const req = task.deadlineRequest || null;
    const reqSig = taskReqSignature(req);
    if (state.taskSeeded && (!prev || prev.reqSig !== reqSig)) {
      const outcome = taskRequestToast(snap, task, req, prev ? prev.reqSig : null);
      if (outcome) toast(outcome[0], outcome[1]);
    }
    state.taskStatus.set(task.id, { status: task.status, reqSig });
  }
  // 清理已从快照消失（滚动淘汰）的本地记录
  for (const id of [...state.taskStatus.keys()]) {
    if (!seen.has(id)) state.taskStatus.delete(id);
  }
  // 若表单所对的行动项已消失或不再允许申请（离开待接受/处理中、已有待审申请），收起表单
  if (state.taskReqFormId) {
    const t = (snap.tasks || []).find((x) => x.id === state.taskReqFormId);
    const openable =
      t &&
      state.me &&
      t.assigneeId === state.me.id &&
      (t.status === 'pending' || t.status === 'active') &&
      !(t.deadlineRequest && t.deadlineRequest.status === 'pending');
    if (!openable) state.taskReqFormId = null;
  }
  state.taskSeeded = true;
}

/** 负责人下拉框：跟随成员列表重建，保留当前选择（默认选自己） */
function syncTaskAssigneeOptions(snap) {
  const sel = els.taskAssignee;
  const prev = sel.value;
  sel.textContent = '';
  const sorted = snap.participants.slice().sort((a, b) => a.joinedAt - b.joinedAt);
  for (const p of sorted) {
    const opt = document.createElement('option');
    opt.value = p.id;
    opt.textContent =
      `${p.nick}${p.id === snap.hostId ? '（主持人）' : ''}${p.online ? '' : '（离线）'}`;
    sel.appendChild(opt);
  }
  const values = [...sel.options].map((o) => o.value);
  if (prev && values.includes(prev)) sel.value = prev;
  else if (state.me && values.includes(state.me.id)) sel.value = state.me.id;
}

/**
 * 行动项看板：按 待接受 / 处理中 / 待确认 / 已完成·已拒绝 分组渲染。
 * 未完结组按截止时间升序（最紧急在前），已完结组按最近更新降序；
 * 每项卡片展示负责人、截止时间与最近一条状态变更记录，
 * 并按「当前状态 + 我的身份」渲染可执行操作（服务端仍会二次校验）。
 */
function renderTasks(snap) {
  const tasks = snap.tasks || [];
  const openCount = tasks.filter(
    (t) => t.status === 'pending' || t.status === 'active' || t.status === 'review'
  ).length;
  els.taskCount.textContent = openCount;
  syncTaskAssigneeOptions(snap);
  const pmap = participantMap(snap);

  for (const g of TASK_GROUPS) {
    const listEl = els.taskGroups.querySelector(`[data-task-list="${g.key}"]`);
    const emptyEl = els.taskGroups.querySelector(`[data-task-empty="${g.key}"]`);
    const countEl = els.taskGroups.querySelector(`[data-task-count="${g.key}"]`);
    const items = tasks.filter((t) => g.statuses.includes(t.status));
    items.sort((a, b) => (g.key === 'closed' ? b.updatedAt - a.updatedAt : a.deadline - b.deadline));
    countEl.textContent = items.length;
    emptyEl.classList.toggle('hidden', items.length > 0);
    listEl.textContent = '';
    for (const t of items) {
      listEl.appendChild(taskCard(t, pmap, g.key === 'closed'));
    }
  }
  // 内联延期表单：填入默认新截止时间（当前截止或现在 +1 小时，取较晚者）并聚焦
  if (state.taskReqFormId) {
    const form = els.taskGroups.querySelector(`form[data-task-req-form="${state.taskReqFormId}"]`);
    const t = tasks.find((x) => x.id === state.taskReqFormId);
    if (form && t) {
      const input = form.querySelector('.task-req-deadline');
      input.value = toLocalInputValue(Math.max(t.deadline, serverNow()) + 3600000);
      form.querySelector('.task-req-reason')?.focus();
    }
  }
  updateTaskTick();
}

function taskCard(t, pmap, closed) {
  const me = state.me;
  const isAssignee = !!(me && me.id === t.assigneeId);
  const isHost = !!(me && me.host);
  const assignee = pmap.get(t.assigneeId);
  const offline = !assignee || !assignee.online;
  const req = t.deadlineRequest || null;

  // 可执行操作：负责人接受/拒绝（待接受）、标记完成（处理中）；主持人确认/退回（待确认）
  let actions = '';
  if (t.status === 'pending' && isAssignee) {
    actions = `
      <div class="task-actions">
        <button class="btn btn-primary btn-mini" data-task-act="accept" data-task-id="${t.id}">接受</button>
        <button class="btn btn-ghost btn-mini" data-task-act="reject" data-task-id="${t.id}">拒绝</button>
      </div>`;
  } else if (t.status === 'active' && isAssignee) {
    actions = `
      <div class="task-actions">
        <button class="btn btn-primary btn-mini" data-task-act="complete" data-task-id="${t.id}">✔ 标记完成</button>
      </div>`;
  } else if (t.status === 'review' && isHost) {
    actions = `
      <div class="task-actions">
        <button class="btn btn-primary btn-mini" data-task-act="confirm" data-task-id="${t.id}">✅ 确认完成</button>
        <button class="btn btn-ghost btn-mini" data-task-act="return" data-task-id="${t.id}">↩ 退回处理中</button>
      </div>`;
  }

  // 延期入口：负责人在待接受/处理中且无待审申请时可发起（同一事项仅一条待审）
  if (
    isAssignee &&
    (t.status === 'pending' || t.status === 'active') &&
    !(req && req.status === 'pending')
  ) {
    actions += `
      <div class="task-actions">
        <button class="btn btn-ghost btn-mini task-req-toggle" data-task-act="deadline-form" data-task-id="${t.id}">⏱ 申请延期</button>
      </div>`;
  }

  const last = t.history && t.history.length ? t.history[t.history.length - 1] : null;
  const li = document.createElement('li');
  li.className = `task-card st-${t.status}`;
  li.innerHTML = `
    <div class="task-card-head">
      <span class="task-status-tag">${TASK_STATUS_TEXT[t.status] || t.status}</span>
      <span class="task-title">${escapeHtml(t.title)}</span>
    </div>
    <div class="task-meta">
      <span>负责人：<b>${escapeHtml(t.assigneeNick)}</b></span>
      ${isAssignee ? '<span class="me-tag">我</span>' : ''}
      ${offline ? '<span class="offline-tag">离线</span>' : ''}
    </div>
    <div class="task-meta">
      <span>截止 ${formatDeadline(t.deadline)}</span>
      ${closed ? '' : `<span class="task-remain" data-deadline="${t.deadline}"></span>`}
    </div>
    ${taskDeadlineRequestHtml(t, req)}
    ${state.taskReqFormId === t.id ? taskDeadlineFormHtml(t) : ''}
    ${last ? `<div class="task-history-line">${formatTime(last.ts)} · ${escapeHtml(last.byNick)} ${taskHistoryText(last)}</div>` : ''}
    ${actions}
  `;
  return li;
}

/** 延期申请区块：展示状态、新截止时间、理由、申请人/审批人及时间；待审批时主持人可批准/拒绝 */
function taskDeadlineRequestHtml(t, req) {
  if (!req) return '';
  const isHost = !!(state.me && state.me.host);
  let lines;
  if (req.status === 'pending') {
    lines = `
      <div class="task-req-line"><span class="task-req-label">申请截止</span><b>${formatDeadline(req.newDeadline)}</b></div>
      <div class="task-req-line"><span class="task-req-label">理由</span>${escapeHtml(req.reason)}</div>
      <div class="task-req-line"><span class="task-req-label">申请人</span>${escapeHtml(req.requestedByNick)} · ${formatTime(req.requestedAt)}</div>`;
  } else if (req.status === 'approved') {
    lines = `
      <div class="task-req-line"><span class="task-req-label">理由</span>${escapeHtml(req.reason)}</div>
      <div class="task-req-line"><span class="task-req-label">申请人</span>${escapeHtml(req.requestedByNick)} · ${formatTime(req.requestedAt)}</div>
      <div class="task-req-line"><span class="task-req-label">批准人</span>${escapeHtml(req.decidedByNick || '主持人')} · ${formatTime(req.decidedAt)}</div>`;
  } else if (req.status === 'rejected') {
    lines = `
      <div class="task-req-line"><span class="task-req-label">申请截止</span>${formatDeadline(req.newDeadline)}（未生效）</div>
      <div class="task-req-line"><span class="task-req-label">理由</span>${escapeHtml(req.reason)}</div>
      <div class="task-req-line"><span class="task-req-label">申请人</span>${escapeHtml(req.requestedByNick)} · ${formatTime(req.requestedAt)}</div>
      <div class="task-req-line"><span class="task-req-label">拒绝人</span>${escapeHtml(req.decidedByNick || '主持人')} · ${formatTime(req.decidedAt)}</div>`;
  } else {
    lines = `
      <div class="task-req-line"><span class="task-req-label">申请截止</span>${formatDeadline(req.newDeadline)}（未生效）</div>
      <div class="task-req-line"><span class="task-req-label">理由</span>${escapeHtml(req.reason)}</div>
      <div class="task-req-line"><span class="task-req-label">申请人</span>${escapeHtml(req.requestedByNick)} · ${formatTime(req.requestedAt)}</div>
      <div class="task-req-line"><span class="task-req-label">作废</span>${escapeHtml(req.decidedByNick || '主持人')} · ${formatTime(req.decidedAt)}</div>`;
  }
  const hostBtns =
    req.status === 'pending' && isHost
      ? `
      <div class="task-actions">
        <button class="btn btn-primary btn-mini" data-task-act="deadline-approve" data-task-id="${t.id}">✅ 批准延期</button>
        <button class="btn btn-ghost btn-mini" data-task-act="deadline-reject" data-task-id="${t.id}">拒绝延期</button>
      </div>`
      : '';
  return `
    <div class="task-deadline-request req-${req.status}">
      <div class="task-req-head">
        <span class="task-req-tag">${TASK_REQ_STATUS_TEXT[req.status] || req.status}</span>
      </div>
      ${lines}
      ${hostBtns}
    </div>`;
}

/** 负责人提交延期申请的内联表单（新截止时间 + 理由；默认值在渲染后填入） */
function taskDeadlineFormHtml(t) {
  return `
    <form class="task-req-form" data-task-req-form="${t.id}">
      <label class="duration-picker" title="申请的新截止时间（须晚于当前截止时间）">
        新截止
        <input type="datetime-local" class="task-req-deadline" />
      </label>
      <input type="text" class="task-req-reason" maxlength="300"
             placeholder="延期理由（如：依赖材料未到位）" autocomplete="off" />
      <div class="task-actions">
        <button class="btn btn-primary btn-mini" type="submit">提交申请</button>
        <button class="btn btn-ghost btn-mini" type="button" data-task-act="deadline-form-cancel" data-task-id="${t.id}">取消</button>
      </div>
    </form>`;
}

/** 未完结行动项的剩余时间随服务端校准时钟跳动，逾期红色醒目提示 */
function updateTaskTick() {
  $$('#task-groups .task-remain').forEach((el) => {
    const dl = Number(el.dataset.deadline);
    if (!Number.isFinite(dl)) return;
    el.textContent = taskRemainText(dl);
    el.classList.toggle('task-overdue', dl < serverNow());
  });
}

// 主持人：打开 / 收起创建表单（默认截止时间为 1 小时后）
els.btnTaskNew.addEventListener('click', () => {
  els.taskForm.classList.remove('is-hidden');
  els.taskDeadline.value = toLocalInputValue(Date.now() + 3600000);
  els.taskTitleInput.focus();
});

els.btnTaskFormCancel.addEventListener('click', () => {
  els.taskForm.classList.add('is-hidden');
});

// 主持人：创建行动项（负责人 + 事项 + 截止时间，走版本守卫）
els.taskForm.addEventListener('submit', (e) => {
  e.preventDefault();
  const title = els.taskTitleInput.value.trim();
  if (!title) return toast('请填写事项内容', 'error');
  const assigneeId = els.taskAssignee.value;
  if (!assigneeId) return toast('请选择负责人', 'error');
  const deadline = new Date(els.taskDeadline.value).getTime();
  if (!Number.isFinite(deadline)) return toast('请选择有效的截止时间', 'error');
  sendCmd({ type: 'task_create', participantId: assigneeId, title, deadline });
  els.taskTitleInput.value = '';
  els.taskForm.classList.add('is-hidden');
});

// 行动项卡片操作：接受 / 拒绝 / 标记完成 / 确认完成 / 退回处理中 / 延期申请与审批
els.taskGroups.addEventListener('click', (e) => {
  const btn = e.target.closest('button[data-task-act]');
  if (!btn) return;
  const type = {
    accept: 'task_accept',
    reject: 'task_reject',
    complete: 'task_complete',
    confirm: 'task_confirm',
    return: 'task_return',
    'deadline-approve': 'task_deadline_approve',
    'deadline-reject': 'task_deadline_reject'
  }[btn.dataset.taskAct];
  if (btn.dataset.taskAct === 'deadline-form') {
    // 展开内联延期申请表单（同一时刻只展开一个）
    state.taskReqFormId = btn.dataset.taskId;
    render();
    return;
  }
  if (btn.dataset.taskAct === 'deadline-form-cancel') {
    state.taskReqFormId = null;
    render();
    return;
  }
  if (!type) return;
  sendCmd({ type, taskId: btn.dataset.taskId });
});

// 延期申请表单提交：新截止时间 + 理由（走版本守卫，STALE_VERSION 时表单保持展开便于重试）
els.taskGroups.addEventListener('submit', (e) => {
  const form = e.target.closest('form[data-task-req-form]');
  if (!form) return;
  e.preventDefault();
  const taskId = form.dataset.taskReqForm;
  const deadline = new Date(form.querySelector('.task-req-deadline').value).getTime();
  const reason = form.querySelector('.task-req-reason').value.trim();
  if (!Number.isFinite(deadline)) return toast('请选择有效的新截止时间', 'error');
  if (!reason) return toast('请填写延期理由', 'error');
  sendCmd({ type: 'task_deadline_request', taskId, deadline, reason });
});

// ---------------------------------------------------------------------------
// 分组讨论：主持人选人/组数/约束生成，全员展示当前方案（保留生成时昵称）
// ---------------------------------------------------------------------------

const GROUP_MIN_MEMBERS = 2;
const GROUP_MAX_MEMBERS = 12;
const GROUP_MIN_COUNT = 2;
const GROUP_MAX_COUNT = 4;

const GROUP_TYPE_TEXT = { together: '必须同组', apart: '必须分开' };

// 生成分组表单草稿：{ memberIds:Set, count:number, constraints:[{a,b,type}] }
let groupDraft = null;

/** 检测新生成的方案：生成命令成功后由快照广播触发成功提示并收起表单 */
function processGroups(snap) {
  const plan = snap.groupPlan || null;
  if (state.groupSeenId === null) {
    // 首次快照（含重连）只播种，不提示
    state.groupSeenId = plan ? plan.id : null;
    return;
  }
  const currentId = plan ? plan.id : null;
  if (currentId !== state.groupSeenId) {
    if (state.groupExpect) {
      toast('分组方案已生成', 'success');
      closeGroupForm();
    }
    state.groupExpect = false;
    state.groupSeenId = currentId;
  }
}

function renderGroups(snap) {
  els.btnGroupNew.classList.toggle('hidden', state.groupFormOpen);
  els.groupForm.classList.toggle('is-hidden', !state.groupFormOpen);
  if (state.groupFormOpen) renderGroupForm(snap);
  renderGroupResult(snap);
}

function closeGroupForm() {
  state.groupFormOpen = false;
  groupDraft = null;
  els.groupFormHint.textContent = '';
}

/** 打开表单：优先沿用上一方案的成员与约束（过滤已离开者），否则空草稿 */
function openGroupForm(snap) {
  const draft = { memberIds: new Set(), count: GROUP_MIN_COUNT, constraints: [] };
  const plan = snap.groupPlan;
  if (plan) {
    const presentIds = new Set(snap.participants.map((p) => p.id));
    const kept = [];
    const dropped = [];
    for (const m of plan.members) {
      if (presentIds.has(m.id)) {
        draft.memberIds.add(m.id);
        kept.push(m);
      } else {
        dropped.push(m.nick);
      }
    }
    draft.count = plan.groupCount;
    for (const c of plan.constraints) {
      if (presentIds.has(c.a) && presentIds.has(c.b)) {
        draft.constraints.push({ a: c.a, b: c.b, type: c.type });
      }
    }
    if (dropped.length > 0) {
      setTimeout(() => toast(`原方案中「${dropped.join('、')}」已离开，已从名单移除`, 'info'), 0);
    }
    if (kept.length > 0) {
      setTimeout(() => toast(`已沿用上次方案的 ${kept.length} 名成员与 ${draft.constraints.length} 条约束`, 'info'), 0);
    }
  }
  groupDraft = draft;
  state.groupFormOpen = true;
  renderGroups(snap);
}

function sortedParticipants(snap) {
  return snap.participants.slice().sort((a, b) => {
    if (a.id === snap.hostId) return -1;
    if (b.id === snap.hostId) return 1;
    return a.joinedAt - b.joinedAt;
  });
}

/** 生成表单：成员多选、组数、约束编辑（每次快照到达时重建，勾选状态取自草稿） */
function renderGroupForm(snap) {
  if (!groupDraft) groupDraft = { memberIds: new Set(), count: GROUP_MIN_COUNT, constraints: [] };

  // 快照推进后，草稿中已离开房间的成员自动移除并提示
  const presentIds = new Set(snap.participants.map((p) => p.id));
  for (const id of [...groupDraft.memberIds]) {
    if (!presentIds.has(id)) groupDraft.memberIds.delete(id);
  }
  groupDraft.constraints = groupDraft.constraints.filter(
    (c) => groupDraft.memberIds.has(c.a) && groupDraft.memberIds.has(c.b)
  );

  // 成员多选
  els.groupMembers.textContent = '';
  for (const p of sortedParticipants(snap)) {
    const label = document.createElement('label');
    label.className = 'group-member' + (p.id === snap.hostId ? ' is-crown' : '');
    label.dataset.id = p.id;
    const checked = groupDraft.memberIds.has(p.id);
    label.innerHTML = `
      <input type="checkbox" data-group-member="${p.id}" ${checked ? 'checked' : ''} />
      <span>${p.id === snap.hostId ? '👑 ' : ''}${escapeHtml(p.nick)}</span>
      ${p.online ? '' : '<span class="offline-tag">离线</span>'}
    `;
    els.groupMembers.appendChild(label);
  }

  const n = groupDraft.memberIds.size;
  els.groupSelectedCount.textContent = `已选 ${n} 人（需 ${GROUP_MIN_MEMBERS}～${GROUP_MAX_MEMBERS} 人）`;
  els.groupSelectedCount.classList.toggle('ok', n >= GROUP_MIN_MEMBERS && n <= GROUP_MAX_MEMBERS);

  // 组数：2～min(4, 人数)；人数不足 2 时禁用并给占位提示
  const maxCount = Math.max(0, Math.min(GROUP_MAX_COUNT, n));
  const prevCount = groupDraft.count;
  els.groupCount.textContent = '';
  if (maxCount < GROUP_MIN_COUNT) {
    const opt = document.createElement('option');
    opt.value = '';
    opt.textContent = n === 0 ? '请先选择成员' : `至少选择 ${GROUP_MIN_COUNT} 人`;
    opt.disabled = true;
    els.groupCount.appendChild(opt);
    groupDraft.count = prevCount; // 恢复选满后由下方逻辑重新钳制
  } else {
    for (let k = GROUP_MIN_COUNT; k <= GROUP_MAX_COUNT; k++) {
      const opt = document.createElement('option');
      opt.value = String(k);
      opt.textContent = `${k} 组`;
      opt.disabled = k > maxCount;
      els.groupCount.appendChild(opt);
    }
    groupDraft.count = Math.min(Math.max(prevCount, GROUP_MIN_COUNT), maxCount);
    els.groupCount.value = String(groupDraft.count);
  }

  syncGroupConstraintSelects(snap);
  renderGroupChips(snap);
  renderGroupFormHint();
}

function renderGroupFormHint() {
  const n = groupDraft ? groupDraft.memberIds.size : 0;
  if (n === 0) {
    els.groupFormHint.textContent = '请从当前成员中选择 2～12 人';
  } else if (n < GROUP_MIN_MEMBERS) {
    els.groupFormHint.textContent = `至少还需选择 ${GROUP_MIN_MEMBERS - n} 人`;
  } else if (n > GROUP_MAX_MEMBERS) {
    els.groupFormHint.textContent = `最多选择 ${GROUP_MAX_MEMBERS} 人，请取消 ${n - GROUP_MAX_MEMBERS} 人`;
  } else {
    const k = groupDraft.count;
    const base = Math.floor(n / k);
    const big = n - base * k;
    els.groupFormHint.textContent =
      `将分为 ${k} 组：${big > 0 ? `${big} 组 ${base + 1} 人、${k - big} 组 ${base} 人` : `每组 ${base} 人`}，人数之差不超过 1`;
  }
}

/** 约束两端的人员下拉框：只列出已选成员，保留当前选择 */
function syncGroupConstraintSelects(snap) {
  const fill = (sel, prefer) => {
    const prev = prefer || sel.value;
    sel.textContent = '';
    const ids = [...groupDraft.memberIds];
    for (const id of ids) {
      const opt = document.createElement('option');
      opt.value = id;
      opt.textContent = nickOf(snap, id);
      sel.appendChild(opt);
    }
    if (ids.includes(prev)) sel.value = prev;
    else if (ids.length > 1) sel.value = ids[sel === els.groupCa ? 0 : 1] || ids[0];
  };
  fill(els.groupCa);
  fill(els.groupCb);
}

function renderGroupChips(snap) {
  els.groupConstraintChips.textContent = '';
  for (let i = 0; i < groupDraft.constraints.length; i++) {
    const c = groupDraft.constraints[i];
    const chip = document.createElement('span');
    chip.className = `group-chip ${c.type}`;
    chip.innerHTML =
      `<b>${escapeHtml(nickOf(snap, c.a))}</b>` +
      `<span>${c.type === 'together' ? '⇄ 必须同组' : '⊘ 必须分开'}</span>` +
      `<b>${escapeHtml(nickOf(snap, c.b))}</b>` +
      `<button type="button" class="chip-x" data-group-remove-constraint="${i}" title="移除约束">✕</button>`;
    els.groupConstraintChips.appendChild(chip);
  }
}

/** 当前分组方案：全员可见；成员已离开按留存昵称展示并标注，不影响分组 */
function renderGroupResult(snap) {
  const plan = snap.groupPlan;
  const box = els.groupResult;
  box.textContent = '';
  if (!plan) {
    const empty = document.createElement('div');
    empty.className = 'group-empty';
    empty.textContent = '尚未生成分组讨论方案';
    box.appendChild(empty);
    return;
  }

  const meta = document.createElement('div');
  meta.className = 'group-meta';
  const sizes = plan.groups.map((g) => g.length).join(' / ');
  const leftCount = plan.members.filter((m) => !m.present).length;
  meta.innerHTML =
    `<span>${plan.groupCount} 个小组 · 每组 ${sizes} 人 · 共 ${plan.members.length} 人</span>` +
    `<span>生成于 ${formatTime(plan.createdAt)}</span>` +
    (plan.constraints.length ? `<span>约束 ${plan.constraints.length} 条</span>` : '') +
    (leftCount ? `<span class="offline-tag">${leftCount} 人已离开房间（不影响本方案）</span>` : '');
  box.appendChild(meta);

  if (plan.constraints.length) {
    const summary = document.createElement('div');
    summary.className = 'group-constraint-summary';
    for (const c of plan.constraints) {
      const chip = document.createElement('span');
      chip.className = `group-chip ${c.type}`;
      chip.innerHTML =
        `<b>${escapeHtml(c.aNick)}</b><span>${c.type === 'together' ? '⇄ 同组' : '⊘ 分开'}</span><b>${escapeHtml(c.bNick)}</b>`;
      summary.appendChild(chip);
    }
    box.appendChild(summary);
  }

  const cards = document.createElement('div');
  cards.className = 'group-cards';
  plan.groups.forEach((members, i) => {
    const card = document.createElement('div');
    card.className = 'group-card';
    const rows = members
      .map((m) => {
        const isMe = state.me && m.id === state.me.id;
        const tag = m.present
          ? (isMe ? '<span class="me-tag">我</span>' : '')
          : '<span class="offline-tag">已离开</span>';
        return `<div class="group-member-row${isMe ? ' is-me' : ''}">
          <span class="people-status ${m.present ? 'online' : 'offline'}"></span>
          <span>${escapeHtml(m.nick)}</span>${tag}
        </div>`;
      })
      .join('');
    card.innerHTML = `
      <div class="group-card-title">
        <span>第 ${i + 1} 组</span>
        <span class="group-card-size">${members.length} 人</span>
      </div>
      ${rows}
    `;
    cards.appendChild(card);
  });
  box.appendChild(cards);
}

els.btnGroupNew.addEventListener('click', () => {
  if (!state.snapshot) return;
  openGroupForm(state.snapshot);
});

els.btnGroupFormCancel.addEventListener('click', () => {
  closeGroupForm();
  renderGroups(state.snapshot);
});

// 勾选/取消成员：更新草稿、组数选项与约束下拉，不整表重建（保留滚动位置）
els.groupMembers.addEventListener('change', (e) => {
  const cb = e.target.closest('input[data-group-member]');
  if (!cb || !groupDraft) return;
  const id = cb.dataset.groupMember;
  if (cb.checked) groupDraft.memberIds.add(id);
  else groupDraft.memberIds.delete(id);
  groupDraft.constraints = groupDraft.constraints.filter((c) => c.a !== id && c.b !== id);
  renderGroupForm(state.snapshot);
});

// 添加一条约束（重复/两端相同拒绝）
els.btnGroupAddConstraint.addEventListener('click', () => {
  if (!groupDraft) return;
  const a = els.groupCa.value;
  const b = els.groupCb.value;
  const type = els.groupCtype.value;
  const snap = state.snapshot;
  if (!a || !b) return toast('请先选择至少 2 名成员', 'error');
  if (a === b) return toast('约束两端不能是同一成员', 'error');
  const dup = groupDraft.constraints.some(
    (c) => c.type === type && ((c.a === a && c.b === b) || (c.a === b && c.b === a))
  );
  if (dup) return toast('该约束已存在', 'error');
  groupDraft.constraints.push({ a, b, type });
  renderGroupChips(snap);
});

// 移除约束
els.groupConstraintChips.addEventListener('click', (e) => {
  const btn = e.target.closest('button[data-group-remove-constraint]');
  if (!btn) return;
  const idx = Number(btn.dataset.groupRemoveConstraint);
  groupDraft.constraints.splice(idx, 1);
  renderGroupChips(state.snapshot);
});

// 组数变更
els.groupCount.addEventListener('change', () => {
  if (!groupDraft) return;
  const k = parseInt(els.groupCount.value, 10);
  if (Number.isInteger(k)) {
    groupDraft.count = k;
    renderGroupFormHint();
  }
});

// 提交生成：服务端在无解时返回 GROUP_NO_SOLUTION 并保留上次方案，表单保持展开
els.groupForm.addEventListener('submit', (e) => {
  e.preventDefault();
  if (!groupDraft) return;
  const memberIds = [...groupDraft.memberIds];
  const n = memberIds.length;
  if (n < GROUP_MIN_MEMBERS || n > GROUP_MAX_MEMBERS) {
    return toast(`请选择 ${GROUP_MIN_MEMBERS}～${GROUP_MAX_MEMBERS} 名成员`, 'error');
  }
  const k = groupDraft.count;
  if (!Number.isInteger(k) || k < GROUP_MIN_COUNT || k > GROUP_MAX_COUNT || k > n) {
    return toast(`小组数须为 ${GROUP_MIN_COUNT}～${GROUP_MAX_COUNT} 且不超过所选人数`, 'error');
  }
  state.groupExpect = true;
  sendCmd({
    type: 'group_generate',
    memberIds,
    groupCount: k,
    constraints: groupDraft.constraints
  });
});

function renderLogs(snap) {
  els.logList.textContent = '';
  // 最新的在最上面
  for (let i = snap.logs.length - 1; i >= 0; i--) {    const log = snap.logs[i];
    const li = document.createElement('li');
    li.className = `log-item kind-${log.kind}`;
    li.innerHTML = `<span class="log-time">${formatTime(log.ts)}</span><span>${escapeHtml(log.text)}</span>`;
    els.logList.appendChild(li);
  }
}

// ---------------------------------------------------------------------------
// 现场问题墙：提交 / 支持切换 / 主持人合并 / 查看合并去向
// ---------------------------------------------------------------------------

const QUESTION_TEXT_MAX = 120;
const QUESTION_ANSWER_MAX = 300;

/** 字数统计（按字符数计，与服务端 trim/slice 口径一致） */
function syncQuestionCharCount() {
  const len = els.questionTextInput.value.length;
  els.questionCharCount.textContent = `${len} / ${QUESTION_TEXT_MAX}`;
  els.questionCharCount.parentElement.classList.toggle('over', len > QUESTION_TEXT_MAX);
}

els.questionTextInput.addEventListener('input', syncQuestionCharCount);

/**
 * 变化检测：首次快照（含重连）只播种，不弹提示。
 * - 我提交的问题出现 -> 成功提示；
 * - 我支持的问题被合并 -> 提示已并入的目标；
 * - 我支持/提交的问题被结案 -> 提示已有答复。
 */
function processQuestions(snap) {
  const qs = snap.questions || { open: [], closed: [], merged: [] };
  const all = [...(qs.open || []), ...(qs.closed || []), ...(qs.merged || [])];
  const seen = new Set();
  const myId = state.me ? state.me.id : null;
  for (const q of all) {
    seen.add(q.id);
    const prev = state.questionSeen.get(q.id);
    if (state.questionSeeded) {
      if (!prev && myId && q.authorId === myId) {
        toast('问题已提交', 'success');
      }
      if (prev && !prev.merged && q.merged && myId && q.supporterIds.includes(myId)) {
        toast(`你支持的问题已并入「${q.mergedIntoText || '另一条问题'}」`, 'info');
      }
      if (prev && !prev.closed && q.closed && myId &&
          (q.authorId === myId || (q.answer && q.answer.supporterIds.includes(myId)))) {
        toast('你关注的问题已有主持人答复', 'info');
      }
    }
    state.questionSeen.set(q.id, {
      merged: q.merged,
      closed: q.closed,
      supportCount: q.supportCount,
      mergedInto: q.mergedInto
    });
  }
  for (const id of [...state.questionSeen.keys()]) {
    if (!seen.has(id)) state.questionSeen.delete(id);
  }
  state.questionSeeded = true;
}

/**
 * 问题墙渲染：
 * - 未合并、未结案问题按快照顺序（服务端按支持人数降序、同票按提交时间升序）；
 * - 已结案问题单独列在「已结案问题」区，固定展示答复、答复人与时间、支持情况；
 * - 已合并问题列在底部可折叠区，标明直接去向、最终目标文本与最终答复。
 */
function renderQuestions(snap) {
  const qs = snap.questions || { open: [], closed: [], merged: [] };
  const open = qs.open || [];
  const closed = qs.closed || [];
  const merged = qs.merged || [];
  const myId = state.me ? state.me.id : null;
  const isHost = !!(state.me && state.me.host);

  els.questionCount.textContent = open.length;
  els.questionEmpty.classList.toggle('hidden', open.length > 0);
  els.questionList.classList.toggle('hidden', open.length === 0);
  els.questionList.textContent = '';

  open.forEach((q, i) => {
    const mine = myId && q.authorId === myId;
    const supported = myId && q.supporterIds.includes(myId);
    const li = document.createElement('li');
    li.className = 'question-item' + (supported ? ' is-supported' : '');
    li.dataset.questionId = q.id;

    const supporterNames = (q.supporters || []).map((s) => s.nick).slice(0, 6).join('、');
    const supporterTip = (q.supporters || []).map((s) => s.nick).join('、');

    li.innerHTML = `
      <span class="question-rank">${i + 1}</span>
      <div class="question-body">
        <div class="question-text">${escapeHtml(q.text)}</div>
        <div class="question-meta">
          <span>${escapeHtml(q.authorNick)}${q.authorPresent ? '' : '（已离开）'} 提交</span>
          ${mine ? '<span class="me-tag">我</span>' : ''}
          <span>${formatTime(q.createdAt)}</span>
          ${q.supportCount > 0 ? `<span class="question-supported-by" title="${escapeHtml(supporterTip)}">👍 ${escapeHtml(supporterNames)}${q.supportCount > 6 ? ` 等 ${q.supportCount} 人` : ''}</span>` : ''}
        </div>
      </div>
      <button type="button"
              class="question-support-btn ${supported ? 'picked' : ''}"
              data-q-act="support" data-q-id="${q.id}"
              title="${supported ? '再次点击取消支持' : '支持这条问题'}">
        <span class="question-support-num">${q.supportCount}</span>
        <span class="question-support-label">${supported ? '已支持' : '支持'}</span>
      </button>
      ${isHost ? `
      <div class="question-host-actions">
        <button type="button" class="btn btn-ghost btn-mini"
                data-q-act="close" data-q-id="${q.id}"
                title="填写答复并结案，固定支持人数与来源问题">✅ 答复结案</button>
        <button type="button" class="btn btn-ghost btn-mini"
                data-q-act="merge" data-q-id="${q.id}"
                title="把另一条问题并入这条问题">🔀 并入此条</button>
      </div>` : ''}
    `;
    els.questionList.appendChild(li);
  });

  renderClosedQuestions(closed);
  renderMergedQuestions(merged);
}

/**
 * 已结案问题区：始终可见的答复记录。
 * 每条固定展示问题正文、答复正文、答复人与结案时间、结案时支持人数，
 * 以及沿合并链归入它的来源问题（结案瞬间快照）。
 */
function renderClosedQuestions(closed) {
  const box = els.questionClosed;
  box.textContent = '';
  if (!closed.length) return;

  const sum = document.createElement('div');
  sum.className = 'question-closed-title';
  sum.textContent = `✅ 已结案问题（${closed.length}）`;
  box.appendChild(sum);

  const list = document.createElement('ul');
  list.className = 'question-closed-list';
  for (const q of closed) {
    const a = q.answer;
    const li = document.createElement('li');
    li.className = 'question-closed-item';
    const supporterNames = (a.supporters || []).map((s) => s.nick).slice(0, 6).join('、');
    const supporterTip = (a.supporters || []).map((s) => s.nick).join('、');
    const sources = (a.sources || [])
      .map((s) => `<li class="question-source-item" title="${escapeHtml(s.supportCount)} 人支持">↳ ${escapeHtml(s.text)}</li>`)
      .join('');
    li.innerHTML = `
      <div class="question-closed-text">${escapeHtml(q.text)}</div>
      <div class="question-answer">
        <span class="question-answer-tag">答复</span>
        <span class="question-answer-text">${escapeHtml(a.text)}</span>
      </div>
      <div class="question-closed-meta">
        <span>${escapeHtml(a.byNick)} 答复 · ${formatTime(a.closedAt)}</span>
        <span class="question-supported-by" title="${escapeHtml(supporterTip)}">
          结案时 👍 ${a.supportCount} 人${a.supportCount > 0 && supporterNames ? `（${escapeHtml(supporterNames)}）` : ''}
        </span>
      </div>
      ${sources ? `<ul class="question-source-list"><li class="question-source-head">随合并归入的来源问题（${a.sources.length}）</li>${sources}</ul>` : ''}
    `;
    list.appendChild(li);
  }
  box.appendChild(list);
}

/** 已合并问题区：折叠展示，标明直接去向与最终目标；目标已结案时一并展示最终答复 */
function renderMergedQuestions(merged) {
  const box = els.questionMerged;
  box.textContent = '';
  if (!merged.length) return;

  const sum = document.createElement('summary');
  sum.textContent = `已合并问题（${merged.length}）`;
  box.appendChild(sum);

  const list = document.createElement('ul');
  list.className = 'question-merged-list';
  for (const q of merged) {
    const li = document.createElement('li');
    li.className = 'question-merged-item';
    const rootAnswer = q.rootAnswer;
    li.innerHTML = `
      <div class="question-merged-text">${escapeHtml(q.text)}</div>
      <div class="question-merged-arrow">
        <span>↳ 已并入</span>
        <span class="question-merged-target" title="合并去向：${escapeHtml(q.mergedRootId || q.mergedInto || '')}">
          「${escapeHtml(q.mergedIntoText || '另一条问题')}」
        </span>
        <span class="question-merged-state">${rootAnswer ? '✅ 已有最终答复' : '⏳ 待答复'}</span>
      </div>
      ${rootAnswer ? `
      <div class="question-answer question-answer-compact">
        <span class="question-answer-tag">最终答复</span>
        <span class="question-answer-text">${escapeHtml(rootAnswer.text)}</span>
      </div>
      <div class="question-closed-meta"><span>${escapeHtml(rootAnswer.byNick)} 答复 · ${formatTime(rootAnswer.closedAt)}</span></div>
      ` : ''}
    `;
    list.appendChild(li);
  }
  box.appendChild(list);
}

// 提交问题（走版本守卫；空内容服务端会拒绝，这里先拦一次）
els.questionForm.addEventListener('submit', (e) => {
  e.preventDefault();
  const text = els.questionTextInput.value.trim();
  if (!text) return toast('请填写问题内容', 'error');
  sendCmd({ type: 'question_submit', text });
  els.questionTextInput.value = '';
  syncQuestionCharCount();
});

// 支持切换 / 主持人合并（事件委托，跟随快照重建）
els.questionList.addEventListener('click', (e) => {
  const btn = e.target.closest('button[data-q-act]');
  if (!btn || btn.disabled) return;
  const id = btn.dataset.qId;
  if (btn.dataset.qAct === 'support') {
    sendCmd({ type: 'question_support', questionId: id });
  }
});

/**
 * 主持人合并：点击某条未合并问题上的「🔀 并入此条」后，选择一条来源问题。
 * 来源以下拉框列出其余未合并问题（服务端也会再次校验目标必须未合并）。
 */
els.questionList.addEventListener('click', (e) => {
  const btn = e.target.closest('button[data-q-act="merge"]');
  if (!btn || !(state.me && state.me.host)) return;
  const targetId = btn.dataset.qId;
  const snap = state.snapshot;
  const candidates = (snap.questions.open || []).filter((q) => q.id !== targetId);
  if (candidates.length === 0) {
    return toast('没有其他未合并问题可并入', 'error');
  }
  const lines = candidates
    .map((q, i) => `${i + 1}. [${q.supportCount} 赞] ${q.text.slice(0, 60)}（${q.authorNick}）`)
    .join('\n');
  const answer = prompt(
    `选择要并入「${(snap.questions.open.find((q) => q.id === targetId)?.text || '').slice(0, 40)}」的来源问题，输入序号：\n\n${lines}`
  );
  if (answer === null) return;
  const idx = parseInt(answer, 10);
  if (!Number.isInteger(idx) || idx < 1 || idx > candidates.length) {
    return toast('序号无效', 'error');
  }
  const sourceId = candidates[idx - 1].id;
  if (!confirm('确定合并？\n来源问题将标明去向且不再接受支持，两边支持者按成员去重合并。')) return;
  sendCmd({ type: 'question_merge', sourceId, targetId });
});

/**
 * 主持人结案：点击未结案问题上的「✅ 答复结案」，填写 1～300 字答复。
 * 结案瞬间由服务端固定支持人数、支持者与沿合并链归入的来源问题。
 */
els.questionList.addEventListener('click', (e) => {
  const btn = e.target.closest('button[data-q-act="close"]');
  if (!btn || !(state.me && state.me.host)) return;
  const questionId = btn.dataset.qId;
  const snap = state.snapshot;
  const q = (snap.questions.open || []).find((x) => x.id === questionId);
  if (!q) return toast('该问题已不在待答复列表', 'error');
  const answer = prompt(
    `为以下问题填写结案答复（1～${QUESTION_ANSWER_MAX} 字）：\n\n${q.text.slice(0, 120)}\n\n` +
      `当前 ${q.supportCount} 人支持，答复后问题将固定记录并移入「已结案问题」，不可再支持或合并。`
  );
  if (answer === null) return;
  const trimmed = answer.trim();
  if (!trimmed) return toast('请填写答复内容', 'error');
  if (trimmed.length > QUESTION_ANSWER_MAX) {
    return toast(`答复不能超过 ${QUESTION_ANSWER_MAX} 字（当前 ${trimmed.length} 字）`, 'error');
  }
  sendCmd({ type: 'question_close', questionId, answer: trimmed });
});

function renderPeople(snap) {
  els.peopleCount.textContent = snap.participants.length;
  els.peopleList.textContent = '';
  const sorted = snap.participants.slice().sort((a, b) => {
    if (a.id === snap.hostId) return -1;
    if (b.id === snap.hostId) return 1;
    return a.joinedAt - b.joinedAt;
  });
  for (const p of sorted) {
    const li = document.createElement('li');
    li.className = 'people-item';
    // 配额余额：当前发言者的余额由 updateQuotaTick 实时跳动
    let quotaHtml = '';
    if (snap.quota && snap.quota.members) {
      const qm = snap.quota.members[p.id];
      if (qm) {
        const speakingNow = snap.speaking && snap.speaking.id === p.id;
        const exhausted = qm.remainingMs <= 0;
        quotaHtml =
          `<span class="people-quota${exhausted ? ' quota-exhausted' : ''}"` +
          `${speakingNow ? ' data-quota-speaking="1"' : ''}>` +
          `${exhausted ? '已用尽' : `余 ${formatDuration(qm.remainingMs)}`}</span>`;
      }
    }
    li.innerHTML = `
      <span class="people-status ${p.online ? 'online' : 'offline'}"></span>
      <span class="people-nick ${p.online ? '' : 'offline-name'}">${escapeHtml(p.nick)}</span>
      ${p.id === snap.hostId ? '<span class="crown" title="主持人">👑</span>' : ''}
      ${state.me && p.id === state.me.id ? '<span class="me-tag">我</span>' : ''}
      ${quotaHtml}
    `;
    els.peopleList.appendChild(li);
  }
}

// ---------------------------------------------------------------------------
// 房间内操作
// ---------------------------------------------------------------------------

els.btnRequest.addEventListener('click', () => {
  sendCmd({ type: 'request_speak' });
});

els.btnCancel.addEventListener('click', () => {
  sendCmd({ type: 'cancel_speak' });
});

els.btnEnd.addEventListener('click', () => {
  sendCmd({ type: 'end_speaking' });
});

els.btnClearAll.addEventListener('click', () => {
  if (!confirm('确定一键清空？\n将清空发言队列、结束当前发言并清空操作记录。')) return;
  sendCmd({ type: 'clear' });
});

els.btnClearLogs.addEventListener('click', () => {
  if (!confirm('确定清空操作记录？')) return;
  sendCmd({ type: 'clear_logs' });
});

// ---------------------------------------------------------------------------
// 队首点名接力
// ---------------------------------------------------------------------------

els.btnRollcallStart.addEventListener('click', () => {
  sendCmd({ type: 'rollcall_start' });
});

// 开启 / 重置发言配额周期（主持人；走版本守卫，重置后全员余额清零）
els.btnQuotaReset.addEventListener('click', () => {
  const durationMin = parseInt(els.quotaDuration.value, 10);
  if (!Number.isInteger(durationMin) || durationMin < 1 || durationMin > 30) {
    return toast('请选择 1～30 分钟的配额时长', 'error');
  }
  if (state.snapshot && state.snapshot.quota) {
    if (!confirm(`确定重置配额周期？\n全员已用额度将清零，按每人 ${durationMin} 分钟开始新周期。`)) return;
  }
  sendCmd({ type: 'quota_reset', durationMin });
});

els.btnRollcallCancel.addEventListener('click', () => {
  sendCmd({ type: 'rollcall_cancel' });
});

els.btnRollcallConfirm.addEventListener('click', () => {
  const rc = state.snapshot && state.snapshot.rollcall;
  if (!rc) return;
  // callId 与 version 一起发送，服务端双重校验
  sendCmd({ type: 'rollcall_confirm', callId: rc.callId });
});

els.btnRollcallSkip.addEventListener('click', () => {
  const rc = state.snapshot && state.snapshot.rollcall;
  if (!rc) return;
  sendCmd({ type: 'rollcall_skip', callId: rc.callId });
});

// ---------------------------------------------------------------------------
// 议题投票
// ---------------------------------------------------------------------------

els.btnTopicNew.addEventListener('click', () => {
  els.topicForm.classList.remove('is-hidden');
  els.topicTitleInput.focus();
});

els.btnTopicFormCancel.addEventListener('click', () => {
  els.topicForm.classList.add('is-hidden');
});

els.topicForm.addEventListener('submit', (e) => {
  e.preventDefault();
  const title = els.topicTitleInput.value.trim();
  const durationSec = parseInt(els.topicDuration.value, 10);
  if (!title) return toast('请填写议题标题', 'error');
  const snap = state.snapshot;
  // 进行中议题会被立即提前结算
  if (snap && snap.topic && !confirm('当前已有进行中的议题，发布新议题将立即结算旧议题，确定继续？')) {
    return;
  }
  sendCmd({ type: 'topic_create', title, durationSec });
  els.topicTitleInput.value = '';
  els.topicForm.classList.add('is-hidden');
});

// 投票（赞成/反对/弃权）：携带 topicId，服务端同时校验版本与议题 id
els.topicCard.addEventListener('click', (e) => {
  const voteBtn = e.target.closest('button[data-topic-choice]');
  if (voteBtn && !voteBtn.disabled) {
    const topic = state.snapshot && state.snapshot.topic;
    if (!topic) return;
    sendCmd({ type: 'topic_vote', topicId: topic.id, choice: voteBtn.dataset.topicChoice });
    return;
  }
  const endBtn = e.target.closest('#btn-topic-end');
  if (endBtn) {
    if (!confirm('确定提前结束当前议题并立即结算？')) return;
    sendCmd({ type: 'topic_end' });
  }
});

// 安全撤销条：撤销最近一次主持人操作（版本冲突仍走 STALE_VERSION 流程）
els.undoBar.addEventListener('click', (e) => {
  const btn = e.target.closest('button[data-act="undo-host"]');
  if (!btn || btn.disabled) return;
  sendCmd({ type: 'undo_host' });
});

// 队列条目：开始发言 / 移出 / 发起换位邀请
els.queueList.addEventListener('click', (e) => {
  const btn = e.target.closest('button[data-act]');
  if (!btn) return;
  const li = e.target.closest('.queue-item');
  const pid = li.dataset.id;
  if (btn.dataset.act === 'start') {
    const cmd = { type: 'start_speaking', participantId: pid };
    const durationMin = chosenDurationMin();
    if (durationMin) cmd.durationMin = durationMin;
    sendCmd(cmd);
  } else if (btn.dataset.act === 'remove') {
    sendCmd({ type: 'remove', participantId: pid });
  } else if (btn.dataset.act === 'swap-invite') {
    if (btn.disabled) return;
    sendCmd({ type: 'swap_invite', participantId: pid });
  }
});

// 换位状态卡：接受 / 拒绝 / 撤回
els.swapBar.addEventListener('click', (e) => {
  const btn = e.target.closest('button[data-swap-act]');
  if (!btn) return;
  const swapId = btn.dataset.swapId;
  const act = btn.dataset.swapAct;
  if (act === 'accept') sendCmd({ type: 'swap_accept', swapId });
  else if (act === 'reject') sendCmd({ type: 'swap_reject', swapId });
  else if (act === 'cancel') sendCmd({ type: 'swap_cancel', swapId });
});

// ---------------------------------------------------------------------------
// 主持人拖动排序（pointer 事件，兼容鼠标与触屏）
// ---------------------------------------------------------------------------

let drag = null;

els.queueList.addEventListener('pointerdown', (e) => {
  if (!state.me || !state.me.host) return;
  const handle = e.target.closest('.drag-handle');
  if (!handle) return;
  const item = handle.closest('.queue-item');
  if (!item) return;
  e.preventDefault();

  drag = {
    item,
    handle,
    startY: e.clientY,
    moved: false
  };
  handle.setPointerCapture(e.pointerId);
});

els.queueList.addEventListener('pointermove', (e) => {
  if (!drag) return;
  if (!drag.moved && Math.abs(e.clientY - drag.startY) < 6) return;
  drag.moved = true;
  drag.item.classList.add('dragging');

  const items = $$('#queue-list .queue-item');
  const dragged = drag.item;
  let target = null;
  for (const it of items) {
    if (it === dragged) continue;
    const box = it.getBoundingClientRect();
    if (e.clientY < box.top + box.height / 2) { target = it; break; }
  }
  $$('#queue-list .queue-item').forEach((it) => it.classList.remove('drag-over'));
  if (target) {
    target.classList.add('drag-over');
    els.queueList.insertBefore(dragged, target);
  } else {
    els.queueList.appendChild(dragged);
  }
});

function endDrag() {
  if (!drag) return;
  const { item, moved, handle } = drag;
  item.classList.remove('dragging');
  $$('#queue-list .queue-item').forEach((it) => it.classList.remove('drag-over'));

  if (moved) {
    const newOrder = $$('#queue-list .queue-item').map((it) => it.dataset.id);
    const oldOrder = state.snapshot.order;
    const changed = newOrder.some((id, i) => id !== oldOrder[i]);
    if (changed) {
      sendCmd({ type: 'reorder', order: newOrder });
    }
  }
  try { handle.releasePointerCapture(); } catch { /* ignore */ }
  drag = null;
}

els.queueList.addEventListener('pointerup', endDrag);
els.queueList.addEventListener('pointercancel', endDrag);

// ---------------------------------------------------------------------------
// 顶栏：复制 / 退出
// ---------------------------------------------------------------------------

els.roomCode.addEventListener('click', async () => {
  await copyText(els.roomCode.textContent);
  toast(`房间码 ${els.roomCode.textContent} 已复制`, 'success');
});

els.btnCopyLink.addEventListener('click', async () => {
  const url = `${location.origin}/?room=${state.creds.room}`;
  await copyText(url);
  toast('邀请链接已复制', 'success');
});

async function copyText(text) {
  try {
    await navigator.clipboard.writeText(text);
  } catch {
    const ta = document.createElement('textarea');
    ta.value = text;
    document.body.appendChild(ta);
    ta.select();
    try { document.execCommand('copy'); } catch { /* ignore */ }
    ta.remove();
  }
}

els.btnExit.addEventListener('click', () => {
  if (!confirm('确定退出房间？你的席位将被移除。')) return;
  state.intentionalClose = true;
  wsSend({ type: 'leave' });
  try { state.ws.close(); } catch { /* ignore */ }
  clearCreds();
  clearTimeout(state.reconnectTimer);
  showHome();
  toast('已退出房间');
});

// 页面卸载：主动断开（服务端会保留离线席位等待重连，不发送 leave）
window.addEventListener('beforeunload', () => {
  state.intentionalClose = true;
  try { state.ws.close(); } catch { /* ignore */ }
});

// 从后台/休眠恢复时，WebSocket 可能已被系统静默断开：立即探测并重连
document.addEventListener('visibilitychange', () => {
  if (document.visibilityState !== 'visible' || !state.creds) return;
  if (!state.ws || state.ws.readyState === WebSocket.CLOSED) {
    clearTimeout(state.reconnectTimer);
    state.intentionalClose = false;
    state.reconnectDelay = 500;
    connect();
  }
});

showHome();
