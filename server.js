require('dotenv').config();
const fs = require('fs');
const path = require('path');
const http = require('http');
const express = require('express');
const cors = require('cors');
const { WebSocketServer } = require('ws');
const { TikTokLiveConnection, WebcastEvent } = require('tiktok-live-connector');

const PORT = process.env.PORT || 3000;
const ADMIN_KEY = process.env.ADMIN_KEY || 'family100';
if (!process.env.ADMIN_KEY) console.warn('[warn] ADMIN_KEY belum diisi, memakai default "family100". Ganti di Railway!');
const MAX_PER_TEAM = +process.env.MAX_PER_TEAM || 5;
const QPM = +process.env.QUESTIONS_PER_MATCH || 3;      // jumlah pertanyaan per match (seri -> tambah pertanyaan)
const ANSWER_SEC = +process.env.ANSWER_SECONDS || 15;   // timer jawaban
const DATA_DIR = process.env.DATA_DIR || path.join(__dirname, 'data');
const STATE_FILE = path.join(DATA_DIR, 'state.json');
const QUESTIONS = JSON.parse(fs.readFileSync(path.join(__dirname, 'questions.json'), 'utf8')).questions;
const KEYS = ['A', 'B', 'C', 'D'];

// ---------- STATE (disimpan ke JSON) ----------
const fresh = () => ({
  teams: Object.fromEntries(KEYS.map(k => [k, { name: 'TIM ' + k, players: [], total: 0 }])),
  locked: false,
  phase: 'lobby', // lobby | ready | question | steal | qend | matchend | champion
  cur: 0,
  matches: [
    { label: 'SEMIFINAL 1', t: ['A', 'B'], s: [0, 0], qs: 0, winner: null },
    { label: 'SEMIFINAL 2', t: ['C', 'D'], s: [0, 0], qs: 0, winner: null },
    { label: 'FINAL', t: [null, null], s: [0, 0], qs: 0, winner: null },
  ],
  q: null, qid: 0, strikes: 0, pot: 0, playing: null, stealer: null,
  timerEnd: 0, used: [], champion: null, tiktok: { state: 'idle', username: '' },
});
let S = fresh();
try { S = { ...fresh(), ...JSON.parse(fs.readFileSync(STATE_FILE, 'utf8')) }; } catch {}

let saveT;
const save = () => {
  clearTimeout(saveT);
  saveT = setTimeout(() => {
    try { fs.mkdirSync(DATA_DIR, { recursive: true }); fs.writeFileSync(STATE_FILE, JSON.stringify(S)); }
    catch (e) { console.error('[save]', e.message); }
  }, 300);
};

// jawaban yang belum dibuka TIDAK dikirim ke layar /live
const view = admin => ({
  ...S, used: undefined, now: Date.now(),
  q: S.q && { text: S.q.text, answers: S.q.answers.map(a => (admin || a.open ? { ...a } : { open: false })) },
});

// ---------- WEBSOCKET ----------
const app = express();
app.use(cors(), express.json(), express.static(path.join(__dirname, 'public'), { extensions: ['html'] }));
app.get('/health', (_, res) => res.send('ok'));
const server = http.createServer(app);
const wss = new WebSocketServer({ server, path: '/ws' });

const send = (ws, type, data) => ws.readyState === 1 && ws.send(JSON.stringify({ type, data }));
const push = () => { wss.clients.forEach(ws => send(ws, 'state', view(ws.admin))); save(); };
const emit = (kind, data = {}) => wss.clients.forEach(ws => send(ws, 'event', { kind, ...data }));

wss.on('connection', (ws, req) => {
  let key = ''; try { key = new URL(req.url, 'http://x').searchParams.get('key') || ''; } catch {}
  ws.admin = key === ADMIN_KEY;
  send(ws, 'state', view(ws.admin));
});

// ---------- LOGIKA GAME ----------
const norm = s => String(s || '').toLowerCase().normalize('NFD').replace(/[\u0300-\u036f]/g, '').replace(/[^a-z0-9]/g, '');
const teamOf = id => KEYS.find(k => S.teams[k].players.some(p => p.id === id));
const curM = () => S.matches[S.cur];
const other = (m, k) => (m.t[0] === k ? m.t[1] : m.t[0]);

let timeT;
function setTimer(sec) {
  clearTimeout(timeT);
  if (!sec) { S.timerEnd = 0; return; }
  S.timerEnd = Date.now() + sec * 1000;
  timeT = setTimeout(() => emit('timeup'), sec * 1000);
}

function join(u) {
  if (S.phase !== 'lobby') return emit('joinfail', { nick: u.nick, why: 'Pendaftaran sudah ditutup' });
  if (S.locked) return emit('joinfail', { nick: u.nick, why: 'Pendaftaran dikunci' });
  if (teamOf(u.id)) return emit('joinfail', { nick: u.nick, why: 'Kamu sudah bergabung' });
  const k = KEYS.find(k => S.teams[k].players.length < MAX_PER_TEAM);
  if (!k) return emit('joinfail', { nick: u.nick, why: 'Semua tim penuh' });
  S.teams[k].players.push({ id: u.id, nick: u.nick, avatar: u.avatar || '', pts: 0 });
  emit('join', { nick: u.nick, team: k });
  push();
}

function startQuestion() {
  const m = curM();
  let pool = QUESTIONS.map((_, i) => i).filter(i => !S.used.includes(i));
  if (!pool.length) { S.used = []; pool = QUESTIONS.map((_, i) => i); }
  const i = pool[Math.floor(Math.random() * pool.length)];
  S.used.push(i);
  const q = QUESTIONS[i];
  S.q = {
    text: q.question,
    answers: [...q.answers].sort((a, b) => b.score - a.score).map(a => ({ text: a.text, score: a.score, open: false, by: '' })),
  };
  S.qid++; S.playing = m.t[m.qs % 2]; S.stealer = null; m.qs++;
  S.strikes = 0; S.pot = 0; S.phase = 'question';
  setTimer(ANSWER_SEC); emit('start'); push();
}

function award(k) {
  const m = curM(), i = m.t.indexOf(k);
  if (k && S.pot && i >= 0) { m.s[i] += S.pot; S.teams[k].total += S.pot; }
  S.phase = 'qend'; setTimer(0);
  emit('award', { team: k, pot: S.pot }); push();
}

function reveal(i, by) {
  const a = S.q?.answers[i];
  if (!a || a.open || !['question', 'steal', 'qend'].includes(S.phase)) return;
  a.open = true; a.by = by?.nick || '';
  if (S.phase === 'qend') return push(); // buka sisa jawaban tanpa poin
  S.pot += a.score;
  const p = by && S.teams[teamOf(by.id)]?.players.find(p => p.id === by.id);
  if (p) p.pts += a.score;
  emit('correct', { i, score: a.score, by: a.by });
  if (S.phase === 'steal') { emit('stealok'); return award(S.stealer); }
  if (S.q.answers.every(x => x.open)) return award(S.playing);
  setTimer(ANSWER_SEC); push();
}

function strike() {
  if (S.phase === 'question') {
    S.strikes++; emit('strike', { n: S.strikes });
    if (S.strikes >= 3) { S.phase = 'steal'; S.stealer = other(curM(), S.playing); emit('steal'); }
    setTimer(ANSWER_SEC); push();
  } else if (S.phase === 'steal') {
    emit('stealfail'); award(S.playing);
  }
}

function finishMatch() {
  const m = curM();
  m.winner = m.s[0] > m.s[1] ? m.t[0] : m.t[1];
  if (S.cur < 2) S.matches[2].t[S.cur] = m.winner; else S.champion = m.winner;
  S.phase = 'matchend'; setTimer(0);
  emit('matchend', { winner: m.winner }); push();
}

function next() {
  const m = curM();
  if (S.phase === 'ready' || S.phase === 'qend') {
    if (m.qs >= QPM && m.s[0] !== m.s[1]) return finishMatch();
    return startQuestion();
  }
  if (S.phase === 'matchend') {
    if (S.cur >= 2) { S.phase = 'champion'; emit('champion'); return push(); }
    S.cur++; S.phase = 'ready'; S.q = null; push();
  }
}

// chat -> !join atau jawaban dari tim yang sedang main / steal
function handleChat(u, text) {
  text = String(text || '').trim();
  if (/^!join\b/i.test(text)) return join(u);
  const k = teamOf(u.id);
  if (!k || !S.q) return;
  const ok = (S.phase === 'question' && k === S.playing) || (S.phase === 'steal' && k === S.stealer);
  if (!ok) return;
  const n = norm(text);
  if (n.length < 2) return;
  const i = S.q.answers.findIndex(a => {
    const an = norm(a.text);
    return !a.open && (an === n || (an.length >= 3 && n.includes(an)));
  });
  if (i >= 0) reveal(i, u);
}

// ---------- ADMIN ----------
function admin(b) {
  switch (b.action) {
    case 'lock': S.locked = !!b.value; break;
    case 'kick': KEYS.forEach(k => { S.teams[k].players = S.teams[k].players.filter(p => p.id !== b.id); }); break;
    case 'move': {
      const from = teamOf(b.id), to = S.teams[b.to];
      if (!from || !to) return 'Pemain / tim tidak ditemukan';
      if (from === b.to) return;
      if (to.players.length >= MAX_PER_TEAM) return 'Tim tujuan sudah penuh';
      const p = S.teams[from].players.find(p => p.id === b.id);
      S.teams[from].players = S.teams[from].players.filter(x => x.id !== b.id);
      to.players.push(p); break;
    }
    case 'rename':
      if (!S.teams[b.team]) return 'Tim tidak ada';
      S.teams[b.team].name = String(b.name || '').trim().slice(0, 16) || 'TIM ' + b.team; break;
    case 'start':
      if (S.phase !== 'lobby') return 'Game sudah dimulai';
      if (KEYS.some(k => !S.teams[k].players.length)) return 'Setiap tim minimal punya 1 pemain';
      S.locked = true; S.phase = 'ready'; emit('start'); break;
    case 'next': return next();
    case 'reveal': return reveal(+b.i);
    case 'strike': return strike();
    case 'endq':
      if (S.phase === 'question') return award(S.playing);
      if (S.phase === 'steal') { emit('stealfail'); return award(S.playing); }
      return;
    case 'timer': setTimer(Math.max(0, +b.sec || 0)); break;
    case 'reset': S = fresh(); S.tiktok = { state: conn ? 'connected' : 'idle', username: wantUser }; setTimer(0); break;
    case 'connect': connect(b.username); return;
    case 'disconnect': disconnect(); return;
    case 'sim': return handleChat({ id: String(b.nick), nick: String(b.nick), avatar: '' }, b.text);
    default: return 'Aksi tidak dikenal';
  }
  push();
}

app.post('/api/admin', (req, res) => {
  if (req.get('x-admin-key') !== ADMIN_KEY) return res.status(401).json({ ok: false, error: 'Admin key salah' });
  const error = admin(req.body || {});
  res.json({ ok: !error, error });
});

// ---------- TIKTOK LIVE ----------
let conn = null, wantUser = '', retry;
const getUser = d => {
  const u = d.user || d;
  return {
    id: String(u.userId || u.uniqueId || u.displayId || u.nickname),
    nick: u.nickname || u.uniqueId || u.displayId || 'anon',
    avatar: u.avatarThumb?.urlList?.[0] || u.profilePicture?.urls?.[0] || u.profilePictureUrl || '',
  };
};
const setTT = p => { Object.assign(S.tiktok, p); push(); };

function disconnect() {
  clearTimeout(retry); wantUser = '';
  try { conn?.removeAllListeners(); conn?.disconnect(); } catch {}
  conn = null; setTT({ state: 'idle', username: '' });
}

async function connect(username) {
  username = String(username || '').replace(/^@/, '').trim();
  if (!username) return;
  try { conn?.removeAllListeners(); conn?.disconnect(); } catch {}
  clearTimeout(retry); wantUser = username;
  setTT({ state: 'connecting', username });
  conn = new TikTokLiveConnection(username, {
    processInitialData: false,
    signApiKey: process.env.SIGN_API_KEY || undefined,
    sessionId: process.env.SESSION_ID || undefined,
  });
  conn.on(WebcastEvent.CHAT, d => handleChat(getUser(d), d.comment ?? d.content ?? ''));
  conn.on(WebcastEvent.STREAM_END, () => setTT({ state: 'ended' }));
  conn.on('disconnected', () => {
    if (wantUser) { setTT({ state: 'reconnecting' }); retry = setTimeout(() => connect(wantUser), 5000); }
  });
  conn.on('error', e => console.error('[tiktok]', e?.info || e?.message || e));
  try {
    await conn.connect();
    setTT({ state: 'connected' });
  } catch (e) {
    console.error('[connect]', e?.message || e);
    setTT({ state: 'error' }); wantUser = '';
  }
}

server.listen(PORT, () => {
  console.log(`Live (OBS) : http://localhost:${PORT}/live\nAdmin      : http://localhost:${PORT}/admin`);
  if (process.env.TIKTOK_USERNAME) connect(process.env.TIKTOK_USERNAME);
});
