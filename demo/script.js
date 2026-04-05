/* ================================================================
   whalu demo — script.js
   Auto-loads data/detections.json + data/audio.mp3.
   Syncs an HTML5 audio player with a detection timeline canvas.
   ================================================================ */

// ── Species registry ───────────────────────────────────────────────
const SP = {
  Bm: { name: "Blue whale",        sci: "Balaenoptera musculus",       emoji: "🐋", color: "#38bdf8" },
  Bp: { name: "Fin whale",         sci: "Balaenoptera physalus",       emoji: "🐳", color: "#22d3ee" },
  Mn: { name: "Humpback whale",    sci: "Megaptera novaeangliae",      emoji: "🐳", color: "#a78bfa" },
  Ba: { name: "Minke whale",       sci: "Balaenoptera acutorostrata",  emoji: "",   color: "#34d399" },
  Bs: { name: "Sei whale",         sci: "Balaenoptera borealis",       emoji: "",   color: "#fbbf24" },
  Be: { name: "Bryde's whale",     sci: "Balaenoptera edeni",          emoji: "",   color: "#fb923c" },
  Eg: { name: "N. Atlantic right whale", sci: "Eubalaena glacialis",   emoji: "",   color: "#f87171" },
  Oo: { name: "Orca",              sci: "Orcinus orca",                emoji: "🐬", color: "#e879f9" },
  Upcall:      { name: "Right whale upcall",  sci: "", emoji: "📡", color: "#818cf8" },
  Gunshot:     { name: "Right whale gunshot", sci: "", emoji: "📡", color: "#c084fc" },
  Call:        { name: "Generic call",        sci: "", emoji: "",   color: "#94a3b8" },
  Echolocation:{ name: "Echolocation",        sci: "", emoji: "",   color: "#64748b" },
  Whistle:     { name: "Whistle",             sci: "", emoji: "",   color: "#475569" },
};

function spMeta(code) {
  return SP[code] || { name: code, sci: "", emoji: "·", color: "#6b7280" };
}

// ── State ──────────────────────────────────────────────────────────
const state = {
  dets:        [],
  duration:    200,
  source:      "",
  currentTime: 0,
  playing:     false,
  audioReady:  false,
  simInterval: null,
};

const WINDOW = 10; // seconds either side shown in active cards

// ── DOM refs ───────────────────────────────────────────────────────
const $ = id => document.getElementById(id);
const audioEl    = $("audio-el");
const playBtn    = $("play-btn");
const iconPlay   = $("icon-play");
const iconPause  = $("icon-pause");
const timeCur    = $("time-cur");
const timeTot    = $("time-tot");
const nextBtn    = $("next-btn");
const scrFill    = $("scrubber-fill");
const scrCursor  = $("scrubber-cursor");
const miniCanvas = $("mini-canvas");
const tlCanvas   = $("tl-canvas");
const tlCursor   = $("tl-cursor");
const tlWrap     = $("tl-wrap");
const tlTooltip  = $("tl-tooltip");
const legend     = $("legend");
const activeCards= $("active-cards");
const emptyMsg   = $("empty-msg");
const statsList  = $("stats-list");
const audioHint  = $("audio-hint");

// ── Init ───────────────────────────────────────────────────────────
async function init() {
  playBtn.addEventListener("click", togglePlay);
  nextBtn.addEventListener("click", jumpToNextDet);

  // Load detections
  let data;
  try {
    const r = await fetch("data/detections.json");
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    const json = await r.json();
    data = Array.isArray(json)
      ? { dets: json, duration: 3600, source: "detections.json", isSample: false }
      : json;
  } catch (err) {
    $("loading").innerHTML = `
      <div class="loading-inner">
        <div class="loading-wave">⚠️</div>
        <p>Could not load detection data.<br>
        Serve this directory over HTTP:<br>
        <code>python -m http.server 8000</code></p>
      </div>`;
    return;
  }

  // Load audio
  await loadAudio();

  $("loading").classList.add("hidden");
  $("vis").classList.remove("hidden");

  requestAnimationFrame(() => loadData(data));
}

async function loadAudio() {
  for (const name of ["audio.mp3", "audio.wav"]) {
    try {
      const r = await fetch(`data/${name}`, { method: "HEAD" });
      if (r.ok) {
        audioEl.src = `data/${name}`;
        audioEl.load();
        audioEl.addEventListener("timeupdate", () => {
          state.currentTime = audioEl.currentTime;
          updatePosition();
          updateActiveCards();
        });
        audioEl.addEventListener("ended", () => setPlaying(false));
        state.audioReady = true;
        break;
      }
    } catch (_) { /* try next */ }
  }
}

function loadData(data) {
  state.source = data.source || "";

  // If the JSON describes a clip window, trim detections to that range
  // and rescale timestamps to 0-based so the timeline matches the audio exactly.
  const offset  = data.audioOffset   || 0;
  const clipDur = data.audioDuration || data.duration || 3600;
  const allDets = data.dets || [];

  state.dets = allDets
    .filter(d => d.t >= offset && d.t < offset + clipDur)
    .map(d => ({ ...d, t: Math.round((d.t - offset) * 10) / 10 }));
  state.duration = clipDur;

  // Human-readable source name
  const sourceLabel = state.source
    .replace(/^mbari_\d{4}_\d{2}_/, "")
    .replace(/_lim[\d.]+h$/, "");

  $("m-source").textContent = sourceLabel;
  $("m-count").textContent  = `${state.dets.length} detections`;
  $("time-tot").textContent = fmtTime(state.duration);

  if (state.audioReady) {
    $("m-mode").textContent = "live audio";
  } else {
    $("m-mode").textContent = "detections only";
  }

  buildLegend();
  drawTimeline();
  drawMini();
  buildStats();
  updateActiveCards();
  updatePosition();
  setupScrubber();
  setupTimelineInteraction();
}

// ── Playback ───────────────────────────────────────────────────────
function togglePlay() {
  if (state.audioReady) {
    if (audioEl.paused) { audioEl.play(); setPlaying(true); }
    else                { audioEl.pause(); setPlaying(false); }
  } else {
    if (state.playing) { stopSim(); setPlaying(false); }
    else               { startSim(); setPlaying(true); }
  }
}

function setPlaying(yes) {
  state.playing = yes;
  playBtn.classList.toggle("playing", yes);
  iconPlay.classList.toggle("hidden", yes);
  iconPause.classList.toggle("hidden", !yes);
}

function startSim() {
  if (state.simInterval) clearInterval(state.simInterval);
  const tick = 250;
  state.simInterval = setInterval(() => {
    state.currentTime = Math.min(state.currentTime + tick / 1000, state.duration);
    if (state.currentTime >= state.duration) { stopSim(); setPlaying(false); return; }
    updatePosition();
    updateActiveCards();
  }, tick);
}

function stopSim() {
  if (state.simInterval) { clearInterval(state.simInterval); state.simInterval = null; }
}

function jumpToNextDet() {
  const next = state.dets.find(d => d.t > state.currentTime + 0.1);
  if (!next) return;
  seekTo(Math.max(0, next.t - 5));
}

function seekTo(t) {
  state.currentTime = Math.max(0, Math.min(t, state.duration));
  if (state.audioReady) audioEl.currentTime = state.currentTime;
  updatePosition();
  updateActiveCards();
}

function updatePosition() {
  const pct = state.duration ? state.currentTime / state.duration : 0;
  timeCur.textContent      = fmtTime(state.currentTime);
  scrFill.style.width      = `${pct * 100}%`;
  scrCursor.style.left     = `${pct * 100}%`;
  tlCursor.style.left      = `${pct * 100}%`;
}

// ── Scrubber interaction ───────────────────────────────────────────
function setupScrubber() {
  const bg = $("scrubber-bg");
  function onScrub(e) {
    const rect = bg.getBoundingClientRect();
    const pct  = Math.max(0, Math.min(1, (e.clientX - rect.left) / rect.width));
    seekTo(pct * state.duration);
  }
  let dragging = false;
  bg.addEventListener("mousedown", e => { dragging = true; onScrub(e); });
  document.addEventListener("mousemove", e => { if (dragging) onScrub(e); });
  document.addEventListener("mouseup",   () => { dragging = false; });
}

// ── Timeline canvas ────────────────────────────────────────────────
function drawTimeline() {
  const dpr = window.devicePixelRatio || 1;
  const W   = tlWrap.offsetWidth || 800;
  const H   = 96;
  tlCanvas.width        = W * dpr;
  tlCanvas.height       = H * dpr;
  tlCanvas.style.width  = W + "px";
  tlCanvas.style.height = H + "px";

  const ctx = tlCanvas.getContext("2d");
  ctx.scale(dpr, dpr);

  ctx.fillStyle = "#0c1a2e";
  ctx.fillRect(0, 0, W, H);

  // Dynamic time grid — adapt tick interval to recording duration
  const dur = state.duration;
  const tickIntervals = [10, 15, 20, 30, 60, 120, 300];
  const tickInterval = tickIntervals.find(t => dur / t <= 20) || 300;
  const labelEvery   = tickInterval * (dur <= 120 ? 2 : dur <= 600 ? 2 : 3);

  ctx.strokeStyle = "rgba(0,180,255,0.05)";
  ctx.lineWidth = 1;
  for (let t = 0; t <= dur; t += tickInterval) {
    const x = (t / dur) * W;
    ctx.beginPath(); ctx.moveTo(x, 0); ctx.lineTo(x, H); ctx.stroke();
    if (t > 0 && t % labelEvery === 0) {
      ctx.fillStyle = "rgba(100,170,220,0.25)";
      ctx.font = `10px 'Space Mono', monospace`;
      ctx.fillText(fmtTime(t), x + 3, H - 4);
    }
  }

  if (!state.dets.length) return;

  const TRACK_H = H - 8;
  state.dets.forEach(d => {
    const meta = spMeta(d.sp);
    const x    = (d.t / state.duration) * W;
    const barH = Math.max(4, d.c * TRACK_H * 0.88);
    const y    = H - barH;

    ctx.globalAlpha = 0.55;
    ctx.strokeStyle = meta.color;
    ctx.lineWidth   = 1.5;
    ctx.beginPath(); ctx.moveTo(x, H); ctx.lineTo(x, y + 3); ctx.stroke();

    ctx.globalAlpha = 0.85;
    ctx.fillStyle   = meta.color;
    ctx.beginPath(); ctx.arc(x, y, 2.5, 0, Math.PI * 2); ctx.fill();
  });

  ctx.globalAlpha = 1;
}

function drawMini() {
  const dpr = window.devicePixelRatio || 1;
  const W   = ($("scrubber-bg").offsetWidth || 600);
  const H   = 40;
  miniCanvas.width        = W * dpr;
  miniCanvas.height       = H * dpr;
  miniCanvas.style.width  = W + "px";
  miniCanvas.style.height = H + "px";

  const ctx = miniCanvas.getContext("2d");
  ctx.scale(dpr, dpr);

  if (!state.dets.length) return;

  state.dets.forEach(d => {
    const meta = spMeta(d.sp);
    const x    = (d.t / state.duration) * W;
    const barH = Math.max(2, d.c * (H - 4) * 0.9);
    ctx.globalAlpha = 0.5;
    ctx.fillStyle   = meta.color;
    ctx.fillRect(x - 0.75, H - barH, 1.5, barH);
  });
  ctx.globalAlpha = 1;
}

let resizeTimer;
window.addEventListener("resize", () => {
  clearTimeout(resizeTimer);
  resizeTimer = setTimeout(() => {
    if (state.dets.length) { drawTimeline(); drawMini(); }
  }, 120);
});

// ── Timeline hover & click ─────────────────────────────────────────
function setupTimelineInteraction() {
  tlWrap.addEventListener("mousemove", e => {
    const rect = tlWrap.getBoundingClientRect();
    const pct  = (e.clientX - rect.left) / rect.width;
    const t    = pct * state.duration;
    const nearest = state.dets.reduce((best, d) => {
      const dist = Math.abs(d.t - t);
      return dist < (best ? Math.abs(best.t - t) : Infinity) ? d : best;
    }, null);

    if (nearest && Math.abs(nearest.t - t) < (state.duration / tlWrap.offsetWidth) * 20) {
      const meta = spMeta(nearest.sp);
      tlTooltip.innerHTML = `
        <div class="tl-tooltip-sp" style="color:${meta.color}">${meta.emoji} ${meta.name}</div>
        <div class="tl-tooltip-detail">${fmtTime(nearest.t)} · ${Math.round(nearest.c * 100)}% conf</div>`;
      const tipW = 160;
      let left   = e.clientX - rect.left - tipW / 2;
      left = Math.max(4, Math.min(rect.width - tipW - 4, left));
      tlTooltip.style.left = left + "px";
      tlTooltip.style.top  = "6px";
      tlTooltip.classList.add("visible");
    } else {
      tlTooltip.classList.remove("visible");
    }
  });

  tlWrap.addEventListener("mouseleave", () => tlTooltip.classList.remove("visible"));

  tlWrap.addEventListener("click", e => {
    const rect = tlWrap.getBoundingClientRect();
    const pct  = (e.clientX - rect.left) / rect.width;
    seekTo(pct * state.duration);
  });
}

// ── Active detection cards ─────────────────────────────────────────
let prevActiveSet = new Set();

function updateActiveCards() {
  const t   = state.currentTime;
  const win = state.dets.filter(d => Math.abs(d.t - t) <= WINDOW);

  const bySpecies = new Map();
  win.forEach(d => {
    const cur = bySpecies.get(d.sp);
    if (!cur || d.c > cur.c) bySpecies.set(d.sp, d);
  });

  const activeSet = new Set(bySpecies.keys());
  const changed =
    activeSet.size !== prevActiveSet.size ||
    [...activeSet].some(k => !prevActiveSet.has(k)) ||
    [...prevActiveSet].some(k => !activeSet.has(k));

  if (!changed) {
    bySpecies.forEach((d, sp) => {
      const card = activeCards.querySelector(`[data-sp="${sp}"]`);
      if (card) {
        card.querySelector(".conf-fill").style.width  = `${d.c * 100}%`;
        card.querySelector(".conf-pct").textContent   = `${Math.round(d.c * 100)}%`;
        card.querySelector(".card-time").textContent  = fmtTime(d.t);
      }
    });
    return;
  }

  prevActiveSet = activeSet;
  activeCards.innerHTML = "";

  if (!bySpecies.size) {
    activeCards.appendChild(emptyMsg);
    emptyMsg.style.display = "";
    return;
  }

  const sorted = [...bySpecies.entries()].sort((a, b) => b[1].c - a[1].c);

  sorted.forEach(([sp, d]) => {
    const meta = spMeta(sp);
    const card = document.createElement("div");
    card.className = "det-card active";
    card.dataset.sp = sp;
    card.style.cssText = `--sp-color:${meta.color}`;

    card.innerHTML = `
      <div class="card-top">
        <span class="card-emoji">${meta.emoji || "🔊"}</span>
        <div class="card-names">
          <div class="card-common">${meta.name}</div>
          <div class="card-sci">${meta.sci || sp}</div>
        </div>
        <span class="card-code" style="color:${meta.color};background:${meta.color}18">${sp}</span>
      </div>
      <div class="conf-row">
        <div class="conf-track">
          <div class="conf-fill" style="width:${d.c * 100}%;background:${meta.color}"></div>
        </div>
        <span class="conf-pct">${Math.round(d.c * 100)}%</span>
      </div>
      <div class="card-time">detected at ${fmtTime(d.t)}</div>`;

    const ring = document.createElement("div");
    ring.className = "pulse-ring";
    ring.style.background = meta.color;
    card.appendChild(ring);

    activeCards.appendChild(card);
  });
}

// ── Legend ─────────────────────────────────────────────────────────
function buildLegend() {
  const present = new Set(state.dets.map(d => d.sp));
  legend.innerHTML = "";
  [...present].forEach(sp => {
    const meta = spMeta(sp);
    const item = document.createElement("div");
    item.className = "legend-item";
    item.innerHTML = `<span class="legend-dot" style="background:${meta.color}"></span>${meta.name}`;
    legend.appendChild(item);
  });
}

// ── Stats ──────────────────────────────────────────────────────────
function buildStats() {
  const totals = new Map();
  state.dets.forEach(d => {
    const cur = totals.get(d.sp) || { count: 0, maxConf: 0 };
    cur.count++;
    cur.maxConf = Math.max(cur.maxConf, d.c);
    totals.set(d.sp, cur);
  });

  const sorted    = [...totals.entries()].sort((a, b) => b[1].count - a[1].count);
  const maxCount  = sorted[0]?.[1].count || 1;

  statsList.innerHTML = "";
  sorted.forEach(([sp, { count, maxConf }]) => {
    const meta = spMeta(sp);
    const mins = (count * 2.5 / 60).toFixed(1);
    const pct  = (count / maxCount) * 100;
    const row  = document.createElement("div");
    row.className = "stat-row";
    row.innerHTML = `
      <div class="stat-name">
        <span class="stat-dot" style="background:${meta.color}"></span>
        ${meta.emoji ? meta.emoji + " " : ""}${meta.name}
      </div>
      <div class="stat-bar-track">
        <div class="stat-bar-fill" style="width:${pct}%;background:${meta.color}"></div>
      </div>
      <div class="stat-num">${count} windows</div>
      <div class="stat-min">${mins} min</div>`;
    statsList.appendChild(row);
  });
}

// ── Helpers ────────────────────────────────────────────────────────
function fmtTime(s) {
  const h   = Math.floor(s / 3600);
  const m   = Math.floor((s % 3600) / 60);
  const sec = Math.floor(s % 60);
  if (h > 0) return `${h}:${String(m).padStart(2, "0")}:${String(sec).padStart(2, "0")}`;
  return `${String(m).padStart(2, "0")}:${String(sec).padStart(2, "0")}`;
}

// ── Kick off ───────────────────────────────────────────────────────
document.addEventListener("DOMContentLoaded", init);
