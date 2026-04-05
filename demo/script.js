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
// ── Timeline layout constants ──────────────────────────────────────
const TL_LABEL_W = 44;   // px reserved for species code labels on the left
const TL_TRACK_H = 18;   // px height of each species row
const TL_TRACK_G = 5;    // px gap between rows
const TL_AXIS_H  = 22;   // px for the time axis at the bottom
const TL_PAD_TOP = 6;    // px top padding
const WIN_S      = 5.0;  // model detection window size in seconds

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
  const pct  = state.duration ? state.currentTime / state.duration : 0;
  const canW = tlCanvas.offsetWidth || tlWrap.offsetWidth || 800;
  timeCur.textContent  = fmtTime(state.currentTime);
  scrFill.style.width  = `${pct * 100}%`;
  scrCursor.style.left = `${pct * 100}%`;
  // Cursor lives inside the plot area (right of label margin)
  tlCursor.style.left  = `${TL_LABEL_W + pct * (canW - TL_LABEL_W)}px`;
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
  const dpr     = window.devicePixelRatio || 1;
  const W       = tlWrap.offsetWidth || 800;
  const species = [...new Set(state.dets.map(d => d.sp))].sort();
  const nSp     = Math.max(species.length, 1);
  const H       = TL_PAD_TOP + nSp * (TL_TRACK_H + TL_TRACK_G) + TL_AXIS_H;
  const plotW   = W - TL_LABEL_W;
  const dur     = state.duration;

  tlCanvas.width        = W * dpr;
  tlCanvas.height       = H * dpr;
  tlCanvas.style.width  = W + "px";
  tlCanvas.style.height = H + "px";

  const ctx = tlCanvas.getContext("2d");
  ctx.scale(dpr, dpr);

  ctx.fillStyle = "#0c1a2e";
  ctx.fillRect(0, 0, W, H);

  // Time grid
  const tickIntervals = [10, 15, 20, 30, 60, 120, 300];
  const tickInterval  = tickIntervals.find(t => dur / t <= 20) || 300;
  const labelEvery    = tickInterval * (dur <= 120 ? 2 : 3);

  for (let t = 0; t <= dur; t += tickInterval) {
    const x = TL_LABEL_W + (t / dur) * plotW;
    ctx.strokeStyle = "rgba(0,180,255,0.07)";
    ctx.lineWidth   = 1;
    ctx.beginPath(); ctx.moveTo(x, TL_PAD_TOP); ctx.lineTo(x, H - TL_AXIS_H); ctx.stroke();
    if (t % labelEvery === 0) {
      ctx.fillStyle  = "rgba(100,170,220,0.35)";
      ctx.font       = `10px 'Space Mono', monospace`;
      ctx.textAlign  = "center";
      ctx.fillText(fmtTime(t), x, H - 5);
    }
  }

  if (!species.length) return;

  // Swim lanes — one row per species
  species.forEach((sp, i) => {
    const meta   = spMeta(sp);
    const trackY = TL_PAD_TOP + i * (TL_TRACK_H + TL_TRACK_G);

    // Species code label
    ctx.textAlign   = "right";
    ctx.font        = `11px 'Space Mono', monospace`;
    ctx.fillStyle   = meta.color;
    ctx.globalAlpha = 0.75;
    ctx.fillText(sp, TL_LABEL_W - 6, trackY + TL_TRACK_H * 0.72);

    // Row background
    ctx.globalAlpha = 1;
    ctx.fillStyle   = "rgba(0,180,255,0.025)";
    ctx.fillRect(TL_LABEL_W, trackY, plotW, TL_TRACK_H);

    // Detection windows — rectangles spanning the 5s window
    state.dets.filter(d => d.sp === sp).forEach(d => {
      const x1    = TL_LABEL_W + (d.t / dur) * plotW;
      const x2    = TL_LABEL_W + (Math.min(d.t + WIN_S, dur) / dur) * plotW;
      const rectW = Math.max(2, x2 - x1);

      // Body fill — opacity encodes confidence
      ctx.globalAlpha = 0.25 + d.c * 0.55;
      ctx.fillStyle   = meta.color;
      ctx.fillRect(x1, trackY + 2, rectW, TL_TRACK_H - 4);

      // Top edge accent
      ctx.globalAlpha = 0.6 + d.c * 0.4;
      ctx.fillRect(x1, trackY + 2, rectW, 2);
    });

    ctx.globalAlpha = 1;
  });
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
  function xToTime(clientX) {
    const rect  = tlWrap.getBoundingClientRect();
    const plotW = rect.width - TL_LABEL_W;
    const px    = clientX - rect.left - TL_LABEL_W;
    return Math.max(0, Math.min(state.duration, (px / plotW) * state.duration));
  }

  tlWrap.addEventListener("mousemove", e => {
    const t = xToTime(e.clientX);
    // Find detection whose window contains t, favouring highest confidence
    const hit = state.dets
      .filter(d => t >= d.t && t < d.t + WIN_S)
      .sort((a, b) => b.c - a.c)[0];

    if (hit) {
      const meta = spMeta(hit.sp);
      const rect = tlWrap.getBoundingClientRect();
      tlTooltip.innerHTML = `
        <div class="tl-tooltip-sp" style="color:${meta.color}">${meta.emoji || ""} ${meta.name}</div>
        <div class="tl-tooltip-detail">${fmtTime(hit.t)}-${fmtTime(hit.t + WIN_S)} · ${Math.round(hit.c * 100)}% conf</div>`;
      const tipW = 170;
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

  tlWrap.addEventListener("click", e => seekTo(xToTime(e.clientX)));
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
