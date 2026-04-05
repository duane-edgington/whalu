/* ================================================================
   whalu demo — script.js
   Loads detections.json (real) or generates sample data.
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

// ── Seeded PRNG (Mulberry32) ───────────────────────────────────────
function mulberry32(seed) {
  return () => {
    seed |= 0; seed = (seed + 0x6D2B79F5) | 0;
    let t = Math.imul(seed ^ (seed >>> 15), 1 | seed);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

// ── Sample data generator ──────────────────────────────────────────
function generateSampleData() {
  const rand = mulberry32(42);
  const dets = [];

  function bout(sp, start, end, interval, lo, hi) {
    let t = start;
    while (t < end) {
      const c = lo + rand() * (hi - lo);
      dets.push({ t: Math.round(t * 10) / 10, sp, c: Math.round(c * 100) / 100 });
      t += interval * (0.7 + rand() * 0.6);
    }
  }

  // Fin whale — three calling bouts (20 Hz pulses)
  bout("Bp", 0,    680,  7.2, 0.56, 0.91);
  bout("Bp", 1480, 1920, 8.8, 0.54, 0.87);
  bout("Bp", 2760, 3320, 7.8, 0.55, 0.90);

  // Humpback — two singing bouts
  bout("Mn", 420,  940,  5.4, 0.61, 0.95);
  bout("Mn", 2180, 2700, 6.2, 0.59, 0.93);

  // Blue whale — D-calls, sparse mid-recording
  bout("Bm", 1020, 1560, 13.5, 0.52, 0.84);
  // Occasional blue whale call at other times
  [180, 340, 2900, 3150, 3380].forEach(t => {
    dets.push({ t, sp: "Bm", c: +(0.52 + rand() * 0.20).toFixed(2) });
  });

  // Right whale upcalls (rare — exciting when they appear)
  bout("Upcall", 520, 760, 22, 0.51, 0.74);
  bout("Upcall", 2260, 2420, 26, 0.50, 0.70);

  // Occasional orca pass
  bout("Oo", 1700, 1820, 9, 0.58, 0.82);

  dets.sort((a, b) => a.t - b.t);
  return { dets, duration: 3600, source: "MARS-20260301T000000Z-16kHz (sample)", isSample: true };
}

// ── State ──────────────────────────────────────────────────────────
let state = {
  dets: [],
  duration: 3600,
  source: "",
  isSample: false,
  currentTime: 0,
  playing: false,
  simInterval: null,
  audioReady: false,
};

const WINDOW = 10; // seconds either side of current time

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

// ── Init ───────────────────────────────────────────────────────────
async function init() {
  $("audio-input").addEventListener("change", onAudioFile);
  $("demo-btn").addEventListener("click", startDemo);
  playBtn.addEventListener("click", togglePlay);
  nextBtn.addEventListener("click", jumpToNextDet);

  // Try loading real detections.json (works when served over HTTP)
  try {
    const r = await fetch("data/detections.json");
    if (r.ok) {
      const json = await r.json();
      const data = Array.isArray(json) ? { dets: json, duration: 3600, source: "detections.json", isSample: false } : json;
      loadData(data);
      $("load-panel").classList.add("hidden");
      $("vis").classList.remove("hidden");
    }
  } catch (_) { /* no file — wait for user action */ }
}

function onAudioFile(e) {
  const file = e.target.files[0];
  if (!file) return;
  audioEl.src = URL.createObjectURL(file);
  audioEl.load();
  state.audioReady = true;

  audioEl.addEventListener("loadedmetadata", () => {
    state.duration = audioEl.duration || 3600;
    $("time-tot").textContent = fmtTime(state.duration);
    if (!state.dets.length) {
      const sample = generateSampleData();
      sample.source = file.name;
      loadData(sample);
    }
    $("m-mode").textContent = "live audio";
    drawTimeline();
    drawMini();
  }, { once: true });

  audioEl.addEventListener("timeupdate", () => {
    state.currentTime = audioEl.currentTime;
    updatePosition();
    updateActiveCards();
  });

  audioEl.addEventListener("ended", () => setPlaying(false));

  $("load-panel").classList.add("hidden");
  $("vis").classList.remove("hidden");
}

function startDemo() {
  $("load-panel").classList.add("hidden");
  $("vis").classList.remove("hidden");
  $("m-mode").textContent = "simulated playback";
  // defer so the vis is laid out (offsetWidth correct) before drawing
  requestAnimationFrame(() => {
    const data = generateSampleData();
    loadData(data);
  });
}

function loadData(data) {
  state.dets     = data.dets || [];
  state.duration = data.duration || 3600;
  state.source   = data.source || "";
  state.isSample = data.isSample || false;

  $("m-source").textContent = state.source;
  $("m-count").textContent  = `${state.dets.length} detections`;
  $("time-tot").textContent = fmtTime(state.duration);

  buildLegend();
  drawTimeline();
  drawMini();
  buildStats();
  updateActiveCards();
  setupScrubber();
}

// ── Playback ───────────────────────────────────────────────────────
function togglePlay() {
  if (state.audioReady) {
    if (audioEl.paused) { audioEl.play(); setPlaying(true); }
    else                { audioEl.pause(); setPlaying(false); }
  } else {
    // Simulated playback
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
  const tick = 250; // ms
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
  const t = state.currentTime;
  const next = state.dets.find(d => d.t > t + 0.1);
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
  timeCur.textContent = fmtTime(state.currentTime);
  scrFill.style.left   = "0";
  scrFill.style.width  = `${pct * 100}%`;
  scrCursor.style.left = `${pct * 100}%`;
  tlCursor.style.left  = `${pct * 100}%`;
}

// ── Scrubber interaction ───────────────────────────────────────────
function setupScrubber() {
  const bg = $("scrubber-bg");
  function onScrub(e) {
    const rect = bg.getBoundingClientRect();
    const pct = Math.max(0, Math.min(1, (e.clientX - rect.left) / rect.width));
    seekTo(pct * state.duration);
  }
  let dragging = false;
  bg.addEventListener("mousedown", e => { dragging = true; onScrub(e); });
  document.addEventListener("mousemove", e => { if (dragging) onScrub(e); });
  document.addEventListener("mouseup", () => { dragging = false; });
}

// ── Timeline canvas ────────────────────────────────────────────────
function drawTimeline() {
  const canvas = tlCanvas;
  const dpr = window.devicePixelRatio || 1;
  const W = tlWrap.offsetWidth || 800;
  const H = 96;
  canvas.width  = W * dpr;
  canvas.height = H * dpr;
  canvas.style.width  = W + "px";
  canvas.style.height = H + "px";

  const ctx = canvas.getContext("2d");
  ctx.scale(dpr, dpr);

  // background
  ctx.fillStyle = "#0c1a2e";
  ctx.fillRect(0, 0, W, H);

  // subtle hour grid lines
  ctx.strokeStyle = "rgba(0,180,255,0.05)";
  ctx.lineWidth = 1;
  for (let m = 0; m <= 60; m += 5) {
    const x = (m / 60) * W;
    ctx.beginPath();
    ctx.moveTo(x, 0); ctx.lineTo(x, H);
    ctx.stroke();
    if (m % 15 === 0 && m > 0) {
      ctx.fillStyle = "rgba(100,170,220,0.25)";
      ctx.font = `10px 'Space Mono', monospace`;
      ctx.fillText(m + "m", x + 3, H - 4);
    }
  }

  if (!state.dets.length) return;

  // draw detections as lollipops
  const TRACK_H = H - 8;
  state.dets.forEach(d => {
    const meta = spMeta(d.sp);
    const x = (d.t / state.duration) * W;
    const barH = Math.max(4, d.c * TRACK_H * 0.88);
    const y = H - barH;

    // stem
    ctx.globalAlpha = 0.55;
    ctx.strokeStyle = meta.color;
    ctx.lineWidth = 1.5;
    ctx.beginPath();
    ctx.moveTo(x, H); ctx.lineTo(x, y + 3);
    ctx.stroke();

    // dot
    ctx.globalAlpha = 0.85;
    ctx.fillStyle = meta.color;
    ctx.beginPath();
    ctx.arc(x, y, 2.5, 0, Math.PI * 2);
    ctx.fill();
  });

  ctx.globalAlpha = 1;
}


function drawMini() {
  const canvas = miniCanvas;
  const dpr = window.devicePixelRatio || 1;
  const W = ($("scrubber-bg").offsetWidth || 600);
  const H = 40;
  canvas.width  = W * dpr;
  canvas.height = H * dpr;
  canvas.style.width  = W + "px";
  canvas.style.height = H + "px";

  const ctx = canvas.getContext("2d");
  ctx.scale(dpr, dpr);

  if (!state.dets.length) return;

  state.dets.forEach(d => {
    const meta = spMeta(d.sp);
    const x = (d.t / state.duration) * W;
    const barH = Math.max(2, d.c * (H - 4) * 0.9);
    ctx.globalAlpha = 0.5;
    ctx.fillStyle = meta.color;
    ctx.fillRect(x - 0.75, H - barH, 1.5, barH);
  });
  ctx.globalAlpha = 1;
}

// Redraw canvases (debounced) on resize
let resizeTimer;
window.addEventListener("resize", () => {
  clearTimeout(resizeTimer);
  resizeTimer = setTimeout(() => {
    if (state.dets.length) { drawTimeline(); drawMini(); }
  }, 120);
});

// ── Timeline hover & click ─────────────────────────────────────────
tlWrap.addEventListener("mousemove", e => {
  const rect = tlWrap.getBoundingClientRect();
  const pct  = (e.clientX - rect.left) / rect.width;
  const t    = pct * state.duration;
  // find nearest detection within 5s
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
    let left = e.clientX - rect.left - tipW / 2;
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

// ── Active detection cards ─────────────────────────────────────────
let prevActiveSet = new Set();

function updateActiveCards() {
  const t   = state.currentTime;
  const win = state.dets.filter(d => Math.abs(d.t - t) <= WINDOW);

  // aggregate: highest confidence per species in window
  const bySpecies = new Map();
  win.forEach(d => {
    const cur = bySpecies.get(d.sp);
    if (!cur || d.c > cur.c) bySpecies.set(d.sp, d);
  });

  const activeSet = new Set(bySpecies.keys());

  // check if anything changed
  const changed =
    activeSet.size !== prevActiveSet.size ||
    [...activeSet].some(k => !prevActiveSet.has(k)) ||
    [...prevActiveSet].some(k => !activeSet.has(k));

  if (!changed) {
    // update confidence bars only
    bySpecies.forEach((d, sp) => {
      const card = activeCards.querySelector(`[data-sp="${sp}"]`);
      if (card) {
        card.querySelector(".conf-fill").style.width = `${d.c * 100}%`;
        card.querySelector(".conf-pct").textContent  = `${Math.round(d.c * 100)}%`;
        card.querySelector(".card-time").textContent = fmtTime(d.t);
      }
    });
    return;
  }

  prevActiveSet = activeSet;

  // full re-render of active cards
  activeCards.innerHTML = "";

  if (!bySpecies.size) {
    activeCards.appendChild(emptyMsg);
    emptyMsg.style.display = "";
    return;
  }

  // sort by confidence desc
  const sorted = [...bySpecies.entries()].sort((a, b) => b[1].c - a[1].c);

  sorted.forEach(([sp, d]) => {
    const meta = spMeta(sp);
    const wasNew = !prevActiveSet.has(sp);
    const card = document.createElement("div");
    card.className = "det-card active";
    card.dataset.sp = sp;
    card.style.setProperty("--sp-color", meta.color);

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

    // colour top border
    card.style.setProperty("--sp-color", meta.color);
    card.style.cssText += `--sp-color:${meta.color}`;
    const before = document.createElement("style");
    card.appendChild(before);

    // pulse ring for newly appearing detection
    if (wasNew) {
      const ring = document.createElement("div");
      ring.className = "pulse-ring";
      ring.style.background = meta.color;
      card.appendChild(ring);
    }

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

  const sorted = [...totals.entries()].sort((a, b) => b[1].count - a[1].count);
  const maxCount = sorted[0]?.[1].count || 1;

  statsList.innerHTML = "";
  sorted.forEach(([sp, { count, maxConf }]) => {
    const meta  = spMeta(sp);
    const mins  = (count * 2.5 / 60).toFixed(1);
    const pct   = (count / maxCount) * 100;
    const row = document.createElement("div");
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
  const h = Math.floor(s / 3600);
  const m = Math.floor((s % 3600) / 60);
  const sec = Math.floor(s % 60);
  if (h > 0) return `${h}:${String(m).padStart(2, "0")}:${String(sec).padStart(2, "0")}`;
  return `${String(m).padStart(2, "0")}:${String(sec).padStart(2, "0")}`;
}

// ── Kick off ───────────────────────────────────────────────────────
document.addEventListener("DOMContentLoaded", init);
