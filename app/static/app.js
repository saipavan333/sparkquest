/* SparkQuest frontend — vanilla JS, no build step. */
(() => {
  "use strict";

  const API = {
    tracks: () => fetch("/api/tracks").then(r => r.json()),
    challenge: (id) => fetch(`/api/challenge/${id}`).then(r => r.json()),
    solution: (id) => fetch(`/api/challenge/${id}/solution`).then(r => r.json()),
    run: (body) => post("/api/run", body),
    submit: (body) => post("/api/submit", body),
    tutor: (body) => post("/api/tutor", body),
    progress: (uid) => fetch(`/api/progress/${encodeURIComponent(uid)}`).then(r => r.json()),
    leaderboard: () => fetch("/api/leaderboard").then(r => r.json()),
    badges: () => fetch("/api/badges").then(r => r.json()),
  };
  function post(url, body) {
    return fetch(url, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(body) })
      .then(r => r.json());
  }

  // ---- State / persistence ----
  const LS = window.localStorage;
  let userId = LS.getItem("sq_uid");
  if (!userId) { userId = "u_" + Math.random().toString(36).slice(2, 10); LS.setItem("sq_uid", userId); }
  const state = { tracks: [], current: null, lastError: "", badgeDefs: [], solved: new Set() };

  const $ = (id) => document.getElementById(id);
  let editor = null;

  // ---- Monaco ----
  require.config({ paths: { vs: "https://cdn.jsdelivr.net/npm/monaco-editor@0.45.0/min/vs" } });
  require(["vs/editor/editor.main"], () => {
    editor = monaco.editor.create($("editor"), {
      value: "# Loading…", language: "python", theme: "vs-dark",
      fontSize: 14, minimap: { enabled: false }, automaticLayout: true,
      scrollBeyondLastLine: false, tabSize: 4,
    });
    boot();
  });

  // ---- Boot ----
  async function boot() {
    const name = LS.getItem("sq_name");
    if (name) $("display-name").value = name;
    $("display-name").addEventListener("change", (e) => LS.setItem("sq_name", e.target.value.trim()));
    try {
      const [{ tracks }, { badges }] = await Promise.all([API.tracks(), API.badges()]);
      state.tracks = tracks; state.badgeDefs = badges;
      renderSidebar();
      await refreshProgress();
      const first = firstChallengeId();
      if (first) selectChallenge(first);
    } catch (e) {
      $("track-list").innerHTML = `<div class="loading">Failed to load: ${e}</div>`;
    }
    wireButtons();
  }

  function firstChallengeId() {
    for (const t of state.tracks) if (t.challenges.length) return t.challenges[0].id;
    return null;
  }

  // ---- Sidebar ----
  function renderSidebar() {
    const el = $("track-list");
    el.innerHTML = "";
    let total = 0;
    for (const t of state.tracks) {
      const wrap = document.createElement("div"); wrap.className = "track";
      wrap.innerHTML = `<div class="track-title">${t.title}<small>${t.subtitle}</small></div>`;
      for (const c of t.challenges) {
        total++;
        const link = document.createElement("div");
        link.className = "lesson-link"; link.dataset.id = c.id;
        link.innerHTML = `<span class="stat" data-stat="${c.id}">○</span>
          <span class="lname">${c.title}</span><span class="lxp">+${c.xp}</span>`;
        link.addEventListener("click", () => selectChallenge(c.id));
        wrap.appendChild(link);
      }
      el.appendChild(wrap);
    }
    el.dataset.total = total;
  }

  function markSidebar() {
    document.querySelectorAll(".lesson-link").forEach(l => {
      const solved = state.solved.has(l.dataset.id);
      l.classList.toggle("solved", solved);
      const stat = l.querySelector(".stat");
      if (stat) stat.textContent = solved ? "✓" : "○";
      if (stat) stat.style.color = solved ? "var(--green)" : "var(--muted)";
      l.classList.toggle("active", state.current && l.dataset.id === state.current.id);
    });
    const total = $("track-list").dataset.total || 0;
    $("solved-summary").textContent = `${state.solved.size}/${total} solved`;
  }

  // ---- Challenge ----
  async function selectChallenge(id) {
    const c = await API.challenge(id);
    state.current = c; state.lastError = "";
    $("lesson-title").textContent = c.title;
    $("lesson-difficulty").textContent = "★".repeat(c.difficulty) + "☆".repeat(5 - c.difficulty);
    $("lesson-xp").textContent = `+${c.xp} XP`;
    $("lesson-spark").hidden = !c.needs_spark;
    $("lesson-brief").innerHTML = window.marked ? marked.parse(c.brief || "") : (c.brief || "");
    $("lesson-concepts").innerHTML = (c.concepts || []).map(x => `<span class="concept-tag">${x}</span>`).join("");
    $("editor-label").textContent = c.track === "python" ? "Python" : (c.track === "streaming" ? "PySpark · Structured Streaming" : "PySpark");
    const saved = LS.getItem("sq_code_" + id);
    if (editor) editor.setValue(saved != null ? saved : (c.starter_code || ""));
    $("console-out").textContent = "Run your code to see output here.";
    $("console-out").classList.remove("error");
    $("checks").innerHTML = ""; $("run-meta").textContent = "";
    markSidebar();
  }

  function persistCode() {
    if (state.current && editor) LS.setItem("sq_code_" + state.current.id, editor.getValue());
  }

  // ---- Run / Submit ----
  function setBusy(busy, label) {
    ["btn-run", "btn-submit"].forEach(b => $(b).disabled = busy);
    if (busy) { $("console-out").classList.remove("error"); $("console-out").textContent = label || "Running…"; $("checks").innerHTML = ""; }
  }

  function showOutput(res) {
    const parts = [];
    if (res.stdout) parts.push(res.stdout.trimEnd());
    if (res.error) parts.push((parts.length ? "\n" : "") + res.error);
    if (res.stderr && res.error) { /* keep console focused; stderr shown only if no error text */ }
    else if (res.stderr) parts.push(res.stderr.trimEnd());
    const out = parts.join("\n").trim();
    $("console-out").textContent = out || "(no output)";
    $("console-out").classList.toggle("error", !!res.error);
    const meta = [];
    if (typeof res.duration_ms === "number") meta.push(`${res.duration_ms} ms`);
    if (res.spark_startup_ms) meta.push(`Spark warm-up ${res.spark_startup_ms} ms`);
    $("run-meta").textContent = meta.join(" · ");
    state.lastError = res.error || "";
  }

  async function doRun() {
    if (!state.current) return;
    persistCode(); setBusy(true, state.current.needs_spark ? "Spinning up Spark…" : "Running…");
    try { showOutput(await API.run({ challenge_id: state.current.id, code: editor.getValue() })); }
    catch (e) { $("console-out").textContent = "Network error: " + e; $("console-out").classList.add("error"); }
    finally { setBusy(false); }
  }

  async function doSubmit() {
    if (!state.current) return;
    persistCode(); setBusy(true, state.current.needs_spark ? "Spinning up Spark & grading…" : "Grading…");
    try {
      const res = await API.submit({ challenge_id: state.current.id, code: editor.getValue(), user_id: userId });
      showOutput(res);
      renderChecks(res);
      if (res.passed) {
        state.solved.add(state.current.id);
        applyProgress(res.total_xp, res.level);
        (res.new_badges || []).forEach(showBadgeToast);
        markSidebar();
      }
    } catch (e) { $("console-out").textContent = "Network error: " + e; $("console-out").classList.add("error"); }
    finally { setBusy(false); }
  }

  function renderChecks(res) {
    const box = $("checks");
    box.innerHTML = "";
    const banner = document.createElement("div");
    banner.className = "banner " + (res.passed ? "win" : "lose");
    banner.textContent = res.passed ? `✓ Passed! +${res.xp_awarded} XP` : "✗ Not yet — see the checks below";
    box.appendChild(banner);
    (res.checks || []).forEach(c => {
      const d = document.createElement("div");
      d.className = "check " + (c.passed ? "pass" : "fail");
      d.innerHTML = `<span class="ico">${c.passed ? "✓" : "✗"}</span><div><div>${escapeHtml(c.message)}</div>${c.detail && !c.passed ? `<div class="detail">${escapeHtml(c.detail)}</div>` : ""}</div>`;
      box.appendChild(d);
    });
    if (!res.checks || res.checks.length === 0) {
      const d = document.createElement("div"); d.className = "check fail";
      d.innerHTML = `<span class="ico">✗</span><div>Your code didn't run to completion — fix the error above and try again.</div>`;
      box.appendChild(d);
    }
  }

  // ---- Progress ----
  async function refreshProgress() {
    try {
      const p = await API.progress(userId);
      state.solved = new Set(p.solved || []);
      applyProgress(p.xp, p.level, p.xp_to_next);
      $("badge-count").textContent = (p.badges || []).length;
      markSidebar();
    } catch (e) { /* ignore */ }
  }
  function applyProgress(xp, level, xpToNext) {
    $("xp-num").textContent = xp; $("level-num").textContent = level;
    const within = xpWithinLevel(xp);
    $("xp-fill").style.width = Math.min(100, within) + "%";
    if (typeof xpToNext === "number") $("xp-next").textContent = `· ${xpToNext} to next`;
  }
  // rough fill: percentage toward an evenly-spaced next milestone
  function xpWithinLevel(xp) {
    const th = [0, 150, 400, 750, 1200, 1800, 2500, 3300, 4200, 5200];
    let lo = 0, hi = th[th.length - 1];
    for (let i = 0; i < th.length; i++) { if (xp >= th[i]) lo = th[i]; if (xp < th[i]) { hi = th[i]; break; } }
    if (hi === lo) return 100;
    return ((xp - lo) / (hi - lo)) * 100;
  }

  // ---- Tutor ----
  async function askTutor(question) {
    const q = (question || $("tutor-question").value).trim();
    if (!q || !state.current) return;
    $("tutor-question").value = "";
    addMsg("user", q);
    const thinking = addMsg("bot", "…");
    try {
      const res = await API.tutor({ challenge_id: state.current.id, code: editor ? editor.getValue() : "", question: q, last_error: state.lastError });
      thinking.textContent = res.reply;
      $("tutor-provider").textContent = "· " + res.provider;
    } catch (e) { thinking.textContent = "Tutor unavailable: " + e; }
  }
  function addMsg(role, text) {
    const m = document.createElement("div"); m.className = "msg " + role; m.textContent = text;
    $("tutor-log").appendChild(m); $("tutor-log").scrollTop = $("tutor-log").scrollHeight; return m;
  }

  // ---- Modal (leaderboard / badges) ----
  async function openLeaderboard() {
    const { leaderboard } = await API.leaderboard();
    $("modal-title").textContent = "🏆 Leaderboard";
    const rows = (leaderboard || []).map((r, i) =>
      `<div class="lb-row"><span class="lb-rank">${i + 1}</span><span>${escapeHtml(displayName(r.user_id))}</span><span>Lv ${r.level}</span><span>${r.xp} XP</span></div>`).join("");
    $("modal-body").innerHTML = `<div class="lb-row head"><span>#</span><span>Quester</span><span>Level</span><span>XP</span></div>${rows || '<div class="loading">No scores yet — be the first!</div>'}`;
    $("modal").hidden = false;
  }
  async function openBadges() {
    const p = await API.progress(userId);
    const earned = new Set(p.badges || []);
    $("modal-title").textContent = "🏅 Badges";
    $("modal-body").innerHTML = state.badgeDefs.map(b =>
      `<div class="badge-item ${earned.has(b.id) ? "" : "locked"}"><span class="badge-emoji">${b.emoji}</span><div><div><b>${b.name}</b></div><div class="muted">${b.description}</div></div></div>`).join("");
    $("modal").hidden = false;
  }
  function displayName(uid) {
    if (uid === userId) return ($("display-name").value.trim() || "You");
    return uid;
  }

  // ---- Toast ----
  function showBadgeToast(badgeId) {
    const def = state.badgeDefs.find(b => b.id === badgeId);
    const t = document.createElement("div"); t.className = "toast";
    t.innerHTML = `<div class="t-title">${def ? def.emoji + " " + def.name : "New badge!"}</div><div class="muted">${def ? def.description : ""}</div>`;
    $("toast-wrap").appendChild(t);
    setTimeout(() => t.remove(), 4000);
    $("badge-count").textContent = String((parseInt($("badge-count").textContent) || 0) + 1);
  }

  // ---- Handbook ----
  let hbLoaded = false;
  let hbFileToSlug = {};
  async function openHandbook() {
    $("handbook").hidden = false;
    if (hbLoaded) return;
    try {
      const { chapters } = await fetch("/api/handbook").then(r => r.json());
      hbFileToSlug = {};
      const nav = $("hb-nav");
      nav.innerHTML = "";
      chapters.forEach(c => {
        hbFileToSlug[c.file] = c.slug;
        const link = document.createElement("div");
        link.className = "hb-link"; link.dataset.slug = c.slug; link.textContent = c.title;
        link.addEventListener("click", () => loadChapter(c.slug));
        nav.appendChild(link);
      });
      hbLoaded = true;
      if (chapters.length) loadChapter(chapters[0].slug);
    } catch (e) {
      $("hb-content").innerHTML = `<div class="loading">Failed to load handbook: ${e}</div>`;
    }
  }
  async function loadChapter(slug) {
    try {
      const res = await fetch("/api/handbook/" + slug).then(r => r.json());
      const html = window.marked ? marked.parse(res.markdown) : escapeHtml(res.markdown);
      $("hb-content").innerHTML = `<div class="markdown-inner">${html}</div>`;
      $("hb-content").scrollTop = 0;
      document.querySelectorAll(".hb-link").forEach(l => l.classList.toggle("active", l.dataset.slug === slug));
      // make in-text .md links navigate within the reader
      $("hb-content").querySelectorAll('a[href$=".md"]').forEach(a => {
        a.addEventListener("click", (e) => {
          const file = a.getAttribute("href").split("/").pop();
          const target = hbFileToSlug[file];
          if (target) { e.preventDefault(); loadChapter(target); }
        });
      });
    } catch (e) {
      $("hb-content").innerHTML = `<div class="loading">Failed to load chapter: ${e}</div>`;
    }
  }

  // ---- Mock interview ----
  const mock = { all: [], pool: [], current: null, seen: 0, got: 0 };
  let mockLoaded = false;
  function shuffle(a) {
    for (let i = a.length - 1; i > 0; i--) { const j = Math.floor(Math.random() * (i + 1)); [a[i], a[j]] = [a[j], a[i]]; }
  }
  async function openMock() {
    $("mock").hidden = false;
    if (mockLoaded) return;
    try {
      const data = await fetch("/api/interview").then(r => r.json());
      mock.all = data.questions;
      const sel = $("mock-cat");
      sel.innerHTML = `<option value="">Mixed · all ${data.total}</option>` +
        data.categories.map(c => `<option value="${c.slug}">${c.title} · ${c.count}</option>`).join("");
      sel.addEventListener("change", () => { mock.seen = 0; mock.got = 0; buildMockPool(); nextMock(); });
      mockLoaded = true;
      buildMockPool();
      nextMock();
    } catch (e) {
      $("mock-q").textContent = "Failed to load questions: " + e;
    }
  }
  function buildMockPool() {
    const cat = $("mock-cat").value;
    mock.pool = mock.all.filter(q => !cat || q.category === cat);
    shuffle(mock.pool);
  }
  function nextMock() {
    if (!mock.pool.length) buildMockPool();
    mock.current = mock.pool.pop();
    const q = mock.current;
    $("mock-cattag").textContent = q.category_title;
    $("mock-diff").textContent = "★".repeat(q.difficulty) + "☆".repeat(5 - q.difficulty);
    $("mock-q").textContent = q.q;
    $("mock-a").hidden = true; $("mock-a").innerHTML = "";
    $("mock-reveal").hidden = false;
    $("mock-rate").hidden = true;
    $("mock-progress").textContent = `· seen ${mock.seen} · got ${mock.got}`;
  }
  function revealMock() {
    const a = mock.current ? mock.current.a : "";
    $("mock-a").innerHTML = window.marked ? marked.parse(a) : escapeHtml(a);
    $("mock-a").hidden = false;
    $("mock-reveal").hidden = true;
    $("mock-rate").hidden = false;
  }
  function rateMock(r) {
    mock.seen++; if (r === "got") mock.got++;
    nextMock();
  }

  // ---- Buttons ----
  function wireButtons() {
    $("btn-run").addEventListener("click", doRun);
    $("btn-submit").addEventListener("click", doSubmit);
    $("btn-reset").addEventListener("click", () => { if (state.current && editor) { editor.setValue(state.current.starter_code || ""); persistCode(); } });
    $("btn-solution").addEventListener("click", async () => {
      if (!state.current) return;
      if (!confirm("Reveal the reference solution? Try the tutor first!")) return;
      const { solution_code } = await API.solution(state.current.id);
      if (editor) { editor.setValue(solution_code || ""); persistCode(); }
    });
    $("tutor-fab").addEventListener("click", () => { $("tutor-panel").hidden = false; $("tutor-fab").hidden = true; });
    $("tutor-close").addEventListener("click", () => { $("tutor-panel").hidden = true; $("tutor-fab").hidden = false; });
    $("tutor-send").addEventListener("click", () => askTutor());
    $("tutor-question").addEventListener("keydown", (e) => { if (e.key === "Enter") askTutor(); });
    document.querySelectorAll(".tutor-quick button").forEach(b => b.addEventListener("click", () => askTutor(b.dataset.q)));
    $("btn-leaderboard").addEventListener("click", openLeaderboard);
    $("btn-badges").addEventListener("click", openBadges);
    $("btn-handbook").addEventListener("click", openHandbook);
    $("hb-close").addEventListener("click", () => { $("handbook").hidden = true; });
    $("btn-mock").addEventListener("click", openMock);
    $("mock-close").addEventListener("click", () => { $("mock").hidden = true; });
    $("mock-reveal").addEventListener("click", revealMock);
    document.querySelectorAll("#mock-rate button[data-rate]").forEach(b =>
      b.addEventListener("click", () => rateMock(b.dataset.rate)));
    $("modal-close").addEventListener("click", () => $("modal").hidden = true);
    $("modal").addEventListener("click", (e) => { if (e.target === $("modal")) $("modal").hidden = true; });
  }

  function escapeHtml(s) {
    return String(s == null ? "" : s).replace(/[&<>"']/g, c => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
  }
})();
