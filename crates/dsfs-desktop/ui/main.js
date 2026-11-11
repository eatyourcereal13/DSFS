const { invoke } = window.__TAURI__.core;

const els = {
  scenarioSelect: document.getElementById("scenarioSelect"),
  loadBtn: document.getElementById("loadBtn"),
  runBtn: document.getElementById("runBtn"),
  pauseBtn: document.getElementById("pauseBtn"),
  resetBtn: document.getElementById("resetBtn"),
  speedSelect: document.getElementById("speedSelect"),
  cluster: document.getElementById("cluster"),
  events: document.getElementById("events"),
  clusterPanel: document.getElementById("clusterPanel"),
  eventsPanel: document.getElementById("eventsPanel"),
  timeVal: document.getElementById("timeVal"),
  seedVal: document.getElementById("seedVal"),
  scenarioVal: document.getElementById("scenarioVal"),
  stateVal: document.getElementById("stateVal"),
  statSent: document.getElementById("statSent"),
  statDropped: document.getElementById("statDropped"),
  statDup: document.getElementById("statDup"),
  statLeaders: document.getElementById("statLeaders"),
  statApplied: document.getElementById("statApplied"),
  faults: document.getElementById("faults"),
  welcome: document.getElementById("welcome"),
  skipTourBtn: document.getElementById("skipTourBtn"),
  tour: document.getElementById("tour"),
  tourTitle: document.getElementById("tourTitle"),
  tourBody: document.getElementById("tourBody"),
  tourNextBtn: document.getElementById("tourNextBtn"),
  tourStopBtn: document.getElementById("tourStopBtn"),
};

async function init() {
  let scenarios = [];
  try {
    scenarios = await invoke("list_scenarios");
  } catch (e) {
    console.error("list_scenarios failed", e);
  }

  for (const s of scenarios) {
    const opt = document.createElement("option");
    opt.value = s;
    opt.textContent = s;
    els.scenarioSelect.appendChild(opt);
  }
  if (scenarios.length > 0) {
    els.scenarioSelect.value = scenarios[0];
  }
}

// ---- Welcome & tour ----

document.querySelectorAll(".tour-card-btn").forEach((btn) => {
  btn.addEventListener("click", async () => {
    const tourName = btn.getAttribute("data-tour");
    try {
      const snap = await invoke("start_tour", { tourName });
      els.welcome.classList.add("hidden");
      els.tour.style.display = "block";
      render(snap);
    } catch (e) {
      console.error(e);
      alert("Не удалось запустить тур: " + e);
    }
  });
});

els.skipTourBtn.addEventListener("click", () => {
  els.welcome.classList.add("hidden");
});

els.tourNextBtn.addEventListener("click", async () => {
  try {
    const snap = await invoke("advance_tour");
    render(snap);
  } catch (e) {
    console.error(e);
  }
});

els.tourStopBtn.addEventListener("click", async () => {
  try {
    await invoke("stop_tour");
    els.tour.style.display = "none";
  } catch (e) {
    console.error(e);
  }
});

// ---- Regular controls ----

els.loadBtn.addEventListener("click", async () => {
  const name = els.scenarioSelect.value;
  if (!name) return;
  try {
    const snap = await invoke("load_scenario", { name });
    render(snap);
    await invoke("set_paused", { paused: true });
  } catch (e) {
    console.error(e);
  }
});

els.runBtn.addEventListener("click", () => invoke("set_paused", { paused: false }));
els.pauseBtn.addEventListener("click", () => invoke("set_paused", { paused: true }));
els.resetBtn.addEventListener("click", async () => {
  try {
    const snap = await invoke("reset");
    render(snap);
  } catch (e) {
    console.error(e);
  }
});
els.speedSelect.addEventListener("change", async () => {
  await invoke("set_speed", { speed: parseFloat(els.speedSelect.value) });
});

// ---- Tick ----

async function tick() {
  try {
    const snap = await invoke("get_state");
    render(snap);
  } catch (e) {
    // swallow
  }
  setTimeout(tick, 100);
}

// ---- Render ----

function render(snap) {
  if (!snap) return;

  els.timeVal.textContent = snap.time_ms;
  els.seedVal.textContent = snap.seed || "-";
  els.scenarioVal.textContent = snap.scenario_name || "-";
  els.stateVal.textContent = !snap.running
    ? "no scenario"
    : snap.paused
      ? "paused"
      : "running";

  els.statSent.textContent = snap.stats.messages_sent;
  els.statDropped.textContent = snap.stats.messages_dropped;
  els.statDup.textContent = snap.stats.messages_duplicated;
  els.statLeaders.textContent = snap.stats.leaders_elected;
  els.statApplied.textContent = snap.stats.applied;

  // ---- Cluster ----
  const clusterHtml = snap.nodes
    .map((n) => {
      const cls =
        n.availability === "Offline" ? "offline" : n.is_leader ? "leader" : "";
      const badge =
        n.availability === "Offline" ? "o" : n.is_leader ? "*" : ".";
      const roleTagCls =
        n.availability === "Offline" ? "offline" : n.role.toLowerCase();
      return `
        <div class="node ${cls}" data-node-id="${n.id}">
          <span class="badge">${badge}</span>
          <div>
            <span class="id">n${n.id}</span>
            <span class="role-tag ${roleTagCls}">${n.role}</span>
          </div>
          <span class="meta">term ${n.term}  log ${n.log_len}  commit ${n.commit}</span>
        </div>
      `;
    })
    .join("");
  els.cluster.innerHTML = clusterHtml || '<div style="color:#5c5c66;padding:10px;">no nodes</div>';

  // ---- Events ----
  const eventsHtml = snap.recent_events
    .slice()
    .reverse()
    .map((e) => {
      const cls = classifyEvent(e.kind_short);
      return `
        <div class="event ${cls}">
          <span class="t">${e.time_ms}ms</span>
          <span class="tag">${escapeHtml(e.kind_short)}</span>
          <span class="desc" title="${escapeHtml(e.kind)}">${escapeHtml(e.kind)}</span>
        </div>
      `;
    })
    .join("");
  els.events.innerHTML =
    eventsHtml ||
    '<div style="color:#5c5c66;padding:10px;">no events yet — click Run</div>';

  // ---- Faults ----
  const faultsHtml = snap.faults
    .map(
      (f) =>
        `<span class="fault-pill ${f.completed ? "completed" : "pending"}">${f.completed ? "[done]" : "[pending]"} ${escapeHtml(f.description)}${f.at_ms != null ? ` @${f.at_ms}ms` : ""}</span>`,
    )
    .join("");
  els.faults.innerHTML = faultsHtml;

  // ---- Tour ----
  renderTour(snap.tour);
}

function renderTour(tour) {
  if (!tour || !tour.active) {
    if (tour && tour.finished) {
      els.tourTitle.textContent = "Демонстрация завершена";
      els.tourBody.textContent =
        "Вы можете перезапустить тур, нажав «Начать заново», или выйти и открыть свободный режим.";
      els.tourNextBtn.textContent = "Начать заново";
      els.tourNextBtn.disabled = false;
    } else {
      els.tour.style.display = "none";
    }
    clearHighlights();
    return;
  }

  els.tour.style.display = "block";
  els.tourTitle.textContent = tour.step_title;
  els.tourBody.textContent = tour.step_body;
  els.tourNextBtn.textContent = tour.step_button_label;
  els.tourNextBtn.disabled = !tour.step_ready;

  applyHighlights(tour.highlights);
}

function clearHighlights() {
  document.querySelectorAll(".highlighted").forEach((el) => {
    el.classList.remove("highlighted");
  });
}

function applyHighlights(highlights) {
  clearHighlights();
  for (const h of highlights) {
    if (h === "ClusterPanel") {
      els.clusterPanel.classList.add("highlighted");
    } else if (h === "EventsPanel") {
      els.eventsPanel.classList.add("highlighted");
    } else if (h === "FaultBar") {
      els.faults.classList.add("highlighted");
    } else if (typeof h === "object" && h.Node !== undefined) {
      const el = document.querySelector(`.node[data-node-id="${h.Node}"]`);
      if (el) el.classList.add("highlighted");
    }
  }
}

function classifyEvent(short) {
  if (short.includes("leader")) return "leader";
  if (short.includes("commit")) return "commit";
  if (short.includes("applied")) return "applied";
  if (short.includes("fault")) return "fault";
  if (short.includes("term")) return "term";
  return "";
}

function escapeHtml(s) {
  return String(s)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

init().then(() => tick());