const panelTitles = {
  overview: "Overview",
  command: "Command Center",
  projects: "Projects",
  review: "Review Inbox",
  coordination: "Coordination Inbox",
  health: "Health / Alerts",
};

const panelLoadMap = {
  overview: "overview",
  projects: "projects",
  review: "review",
  coordination: "coordination",
  health: "health",
};

const commandDefinitions = {
  "codex-exec": {
    description: "Run a fresh Codex task through the shared broker against the Workspace Hub mainline.",
    promptPlaceholder: "Describe the task for codex exec.",
    sessionPlaceholder: "Not used for codex exec.",
    usesPrompt: true,
    promptRequired: true,
    usesSession: false,
  },
  "codex-resume": {
    description: "Resume an existing Codex session through the shared broker with non-interactive `codex exec resume`. Session ID is required; prompt is optional but recommended for deterministic handoff.",
    promptPlaceholder: "Optional follow-up prompt for codex exec resume.",
    sessionPlaceholder: "Paste the session ID to resume.",
    usesPrompt: true,
    promptRequired: false,
    usesSession: true,
  },
  "open-codex-app": {
    description: "Open Codex App pointed at the shared Workspace Hub root. No prompt or session ID is needed.",
    promptPlaceholder: "Not used when opening Codex App.",
    sessionPlaceholder: "Not used when opening Codex App.",
    usesPrompt: false,
    promptRequired: false,
    usesSession: false,
  },
};

const priorityOrder = {
  high: 0,
  medium: 1,
  low: 2,
};

const state = {
  activePanel: "overview",
  metadata: null,
  panelData: {},
  panelResponses: {},
  selectedProjectIndex: 0,
  panelScopes: {
    review: "",
    coordination: "",
  },
  commandHistory: [],
};

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function formatValue(value, fallback = "n/a") {
  const text = String(value ?? "").trim();
  return text || fallback;
}

function chooseFirstValue(...values) {
  for (const candidate of values) {
    if (candidate == null) continue;
    const text = String(candidate).trim();
    if (text) return text;
  }
  return "";
}

function formatCommand(command) {
  return Array.isArray(command) ? command.join(" ") : "";
}

function looksLikePath(value) {
  return String(value || "").startsWith("/");
}

function pillTone(value) {
  const normalized = String(value || "").toLowerCase();
  if (normalized.includes("high") || normalized.includes("error") || normalized.includes("critical")) return "danger";
  if (normalized.includes("medium") || normalized.includes("warn")) return "warning";
  if (normalized.includes("active") || normalized.includes("ok") || normalized.includes("info")) return "success";
  return "neutral";
}

function setPanelFeedback(kind, title = "", body = "", command = "") {
  const host = document.getElementById("panel-feedback");
  if (!kind) {
    host.hidden = true;
    host.className = "panel-feedback";
    host.innerHTML = "";
    return;
  }
  host.hidden = false;
  host.className = `panel-feedback ${kind}`;
  host.innerHTML = `
    <div class="feedback-title">${escapeHtml(title)}</div>
    ${body ? `<p>${escapeHtml(body)}</p>` : ""}
    ${command ? `<pre class="feedback-command">${escapeHtml(command)}</pre>` : ""}
  `;
}

function syncPanelFeedback(panel) {
  const response = state.panelResponses[panel];
  if (!response || response.ok) {
    setPanelFeedback();
    return;
  }
  const tone = response.unavailable ? "warning" : "danger";
  const title = response.unavailable ? "Shared broker unavailable" : "Broker request failed";
  const body = response.stderr || "The console could not load fresh data from the shared broker.";
  setPanelFeedback(tone, title, body, formatCommand(response.command));
}

function activatePanel(panel) {
  state.activePanel = panel;
  document.querySelectorAll(".nav-link").forEach((button) => {
    button.classList.toggle("active", button.dataset.panel === panel);
  });
  document.querySelectorAll(".panel").forEach((section) => {
    section.classList.toggle("active", section.id === `panel-${panel}`);
  });
  document.getElementById("panel-title").textContent = panelTitles[panel];
  syncPanelFeedback(panel);
}

function renderMetadata(metadata) {
  const list = document.getElementById("metadata-list");
  const items = [
    `Codex available: ${metadata.codex_available}`,
    `Broker available: ${metadata.broker_available}`,
    `Broker mode: ${metadata.broker_mode}`,
    `Console source: ${metadata.workspace_root}`,
    `Shared workspace: ${metadata.broker_workspace_root || metadata.workspace_root}`,
    `Broker: ${metadata.broker_path}`,
  ];
  list.innerHTML = items.map((item) => `<li>${item}</li>`).join("");
}

function renderCommandReadiness() {
  const host = document.getElementById("command-readiness");
  const metadata = state.metadata || {};
  const cards = [
    {
      label: "Codex CLI",
      value: metadata.codex_available ? "Ready" : "Missing",
      tone: metadata.codex_available ? "success" : "danger",
      body: metadata.codex_available ? "CLI entry points are available for command-center actions." : "Install or expose the codex binary before using command-center actions.",
    },
    {
      label: "Shared Broker",
      value: metadata.broker_available ? "Connected" : "Unavailable",
      tone: metadata.broker_available ? "success" : "warning",
      body: metadata.broker_available
        ? `Using ${metadata.broker_mode || "shared"} broker at ${metadata.broker_workspace_root || metadata.workspace_root}.`
        : "Panels will fall back to placeholders until the shared broker is reachable.",
    },
    {
      label: "Workspace Target",
      value: metadata.broker_workspace_root === metadata.workspace_root ? "Local" : "Shared",
      tone: "neutral",
      body: `Command center targets ${metadata.broker_workspace_root || metadata.workspace_root || "the current workspace"}.`,
    },
  ];
  host.innerHTML = cards
    .map(
      (card) => `
        <article class="detail-card readiness-card">
          <div class="section-head">
            <h4>${escapeHtml(card.label)}</h4>
            <span class="pill ${pillTone(card.tone)}">${escapeHtml(card.value)}</span>
          </div>
          <p>${escapeHtml(card.body)}</p>
        </article>
      `,
    )
    .join("");
}

function makeTable(rows, emptyMessage = "No rows yet.") {
  if (!rows.length) {
    return `<div class="empty-state">${escapeHtml(emptyMessage)}</div>`;
  }
  const headers = Object.keys(rows[0]);
  const head = headers.map((header) => `<th>${escapeHtml(header)}</th>`).join("");
  const body = rows
    .map((row) => {
      const cells = headers
        .map((header) => `<td title="${escapeHtml(formatValue(row[header], ""))}">${escapeHtml(formatValue(row[header], ""))}</td>`)
        .join("");
      return `<tr>${cells}</tr>`;
    })
    .join("");
  return `<table><thead><tr>${head}</tr></thead><tbody>${body}</tbody></table>`;
}

function wirePanelActions(scope = document) {
  scope.querySelectorAll("[data-open-path]").forEach((button) => {
    button.addEventListener("click", async () => {
      const targetPath = button.dataset.openPath;
      if (targetPath) {
        await window.workspaceHubAPI.openPath(targetPath);
      }
    });
  });
  scope.querySelectorAll("[data-nav-panel]").forEach((button) => {
    button.addEventListener("click", async () => {
      const targetPanel = button.dataset.navPanel;
      if (!targetPanel) return;
      activatePanel(targetPanel);
      if (panelLoadMap[targetPanel]) {
        await loadPanel(targetPanel);
      }
    });
  });
  scope.querySelectorAll("[data-load-panel]").forEach((button) => {
    button.addEventListener("click", async () => {
      const targetPanel = button.dataset.loadPanel;
      if (!targetPanel) return;
      await loadPanel(targetPanel);
    });
  });
  scope.querySelectorAll("[data-use-project]").forEach((button) => {
    button.addEventListener("click", () => {
      seedCommandCenter(
        button.dataset.useProject || "",
        button.dataset.usePrompt || "",
        button.dataset.useAction || "codex-exec",
        button.dataset.useMessage || "",
      );
      activatePanel("command");
    });
  });
  scope.querySelectorAll("[data-focus-panel]").forEach((button) => {
    button.addEventListener("click", async () => {
      const targetPanel = button.dataset.focusPanel;
      const projectName = button.dataset.focusProject || "";
      if (!targetPanel || !panelLoadMap[targetPanel]) return;
      state.panelScopes[targetPanel] = projectName;
      activatePanel(targetPanel);
      await loadPanel(targetPanel, { projectName });
    });
  });
  scope.querySelectorAll("[data-clear-scope]").forEach((button) => {
    button.addEventListener("click", async () => {
      const targetPanel = button.dataset.clearScope;
      if (!targetPanel || !panelLoadMap[targetPanel]) return;
      state.panelScopes[targetPanel] = "";
      await loadPanel(targetPanel, { projectName: "" });
    });
  });
}

function renderOverview(data) {
  const cards = document.getElementById("overview-cards");
  const note = document.getElementById("overview-note");
  const highlights = document.getElementById("overview-highlights");
  const projectRows = [...(state.panelData.projects?.rows || [])].sort((left, right) => {
    const leftRank = priorityOrder[String(left.priority || "").toLowerCase()] ?? 99;
    const rightRank = priorityOrder[String(right.priority || "").toLowerCase()] ?? 99;
    if (leftRank !== rightRank) return leftRank - rightRank;
    return String(right.updated_at || "").localeCompare(String(left.updated_at || ""));
  });
  const topProjects = projectRows.slice(0, 3);
  const healthAlert = (state.panelData.health?.alerts || state.panelData.health?.rows || [])[0];
  cards.innerHTML = (data.cards || [])
    .map(
      (card) => `
        <article class="metric-card">
          <div class="label">${escapeHtml(card.label)}</div>
          <div class="value">${escapeHtml(card.value)}</div>
        </article>
      `,
    )
    .join("");
  note.textContent = data.note || "Broker data not available yet.";
  highlights.innerHTML = `
    <article class="detail-card compact-card">
      <div class="section-head">
        <h3>Shared Substrate</h3>
        <span class="pill ${pillTone(state.metadata?.broker_mode)}">${escapeHtml(state.metadata?.broker_mode || "unknown")}</span>
      </div>
      <p>Panels are fed from the shared broker/runtime layer. This shell stays read-only against Vault truth and focuses on operator clarity.</p>
      <ul class="signal-list">
        <li>Broker root: ${escapeHtml(formatValue(state.metadata?.broker_workspace_root || state.metadata?.workspace_root))}</li>
        <li>Broker path: ${escapeHtml(formatValue(state.metadata?.broker_path))}</li>
        <li>Codex commands: ${escapeHtml((state.metadata?.codex_commands || []).join(", "))}</li>
      </ul>
      <div class="button-row">
        <button class="ghost-button small-button" data-nav-panel="command">Open Command Center</button>
        <button class="ghost-button small-button" data-load-panel="health">Refresh Health</button>
      </div>
    </article>
    <article class="detail-card compact-card">
      <div class="section-head">
        <h3>Highest-Signal Projects</h3>
        <button class="ghost-button small-button" data-nav-panel="projects">Inspect Projects</button>
      </div>
      ${
        topProjects.length
          ? `
            <div class="signal-stack">
              ${topProjects
                .map(
                  (row) => `
                    <div class="signal-row">
                      <div>
                        <strong>${escapeHtml(row.project_name)}</strong>
                        <p>${escapeHtml(formatValue(row.next_action))}</p>
                      </div>
                      <span class="pill ${pillTone(row.priority)}">${escapeHtml(formatValue(row.priority))}</span>
                    </div>
                  `,
                )
                .join("")}
            </div>
          `
          : '<div class="empty-state">Projects have not been loaded from the shared broker yet.</div>'
      }
      ${
        healthAlert
          ? `
            <div class="overview-alert">
              <span class="pill ${pillTone(healthAlert.severity)}">${escapeHtml(formatValue(healthAlert.severity))}</span>
              <p>${escapeHtml(formatValue(healthAlert.summary))}</p>
            </div>
          `
          : ""
      }
    </article>
  `;
  wirePanelActions(highlights);
}

function renderHealth(data) {
  const host = document.getElementById("health-stack");
  const alerts = data.alerts || data.rows || [];
  if (!alerts.length) {
    host.innerHTML = '<div class="empty-state">No health alerts right now.</div>';
    return;
  }
  host.innerHTML = alerts
    .map((alert, index) => {
      const level = String(alert.level || alert.severity || "").toLowerCase();
      const levelClass = level.includes("error") || level.includes("critical") ? "error" : level.includes("warn") ? "warning" : "";
      return `
        <article class="health-card ${levelClass}">
          <div class="section-head">
            <strong>${escapeHtml(alert.title || alert.alert_key || "Alert")}</strong>
            <span class="pill ${pillTone(level)}">${escapeHtml(formatValue(level || "info"))}</span>
          </div>
          <p>${escapeHtml(alert.summary || alert.current_summary || "")}</p>
          ${
            alert.report_path
              ? `<div class="button-row"><button class="ghost-button small-button" data-open-path="${escapeHtml(alert.report_path)}">Open Report</button></div>`
              : ""
          }
        </article>
      `;
    })
    .join("");
  wirePanelActions(host);
}

function renderProjects(data) {
  const listHost = document.getElementById("projects-list");
  const detailHost = document.getElementById("project-detail");
  const rows = data.rows || [];
  const reviewCounts = buildWorkflowCounts(state.panelData.review?.rows || []);
  const coordinationCounts = buildWorkflowCounts(state.panelData.coordination?.rows || []);
  if (!rows.length) {
    listHost.innerHTML = '<div class="empty-state">No project rows are available from the shared broker.</div>';
    detailHost.innerHTML = '<div class="empty-state">Select a project when broker data becomes available.</div>';
    return;
  }
  if (state.selectedProjectIndex >= rows.length) {
    state.selectedProjectIndex = 0;
  }
  listHost.innerHTML = rows
    .map(
      (row, index) => `
        <button class="project-card ${index === state.selectedProjectIndex ? "active" : ""}" data-project-index="${index}">
          <div class="section-head">
            <strong>${escapeHtml(row.project_name)}</strong>
            <span class="pill ${pillTone(row.priority)}">${escapeHtml(formatValue(row.priority))}</span>
          </div>
          <p>${escapeHtml(formatValue(row.next_action))}</p>
          <div class="meta-row">
            <span>${escapeHtml(formatValue(row.status))}</span>
            <span>${escapeHtml(formatValue(row.updated_at))}</span>
          </div>
          <div class="workflow-metrics">
            <span class="pill ${pillTone(reviewCounts[row.project_name] ? "warning" : "neutral")}">Review ${reviewCounts[row.project_name] || 0}</span>
            <span class="pill ${pillTone(coordinationCounts[row.project_name] ? "warning" : "neutral")}">Coord ${coordinationCounts[row.project_name] || 0}</span>
          </div>
        </button>
      `,
    )
    .join("");
  listHost.querySelectorAll("[data-project-index]").forEach((button) => {
    button.addEventListener("click", () => {
      state.selectedProjectIndex = Number(button.dataset.projectIndex || 0);
      renderProjects({ rows });
    });
  });
  const selectedRow = rows[state.selectedProjectIndex];
  const selectedProjectName = selectedRow.project_name;
  const selectedReviewCount = reviewCounts[selectedProjectName] || 0;
  const selectedCoordinationCount = coordinationCounts[selectedProjectName] || 0;
  const projectReviewRows = getWorkflowRowsForProject("review", selectedProjectName);
  const projectCoordinationRows = getWorkflowRowsForProject("coordination", selectedProjectName);
  detailHost.innerHTML = `
    <article class="detail-card sticky-card">
      <div class="section-head">
        <div>
          <p class="eyebrow">Project Detail</p>
          <h3>${escapeHtml(selectedRow.project_name)}</h3>
        </div>
        <div class="pill-row">
          <span class="pill ${pillTone(selectedRow.status)}">${escapeHtml(formatValue(selectedRow.status))}</span>
          <span class="pill ${pillTone(selectedRow.priority)}">${escapeHtml(formatValue(selectedRow.priority))}</span>
        </div>
      </div>
      <p>${escapeHtml(formatValue(selectedRow.next_action))}</p>
      <dl class="detail-grid">
        <div>
          <dt>Updated</dt>
          <dd>${escapeHtml(formatValue(selectedRow.updated_at))}</dd>
        </div>
        <div>
          <dt>Board</dt>
          <dd class="path-value">${escapeHtml(formatValue(selectedRow.board_path))}</dd>
        </div>
      </dl>
      <div class="button-row">
        <button class="ghost-button small-button" data-open-path="${escapeHtml(selectedRow.board_path)}">Open Board</button>
        <button
          class="primary-button small-button"
          data-use-project="${escapeHtml(selectedRow.project_name)}"
          data-use-prompt="${escapeHtml(`Continue work for ${selectedRow.project_name}. Current next action: ${formatValue(selectedRow.next_action)}.`)}"
          data-use-message="${escapeHtml(`Command Center seeded for ${selectedRow.project_name}.`)}"
        >Use In Command Center</button>
        <button
          class="ghost-button small-button"
          data-use-project="${escapeHtml(selectedRow.project_name)}"
          data-use-prompt="${escapeHtml(formatValue(selectedRow.next_action))}"
          data-use-message="${escapeHtml(`Loaded current next action for ${selectedRow.project_name}.`)}"
        >Prep Next Action</button>
        <button class="ghost-button small-button" data-focus-panel="review" data-focus-project="${escapeHtml(selectedRow.project_name)}">Review For Project</button>
        <button class="ghost-button small-button" data-focus-panel="coordination" data-focus-project="${escapeHtml(selectedRow.project_name)}">Coordination For Project</button>
      </div>
      <div class="workflow-metrics detail-metrics">
        <span class="pill ${pillTone(selectedReviewCount ? "warning" : "neutral")}">Review ${selectedReviewCount}</span>
        <span class="pill ${pillTone(selectedCoordinationCount ? "warning" : "neutral")}">Coord ${selectedCoordinationCount}</span>
      </div>
      <section class="workflow-project-summary">
        <div class="project-summary-column">
          <h4 class="project-summary-heading">Review Items</h4>
          ${renderProjectWorkflowList("review", projectReviewRows)}
        </div>
        <div class="project-summary-column">
          <h4 class="project-summary-heading">Coordination Items</h4>
          ${renderProjectWorkflowList("coordination", projectCoordinationRows)}
        </div>
      </section>
    </article>
  `;
  wirePanelActions(detailHost);
}

function renderWorkflowTable(hostId, rows, emptyMessage) {
  document.getElementById(hostId).innerHTML = makeTable(rows, emptyMessage);
}

function formatWorkflowStatusLabel(status) {
  const normalized = String(status || "unknown").replace(/_/g, " ").trim();
  return normalized || "unknown";
}

function buildWorkflowCounts(rows) {
  const counts = {};
  (rows || []).forEach((row) => {
    const project = String(row.project_name || "unassigned").trim();
    counts[project] = (counts[project] || 0) + 1;
  });
  return counts;
}

function buildTopProjectBadges(rows, limit = 3) {
  const counts = buildWorkflowCounts(rows);
  const sorted = Object.entries(counts)
    .filter(([, count]) => count)
    .sort((a, b) => b[1] - a[1]);
  return sorted.slice(0, limit);
}

function formatDueDate(value) {
  if (!value) return "";
  const parsed = new Date(String(value));
  if (Number.isNaN(parsed)) return String(value);
  return parsed.toLocaleDateString();
}

function findEarliestDueRow(rows) {
  const candidates = (rows || [])
    .map((row) => {
      const timestamp = Date.parse(row.due_at || row.due || "");
      if (Number.isNaN(timestamp)) return null;
      return { row, timestamp };
    })
    .filter(Boolean);
  if (!candidates.length) return null;
  candidates.sort((a, b) => a.timestamp - b.timestamp);
  return candidates[0].row;
}

function renderWorkflowScope(panel, rows) {
  const host = document.getElementById(`${panel}-scope`);
  const scope = state.panelScopes[panel] || "";
  const count = Array.isArray(rows) ? rows.length : 0;
  if (!scope) {
    host.innerHTML = `
      <div class="scope-card">
        <div>
          <strong>Scope: all projects</strong>
          <p>Showing the shared ${panel} projection across the whole workspace.</p>
        </div>
        <span class="pill ${pillTone("info")}">${count} row${count === 1 ? "" : "s"}</span>
      </div>
    `;
    return;
  }
  host.innerHTML = `
    <div class="scope-card scoped">
      <div>
        <strong>Scoped to ${escapeHtml(scope)}</strong>
        <p>Showing only ${panel} items projected for the selected project.</p>
      </div>
      <div class="pill-row">
        <span class="pill ${pillTone("active")}">${count} row${count === 1 ? "" : "s"}</span>
        <button class="ghost-button small-button" data-clear-scope="${panel}">Clear scope</button>
      </div>
    </div>
  `;
  wirePanelActions(host);
}

function renderWorkflowSummary(panel, rows) {
  const host = document.getElementById(`${panel}-summary`);
  if (!host) return;
  const total = Array.isArray(rows) ? rows.length : 0;
  if (!total) {
    host.innerHTML = `<div class="summary-empty">Awaiting ${panel === "review" ? "review" : "coordination"} rows from the shared broker.</div>`;
    return;
  }
  const statusField = panel === "review" ? "review_status" : "status";
  const statusCounts = rows.reduce((acc, row) => {
    const key = formatWorkflowStatusLabel(row[statusField]);
    acc[key] = (acc[key] || 0) + 1;
    return acc;
  }, {});
  const projectBadges = buildTopProjectBadges(rows);
  const pillHtml = Object.entries(statusCounts)
    .map(([status, count]) => `<span class="pill ${pillTone(status)} summary-pill">${count} ${escapeHtml(status)}</span>`)
    .join("");
  const dueRow = findEarliestDueRow(rows);
  const dueHtml = dueRow
    ? `<div class="summary-due"><strong>Due soon</strong>: ${escapeHtml(
        panel === "review"
          ? dueRow.task_ref || dueRow.review_status || "review item"
          : dueRow.coordination_id || dueRow.requested_action || "coordination item",
      )} (${escapeHtml(formatDueDate(dueRow.due_at || dueRow.due))})</div>`
    : `<div class="summary-due summary-due-empty">No due dates captured yet for this projection.</div>`;
  host.innerHTML = `
    <div class="summary-count">
      <strong>${total}</strong>
      <span>${panel === "review" ? "review item" : "coordination item"}${total === 1 ? "" : "s"}</span>
    </div>
    <div class="pill-row summary-pill-row">
      ${pillHtml}
    </div>
    ${dueHtml}
    ${
      projectBadges.length
        ? `<div class="summary-projects">
            ${projectBadges
              .map(
                ([project, count]) => `<span class="pill summary-project-pill">${escapeHtml(project)} × ${count}</span>`,
              )
              .join("")}
          </div>`
        : `<div class="summary-projects summary-projects-empty">No project-specific rows yet.</div>`
    }
  `;
}

function getWorkflowRowsForProject(panel, projectName) {
  if (!projectName) return [];
  const data = panel === "review" ? state.panelData.review?.rows : state.panelData.coordination?.rows;
  return (data || []).filter((row) => String(row.project_name || row.to_project || row.from_project || "")
    .trim()
    .toLowerCase()
    .includes(projectName.toLowerCase()));
}

function renderProjectWorkflowList(panel, rows) {
  if (!rows.length) {
    return `<div class="project-summary-empty">No ${panel} rows for this project yet.</div>`;
  }
  return rows
    .slice(0, 2)
    .map((row) => {
      const title = panel === "review" ? row.task_ref || "Review item" : row.coordination_id || "Coordination item";
      const secondary = panel === "review" ? row.deliverable_ref : row.requested_action;
      const due = panel === "review" ? formatDueDate(row.due_at || row.due) : formatDueDate(row.due_at || row.due);
      const status = panel === "review" ? row.review_status : row.status;
      const projectName = panel === "review" ? row.project_name : row.to_project || row.from_project;
      return `
        <article class="project-summary-item">
          <div class="section-head">
            <div>
              <p class="eyebrow">${panel === "review" ? "Review" : "Coordination"} item</p>
              <h4>${escapeHtml(title)}</h4>
            </div>
            <span class="pill ${pillTone(status)}">${escapeHtml(formatValue(status, "pending"))}</span>
          </div>
          <p>${escapeHtml(formatValue(secondary, "No deliverable/ask captured yet."))}</p>
          <div class="project-summary-meta">
            <span>Project: ${escapeHtml(formatValue(projectName, "n/a"))}</span>
            <span>Due: ${escapeHtml(due || "n/a")}</span>
          </div>
          <div class="button-row">
            <button class="ghost-button small-button" data-focus-panel="${panel}" data-focus-project="${escapeHtml(projectName)}">
              Focus in ${panel === "review" ? "Review" : "Coordination"} Inbox
            </button>
            <button
              class="primary-button small-button"
              data-use-project="${escapeHtml(projectName)}"
              data-use-prompt="${escapeHtml(`Continue ${panel === "review" ? "review" : "coordination"} work for ${formatValue(projectName)}.`)}"
            >
              Route to Command Center
            </button>
          </div>
        </article>
      `;
    })
    .join("");
}

function renderWorkflowEmptyState(hostId, panel, emptyMessage) {
  const host = document.getElementById(hostId);
  const scope = state.panelScopes[panel] || "";
  const scopedActions = scope
    ? `
      <div class="button-row">
        <button
          class="primary-button small-button"
          data-use-project="${escapeHtml(scope)}"
          data-use-prompt="${escapeHtml(`Continue workflow follow-up for ${scope}.`)}"
          data-use-message="${escapeHtml(`Scoped ${scope} handoff loaded into Command Center.`)}"
        >Use ${escapeHtml(scope)} In Command Center</button>
        <button class="ghost-button small-button" data-nav-panel="projects">Back To Projects</button>
        <button class="ghost-button small-button" data-clear-scope="${panel}">Clear scope</button>
      </div>
    `
    : `
      <div class="button-row">
        <button class="ghost-button small-button" data-nav-panel="projects">Inspect Projects</button>
        <button class="ghost-button small-button" data-nav-panel="command">Open Command Center</button>
      </div>
    `;
  host.innerHTML = `
    <article class="empty-state contextual-empty-state">
      <strong>${escapeHtml(emptyMessage)}</strong>
      <p>${
        scope
          ? escapeHtml(`The shared ${panel} projection is currently empty for ${scope}. You can route work through Command Center or clear the project scope.`)
          : escapeHtml(`The shared ${panel} projection is currently empty. Inspect projects to pick a scope or use Command Center to create the next operator action.`)
      }</p>
      ${scopedActions}
    </article>
  `;
  wirePanelActions(host);
}

function renderWorkflowCards(hostId, rows, emptyMessage, kind, panel) {
  const host = document.getElementById(hostId);
  renderWorkflowScope(panel, rows);
  renderWorkflowSummary(panel, rows);
  if (!rows.length) {
    renderWorkflowEmptyState(hostId, panel, emptyMessage);
    return;
  }
  if (kind === "review") {
    host.innerHTML = rows
      .map(
        (row) => {
          const title = chooseFirstValue(row.task_ref, row.deliverable_ref, "Review item");
          const context = chooseFirstValue(row.decision_note, row.review_status, row.deliverable_ref, row.next_action, "Waiting for reviewer input.");
          const dueDate = formatDueDate(row.due_at || row.due);
          return `
            <article class="detail-card workflow-card review-card">
              <div class="section-head">
                <div>
                  <p class="eyebrow">Review Item</p>
                  <h4>${escapeHtml(title)}</h4>
                </div>
                <div class="pill-row">
                  <span class="pill ${pillTone(row.review_status)}">${escapeHtml(formatValue(row.review_status, "draft"))}</span>
                  <span class="pill ${pillTone("active")}">${escapeHtml(formatValue(row.project_name))}</span>
                </div>
              </div>
              <p class="workflow-context">${escapeHtml(context)}</p>
              <dl class="workflow-details">
                <div>
                  <dt>Deliverable</dt>
                  <dd>${escapeHtml(formatValue(row.deliverable_ref, "n/a"))}</dd>
                </div>
                <div>
                  <dt>Reviewer</dt>
                  <dd>${escapeHtml(formatValue(row.reviewer, "unassigned"))}</dd>
                </div>
                <div>
                  <dt>Due</dt>
                  <dd>${escapeHtml(dueDate || "n/a")}</dd>
                </div>
                <div>
                  <dt>Source</dt>
                  <dd class="path-value">${escapeHtml(formatValue(row.source_path, "n/a"))}</dd>
                </div>
              </dl>
              <div class="button-row">
                <button
                  class="primary-button small-button"
                  data-use-project="${escapeHtml(row.project_name)}"
                  data-use-prompt="${escapeHtml(`Review ${title} for ${formatValue(row.project_name)}.`)}"
                  data-use-message="${escapeHtml(`Loaded review workflow for ${title}.`)}"
                >Use In Command Center</button>
                ${
                  looksLikePath(row.source_path)
                    ? `<button class="ghost-button small-button" data-open-path="${escapeHtml(row.source_path)}">Open Source</button>`
                    : ""
                }
              </div>
            </article>
          `;
        },
      )
      .join("");
    wirePanelActions(host);
    return;
  }
  host.innerHTML = rows
    .map(
      (row) => `
        <article class="detail-card workflow-card">
          <div class="section-head">
            <div>
              <p class="eyebrow">Coordination</p>
              <h4>${escapeHtml(formatValue(row.coordination_id))}</h4>
            </div>
            <div class="pill-row">
              <span class="pill ${pillTone(row.status)}">${escapeHtml(formatValue(row.status))}</span>
              <span class="pill ${pillTone("neutral")}">${escapeHtml(formatValue(row.assignee, "unassigned"))}</span>
            </div>
          </div>
          <p>${escapeHtml(formatValue(row.requested_action, "No requested action captured."))}</p>
          <dl class="detail-grid">
            <div>
              <dt>From</dt>
              <dd>${escapeHtml(formatValue(row.from_project))}</dd>
            </div>
            <div>
              <dt>To</dt>
              <dd>${escapeHtml(formatValue(row.to_project))}</dd>
            </div>
            <div>
              <dt>Due</dt>
              <dd>${escapeHtml(formatValue(row.due_at, "n/a"))}</dd>
            </div>
            <div>
              <dt>Ref</dt>
              <dd class="path-value">${escapeHtml(formatValue(row.source_ref, "n/a"))}</dd>
            </div>
          </dl>
          <div class="button-row">
            <button
              class="primary-button small-button"
              data-use-project="${escapeHtml(row.to_project)}"
              data-use-prompt="${escapeHtml(`Coordinate ${formatValue(row.requested_action, "the requested action")} for ${formatValue(row.to_project)}. Source project: ${formatValue(row.from_project)}. Coordination ID: ${formatValue(row.coordination_id)}.`)}"
              data-use-message="${escapeHtml(`Loaded coordination handoff for ${formatValue(row.coordination_id)}.`)}"
            >Route To Project</button>
            ${
              looksLikePath(row.source_ref)
                ? `<button class="ghost-button small-button" data-open-path="${escapeHtml(row.source_ref)}">Open Ref</button>`
                : ""
            }
          </div>
        </article>
      `,
    )
    .join("");
  wirePanelActions(host);
}

function renderPanelLoading(panel) {
  if (panel === "overview") {
    document.getElementById("overview-cards").innerHTML = Array.from({ length: 4 }, () => `
      <article class="metric-card loading-card">
        <div class="label">Loading</div>
        <div class="value">...</div>
      </article>
    `).join("");
    document.getElementById("overview-note").textContent = "Loading broker overview...";
    document.getElementById("overview-highlights").innerHTML = '<div class="empty-state">Waiting for shared broker projections...</div>';
    return;
  }
  if (panel === "projects") {
    document.getElementById("projects-list").innerHTML = '<div class="empty-state">Loading project list...</div>';
    document.getElementById("project-detail").innerHTML = '<div class="empty-state">Loading project detail...</div>';
    return;
  }
  if (panel === "review") {
    renderWorkflowTable("review-table", [], "Loading review inbox...");
    return;
  }
  if (panel === "coordination") {
    renderWorkflowTable("coordination-table", [], "Loading coordination inbox...");
    return;
  }
  if (panel === "health") {
    document.getElementById("health-stack").innerHTML = '<div class="empty-state">Loading health signals...</div>';
  }
}

function refreshOverviewIfNeeded(sourcePanel) {
  if (state.activePanel === "overview" && state.panelData.overview && (sourcePanel === "projects" || sourcePanel === "health")) {
    renderOverview(state.panelData.overview);
  }
}

async function loadPanel(panel, options = {}) {
  const logicalPanel = panelLoadMap[panel];
  if (!logicalPanel) return;
  const scopedProjectName = Object.prototype.hasOwnProperty.call(options, "projectName")
    ? options.projectName
    : state.panelScopes[panel] || "";
  if (!options.background) {
    setPanelFeedback("loading", `Loading ${panelTitles[panel]}`, "Fetching the latest projection from the shared broker.");
    renderPanelLoading(panel);
  }
  const response = await window.workspaceHubAPI.getPanel(
    scopedProjectName ? { panelName: logicalPanel, projectName: scopedProjectName } : logicalPanel,
  );
  state.panelResponses[panel] = response;
  state.panelData[panel] = response.data || {};
  const data = response.data || {};
  if (panel === "overview") {
    renderOverview(data);
  } else if (panel === "projects") {
    renderProjects(data);
  } else if (panel === "review") {
    renderWorkflowCards(
      "review-table",
      data.rows || [],
      scopedProjectName
        ? `No review items are projected for ${scopedProjectName} right now.`
        : "No review items are projected right now.",
      "review",
      "review",
    );
  } else if (panel === "coordination") {
    renderWorkflowCards(
      "coordination-table",
      data.rows || [],
      scopedProjectName
        ? `No coordination items are projected for ${scopedProjectName} right now.`
        : "No coordination items are projected right now.",
      "coordination",
      "coordination",
    );
  } else if (panel === "health") {
    renderHealth(data);
  }
  if (!options.background || state.activePanel === panel) {
    syncPanelFeedback(panel);
  }
  refreshOverviewIfNeeded(panel);
}

function renderCommandStatus(kind, message) {
  const host = document.getElementById("command-status");
  host.hidden = false;
  host.className = `panel-feedback inline-feedback ${kind}`;
  host.innerHTML = `<div class="feedback-title">${escapeHtml(message)}</div>`;
}

function renderCommandHistory() {
  const host = document.getElementById("command-history");
  if (!state.commandHistory.length) {
    host.innerHTML = '<div class="empty-state">No command results yet. Run a broker-backed Codex action to inspect stdout, stderr, and fallback behavior.</div>';
    return;
  }
  host.innerHTML = state.commandHistory
    .map(
      (entry) => `
        <article class="detail-card workflow-card compact-history-card">
          <div class="section-head">
            <div>
              <strong>${escapeHtml(entry.action)}</strong>
              <p>${escapeHtml(entry.timestamp)}</p>
            </div>
            <span class="pill ${pillTone(entry.status)}">${escapeHtml(entry.status)}</span>
          </div>
          <p>${escapeHtml(entry.summary)}</p>
          ${entry.command ? `<pre class="feedback-command">${escapeHtml(entry.command)}</pre>` : ""}
        </article>
      `,
    )
    .join("");
}

function recordCommandResult(payload, response) {
  const data = response.data || {};
  const status = response.ok ? "success" : response.unavailable ? "warning" : "error";
  const summary = response.ok
    ? data.stdout?.trim() || data.result_status || "Command completed."
    : response.stderr || "Command failed before a broker result was returned.";
  state.commandHistory.unshift({
    action: payload.action,
    status,
    summary,
    timestamp: new Date().toLocaleString(),
    command: formatCommand(data.command || response.command || []),
  });
  state.commandHistory = state.commandHistory.slice(0, 5);
  renderCommandHistory();
}

function applyCommandDefinition() {
  const form = document.getElementById("command-form");
  const action = form.elements.action.value;
  const config = commandDefinitions[action];
  const hint = document.getElementById("command-hint");
  const promptField = form.elements.prompt;
  const sessionField = form.elements.session_id;
  hint.textContent = config.description;
  promptField.disabled = !config.usesPrompt;
  sessionField.disabled = !config.usesSession;
  promptField.placeholder = config.promptPlaceholder;
  sessionField.placeholder = config.sessionPlaceholder;
}

function seedCommandCenter(projectName, prompt = "", action = "codex-exec", message = "") {
  const form = document.getElementById("command-form");
  form.elements.project_name.value = projectName;
  form.elements.action.value = action;
  if (prompt) {
    form.elements.prompt.value = prompt;
  }
  applyCommandDefinition();
  renderCommandStatus("success", message || `Command Center is now scoped to ${projectName}.`);
}

async function bootstrap() {
  const metadata = await window.workspaceHubAPI.getMetadata();
  state.metadata = metadata;
  renderMetadata(metadata);
  renderCommandReadiness();
  renderCommandHistory();
  applyCommandDefinition();
  await loadPanel("overview");
  await Promise.all([loadPanel("projects", { background: true }), loadPanel("health", { background: true })]);

  document.querySelectorAll(".nav-link").forEach((button) => {
    button.addEventListener("click", async () => {
      const panel = button.dataset.panel;
      activatePanel(panel);
      if (panelLoadMap[panel]) {
        await loadPanel(panel);
      }
    });
  });

  document.querySelectorAll("[data-load-panel]").forEach((button) => {
    button.addEventListener("click", async () => {
      await loadPanel(button.dataset.loadPanel);
    });
  });

  document.getElementById("refresh-panel").addEventListener("click", async () => {
    const panel = state.activePanel;
    if (panelLoadMap[panel]) {
      await loadPanel(panel);
    }
  });

  document.getElementById("command-form").addEventListener("submit", async (event) => {
    event.preventDefault();
    const form = new FormData(event.currentTarget);
    const payload = Object.fromEntries(form.entries());
    const output = document.getElementById("command-output");
    const config = commandDefinitions[payload.action];
    if (config.promptRequired && !String(payload.prompt || "").trim()) {
      renderCommandStatus("danger", `Prompt is required for ${payload.action}.`);
      return;
    }
    if (config.usesSession && !String(payload.session_id || "").trim()) {
      renderCommandStatus("danger", "Session ID is required for codex resume.");
      return;
    }
    renderCommandStatus("loading", `Running ${payload.action}...`);
    output.textContent = "Running...";
    const response = await window.workspaceHubAPI.runCommandCenter(payload);
    renderCommandStatus(response.ok ? "success" : "danger", response.ok ? `${payload.action} completed.` : `${payload.action} failed.`);
    output.textContent = JSON.stringify(response.data || response, null, 2);
    recordCommandResult(payload, response);
  });

  document.getElementById("command-form").elements.action.addEventListener("change", () => {
    applyCommandDefinition();
  });

  document.getElementById("open-workspace").addEventListener("click", async () => {
    const targetPath = state.metadata?.broker_workspace_root || state.metadata?.workspace_root;
    if (!targetPath) return;
    await window.workspaceHubAPI.openPath(targetPath);
  });

  document.getElementById("open-broker").addEventListener("click", async () => {
    const targetPath = state.metadata?.broker_path;
    if (!targetPath) return;
    await window.workspaceHubAPI.openPath(targetPath);
  });

  document.getElementById("clear-command-history").addEventListener("click", () => {
    state.commandHistory = [];
    renderCommandHistory();
  });
}

bootstrap();
