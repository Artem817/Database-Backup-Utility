const state = {
  backups: [],
  jobs: [],
  selectedJobId: null,
};

const views = {
  dashboard: document.querySelector("#dashboard-view"),
  backup: document.querySelector("#backup-view"),
  validation: document.querySelector("#validation-view"),
  jobs: document.querySelector("#jobs-view"),
};

const viewTitles = {
  dashboard: "Dashboard",
  backup: "Run Backup",
  validation: "Restore Check",
  jobs: "Jobs",
};

function qs(selector, root = document) {
  return root.querySelector(selector);
}

function qsa(selector, root = document) {
  return Array.from(root.querySelectorAll(selector));
}

async function api(path, options = {}) {
  const response = await fetch(path, {
    headers: {
      "content-type": "application/json",
      ...(options.headers || {}),
    },
    ...options,
  });
  const payload = await response.json();
  if (!response.ok) {
    throw new Error(payload.error || "Request failed");
  }
  return payload;
}

function showView(name) {
  Object.entries(views).forEach(([key, element]) => {
    element.classList.toggle("active", key === name);
  });
  qsa(".nav-item").forEach((button) => {
    button.classList.toggle("active", button.dataset.view === name);
  });
  qs("#view-title").textContent = viewTitles[name];
}

function badge(value) {
  const normalized = String(value || "unknown").toLowerCase();
  return `<span class="badge ${escapeHtml(normalized)}">${escapeHtml(value || "unknown")}</span>`;
}

function escapeHtml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

function formatBytes(value) {
  const bytes = Number(value || 0);
  if (!bytes) return "0 B";
  const units = ["B", "KB", "MB", "GB", "TB"];
  const index = Math.min(Math.floor(Math.log(bytes) / Math.log(1024)), units.length - 1);
  return `${(bytes / 1024 ** index).toFixed(index === 0 ? 0 : 1)} ${units[index]}`;
}

function formatDate(value) {
  if (!value) return "n/a";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  return date.toLocaleString();
}

function formPayload(form) {
  const formData = new FormData(form);
  const payload = {};
  for (const [key, value] of formData.entries()) {
    payload[key] = value;
  }
  qsa("input[type='checkbox']", form).forEach((input) => {
    payload[input.name] = input.checked;
  });
  return payload;
}

async function refreshAll() {
  await Promise.all([loadHealth(), loadCatalog(), loadJobs()]);
}

async function loadHealth() {
  const payload = await api("/api/health");
  qs("#health-status").textContent = payload.status;
  qs("#catalog-path").textContent = payload.catalog;
}

async function loadCatalog() {
  const payload = await api("/api/catalog");
  state.backups = payload.backups || [];
  renderCatalog();
  renderMetrics();
}

async function loadJobs() {
  const payload = await api("/api/jobs");
  state.jobs = payload.jobs || [];
  renderJobs();
  renderMetrics();
  if (state.selectedJobId) {
    const selected = state.jobs.find((job) => job.id === state.selectedJobId);
    if (selected) renderJobLog(selected);
  }
}

function renderMetrics() {
  const completed = state.backups.filter((item) => item.status === "completed").length;
  const failed = state.backups.filter((item) => item.status === "failed").length;
  const restorePassed = state.backups.filter((item) => item.restore_validation_state === "passed" || item.recovery_status === "passed").length;
  const running = state.jobs.filter((job) => ["queued", "running"].includes(job.status)).length;

  qs("#metric-completed").textContent = completed;
  qs("#metric-failed").textContent = failed;
  qs("#metric-restore").textContent = restorePassed;
  qs("#metric-running").textContent = running;
  qs("#catalog-count").textContent = `${state.backups.length} backups`;
}

function renderCatalog() {
  const list = qs("#backup-list");
  if (!state.backups.length) {
    list.innerHTML = `<div class="empty-state">No backups in catalog.</div>`;
    return;
  }

  list.innerHTML = state.backups.map((backup) => {
    const restoreState = backup.restore_validation_state || backup.recovery_status || "not_tested";
    return `
      <article class="backup-row">
        <div class="item-title">
          <strong>${escapeHtml(backup.id || "unknown")}</strong>
          <span>${escapeHtml(backup.database_name || "database")} · ${escapeHtml(backup.engine || "engine")} · ${escapeHtml(backup.backup_type || "backup")}</span>
        </div>
        <div>${badge(backup.status)}</div>
        <div>${badge(restoreState)}</div>
        <div class="backup-meta">
          <div>${escapeHtml(formatBytes(backup.backup_size_bytes))}</div>
          <div>${escapeHtml(formatDate(backup.timestamp_start))}</div>
        </div>
        <div class="row-actions">
          <button class="secondary-button" data-document="metadata" data-backup-id="${escapeHtml(backup.id)}">Metadata</button>
          <button class="secondary-button" data-document="validation" data-backup-id="${escapeHtml(backup.id)}">Validation</button>
        </div>
      </article>
    `;
  }).join("");
}

function renderJobs() {
  const list = qs("#job-list");
  if (!state.jobs.length) {
    list.innerHTML = `<div class="empty-state">No jobs yet.</div>`;
    renderJobLog(null);
    return;
  }

  list.innerHTML = state.jobs.map((job) => `
    <button class="job-row ${job.id === state.selectedJobId ? "active" : ""}" data-job-id="${escapeHtml(job.id)}">
      <div class="item-title">
        <strong>${escapeHtml(job.title)} · ${escapeHtml(job.id)}</strong>
        <span>${escapeHtml(formatDate(job.created_at))}</span>
      </div>
      <div>${badge(job.status)}</div>
    </button>
  `).join("");

  if (!state.selectedJobId && state.jobs[0]) {
    state.selectedJobId = state.jobs[0].id;
    renderJobs();
    return;
  }
}

function renderJobLog(job) {
  const title = qs("#log-title");
  const status = qs("#log-status");
  const log = qs("#job-log");

  if (!job) {
    title.textContent = "Job Log";
    status.textContent = "idle";
    log.textContent = "";
    return;
  }

  title.textContent = `${job.title} · ${job.id}`;
  status.textContent = job.status;
  const lines = (job.logs || []).map((entry) => {
    return `[${formatDate(entry.time)}] ${entry.level.toUpperCase()} ${entry.message}`;
  });
  if (job.error) {
    lines.push(`ERROR ${job.error}`);
  }
  if (job.result) {
    lines.push("");
    lines.push(JSON.stringify(job.result, null, 2));
  }
  log.textContent = lines.join("\n");
  log.scrollTop = log.scrollHeight;
}

async function submitBackup(event) {
  event.preventDefault();
  const payload = formPayload(event.currentTarget);
  const response = await api("/api/backup/start", {
    method: "POST",
    body: JSON.stringify(payload),
  });
  state.selectedJobId = response.job.id;
  showView("jobs");
  await loadJobs();
}

async function submitValidation(event) {
  event.preventDefault();
  const payload = formPayload(event.currentTarget);
  const response = await api("/api/restore/validate", {
    method: "POST",
    body: JSON.stringify(payload),
  });
  state.selectedJobId = response.job.id;
  showView("jobs");
  await loadJobs();
}

async function openDocument(backupId, documentType) {
  const payload = await api(`/api/backup/document?id=${encodeURIComponent(backupId)}&type=${encodeURIComponent(documentType)}`);
  qs("#document-title").textContent = `${documentType}.json`;
  qs("#document-viewer").textContent = JSON.stringify(payload.document, null, 2);
  qs("#document-dialog").showModal();
}

function updateEngineMode() {
  const engine = qs("#backup-engine").value;
  const form = qs("#backup-form");
  form.classList.toggle("engine-mysql", engine === "mysql");
  qs("input[name='port']", form).value = engine === "mysql" ? "3306" : "5432";
}

function wireEvents() {
  qsa(".nav-item").forEach((button) => {
    button.addEventListener("click", () => showView(button.dataset.view));
  });

  qs("#refresh-button").addEventListener("click", refreshAll);
  qs("#backup-form").addEventListener("submit", submitBackup);
  qs("#validation-form").addEventListener("submit", submitValidation);
  qs("#backup-engine").addEventListener("change", updateEngineMode);
  qs("#document-close").addEventListener("click", () => qs("#document-dialog").close());

  document.addEventListener("click", (event) => {
    const documentButton = event.target.closest("[data-document]");
    if (documentButton) {
      openDocument(documentButton.dataset.backupId, documentButton.dataset.document);
      return;
    }

    const jobButton = event.target.closest("[data-job-id]");
    if (jobButton) {
      state.selectedJobId = jobButton.dataset.jobId;
      const job = state.jobs.find((item) => item.id === state.selectedJobId);
      renderJobs();
      renderJobLog(job);
    }
  });
}

wireEvents();
updateEngineMode();
refreshAll().catch((error) => {
  qs("#health-status").textContent = "offline";
  qs("#backup-list").innerHTML = `<div class="empty-state">${escapeHtml(error.message)}</div>`;
});
setInterval(() => {
  loadJobs().catch(() => {});
}, 2000);
setInterval(() => {
  loadCatalog().catch(() => {});
}, 8000);
