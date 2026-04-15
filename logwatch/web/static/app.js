const APP_ENDPOINTS = {
  hosts: '/api/hosts',
  containers: (host) => `/api/hosts/${encodeURIComponent(host)}/containers`,
  wsTicket: '/api/ws-ticket',
  chat: '/ws/chat',
};

const STORAGE_KEYS = {
  session: 'logwatch.chat.session',
};

const POLL_INTERVAL_MS = 15000;

const state = {
  token: '',
  sessionId: sessionStorage.getItem(STORAGE_KEYS.session) || createSessionId(),
  hosts: [],
  containersByHost: new Map(),
  selectedHost: null,
  pollHandle: null,
  ws: null,
};

sessionStorage.setItem(STORAGE_KEYS.session, state.sessionId);

const metricValues = document.querySelectorAll('.metric-value');
const refs = {};
refs.authForm = document.querySelector('#auth-form');
refs.tokenInput = document.querySelector('#token-input');
refs.refreshButton = document.querySelector('#refresh-button');
refs.pollStatus = document.querySelector('#poll-status');
refs.hostList = document.querySelector('#host-list');
refs.totalHosts = document.querySelector('#total-hosts');
refs.connectedHosts = document.querySelector('#connected-hosts');
refs.selectedTotal = metricValues.item(2);
refs.runningTotal = metricValues.item(3);
refs.selectedHostLabel = document.querySelector('#selected-host-label');
refs.containerTableBody = document.querySelector('#container-table-body');
refs.chatConnectionState = document.querySelector('#chat-connection-state');
refs.chatLog = document.querySelector('#chat-log');
refs.connectChatButton = document.querySelector('#connect-chat-button');
refs.messageForm = document.querySelector('#message-form');
refs.messageInput = document.querySelector('#message-input');

refs.tokenInput.value = state.token;

refs.authForm.addEventListener('submit', async (event) => {
  event.preventDefault();
  await applyTokenAndSync();
});

refs.refreshButton.addEventListener('click', async () => {
  await applyTokenAndSync({ reconnectChat: false });
});

refs.connectChatButton.addEventListener('click', async () => {
  syncTokenFromInput();
  if (!state.token) {
    pushSystemMessage('Enter WEB_AUTH_TOKEN before opening chat.');
    return;
  }
  await connectChat({ announce: true, force: true });
});

refs.messageForm.addEventListener('submit', async (event) => {
  event.preventDefault();
  const message = refs.messageInput.value.trim();
  if (!message) {
    return;
  }

  syncTokenFromInput();
  if (!state.token) {
    pushSystemMessage('A bearer token is required for chat.');
    return;
  }

  const ready = await connectChat({ announce: false });
  if (!ready || state.ws === null || state.ws.readyState !== WebSocket.OPEN) {
    pushSystemMessage('Chat socket is not ready yet.');
    return;
  }

  appendMessage({ role: 'user', text: message });
  state.ws.send(message);
  refs.messageInput.value = '';
});

window.addEventListener('beforeunload', () => {
  if (state.ws !== null) {
    state.ws.close(1000, 'page-unload');
  }
});

pushSystemMessage(
  'Provide WEB_AUTH_TOKEN to poll /api/hosts and open /ws/chat through one-time WS tickets.',
);

if (state.token) {
  applyTokenAndSync().catch((error) => {
    pushSystemMessage(error.message);
  });
} else {
  renderHosts();
  renderContainers();
  renderSummary();
}

async function applyTokenAndSync({ reconnectChat = true } = {}) {
  syncTokenFromInput();
  if (!state.token) {
    refs.pollStatus.textContent = 'Waiting for token';
    stopPolling();
    closeChat();
    renderHosts();
    renderContainers();
    renderSummary();
    return;
  }

  await refreshDashboard();
  startPolling();
  if (reconnectChat) {
    await connectChat({ announce: true, force: true });
  }
}

function syncTokenFromInput() {
  state.token = refs.tokenInput.value.trim();
}

async function refreshDashboard() {
  refs.pollStatus.textContent = 'Refreshing fleet status...';
  try {
    const payload = await fetchJson(APP_ENDPOINTS.hosts);
    state.hosts = Array.isArray(payload.hosts) ? payload.hosts : [];

    const selectedStillExists = state.hosts.some(
      (host) => host.name === state.selectedHost,
    );
    if (!selectedStillExists) {
      const preferredHost =
        state.hosts.find((host) => host.status === 'connected') || state.hosts[0] || null;
      state.selectedHost = preferredHost ? preferredHost.name : null;
    }

    renderHosts();
    if (state.selectedHost) {
      await refreshContainersForHost(state.selectedHost);
    } else {
      renderContainers();
      renderSummary();
    }
    refs.pollStatus.textContent = `Last sync ${formatClock(new Date())}`;
  } catch (error) {
    refs.pollStatus.textContent = error.message;
    pushSystemMessage(`Dashboard sync failed: ${error.message}`);
    throw error;
  }
}

async function refreshContainersForHost(hostName) {
  refs.selectedHostLabel.textContent = `${hostName} · loading containers...`;
  const payload = await fetchJson(APP_ENDPOINTS.containers(hostName));
  const containers = Array.isArray(payload.containers) ? payload.containers : [];
  state.selectedHost = hostName;
  state.containersByHost.set(hostName, containers);
  renderHosts();
  renderContainers();
  renderSummary();
}

function renderHosts() {
  refs.hostList.textContent = '';
  if (state.hosts.length === 0) {
    refs.hostList.appendChild(
      createEmptyState('No hosts loaded yet. Add a token to poll /api/hosts.'),
    );
    return;
  }

  for (const host of state.hosts) {
    const button = document.createElement('button');
    button.type = 'button';
    button.className = `host-card${host.name === state.selectedHost ? ' is-active' : ''}`;
    button.addEventListener('click', async () => {
      try {
        await refreshContainersForHost(host.name);
      } catch (error) {
        refs.pollStatus.textContent = error.message;
      }
    });

    const header = document.createElement('div');
    header.className = 'host-card-header';
    header.appendChild(createTextSpan('host-name', host.name || 'unknown'));
    header.appendChild(createToneBadge(host.status || 'unknown'));

    button.appendChild(header);
    button.appendChild(createTextSpan('host-url mono', host.url || ''));

    const meta = document.createElement('div');
    meta.className = 'host-card-meta';
    meta.appendChild(createTextSpan('mono', `Failures ${host.failure_count || 0}`));
    meta.appendChild(
      createTextSpan(
        'mono',
        host.last_connected_at
          ? `Last seen ${formatTimeAgo(host.last_connected_at)}`
          : 'No successful check yet',
      ),
    );
    button.appendChild(meta);

    if (host.last_error) {
      button.appendChild(createTextSpan('host-error', sanitizeErrorText(host.last_error)));
    }

    refs.hostList.appendChild(button);
  }
}

function renderContainers() {
  refs.containerTableBody.textContent = '';
  const containers = getSelectedContainers();
  if (!state.selectedHost) {
    refs.selectedHostLabel.textContent = 'No host selected';
    appendEmptyTableRow('Choose a host to inspect its containers.');
    return;
  }

  refs.selectedHostLabel.textContent = `${state.selectedHost} · ${containers.length} container(s)`;
  if (containers.length === 0) {
    appendEmptyTableRow('No containers returned for the selected host.');
    return;
  }

  for (const container of containers) {
    const row = document.createElement('tr');
    row.appendChild(createCell(container.name || container.id || 'container'));

    const statusCell = document.createElement('td');
    statusCell.appendChild(createToneBadge(container.status || 'unknown'));
    row.appendChild(statusCell);

    row.appendChild(createCell(String(container.restart_count || 0)));

    const idCell = createCell(container.id || '');
    idCell.classList.add('mono');
    row.appendChild(idCell);
    refs.containerTableBody.appendChild(row);
  }
}

function renderSummary() {
  const containers = getSelectedContainers();
  refs.totalHosts.textContent = String(state.hosts.length);
  refs.connectedHosts.textContent = String(
    state.hosts.filter((host) => host.status === 'connected').length,
  );
  refs.selectedTotal.textContent = String(containers.length);
  refs.runningTotal.textContent = String(
    containers.filter((container) => container.status === 'running').length,
  );
}

function startPolling() {
  stopPolling();
  state.pollHandle = window.setInterval(() => {
    refreshDashboard().catch(() => undefined);
  }, POLL_INTERVAL_MS);
}

function stopPolling() {
  if (state.pollHandle !== null) {
    window.clearInterval(state.pollHandle);
    state.pollHandle = null;
  }
}

async function connectChat({ announce = true, force = false } = {}) {
  if (!state.token) {
    setChatState('Offline', 'tone-warn');
    return false;
  }

  if (
    !force &&
    state.ws !== null &&
    (state.ws.readyState === WebSocket.OPEN || state.ws.readyState === WebSocket.CONNECTING)
  ) {
    return state.ws.readyState === WebSocket.OPEN;
  }

  closeChat();
  setChatState('Connecting', 'tone-warn');
  let wsTicket;
  try {
    wsTicket = await requestWsTicket();
  } catch (error) {
    setChatState('Error', 'tone-danger');
    if (announce) {
      pushSystemMessage(`WS ticket request failed: ${error.message}`);
    }
    return false;
  }
  const wsUrl = buildWsUrl(wsTicket);

  return await new Promise((resolve) => {
    let settled = false;
    const socket = new WebSocket(wsUrl);
    state.ws = socket;

    socket.addEventListener('open', () => {
      setChatState('Live', 'tone-ok');
      if (announce) {
        pushSystemMessage('Chat connected through /ws/chat.');
      }
      settled = true;
      resolve(true);
    });

    socket.addEventListener('message', (event) => {
      appendMessage({ role: 'assistant', text: String(event.data || '') });
    });

    socket.addEventListener('error', () => {
      if (!settled) {
        settled = true;
        resolve(false);
      }
      setChatState('Error', 'tone-danger');
      if (announce) {
        pushSystemMessage('Chat handshake failed. Confirm the token and retry.');
      }
    });

    socket.addEventListener('close', (event) => {
      if (state.ws === socket) {
        state.ws = null;
      }
      setChatState('Offline', event.code === 1000 ? 'tone-warn' : 'tone-danger');
      if (announce && event.code !== 1000) {
        pushSystemMessage(`Chat disconnected (${event.code}).`);
      }
      if (!settled) {
        settled = true;
        resolve(false);
      }
    });
  });
}

function closeChat() {
  if (state.ws !== null) {
    state.ws.close(1000, 'refresh');
    state.ws = null;
  }
  setChatState('Offline', 'tone-warn');
}

async function fetchJson(path) {
  const response = await fetch(path, {
    method: 'GET',
    headers: {
      Authorization: `Bearer ${state.token}`,
    },
  });

  if (!response.ok) {
    const detail = await safeReadResponse(response);
    throw new Error(`${response.status} ${response.statusText}${detail ? ` · ${detail}` : ''}`);
  }
  return await response.json();
}

async function requestWsTicket() {
  const response = await fetch(APP_ENDPOINTS.wsTicket, {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${state.token}`,
    },
  });
  if (!response.ok) {
    const detail = await safeReadResponse(response);
    throw new Error(`${response.status} ${response.statusText}${detail ? ` · ${detail}` : ''}`);
  }
  const payload = await response.json();
  const ticket = String(payload.ticket || '').trim();
  if (!ticket) {
    throw new Error('ticket response missing ticket field');
  }
  return ticket;
}

function appendMessage({ role, text }) {
  const item = document.createElement('article');
  item.className = `message ${role}`;
  item.appendChild(createTextSpan('message-role', roleLabel(role)));
  item.appendChild(createTextSpan('message-meta', formatClock(new Date())));

  const body = document.createElement('p');
  body.textContent = text;
  item.appendChild(body);

  refs.chatLog.appendChild(item);
  refs.chatLog.scrollTop = refs.chatLog.scrollHeight;
}

function pushSystemMessage(text) {
  appendMessage({ role: 'system', text });
}

function createToneBadge(status) {
  return createTextSpan(`status-pill ${toneForStatus(status)}`, status);
}

function createTextSpan(className, text) {
  const node = document.createElement('span');
  node.className = className;
  node.textContent = text;
  return node;
}

function createCell(text) {
  const cell = document.createElement('td');
  cell.textContent = text;
  return cell;
}

function appendEmptyTableRow(text) {
  const row = document.createElement('tr');
  const cell = document.createElement('td');
  cell.colSpan = 4;
  cell.appendChild(createEmptyState(text));
  row.appendChild(cell);
  refs.containerTableBody.appendChild(row);
}

function createEmptyState(text) {
  const empty = document.createElement('div');
  empty.className = 'empty-state';
  empty.textContent = text;
  return empty;
}

function getSelectedContainers() {
  if (!state.selectedHost) {
    return [];
  }
  return state.containersByHost.get(state.selectedHost) || [];
}

function toneForStatus(status) {
  const normalized = String(status || 'unknown').toLowerCase();
  if (['connected', 'running', 'healthy', 'ok'].includes(normalized)) {
    return 'tone-ok';
  }
  if (['created', 'paused', 'restarting', 'offline'].includes(normalized)) {
    return 'tone-warn';
  }
  return 'tone-danger';
}

function roleLabel(role) {
  if (role === 'user') {
    return 'Operator';
  }
  if (role === 'assistant') {
    return 'LogWatch Agent';
  }
  return 'System';
}

function setChatState(text, toneClass) {
  refs.chatConnectionState.className = `chat-indicator ${toneClass}`;
  refs.chatConnectionState.textContent = text;
}

function buildWsUrl(ticket) {
  const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
  const url = new URL(APP_ENDPOINTS.chat, `${protocol}//${window.location.host}`);
  url.searchParams.set('session', state.sessionId);
  url.searchParams.set('ticket', String(ticket || ''));
  return url.toString();
}

function createSessionId() {
  if (window.crypto && typeof window.crypto.randomUUID === 'function') {
    return window.crypto.randomUUID();
  }
  return `session-${Date.now()}`;
}

function formatTimeAgo(value) {
  const timestamp = Date.parse(value);
  if (Number.isNaN(timestamp)) {
    return value;
  }
  const diffMs = Date.now() - timestamp;
  const diffMinutes = Math.max(0, Math.round(diffMs / 60000));
  if (diffMinutes < 1) {
    return 'just now';
  }
  if (diffMinutes < 60) {
    return `${diffMinutes}m ago`;
  }
  const diffHours = Math.round(diffMinutes / 60);
  if (diffHours < 24) {
    return `${diffHours}h ago`;
  }
  return `${Math.round(diffHours / 24)}d ago`;
}

function formatClock(date) {
  return new Intl.DateTimeFormat(undefined, {
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
  }).format(date);
}

async function safeReadResponse(response) {
  const contentType = response.headers.get('content-type') || '';
  if (contentType.includes('application/json')) {
    try {
      const payload = await response.json();
      return sanitizeErrorText(payload.detail || JSON.stringify(payload));
    } catch {
      return '';
    }
  }
  return sanitizeErrorText((await response.text()).trim());
}

function sanitizeErrorText(value) {
  const text = String(value || '');
  return text
    .replace(/bearer\s+\S+/gi, 'Bearer ***REDACTED***')
    .replace(/\b(api[_-]?key|password|token)\b\s*[:=]\s*\S+/gi, '$1=***REDACTED***')
    .slice(0, 256);
}
