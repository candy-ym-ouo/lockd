const state = { locks: [], selected: null, token: '', events: [], expired: 0, source: null };
const $ = (selector) => document.querySelector(selector);

async function api(path, options = {}) {
  const response = await fetch(path, {
    ...options,
    headers: { 'Content-Type': 'application/json', ...(options.headers || {}) },
  });
  const payload = await response.json();
  if (!response.ok || (payload && payload.code && payload.code !== 0)) {
    throw new Error(payload.msg || `请求失败 (${response.status})`);
  }
  return payload && Object.prototype.hasOwnProperty.call(payload, 'data') ? payload.data : payload;
}

function namespace() { return $('#namespace').value.trim() || 'default'; }
function escapeHTML(value) {
  return String(value ?? '').replace(/[&<>'"]/g, char => ({ '&':'&amp;', '<':'&lt;', '>':'&gt;', "'":'&#39;', '"':'&quot;' })[char]);
}
function toast(message) {
  const element = $('#toast');
  element.textContent = message;
  element.classList.add('show');
  clearTimeout(toast.timer);
  toast.timer = setTimeout(() => element.classList.remove('show'), 2600);
}
function remaining(expiresAt) {
  if (!expiresAt) return '—';
  const seconds = Math.max(0, Math.ceil((new Date(expiresAt) - Date.now()) / 1000));
  return `${String(Math.floor(seconds / 60)).padStart(2, '0')}:${String(seconds % 60).padStart(2, '0')}`;
}

async function loadLocks(silent = false) {
  try {
    state.locks = await api(`/api/v1/locks?namespace=${encodeURIComponent(namespace())}`);
    renderLocks();
    if (state.selected) await selectLock(state.selected.namespace, state.selected.name, true);
  } catch (error) {
    if (!silent) toast(error.message);
  }
}

function renderLocks() {
  $('#total').textContent = state.locks.length;
  $('#held').textContent = state.locks.filter(item => item.state === 'held').length;
  $('#waiters').textContent = state.locks.reduce((sum, item) => sum + item.queue_length, 0);
  $('#expired').textContent = state.expired;
  $('#locks').innerHTML = state.locks.map(item => `
    <tr data-ns="${escapeHTML(item.namespace)}" data-name="${escapeHTML(item.name)}">
      <td>${escapeHTML(item.full_name)}</td>
      <td><span class="badge ${item.state}">${item.state === 'held' ? '● 持锁' : '○ 空闲'}</span></td>
      <td>${escapeHTML(item.holder || '—')}</td>
      <td class="countdown" data-expires="${escapeHTML(item.expires_at || '')}">${remaining(item.expires_at)}</td>
      <td>${item.queue_length}</td><td>›</td>
    </tr>`).join('') || '<tr><td colspan="6">暂无锁，点击“创建锁”开始。</td></tr>';
  document.querySelectorAll('#locks tr[data-name]').forEach(row => row.onclick = () => selectLock(row.dataset.ns, row.dataset.name));
}

async function selectLock(ns, name, silent = false) {
  try {
    const item = await api(`/api/v1/locks/${encodeURIComponent(ns)}/${encodeURIComponent(name)}`);
    state.selected = item;
    renderDetail(item);
  } catch (error) {
    state.selected = null;
    $('#detail').className = 'detail empty';
    $('#detail').textContent = '锁已不存在';
    if (!silent) toast(error.message);
  }
}

function renderDetail(item) {
  const queueRows = (item.queue || []).map(waiter => `<tr><td>${waiter.seq}</td><td>${escapeHTML(waiter.holder)}</td><td>${escapeHTML(waiter.request_id || '—')}</td><td>${new Date(waiter.waiting_since).toLocaleTimeString()}</td></tr>`).join('');
  $('#detail').className = 'detail';
  $('#detail').innerHTML = `
    <div class="detail-grid">
      <div class="datum"><small>完整名称</small>${escapeHTML(item.full_name)}</div>
      <div class="datum"><small>持有者</small>${escapeHTML(item.holder || '—')}</div>
      <div class="datum"><small>令牌</small>${escapeHTML(item.token_hint || '—')}</div>
      <div class="datum"><small>重入深度</small>${item.depth} / ${item.max_depth}</div>
      <div class="datum"><small>版本</small>${item.version}</div>
      <div class="datum"><small>过期时间</small>${item.expires_at ? new Date(item.expires_at).toLocaleString() : '—'}</div>
    </div>
    <div class="actions">
      <button class="primary" id="acquireLock">获取锁</button>
      <button id="releaseLock" ${state.token ? '' : 'disabled'}>释放本页持有锁</button>
      <button class="danger" id="stealLock">强占</button>
      <button class="danger" id="deleteLock">删除</button>
    </div>
    <div class="queue"><h3>等待队列</h3><table><thead><tr><th>序号</th><th>持有者</th><th>请求 ID</th><th>开始等待</th></tr></thead><tbody>${queueRows || '<tr><td colspan="4">队列为空</td></tr>'}</tbody></table></div>`;
  $('#acquireLock').onclick = acquireSelected;
  $('#releaseLock').onclick = releaseSelected;
  $('#stealLock').onclick = stealSelected;
  $('#deleteLock').onclick = deleteSelected;
}

async function acquireSelected() {
  const holder = prompt('持有者名称', 'web-demo');
  if (!holder) return;
  try {
    const result = await api(`/api/v1/locks/${state.selected.namespace}/${state.selected.name}/acquire`, { method:'POST', body:JSON.stringify({ holder, ttl:'30s', wait:false }) });
    state.token = result.token;
    toast(`获取成功，token ${result.token.slice(0, 10)}…`);
    await loadLocks(true);
  } catch (error) { toast(error.message); }
}

async function releaseSelected() {
  if (!state.token) return toast('本页面没有可用 token');
  try {
    await api(`/api/v1/locks/${state.selected.namespace}/${state.selected.name}/release`, { method:'POST', body:JSON.stringify({ token:state.token }) });
    state.token = '';
    toast('释放成功');
    await loadLocks(true);
  } catch (error) { toast(error.message); }
}

async function stealSelected() {
  const forceToken = prompt('请输入 force token');
  if (!forceToken) return;
  try {
    const result = await api(`/api/v1/locks/${state.selected.namespace}/${state.selected.name}/steal`, { method:'POST', headers:{'X-Force-Token':forceToken}, body:JSON.stringify({ holder:'web-ops', ttl:'30s', reason:'console' }) });
    state.token = result.token;
    toast('强占成功，新 token 已保存到本页');
    await loadLocks(true);
  } catch (error) { toast(error.message); }
}

async function deleteSelected() {
  if (!confirm(`确定删除 ${state.selected.full_name}？`)) return;
  const headers = {};
  if (state.selected.state === 'held') headers['X-Force-Token'] = prompt('持锁中，请输入 force token') || '';
  try {
    await api(`/api/v1/locks/${state.selected.namespace}/${state.selected.name}`, { method:'DELETE', headers });
    state.selected = null; state.token = '';
    $('#detail').className = 'detail empty'; $('#detail').textContent = '选择一把锁查看详情';
    toast('删除成功'); await loadLocks(true);
  } catch (error) { toast(error.message); }
}

async function createLock() {
  const name = prompt('锁名称', 'demo-lock');
  if (!name) return;
  try {
    await api('/api/v1/locks', { method:'POST', body:JSON.stringify({ namespace:namespace(), name, reentrant:true, max_depth:64, ttl:'30s' }) });
    toast('创建成功'); await loadLocks(true);
  } catch (error) { toast(error.message); }
}

function connectEvents() {
  if (state.source) state.source.close();
  const source = new EventSource(`/api/v1/events?namespace=${encodeURIComponent(namespace())}`);
  state.source = source;
  source.onopen = () => { $('#connection').textContent = '● 实时事件已连接'; $('#connection').classList.add('online'); };
  source.onerror = () => { $('#connection').textContent = '事件流重连中…'; $('#connection').classList.remove('online'); };
  source.addEventListener('lock', message => {
    const event = JSON.parse(message.data);
    if (event.event === 'expired') state.expired++;
    state.events.unshift(event); state.events = state.events.slice(0, 100);
    renderEvents(); clearTimeout(connectEvents.refreshTimer);
    connectEvents.refreshTimer = setTimeout(() => loadLocks(true), 1000);
  });
}

function renderEvents() {
  $('#events').innerHTML = state.events.map(event => `<li><time>${new Date(event.at).toLocaleTimeString()}</time><strong class="${event.event}">${escapeHTML(event.event)}</strong><br>${escapeHTML(event.lock)} · ${escapeHTML(event.reason)}</li>`).join('') || '<li>等待事件…</li>';
  $('#expired').textContent = state.expired;
}

$('#refresh').onclick = () => loadLocks();
$('#create').onclick = createLock;
$('#clearEvents').onclick = () => { state.events = []; renderEvents(); };
$('#namespace').onchange = () => { state.selected = null; state.token = ''; loadLocks(); connectEvents(); };
setInterval(() => document.querySelectorAll('.countdown').forEach(cell => cell.textContent = remaining(cell.dataset.expires)), 1000);
loadLocks(); renderEvents(); connectEvents();
