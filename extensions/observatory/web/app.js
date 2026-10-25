"use strict";
const $ = (id) => document.getElementById(id);
const svgNS = "http://www.w3.org/2000/svg";
let state = null,
  selected = null,
  events = [],
  boot = "",
  lastID = "",
  run = 0,
  controller = null,
  token = "";
let graphKey = "";
let detailsKey = "",
  detailTree = "",
  instanceKey = "",
  eventKey = "";
const expandedHistory = new Set();
let resetNotice = "";
let runtimeReading = false;
async function readRuntime(currentRun) {
  if (runtimeReading) return;
  runtimeReading = true;
  try {
    const response = await fetch("/api/v1/runtime", { headers: { Authorization: "Bearer " + token }, cache: "no-store", signal: controller.signal });
    if (currentRun !== run) return;
    if (!response.ok) throw new Error("运行数据未配置或不可用；未知不等于零调用。");
    const data = await response.json();
    if (currentRun !== run) return;
    if (!Array.isArray(data.operations)) throw new Error("运行数据格式未知。");
    $("runtime-coverage").textContent = (data.coverage || "采集范围未知") + " · 采样 " + (data.sampled_at || "未知") + " · 丢弃详情 " + (data.dropped_details ?? "未知");
    $("runtime-operations").replaceChildren(...data.operations.slice(0, 128).map((op) => {
      const row = document.createElement("tr");
      for (const value of [op.name, op.calls, op.errors, op.inflight, Number(op.seconds || 0).toFixed(4), op.fresh ? "近期有采集" : "无近期采集／未调用"]) row.append(text("td", String(value ?? "未知")));
      return row;
    }));
  } catch (error) {
    if (currentRun === run) { $("runtime-coverage").textContent = error.message; $("runtime-operations").replaceChildren(); }
  } finally { runtimeReading = false; }
}
const text = (tag, value, className) => {
  const e = document.createElement(tag);
  e.textContent = value;
  if (className) e.className = className;
  return e;
};
const setStatus = (value, live = false) => {
  $("status").textContent = value;
  $("status").dataset.live = String(live);
};
function notice(value) {
  $("notice").hidden = !value;
  $("notice").textContent = value;
}
function updateFrame(frame, reset = false) {
  if (reset || boot !== frame.boot_id) {
    events = [];
    boot = frame.boot_id;
  }
  for (const e of frame.events || []) addEvent(e);
  state = frame.snapshot;
  if (!state.trees.some((t) => t.id === selected)) selected = null;
  $("generation").textContent = state.generation;
  for (const [id, key] of [
    ["tree-count", "trees"],
    ["nest-count", "nest_edges"],
    ["link-count", "links"],
    ["binding-count", "nest_bindings"],
    ["draining-count", "draining"],
  ])
    $(id).textContent = state.counts[key];
  $("sample-time").textContent =
    "采样于 " + new Date(frame.sampled_at).toLocaleTimeString();
  $("graph-empty").textContent = "森林中暂时没有树。";
  $("graph-empty").hidden = state.trees.length > 0;
  setStatus("已连接", true);
  notice(resetNotice);
  renderGraph();
  renderDetails();
  renderInstances();
  renderEvents();
  readRuntime(run);
}
function addEvent(event) {
  if (!events.some((e) => e.sequence === event.sequence)) events.push(event);
  events.sort((a, b) => (BigInt(a.sequence) < BigInt(b.sequence) ? 1 : -1));
  events = events.slice(0, 128);
}
function svg(tag, attrs = {}) {
  const el = document.createElementNS(svgNS, tag);
  for (const [k, v] of Object.entries(attrs)) el.setAttribute(k, v);
  return el;
}
function renderGraph() {
  if (!state) return;
  const filter = $("relationship").value;
  const key = JSON.stringify([state.trees, state.bindings, selected, filter]);
  if (key === graphKey) return;
  graphKey = key;
  const focused = document.activeElement?.dataset?.treeId;
  const graph = $("graph");
  graph.replaceChildren();
  const defs = svg("defs");
  for (const [kind, color] of [
    ["link", "#496a99"],
    ["nest_binding", "#af6544"],
  ]) {
    const marker = svg("marker", {
      id: "arrow-" + kind,
      viewBox: "0 0 10 10",
      refX: 9,
      refY: 5,
      markerWidth: 6,
      markerHeight: 6,
      orient: "auto-start-reverse",
    });
    marker.append(svg("path", { d: "M 0 0 L 10 5 L 0 10 z", fill: color }));
    defs.append(marker);
  }
  graph.append(defs);
  const positions = new Map(),
    children = new Map();
  let row = 0,
    maxDepth = 0;
  for (const t of state.trees) {
    if (!children.has(t.parent_id)) children.set(t.parent_id, []);
    children.get(t.parent_id).push(t);
  }
  function place(tree, depth) {
    const y = 30 + row++ * 107;
    positions.set(tree.id, { x: 30 + depth * 265, y });
    maxDepth = Math.max(maxDepth, depth);
    for (const c of children.get(tree.id) || []) place(c, depth + 1);
  }
  for (const root of children.get("") || []) place(root, 0);
  const width = Math.max(750, (maxDepth + 1) * 265 + 110),
    height = Math.max(380, row * 107 + 30);
  graph.setAttribute("viewBox", `0 0 ${width} ${height}`);
  graph.style.minWidth = Math.min(width, 900) + "px";
  function edge(from, to, kind, label) {
    if (filter !== "all" && filter !== kind) return;
    const a = positions.get(from),
      b = positions.get(to);
    if (!a || !b) return;
    const shift = kind === "nest_binding" ? 17 : 0;
    let d;
    if (a.x !== b.x)
      d = `M ${a.x + 205} ${a.y + 38 + shift} C ${a.x + 245} ${a.y + 38 + shift}, ${b.x - 40} ${b.y + 38 + shift}, ${b.x} ${b.y + 38 + shift}`;
    else
      d = `M ${a.x + 205} ${a.y + 38} C ${a.x + 300} ${a.y + 38}, ${b.x + 300} ${b.y + 38}, ${b.x + 205} ${b.y + 38}`;
    const emphasis = selected
      ? from === selected || to === selected
        ? " direct"
        : " distant"
      : "";
    const p = svg("path", { d, class: "edge " + kind + emphasis });
    if (kind !== "nest")
      p.setAttribute("marker-end", "url(#arrow-" + kind + ")");
    const title = svg("title");
    title.textContent = label;
    p.append(title);
    graph.append(p);
  }
  for (const n of state.nest_edges)
    edge(n.parent, n.child, "nest", `Nest ${n.parent} / ${n.child}`);
  for (const b of state.bindings)
    edge(
      b.consumer,
      b.provider,
      b.kind,
      `${b.id}: ${b.consumer}.${b.port} → ${b.provider}.${b.capability}`,
    );
  for (const tree of state.trees) {
    const p = positions.get(tree.id);
    if (!p) continue;
    const node = svg("g", {
      class: "node" + (tree.id === selected ? " selected" : ""),
      transform: `translate(${p.x} ${p.y})`,
      tabindex: "0",
      role: "button",
      "aria-label": `${tree.id}，${tree.sampled_state}`,
      "aria-pressed": String(tree.id === selected),
      "data-tree-id": tree.id,
    });
    node.append(svg("rect", { width: 205, height: 78 }));
    const name = svg("text", { x: 15, y: 26, class: "name" });
    name.textContent =
      tree.id.length > 22 ? tree.id.slice(0, 20) + "…" : tree.id;
    const meta = svg("text", { x: 15, y: 47, class: "meta" });
    meta.textContent =
      tree.type_id.length > 28 ? tree.type_id.slice(0, 26) + "…" : tree.type_id;
    const status = svg("text", { x: 15, y: 65, class: "state" });
    status.textContent = `${tree.sampled_state} / v${tree.version}`;
    const title = svg("title");
    title.textContent = tree.id;
    node.append(name, meta, status, title);
    const choose = () => {
      selected = tree.id;
      renderGraph();
      renderDetails();
    };
    node.addEventListener("click", choose);
    node.addEventListener("keydown", (e) => {
      if (e.key === "Enter" || e.key === " ") {
        e.preventDefault();
        choose();
      }
    });
    graph.append(node);
  }
  if (focused)
    graph
      .querySelector(`[data-tree-id="${CSS.escape(focused)}"]`)
      ?.focus({ preventScroll: true });
}
function renderDetails() {
  const box = $("detail-content");
  const tree = state?.trees.find((t) => t.id === selected);
  const bindings =
    state?.bindings.filter(
      (b) => b.consumer === tree?.id || b.provider === tree?.id,
    ) || [];
  const instances =
    state?.instances.filter((i) => i.tree_id === tree?.id) || [];
  const historyBefore = box.querySelector("details");
  if (historyBefore && detailTree) {
    if (historyBefore.open) expandedHistory.add(detailTree);
    else expandedHistory.delete(detailTree);
  }
  const endpointTypes = bindings.map((b) => [
    state.trees.find((t) => t.id === b.consumer),
    state.trees.find((t) => t.id === b.provider),
  ]);
  const key = JSON.stringify([tree, bindings, endpointTypes, instances]);
  if (key === detailsKey) return;
  detailsKey = key;
  const keepFocus =
    document.activeElement === historyBefore?.querySelector("summary") &&
    detailTree === tree?.id;
  const scrollTop = detailTree === tree?.id ? box.scrollTop : 0;
  detailTree = tree?.id || "";
  box.replaceChildren();
  if (!tree) {
    box.append(
      text("p", "选择图中的树。可使用 Tab 聚焦，按 Enter 选择。", "empty"),
    );
    return;
  }
  box.append(text("h3", tree.id, "tree-name"));
  const dl = document.createElement("dl");
  for (const [label, value] of [
    ["类型", tree.type_id],
    ["定义版本", tree.version],
    ["实例", tree.instance_id],
    ["采样状态", tree.sampled_state],
    ["归属", tree.parent_id || "森林根"],
  ])
    dl.append(text("dt", label), text("dd", value));
  box.append(dl);
  function section(title, items, describe) {
    box.append(text("h3", title));
    if (!items.length) {
      box.append(text("p", "无", "empty"));
      return;
    }
    const list = document.createElement("ul");
    for (const item of items) {
      const li = document.createElement("li");
      describe(li, item);
      list.append(li);
    }
    box.append(list);
  }
  section("端口", tree.ports, (li, p) => {
    li.append(
      text("strong", p.id),
      text("span", p.type),
      text(
        "span",
        `${p.required ? "必需" : "可选"} / ${p.many ? "多值" : "单值"} / ${p.phase}`,
      ),
      text(
        "span",
        p.contract_id
          ? `${p.contract_id} [${p.min_version}, ${p.max_version === "0" ? "不限" : p.max_version}]`
          : "未声明语义契约",
      ),
    );
  });
  section("能力", tree.capabilities, (li, c) => {
    li.append(
      text("strong", c.id),
      text("span", c.type),
      text("span", c.visibility.join(", ") || "private"),
      text(
        "span",
        c.contract_id
          ? `${c.contract_id} v${c.contract_version}`
          : "未声明语义契约",
      ),
    );
  });
  section("依赖关系", bindings, (li, b) => {
    const port = state.trees
      .find((t) => t.id === b.consumer)
      ?.ports.find((p) => p.id === b.port);
    const capability = state.trees
      .find((t) => t.id === b.provider)
      ?.capabilities.find((c) => c.id === b.capability);
    li.append(
      text("strong", `${b.kind} / ${b.id}`),
      text("span", `${b.consumer}.${b.port} → ${b.provider}.${b.capability}`),
      text("span", "绑定顺序 " + b.order),
      text("span", "阶段 " + (port?.phase || "未知")),
      text(
        "span",
        "需求契约 " +
          (port?.contract_id
            ? `${port.contract_id} [${port.min_version}, ${port.max_version === "0" ? "不限" : port.max_version}]`
            : "未声明"),
      ),
      text(
        "span",
        "提供契约 " +
          (capability?.contract_id
            ? `${capability.contract_id} v${capability.contract_version}`
            : "未声明"),
      ),
    );
  });
  const describeInstance = (li, i) => {
    li.append(
      text("strong", `${i.id} / ${i.state}`),
      text("span", `${i.type_id} v${i.version}`),
    );
    if (i.diagnostic)
      li.append(
        text("span", `${i.diagnostic.code} ${i.diagnostic.stage || ""}`),
      );
  };
  section(
    "实例诊断",
    instances.filter((i) => i.state !== "stopped"),
    describeInstance,
  );
  const stopped = instances.filter((i) => i.state === "stopped");
  if (stopped.length) {
    const history = document.createElement("details");
    history.open = expandedHistory.has(tree.id);
    history.append(text("summary", `已停止历史（${stopped.length}）`));
    const list = document.createElement("ul");
    for (const i of stopped) {
      const li = document.createElement("li");
      describeInstance(li, i);
      list.append(li);
    }
    history.append(list);
    box.append(history);
    if (keepFocus)
      history.querySelector("summary").focus({ preventScroll: true });
  }
  box.scrollTop = scrollTop;
}
function renderInstances() {
  if (!state) return;
  const showStopped = $("show-stopped").checked;
  const key = JSON.stringify([
    state.instances,
    state.trees.map((t) => t.instance_id),
    showStopped,
  ]);
  if (key === instanceKey) return;
  instanceKey = key;
  const current = new Set(state.trees.map((t) => t.instance_id));
  const visible = state.instances.filter(
    (i) => showStopped || i.state !== "stopped",
  );
  const body = $("instances");
  body.replaceChildren();
  for (const i of visible) {
    const row = document.createElement("tr");
    row.dataset.instanceId = i.id;
    for (const value of [
      i.id,
      i.tree_id,
      `${i.type_id} v${i.version}`,
      i.state,
      current.has(i.id) ? "当前实例" : "候选或历史",
      i.diagnostic ? `${i.diagnostic.code} ${i.diagnostic.stage || ""}` : "无",
    ])
      row.append(text("td", value));
    body.append(row);
  }
  if (!visible.length) {
    const row = document.createElement("tr"),
      cell = text("td", "没有符合筛选的实例。", "empty");
    cell.colSpan = 6;
    row.append(cell);
    body.append(row);
  }
  $("instance-summary").textContent =
    `显示 ${visible.length} 个，共 ${state.instances.length} 个实例。已停止实例默认折叠。`;
}
function renderEvents() {
  const key = JSON.stringify(events);
  if (key === eventKey) return;
  eventKey = key;
  $("event-count").textContent = events.length + " 条记录";
  const list = $("events");
  const scrollTop = list.scrollTop;
  list.replaceChildren();
  if (!events.length) {
    list.append(text("li", "暂无观察记录。", "empty"));
    return;
  }
  for (const e of events) {
    const li = document.createElement("li");
    li.dataset.sequence = e.sequence;
    li.append(
      text("span", `#${e.sequence} / G${e.generation}`),
      text("span", e.result, "result " + e.result),
    );
    const description = text(
      "div",
      e.operations.length ? e.operations.join(", ") : "空计划",
      "event-description",
    );
    description.append(
      text("small", [...e.trees, ...e.links].join(", ") || "没有拓扑操作"),
    );
    if (e.diagnostic)
      description.append(
        text("small", `${e.diagnostic.code} ${e.diagnostic.stage || ""}`),
      );
    li.append(
      description,
      text("time", new Date(e.timestamp).toLocaleTimeString()),
    );
    list.append(li);
  }
  list.scrollTop = scrollTop;
}
function consume(block) {
  let kind = "message",
    id = "",
    data = [];
  for (const line of block.split("\n")) {
    if (line.startsWith("event:")) kind = line.slice(6).trim();
    else if (line.startsWith("id:")) id = line.slice(3).trim();
    else if (line.startsWith("data:")) data.push(line.slice(5).trimStart());
  }
  if (!data.length) return;
  const value = JSON.parse(data.join("\n"));
  if (kind === "sync" || kind === "reset") {
    if (kind === "reset")
      resetNotice =
        value.reset_reason === "history_gap"
          ? "事件历史出现缺口，已恢复当前状态。"
          : "观察会话已更换，已重新同步。";
    updateFrame(value, kind === "reset");
  } else if (kind === "snapshot") updateFrame(value);
  else if (kind === "change") {
    addEvent(value);
    renderEvents();
  }
  if (id && ["sync", "reset", "change"].includes(kind)) lastID = id;
}
async function follow(currentRun) {
  let delay = 500;
  while (currentRun === run && token) {
    controller = new AbortController();
    setStatus(state ? "重新连接中，快照已过期" : "正在连接");
    try {
      const headers = { Authorization: "Bearer " + token };
      if (lastID) headers["Last-Event-ID"] = lastID;
      const response = await fetch("/api/v1/events", {
        headers,
        signal: controller.signal,
        cache: "no-store",
      });
      if (response.status === 401) {
        if (currentRun === run) {
          token = "";
          setStatus("凭据无效");
          notice("请检查观察 token，再次连接。");
          $("disconnect").disabled = true;
        }
        return;
      }
      if (!response.ok)
        throw new Error("无法读取观察接口（" + response.status + "）");
      const reader = response.body.getReader(),
        decoder = new TextDecoder();
      let buffer = "";
      delay = 500;
      while (currentRun === run) {
        const chunk = await reader.read();
        if (currentRun !== run) return;
        if (chunk.done) break;
        buffer += decoder.decode(chunk.value, { stream: true });
        buffer = buffer.replace(/\r\n/g, "\n");
        let split;
        while ((split = buffer.indexOf("\n\n")) >= 0) {
          consume(buffer.slice(0, split));
          buffer = buffer.slice(split + 2);
        }
        if (buffer.length > 16 * 1024 * 1024) throw new Error("观察数据过大");
      }
      if (currentRun === run) throw new Error("观察连接已断开");
    } catch (error) {
      if (currentRun !== run) return;
      controller.abort();
      setStatus(state ? "连接中断，快照已过期" : "连接失败");
      notice(error.message + "，正在重试。");
      $("runtime-coverage").textContent = "观察连接中断；运行数据已过期。";
      $("runtime-operations").replaceChildren();
    }
    await new Promise((resolve) => setTimeout(resolve, delay));
    delay = Math.min(delay * 2, 8000);
  }
}
$("connect-form").addEventListener("submit", (e) => {
  e.preventDefault();
  controller?.abort();
  run++;
  token = $("token").value;
  $("token").value = "";
  lastID = "";
  boot = "";
  events = [];
  resetNotice = "";
  notice("");
  $("disconnect").disabled = false;
  follow(run);
});
$("disconnect").addEventListener("click", () => {
  run++;
  token = "";
  controller?.abort();
  $("disconnect").disabled = true;
  setStatus("已断开，快照已过期");
  notice("");
  $("runtime-coverage").textContent = "已断开；运行数据已过期。";
  $("runtime-operations").replaceChildren();
});
$("relationship").addEventListener("change", renderGraph);
$("show-stopped").addEventListener("change", renderInstances);
