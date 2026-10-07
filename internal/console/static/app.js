const page = document.body.dataset.page;
const state = { overview: null };

function text(value) {
  return value === undefined || value === null ? "-" : String(value);
}

function panel(title, rows) {
  const body = rows.map(([name, value]) => `<dt>${name}</dt><dd>${text(value)}</dd>`).join("");
  return `<article class="panel"><h3>${title}</h3><dl>${body}</dl></article>`;
}

function metrics(data) {
  const active = data.zones.filter((zone) => zone.phase === "active").length;
  const online = data.stations.filter((station) => station.online).length;
  const incidents = data.incidents.filter((incident) => incident.active).length;
  const published = data.plans.filter((plan) => plan.status === "published").length;
  return [["运行区域", active], ["在线换热站", online], ["活动事故", incidents], ["已发布计划", published]]
    .map(([name, value]) => `<div class="metric"><span>${name}</span><strong>${value}</strong></div>`).join("");
}

function pageContent(data) {
  if (page === "network") return data.zones.map((zone) => panel(zone.name, [["阶段", zone.phase], ["泵级", zone.pump_stage], ["旁通", zone.bypass_open ? "开启" : "关闭"], ["压差", (zone.supply_pressure - zone.return_pressure).toFixed(2) + " MPa"]])).join("");
  if (page === "stations") return data.stations.map((station) => panel(station.name, [["区域", station.zone_id], ["通信", station.online ? "在线" : "离线"], ["目标", station.target_celsius + " °C"], ["控制代际", station.controller_epoch], ["人工保持", station.hold ? "是" : "否"]])).join("");
  if (page === "incidents") return data.incidents.length ? data.incidents.map((incident) => panel(incident.kind, [["站点", incident.station_id], ["等级", incident.severity], ["状态", incident.active ? "处理中" : "已解除"], ["说明", incident.message]])).join("") : panel("当前无活动事故", [["隔离会话", data.isolations.length], ["系统状态", "稳定"]]);
  return data.plans.length ? data.plans.map((plan) => panel(plan.id.slice(0, 8), [["状态", plan.status], ["来源 revision", plan.source_revision], ["站点数", Object.keys(plan.targets || {}).length], ["说明", plan.message || "-"]])).join("") : panel("等待调度计划", [["当前 revision", Math.max(...data.stations.map((station) => station.target_revision))], ["人工保持", data.stations.filter((station) => station.hold).length]]);
}

function timelineMetrics(data) {
  const missing = data.partitions.reduce((total, partition) => total + (partition.missing || []).length, 0);
  const outOfOrder = data.partitions.reduce((total, partition) => total + partition.out_of_order, 0);
  return [["分区", data.partitions.length], ["事件", data.records.length], ["缺失水印", missing], ["乱序记录", outOfOrder]]
    .map(([name, value]) => `<div class="metric"><span>${name}</span><strong>${value}</strong></div>`).join("");
}

function timelineContent(data) {
  if (!data.records.length) return panel("暂无运维事件", [["提示", "泵阀、锁存、隔离、恢复与目标变更会在此串联"]]);
  return data.records.slice().reverse().map((record) => {
    const flags = [];
    if (record.marks && record.marks.length) flags.push("乱序");
    if (record.missing_before && record.missing_before.length) flags.push("前置缺失 " + record.missing_before.join("/"));
    return panel(`${record.partition} #${record.watermark} ${record.kind}`, [
      ["时间", record.at],
      ["实体", record.entity_id],
      ["摘要", record.summary],
      ["标记", flags.length ? flags.join("，") : "完整"],
    ]);
  }).join("");
}

async function loadTimeline() {
  const connection = document.querySelector("#connection");
  try {
    const response = await fetch("/api/v1/timeline", { cache: "no-store" });
    const payload = await response.json();
    if (!response.ok) throw new Error(payload.error || "request failed");
    document.querySelector("#summary").innerHTML = timelineMetrics(payload.data);
    document.querySelector("#content").innerHTML = timelineContent(payload.data);
    connection.textContent = "实时连接";
  } catch (error) {
    connection.textContent = "连接异常";
    document.querySelector("#content").innerHTML = panel("无法加载时间线", [["错误", error.message]]);
  }
}

async function load() {
  if (page === "timeline") return loadTimeline();
  const connection = document.querySelector("#connection");
  try {
    const response = await fetch("/api/v1/overview", { cache: "no-store" });
    const payload = await response.json();
    if (!response.ok) throw new Error(payload.error || "request failed");
    state.overview = payload.data;
    document.querySelector("#summary").innerHTML = metrics(payload.data);
    document.querySelector("#content").innerHTML = pageContent(payload.data);
    connection.textContent = "实时连接";
  } catch (error) {
    connection.textContent = "连接异常";
    document.querySelector("#content").innerHTML = panel("无法加载状态", [["错误", error.message]]);
  }
}

document.querySelector("#refresh").addEventListener("click", load);
load();
