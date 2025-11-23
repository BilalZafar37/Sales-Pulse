// === Sidebar behavior: open/close, dropdowns, badges, memory ===
const sidebar   = document.getElementById('sidebar');
const toggleBtn = document.getElementById('toggle-btn');

// remember open/close
function remember(state){
  try { localStorage.setItem('sp_sidebar', state); } catch(e){}
  document.cookie = `sp_sidebar=${state}; path=/; max-age=31536000`;
}

// load saved state on first paint
(function initState(){
  const saved = (localStorage.getItem('sp_sidebar') || (document.cookie.match(/sp_sidebar=(\w+)/)||[])[1]);
  if(saved === 'close'){ sidebar.classList.add('close'); }
})();

toggleBtn?.addEventListener('click', () => {
  sidebar.classList.toggle('close');
  remember(sidebar.classList.contains('close') ? 'close' : 'open');
  if(sidebar.classList.contains('close')) closeAllSubMenus();
});

// toggle a dropdown
function toggleSubMenu(button){
  const li  = button.closest('li.has-flyout');
  const sub = button.nextElementSibling;
  const willOpen = !sub.classList.contains('show');

  // close others first for single-open behavior
  closeAllSubMenus();

  if (willOpen){
    sub.classList.add('show');
    button.classList.add('rotate');
    button.setAttribute('aria-expanded','true');
    li.classList.add('open');                       // used by CSS: active parent + badge swap
    // if sidebar is collapsed, expand so user can see labels
    if(sidebar.classList.contains('close')){
      sidebar.classList.remove('close');
      remember('open');
    }
  }else{
    sub.classList.remove('show');
    button.classList.remove('rotate');
    button.setAttribute('aria-expanded','false');
    li.classList.remove('open');
  }
}

// close all drops
function closeAllSubMenus(){
  document.querySelectorAll('#sidebar .sub-menu.show').forEach(s => s.classList.remove('show'));
  document.querySelectorAll('#sidebar .dropdown-btn.rotate').forEach(b => b.classList.remove('rotate'));
  document.querySelectorAll('#sidebar li.has-flyout.open').forEach(li => li.classList.remove('open'));
}

// expose toggle for inline onclick="" if you still use it
window.toggleSubMenu = toggleSubMenu;



// --------- tiny utils ---------
const qs = new URLSearchParams(window.location.search);

function getQS() { return new URLSearchParams(window.location.search); }
function setQS(q) {
  const url = `${location.pathname}?${q.toString()}`;
  window.history.replaceState({}, "", url);
}

function val(id){ return (document.getElementById(id)?.value || "").trim(); }
function setVal(id, v){ const el=document.getElementById(id); if(el){ el.value=v||""; } }

function fmtNum(n){
  const x = Number(n ?? 0);
  if (Math.abs(x) >= 1000000) return (x/1000000).toFixed(2) + "M";
  if (Math.abs(x) >= 1000) return (x/1000).toFixed(1) + "k";
  return x.toLocaleString(undefined, { maximumFractionDigits: 2 });
}
function fmtPct(n){
  if(n===null || n===undefined) return "-- %";
  return `${Number(n).toFixed(2)}%`;
}
function fmtDate(d){
  if(!d) return "—";
  try { return new Date(d).toISOString().slice(0,10); } catch(e){ return d; }
}
function relTime(d){
  if(!d) return "—";
  const t = new Date(d);
  const diff = (Date.now() - t.getTime())/1000; // seconds
  if(diff < 60) return "just now";
  if(diff < 3600) return `${Math.floor(diff/60)} min ago`;
  if(diff < 86400) return `${Math.floor(diff/3600)} h ago`;
  return fmtDate(d);
}

async function getJSON(path){
  const url = new URL(path, window.location.origin);
  const q = getQS();
  url.search = q.toString();
  const res = await fetch(url, { headers: { "Accept":"application/json" }});
  if(!res.ok) throw new Error(`HTTP ${res.status}`);
  return await res.json();
}

// --------- filters <-> querystring ---------
function setFormFromURL(){
  setVal("filter-from", qs.get("from"));
  setVal("filter-to", qs.get("to"));
  setVal("filter-brand", qs.get("brand"));
  setVal("filter-customer", qs.get("customer_id") || qs.get("sold_to"));
  setVal("filter-category", qs.get("category_id"));
  setVal("filter-sku", qs.get("sku"));
  setVal("filter-salesgroup", qs.get("sales_group"));
  setVal("filter-channel", qs.get("channel"));
  setVal("filter-site", qs.get("site"));
}

function buildQSFromForm(){
  const q = new URLSearchParams();
  if(val("filter-from")) q.set("from", val("filter-from"));
  if(val("filter-to")) q.set("to", val("filter-to"));
  if(val("filter-brand")) q.set("brand", val("filter-brand"));
  if(val("filter-customer")) q.set("customer_id", val("filter-customer"));
  if(val("filter-category")) q.set("category_id", val("filter-category"));
  if(val("filter-sku")) q.set("sku", val("filter-sku"));
  if(val("filter-salesgroup")) q.set("sales_group", val("filter-salesgroup"));
  if(val("filter-channel")) q.set("channel", val("filter-channel"));
  if(val("filter-site")) q.set("site", val("filter-site"));
  return q;
}

function resetFilters(){
  ["filter-from","filter-to","filter-brand","filter-customer","filter-category",
   "filter-sku","filter-salesgroup","filter-channel","filter-site"].forEach(id=>setVal(id,""));
  const q = new URLSearchParams(); setQS(q); loadAll();
}

// --------- interactive tables hover effect ---------
function wireInteractiveTables() {
  document.querySelectorAll('.interactive-table').forEach(table => {
    table.querySelectorAll('th, td').forEach(cell => {
      cell.addEventListener('mouseenter', () => {
        const row = cell.closest('tr'); if (row) row.classList.add('hover-row');
        const colIndex = cell.cellIndex;
        Array.from(table.querySelectorAll('tr')).forEach(r => {
          if (r.cells[colIndex]) r.cells[colIndex].classList.add('hover-col');
        });
      });
      cell.addEventListener('mouseleave', () => {
        const row = cell.closest('tr'); if (row) row.classList.remove('hover-row');
        const colIndex = cell.cellIndex;
        Array.from(table.querySelectorAll('tr')).forEach(r => {
          if (r.cells[colIndex]) r.cells[colIndex].classList.remove('hover-col');
        });
      });
    });
  });
}

// --------- KPI cards ---------
async function loadSummary(){
  const data = await getJSON("/dashboard/api/summary");

  // Card 1
  document.getElementById("kpi-sellthrough").textContent = fmtPct(data.sell_through_pct);
  document.getElementById("kpi-sellin-qty").textContent = fmtNum(data.sell_in.qty);
  document.getElementById("kpi-sellout-qty").textContent = fmtNum(data.sell_out.qty);

  // Card 2
  document.getElementById("kpi-inv-balance").textContent = fmtNum(data.inventory_balance);
  document.getElementById("kpi-customers-reporting").textContent = fmtNum(data.coverage.customers_reporting);
  document.getElementById("kpi-active-skus").textContent = fmtNum(data.coverage.active_skus);

  // Card 3
  document.getElementById("kpi-pending-uploads").textContent = fmtNum(data.reporting.pending_uploads);
  document.getElementById("kpi-last-upload").textContent = relTime(data.reporting.last_upload_at);
  document.getElementById("kpi-potential-negatives").textContent = fmtNum(data.reporting.potential_negatives);
}

// --------- charts ---------
const charts = {};

function destroyChart(id){ if(charts[id]){ try{ charts[id].destroy(); }catch(_){} charts[id]=null; } }

// working before sell-out value inttroduction
// function loadChartsMonthly(monthly){
//   destroyChart("chart1");
//   const ctx = document.getElementById("chart1").getContext("2d");

//   charts.chart1 = new Chart(ctx, {
//     type: "bar",
//     data: {
//       labels: monthly.labels,
//       datasets: [
//         { type:"bar", label:"Sell-In Value", data: monthly.sellin_value, backgroundColor:"#e3e0f8", borderColor:"#4527a0", borderWidth:1, yAxisID:"y1" },
//         { type:"line", label:"Sell-Out Qty", data: monthly.sellout_qty, borderColor:"#5c2d91", backgroundColor:"rgba(92,45,145,.15)", fill:true, tension:.35, yAxisID:"y" },
//         { type:"line", label:"Sell-Through %", data: monthly.sellthrough_pct, borderColor:"#c2185b", borderDash:[6,4], yAxisID:"y2" }
//       ]
//     },
//     options: {
//       responsive:true, maintainAspectRatio:false,
//       scales:{
//         y:   { type:"linear", position:"left", title:{display:true, text:"Qty"} },
//         y1:  { type:"linear", position:"right", grid:{ drawOnChartArea:false }, title:{display:true, text:"Value"} },
//         y2:  { type:"linear", position:"right", grid:{ drawOnChartArea:false }, min:0, max:100, ticks:{ callback:(v)=>v+"%" }, title:{display:true, text:"Sell-Through %"} }
//       },
//       plugins:{ legend:{ position:"top" } }
//     }
//   });
// }


function formatNumber(n){
  const v = Number(n ?? 0);
  if (Math.abs(v) >= 1_000_000) return (v/1_000_000).toFixed(2) + 'M';
  if (Math.abs(v) >= 1_000)     return (v/1_000).toFixed(1) + 'k';
  return v.toLocaleString();
}
function formatCurrency(n){
  const v = Number(n ?? 0);
  return v.toLocaleString(undefined, { maximumFractionDigits: 0 });
}

function loadChartsMonthly(monthly){
  destroyChart("chart1");
  const ctx = document.getElementById("chart1").getContext("2d");

  const hasSOVal = Array.isArray(monthly.sellout_value);

  charts.chart1 = new Chart(ctx, {
    data: {
      labels: monthly.labels,
      datasets: [
        // Money (grouped bars)
        {
          type: "bar",
          label: "Sell-In Value",
          data: monthly.sellin_value,
          yAxisID: "yValue",
          backgroundColor: "rgba(108, 99, 255, .18)",
          borderColor: "#6f42c1",
          borderWidth: 1.2,
          barPercentage: 0.45,
          categoryPercentage: 0.7,
          order: 1
        },
        hasSOVal && {
          type: "bar",
          label: "Sell-Out Value",
          data: monthly.sellout_value,
          yAxisID: "yValue",
          backgroundColor: "rgba(0, 123, 255, .18)",
          borderColor: "#0d6efd",
          borderWidth: 1.2,
          barPercentage: 0.45,
          categoryPercentage: 0.7,
          order: 1
        },

        // Volumes
        {
          type: "line",
          label: "Sell-Out Qty",
          data: monthly.sellout_qty,
          yAxisID: "yQty",
          borderColor: "#5c2d91",
          pointRadius: 3,
          borderWidth: 2,
          tension: .35,
          fill: false,
          order: 3
        },

        // Ratio
        {
          type: "line",
          label: "Sell-Through %",
          data: monthly.sellthrough_pct,
          yAxisID: "yPct",
          borderColor: "#c2185b",
          borderDash: [6,4],
          pointRadius: 3,
          borderWidth: 2,
          fill: false,
          order: 4
        }
      ].filter(Boolean)
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      interaction: { mode: "index", intersect: false },
      scales: {
        yQty:   { position: "left",  title: { display: true, text: "Qty"   },
                  ticks: { callback: formatNumber } },
        yValue: { position: "right", title: { display: true, text: "Value" },
                  grid: { drawOnChartArea: false },
                  ticks: { callback: formatCurrency } },
        yPct:   { position: "right", offset: true, min: 0, max: 100,
                  grid: { drawOnChartArea: false },
                  title: { display: true, text: "Sell-Through %" },
                  ticks: { callback: v => v + "%" } }
      },
      plugins: {
        legend: { position: "top" },
        tooltip: {
          mode: "index", intersect: false,
          callbacks: {
            label: (ctx) => {
              const ds = ctx.dataset;
              if (ds.yAxisID === "yPct")   return `${ds.label}: ${ctx.formattedValue}%`;
              if (ds.yAxisID === "yValue") return `${ds.label}: ${formatCurrency(ctx.raw)}`;
              return `${ds.label}: ${formatNumber(ctx.raw)}`;
            }
          }
        }
      }
    }
  });
}


function loadChartsOthers(data){
  // Category mix
  destroyChart("chart2");
  charts.chart2 = new Chart(document.getElementById("chart2"), {
    type:"doughnut",
    data:{ labels:data.category_mix.labels, datasets:[{ data:data.category_mix.values, backgroundColor:["#5c2d91","#c2185b","#00796b","#f57c00","#9c27b0","#0097a7"] }] },
    options:{ responsive:true, maintainAspectRatio:false }
  });

  // Brand mix
  destroyChart("chart3");
  charts.chart3 = new Chart(document.getElementById("chart3"), {
    type:"bar",
    data:{ labels:data.brand_mix.labels, datasets:[{ label:"Qty", data:data.brand_mix.values, backgroundColor:"#5c2d91" }] },
    options:{ responsive:true, maintainAspectRatio:false, scales:{ y:{ beginAtZero:true } } }
  });

  // Sell-through monthly only
  destroyChart("chart4");
  charts.chart4 = new Chart(document.getElementById("chart4"), {
    type:"line",
    data:{ labels:data.monthly.labels, datasets:[{ label:"Sell-Through %", data:data.monthly.sellthrough_pct, borderColor:"#c2185b", backgroundColor:"rgba(194,24,91,.15)", fill:true, tension:.35 }] },
    options:{ responsive:true, maintainAspectRatio:false, scales:{ y:{ min:0, max:100, ticks:{ callback:v=>v+"%" } } } }
  });

  // Repeat vs New
  destroyChart("chart5");
  charts.chart5 = new Chart(document.getElementById("chart5"), {
    type:"pie",
    data:{ labels:data.repeat_vs_new.labels, datasets:[{ data:data.repeat_vs_new.values, backgroundColor:["#5c2d91","#c2185b"] }] },
    options:{ responsive:true, maintainAspectRatio:false }
  });
}

async function loadCharts(){
  const data = await getJSON("/dashboard/api/charts");
  loadChartsMonthly(data.monthly);
  loadChartsOthers(data);
}

// --------- tables ---------
async function loadTableCustomers(){
  const data = await getJSON("/dashboard/api/table/customers");
  const tb = document.getElementById("table-customers-body");
  tb.innerHTML = "";
  data.rows.forEach(r=>{
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td>${r.id}</td>
      <td>${r.name ?? ""}</td>
      <td>${r.level ?? ""}</td>
      <td>${(r.orders ?? 0).toLocaleString()}</td>
      <td>${fmtNum(r.total_qty)}</td>
      <td>${fmtDate(r.last_reported)}</td>
    `;
    tb.appendChild(tr);
  });
}

async function loadTableProducts(){
  const data = await getJSON("/dashboard/api/table/products");
  const tb = document.getElementById("table-products-body");
  tb.innerHTML = "";
  data.rows.forEach(r=>{
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td>${r.id}</td>
      <td>${r.sku ?? ""}</td>
      <td>${r.product ?? ""}</td>
      <td>${r.brand ?? ""}</td>
      <td>${r.category ?? "—"}</td>
      <td>${fmtNum(r.sold_qty)}</td>
      <td>${fmtNum(r.stock)}</td>
      <td>${fmtDate(r.last_sellout)}</td>
    `;
    tb.appendChild(tr);
  });
}

// true for /dashboard and /dashboard/
const isDashboard = /^\/dashboard\/?$/.test(location.pathname);

// --------- master loader ---------
async function loadAll(){
  if (!isDashboard) return;              // <- guard
  try{
    await Promise.all([
      loadSummary(),
      loadCharts(),
      loadTableCustomers(),
      loadTableProducts()
    ]);
    wireInteractiveTables();
  }catch(err){
    console.error("Dashboard load error:", err);
  }
}

// --------- UI wiring ---------
document.addEventListener("DOMContentLoaded", () => {
  if (!isDashboard) { return}
  // animate cards
  document.querySelectorAll('.top-cards .card-custom').forEach((card, index) => {
    setTimeout(() => card.classList.add('animate'), index * 300);
  });

  setFormFromURL();
  loadAll();

  // filters
  document.getElementById("applyFiltersBtn").addEventListener("click", () => {
    const q = buildQSFromForm();
    setQS(q);
    loadAll();
  });
  document.getElementById("resetFiltersBtn").addEventListener("click", resetFilters);

  // collapse chevron
  const collapseEl = document.getElementById('filtersCollapse');
  const chevron = document.getElementById('filtersChevron');
  collapseEl.addEventListener('show.bs.collapse', () => {
    chevron.classList.remove('fa-chevron-down'); chevron.classList.add('fa-chevron-up');
  });
  collapseEl.addEventListener('hide.bs.collapse', () => {
    chevron.classList.remove('fa-chevron-up'); chevron.classList.add('fa-chevron-down');
  });

  // table toggles
  const table1 = document.getElementById("table1Wrapper");
  const table2 = document.getElementById("table2Wrapper");
  const btn1 = document.getElementById("showTable1Btn");
  const btn2 = document.getElementById("showTable2Btn");

  btn1.addEventListener("click", () => {
    table1.style.display = "block"; table2.style.display = "none";
    btn1.classList.add("btn-primary"); btn1.classList.remove("btn-outline-primary");
    btn2.classList.add("btn-outline-success"); btn2.classList.remove("btn-success");
  });
  btn2.addEventListener("click", () => {
    table1.style.display = "none"; table2.style.display = "block";
    btn2.classList.add("btn-success"); btn2.classList.remove("btn-outline-success");
    btn1.classList.add("btn-outline-primary"); btn1.classList.remove("btn-primary");
  });
});




