// ===== CONFIG =====
const CLOUD_RUN_URL = "https://etl-transform-service-564878881238.asia-south1.run.app/";
const INSERT_URL = CLOUD_RUN_URL + "insert";
// ==================

const runBtn = document.getElementById("runBtn");
const runQuietBtn = document.getElementById("runQuietBtn");
const retryBtn = document.getElementById("retryBtn");
const statusPill = document.getElementById("statusPill");
const lastRunEl = document.getElementById("lastRun");
const lastResult = document.getElementById("lastResult");
const logBox = document.getElementById("logBox");
const historyList = document.getElementById("historyList");
const copyLogsBtn = document.getElementById("copyLogs");
const clearHistoryBtn = document.getElementById("clearHistory");
const methodEl = document.getElementById("method");
const toast = document.getElementById("toast");

// Utility
function toastMsg(msg, ok=true){
  toast.textContent = msg;
  toast.style.background = ok ? "#0fa36b" : "#cc2e43";
  toast.classList.add("show");
  setTimeout(()=> toast.classList.remove("show"), 2200);
}

function setStatus(text, cls="status-wait"){
  statusPill.textContent = text;
  statusPill.className = "status " + cls;
}

function ts(){ return new Date().toLocaleString(); }

function appendLog(msg){
  logBox.textContent = `[${ts()}] ${msg}\n\n` + logBox.textContent;
}

// ----- History -----
const HISTORY_KEY = "etl_dashboard_history";

function loadHistory(){ return JSON.parse(localStorage.getItem(HISTORY_KEY) || "[]"); }
function saveHistory(h){ localStorage.setItem(HISTORY_KEY, JSON.stringify(h)); }

function addHistory(entry){
  const h = loadHistory();
  h.unshift(entry);
  if(h.length > 50) h.pop();
  saveHistory(h);
  renderHistory();
}

function renderHistory(){
  const h = loadHistory();
  historyList.innerHTML = "";
  if(!h.length){
    historyList.innerHTML = `<div class="muted">No runs yet.</div>`;
    return;
  }

  h.forEach((it,i)=>{
    const div = document.createElement("div");
    div.className = "item";
    div.innerHTML = `
      <div>
        <div style="font-weight:700">${it.status} · ${it.ts}</div>
        <div class="small">${it.method} · ${it.duration_ms}ms · HTTP ${it.http_status}</div>
      </div>
      <button class="btn ghost small" data-i="${i}">View</button>
    `;
    historyList.appendChild(div);
  });

  [...historyList.querySelectorAll("button")].forEach(btn=>{
    btn.addEventListener("click", e=>{
      const idx = e.target.dataset.i;
      const h = loadHistory()[idx];
      logBox.textContent =
        `=== Run ${idx} — ${h.ts} ===\nStatus: ${h.status}\nHTTP: ${h.http_status}\nMethod: ${h.method}\nDuration: ${h.duration_ms}\n\n${h.body}\n\n`
      + logBox.textContent;
    });
  });
}

// ----- ETL TRIGGER -----
let lastRequest=null;

async function triggerETL({quiet=false, method="GET"}={}){
  try{
    setStatus("Running...", "status-wait");
    appendLog("Triggering ETL...");

    const start=performance.now();
    const res = await fetch(CLOUD_RUN_URL, {method});
    const text = await res.text();
    const duration=Math.round(performance.now()-start);

    addHistory({
      ts: ts(), status: res.ok?"SUCCESS":"FAILED", http_status: res.status,
      method, duration_ms: duration, body: text
    });

    lastRunEl.textContent = "Last run: " + ts();
    lastResult.textContent = res.ok ? "Success" : "Error";

    if(res.ok){
      toastMsg("ETL Success!");
      setStatus("Success","status-ok");
    } else {
      toastMsg("ETL Failed!", false);
      setStatus("Failed","status-fail");
    }

    appendLog(`HTTP ${res.status} → ${text}`);

  } catch(err){
    setStatus("Failed","status-fail");
    appendLog("EXCEPTION: " + err.message);
  }
}

// ----- INSERT NEW RECORD -----
document.getElementById("addRecordBtn").addEventListener("click", async()=>{

  const payload = {
    Invoice: document.getElementById("inv").value,
    StockCode: document.getElementById("stock").value,
    Description: document.getElementById("desc").value,
    Quantity: Number(document.getElementById("qty").value),
    Price: Number(document.getElementById("price").value),
    CustomerID: Number(document.getElementById("cust").value),
    Country: document.getElementById("country").value
  };

  appendLog("Submitting new record...");

  // basic validation
  if(!payload.Invoice || !payload.StockCode){
    toastMsg("Missing required fields!", false);
    return;
  }

  try{
    const res = await fetch(INSERT_URL, {
      method:"POST",
      headers:{"Content-Type":"application/json"},
      body: JSON.stringify(payload)
    });

    const text = await res.text();
    if(res.ok){
      toastMsg("Record Added!");
      appendLog("Insert OK → " + text);
    } else {
      toastMsg("Insert Failed", false);
      appendLog("Insert ERROR → " + text);
    }

  }catch(err){
    toastMsg("Insert Exception", false);
    appendLog("EXCEPTION: " + err.message);
  }
});

// ----- WIRE BUTTONS -----
runBtn.onclick = ()=> triggerETL({method:methodEl.value});
runQuietBtn.onclick = ()=> triggerETL({quiet:true, method:methodEl.value});
retryBtn.onclick = ()=> lastRequest && triggerETL(lastRequest);
copyLogsBtn.onclick = ()=> navigator.clipboard.writeText(logBox.textContent);
clearHistoryBtn.onclick = ()=>{ localStorage.removeItem(HISTORY_KEY); renderHistory(); };

// Initial load
renderHistory();
setStatus("Idle","status-wait");
