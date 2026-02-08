document.getElementById("addRecordBtn").addEventListener("click", async () => {
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

  try {
    const res = await fetch(INSERT_URL, {
      method: "POST",
      headers: {"Content-Type": "application/json"},
      body: JSON.stringify(payload)
    });

    const text = await res.text();
    if (res.ok) {
      setStatus("Success", "status-ok");
      appendLog("Record Added → " + text);
    } else {
      setStatus("Error", "status-fail");
      appendLog("Insert Error → HTTP " + res.status + ": " + text);
    }
  } catch (err) {
    setStatus("Error", "status-fail");
    appendLog("EXCEPTION → " + err.message);
  }
});


// ===== CONFIG =====
const CLOUD_RUN_URL = "https://etl-transform-service-564878881238.asia-south1.run.app/";
// ==================

// Get UI elements
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

// These two DO NOT exist in your HTML — so we skip them
const timeoutEl = { value: 30 };
const serviceUrlShort = null;

serviceUrlShort && (serviceUrlShort.textContent = CLOUD_RUN_URL.replace(/^https?:\/\//, "").replace(/\/$/, ""));

// Utility
function setStatus(text, cls="status-wait"){ statusPill.textContent = text; statusPill.className = "status " + cls; }
function ts(){ return new Date().toLocaleString(); }

function appendLog(msg){
  logBox.textContent = `[${ts()}] ${msg}\n\n` + logBox.textContent;
}

// History in localStorage
const HISTORY_KEY = "etl_dashboard_history";
function loadHistory(){ return JSON.parse(localStorage.getItem(HISTORY_KEY) || "[]"); }
function saveHistory(h){ localStorage.setItem(HISTORY_KEY, JSON.stringify(h)); }
function addHistory(entry){
  const h = loadHistory();
  h.unshift(entry);
  if(h.length>50) h.pop();
  saveHistory(h);
  renderHistory();
}

function renderHistory(){
  const h = loadHistory();
  historyList.innerHTML = "";
  if(h.length===0){ historyList.innerHTML = `<div class="muted">No runs yet.</div>`; return; }
  h.forEach((it, i) => {
    const div = document.createElement("div");
    div.className = "item";
    div.innerHTML = `<div>
        <div style="font-weight:700">${it.status} · ${it.ts}</div>
        <div class="small">${it.method} · ${it.duration_ms}ms · HTTP ${it.http_status||"-"}</div>
      </div>
      <div>
        <button data-i="${i}" class="btn ghost small view">View</button>
      </div>`;
    historyList.appendChild(div);
  });
  // attach listeners
  [...historyList.querySelectorAll("button.view")].forEach(b=>{
    b.addEventListener("click", (e)=>{
      const idx = Number(e.currentTarget.dataset.i);
      const h = loadHistory()[idx];
      logBox.textContent = `=== Run ${idx} — ${h.ts} ===\nStatus: ${h.status}\nHTTP: ${h.http_status}\nMethod: ${h.method}\nDuration: ${h.duration_ms} ms\n\nResponse:\n${h.body}\n\n` + logBox.textContent;
      lastRunEl.textContent = "Last run: " + h.ts;
      lastResult.textContent = `Last: ${h.status} · HTTP ${h.http_status||"-"}`;
    });
  });
}

// Main trigger
let lastRequest = null;
async function triggerETL({quiet=false, method="GET"}={}){
  try{
    setStatus("Running...", "status-wait");
    appendLog("Triggering ETL...");
    const t = Number(timeoutEl.value || 30) * 1000;
    const controller = new AbortController();
    const id = setTimeout(()=>controller.abort(), t);
    const start = performance.now();

    let res;
    if(method === "GET"){
      res = await fetch(CLOUD_RUN_URL, {method:"GET", signal: controller.signal});
    } else {
      // ---------- UPDATED POST BODY ----------
      res = await fetch(CLOUD_RUN_URL, {
        method:"POST",
        headers: {"Content-Type":"application/json"},
        body: JSON.stringify({
          trigger: "manual",
          module: document.getElementById("module").value,
          ts: Date.now()
        }),
        signal: controller.signal
      });
      // ---------------------------------------
    }

    clearTimeout(id);
    const duration = Math.round(performance.now() - start);
    const text = await res.text().catch(()=>"[non-text-response]");

    const entry = {
      ts: ts(),
      status: res.ok ? "SUCCESS" : "FAILED",
      http_status: res.status,
      method,
      duration_ms: duration,
      body: text
    };
    lastRequest = {method};
    addHistory(entry);

    lastRunEl.textContent = "Last run: " + entry.ts;
    lastResult.textContent = entry.status + " · HTTP " + entry.http_status;

    if(res.ok){
      setStatus("Success", "status-ok");
      appendLog("SUCCESS: HTTP " + res.status + " — " + (text||"(empty)"));
    } else {
      setStatus("Error", "status-fail");
      appendLog("ERROR: HTTP " + res.status + " — " + (text||"(empty)"));
    }
    if(!quiet){
      logBox.textContent = `=== HTTP ${res.status} ===\n${text}\n\n` + logBox.textContent;
    }
  } catch (err){
    setStatus("Failed", "status-fail");
    appendLog("EXCEPTION: " + (err.message||String(err)));
    addHistory({ts:ts(), status:"EXCEPTION", method, duration_ms:0, http_status:null, body:err.message||String(err)});
  }
}

// UI wiring
runBtn.addEventListener("click", ()=> triggerETL({quiet:false, method: methodEl.value}));
runQuietBtn.addEventListener("click", ()=> triggerETL({quiet:true, method: methodEl.value}));
retryBtn.addEventListener("click", ()=> {
  if(lastRequest) triggerETL({quiet:false, method: lastRequest.method});
  else appendLog("No last request to retry.");
});
copyLogsBtn.addEventListener("click", async ()=>{
  try{
    await navigator.clipboard.writeText(logBox.textContent);
    appendLog("Logs copied to clipboard.");
  }catch(e){ appendLog("Copy failed: " + (e.message||e)); }
});
clearHistoryBtn.addEventListener("click", ()=> {
  localStorage.removeItem(HISTORY_KEY);
  renderHistory();
  appendLog("History cleared.");
});

// initial
renderHistory();
setStatus("Idle", "status-wait");
serviceUrlShort && (serviceUrlShort.textContent = CLOUD_RUN_URL.replace(/^https?:\/\//, "").replace(/\/$/, ""));
