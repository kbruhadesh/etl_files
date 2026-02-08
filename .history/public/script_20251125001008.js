
const CLOUD_RUN_URL = "https://etl-transform-service-564878881238.asia-south1.run.app/";

const runBtn = document.getElementById("runETL");
const statusBox = document.getElementById("statusBox");
const logOutput = document.getElementById("logOutput");

function setStatus(text) {
  statusBox.textContent = "Status: " + text;
}

function log(msg) {
  const ts = new Date().toLocaleString();
  logOutput.textContent = `[${ts}] ${msg}\n\n` + logOutput.textContent;
}

runBtn.addEventListener("click", async () => {
  setStatus("Running...");
  log("Triggering ETL...");

  try {
    const res = await fetch(CLOUD_RUN_URL);   // ← FIXED

    const text = await res.text();
    if (res.ok) {
      setStatus("Success");
      log("SUCCESS → " + text);
    } else {
      setStatus("Failed");
      log("ERROR → HTTP " + res.status + ": " + text);
    }
  } catch (err) {
    setStatus("Failed");
    log("FETCH ERROR → " + err.message);
  }
});
