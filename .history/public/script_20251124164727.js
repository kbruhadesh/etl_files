const runBtn = document.getElementById("runETL");
const statusBox = document.getElementById("statusBox");
const logOutput = document.getElementById("logOutput");

const CLOUD_RUN_URL = "https://etl-transform-service-564878881238.asia-south1.run.app/";

runBtn.addEventListener("click", async () => {
  statusBox.textContent = "Status: Running ETL...";
  logOutput.textContent = "Triggering ETL...";

  try {
    const response = await fetch(CLOUD_RUN_URL);

    const text = await response.text();

    statusBox.textContent = "Status: Completed ✔";
    logOutput.textContent += "\n\n--- RESPONSE ---\n" + text;

  } catch (error) {
    statusBox.textContent = "Status: Failed ✖";
    logOutput.textContent += "\n\n--- ERROR ---\n" + error;
  }
});
