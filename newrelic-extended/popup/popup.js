document.addEventListener("DOMContentLoaded", () => {
  const apiStatus = document.getElementById("apiStatus");
  const apiStatusText = document.getElementById("apiStatusText");
  const accountStatus = document.getElementById("accountStatus");
  const accountStatusText = document.getElementById("accountStatusText");

  chrome.storage.local.get(["nrx_api_key", "nrx_account_id", "nrx_region"], (data) => {
    if (data.nrx_api_key) {
      apiStatus.classList.add("ok");
      apiStatusText.textContent = "API Key configured";
    } else {
      apiStatus.classList.add("warn");
      apiStatusText.textContent = "API Key not set — open Settings";
    }

    if (data.nrx_account_id) {
      accountStatus.classList.add("ok");
      accountStatusText.textContent = `Account: ${data.nrx_account_id} (${data.nrx_region || "US"})`;
    } else {
      accountStatus.classList.add("warn");
      accountStatusText.textContent = "Account ID not set (auto-detected from page)";
    }
  });

  document.getElementById("openSettings").addEventListener("click", () => {
    chrome.runtime.openOptionsPage();
  });
});
