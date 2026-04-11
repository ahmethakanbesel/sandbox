document.addEventListener("DOMContentLoaded", () => {
  const apiKeyInput = document.getElementById("apiKey");
  const accountIdInput = document.getElementById("accountId");
  const regionSelect = document.getElementById("region");
  const saveBtn = document.getElementById("saveBtn");
  const status = document.getElementById("status");
  const toggleKey = document.getElementById("toggleKey");

  // Load saved settings
  chrome.storage.local.get(["nrx_api_key", "nrx_account_id", "nrx_region"], (data) => {
    if (data.nrx_api_key) apiKeyInput.value = data.nrx_api_key;
    if (data.nrx_account_id) accountIdInput.value = data.nrx_account_id;
    if (data.nrx_region) regionSelect.value = data.nrx_region;
  });

  // Toggle API key visibility
  toggleKey.addEventListener("click", () => {
    const isPassword = apiKeyInput.type === "password";
    apiKeyInput.type = isPassword ? "text" : "password";
    toggleKey.textContent = isPassword ? "hide" : "show";
  });

  // Save settings
  saveBtn.addEventListener("click", () => {
    chrome.storage.local.set(
      {
        nrx_api_key: apiKeyInput.value.trim(),
        nrx_account_id: accountIdInput.value.trim(),
        nrx_region: regionSelect.value,
      },
      () => {
        status.classList.add("visible");
        setTimeout(() => {
          status.classList.remove("visible");
        }, 2500);
      }
    );
  });
});
