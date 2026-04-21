chrome.runtime.onInstalled.addListener(() => {
  console.log("NewRelic Extended installed");
});

chrome.runtime.onMessage.addListener((message, sender, sendResponse) => {
  if (message.type === "OPEN_OPTIONS") {
    chrome.runtime.openOptionsPage().catch((err) => {
      console.warn("openOptionsPage failed, falling back to tab:", err);
      chrome.tabs.create({ url: chrome.runtime.getURL("options/options.html") });
    });
  }
});
