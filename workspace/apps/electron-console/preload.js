const { contextBridge, ipcRenderer } = require("electron");

contextBridge.exposeInMainWorld("workspaceHubAPI", {
  getMetadata: () => ipcRenderer.invoke("app:metadata"),
  getCodexModelSettings: () => ipcRenderer.invoke("broker:codex-models", {}),
  saveCodexModelSettings: (settings = {}) => ipcRenderer.invoke("broker:codex-models", { settings }),
  getPanel: (panelName) => ipcRenderer.invoke("broker:panel", panelName),
  getBridgeStatus: (bridge = "feishu") => ipcRenderer.invoke("broker:bridge-status", { bridge }),
  getBridgeSettings: (bridge = "feishu") => ipcRenderer.invoke("broker:bridge-settings", { bridge }),
  getBridgeConversations: (bridge = "feishu", limit = 50) =>
    ipcRenderer.invoke("broker:bridge-conversations", { bridge, limit }),
  getMaterialSuggest: (projectName = "", prompt = "") =>
    ipcRenderer.invoke("broker:material-suggest", { projectName, prompt }),
  getBridgeMessages: (bridge = "feishu", chatRef = "", limit = 100) =>
    ipcRenderer.invoke("broker:bridge-messages", { bridge, chatRef, limit }),
  updateBridgeSettings: (bridge = "feishu", settings = {}) =>
    ipcRenderer.invoke("broker:bridge-settings", { bridge, settings }),
  connectBridge: (bridge = "feishu") => ipcRenderer.invoke("bridge:connect", { bridge }),
  disconnectBridge: (bridge = "feishu") => ipcRenderer.invoke("bridge:disconnect", { bridge }),
  reconnectBridge: (bridge = "feishu") => ipcRenderer.invoke("bridge:reconnect", { bridge }),
  getCoCoServiceStatus: () => ipcRenderer.invoke("service:coco-status"),
  installCoCoService: () => ipcRenderer.invoke("service:coco-install"),
  restartCoCoService: () => ipcRenderer.invoke("service:coco-restart"),
  uninstallCoCoService: () => ipcRenderer.invoke("service:coco-uninstall"),
  verifyCoCoServicePersistence: () => ipcRenderer.invoke("service:coco-verify"),
  getLauncherStatus: () => ipcRenderer.invoke("launcher:status"),
  installLauncher: () => ipcRenderer.invoke("launcher:install"),
  uninstallLauncher: () => ipcRenderer.invoke("launcher:uninstall"),
  getUserProfile: () => ipcRenderer.invoke("broker:user-profile"),
  saveUserProfile: (profile = {}) => ipcRenderer.invoke("broker:user-profile", { profile }),
  runCommandCenter: (payload) => ipcRenderer.invoke("broker:command-center", payload),
  openPath: (targetPath) => ipcRenderer.invoke("app:open-path", targetPath),
});
