"use client";

import { useEffect, useMemo, useState } from "react";

const DEFAULT_BRIDGE_SETTINGS = {
  app_id: "",
  app_secret: "",
  domain: "feishu",
  allowed_users: [],
  group_policy: "mentions_only",
  require_mention: true,
};

const ONBOARDING_STORAGE_KEY = "workspace-hub-electron-console-coco-onboarding-completed";
const ACTIVE_VIEW_STORAGE_KEY = "workspace-hub-electron-console-active-view";
const CODEX_THREADS_STORAGE_KEY = "workspace-hub-electron-console-codex-threads";
const ACTIVE_CODEX_THREAD_STORAGE_KEY = "workspace-hub-electron-console-active-codex-thread";
const CODEX_ACCESS_MODE_STORAGE_KEY = "workspace-hub-electron-console-codex-access-mode";
const CODEX_MODEL_STORAGE_KEY = "workspace-hub-electron-console-codex-model";
const AUTO_REFRESH_INTERVAL_MS = 15_000;
const PRIMARY_VIEWS = [
  { id: "projects", icon: "projects", label: "项目看板", subtitle: "工作区项目总览与跟进" },
  { id: "codex", icon: "codex", label: "Codex 交互", subtitle: "桌面主入口与会话控制" },
  { id: "ops", icon: "ops", label: "系统与设置", subtitle: "服务、快捷入口、运行说明与运维参数" },
];
const COMMAND_ACTIONS = {
  "codex-exec": {
    label: "发起新任务",
    description: "针对当前项目发起一个新的 Codex 执行任务。",
    promptPlaceholder: "直接输入你希望 Codex 处理的任务。",
    usesPrompt: true,
    promptRequired: true,
    usesSession: false,
  },
  "codex-resume": {
    label: "继续会话",
    description: "继续一条已有的 Codex 会话，可带补充提示。",
    promptPlaceholder: "补充说明下一步要继续处理什么。",
    usesPrompt: true,
    promptRequired: false,
    usesSession: true,
  },
  "open-codex-app": {
    label: "打开 Codex App",
    description: "作为兜底入口打开原生 Codex App，并指向当前主干工作区。",
    promptPlaceholder: "此动作不需要输入提示词。",
    usesPrompt: false,
    promptRequired: false,
    usesSession: false,
  },
};

function truncateText(value, limit = 80) {
  const text = String(value || "").trim();
  if (!text) return "";
  return text.length <= limit ? text : `${text.slice(0, limit - 1)}…`;
}

function executionTone(value) {
  switch (String(value || "").trim()) {
    case "running":
      return "tone-primary";
    case "reported":
      return "tone-info";
    case "failed":
      return "tone-warning";
    default:
      return "tone-subtle";
  }
}

function formatExecutionStateLabel(value) {
  switch (String(value || "").trim()) {
    case "running":
      return "执行中";
    case "reported":
      return "已汇报";
    case "failed":
      return "执行失败";
    case "idle":
      return "待命";
    default:
      return String(value || "待命");
  }
}

function formatAttentionLabel(reason) {
  switch (String(reason || "").trim()) {
    case "binding_required":
      return "等待项目线索";
    case "ack_delayed":
      return "确认延迟";
    case "awaiting_report":
      return "等待汇报";
    case "last_execution_failed":
      return "最近执行失败";
    case "approval_pending":
      return "等待授权";
    default:
      return "";
  }
}

function isWorkspaceAdminThread(row) {
  return Boolean(row && row.chat_type === "p2p" && !String(row.project_name || "").trim());
}

function formatThreadLabel(row) {
  if (!row) return "未命名线程";
  if (isWorkspaceAdminThread(row)) return "CoCo 私聊管理线程";
  return row.thread_label || row.binding_label || row.project_name || row.chat_ref || "未命名线程";
}

function formatBindingDisplay(row) {
  if (!row) return "工作区路由";
  if (isWorkspaceAdminThread(row)) return "工作区管理";
  return row.binding_label || row.project_name || "自动项目路由";
}

function formatChatSubtitle(row) {
  if (!row) return "";
  const parts = [];
  if (row.chat_type) parts.push(formatChatTypeLabel(row.chat_type));
  if (row.chat_ref) parts.push(row.chat_ref);
  return parts.join(" · ");
}

function formatChatTypeLabel(value) {
  switch (String(value || "").trim()) {
    case "p2p":
      return "私聊";
    case "group":
      return "群聊";
    default:
      return String(value || "聊天");
  }
}

function formatDeliveryLabel(value) {
  switch (String(value || "").trim()) {
    case "inbound":
      return "收到消息";
    case "outbound":
      return "已回消息";
    default:
      return String(value || "最近活动");
  }
}

function formatRecentRequest(row) {
  if (!row) return "尚未收到请求。";
  return row.last_user_request
    ? truncateText(row.last_user_request, 80)
    : row.last_message_preview
    ? truncateText(row.last_message_preview, 80)
    : "尚无用户请求。";
}

function formatBindingScopeLabel(value) {
  switch (String(value || "").trim()) {
    case "chat":
      return "聊天线程";
    case "project":
      return "项目上下文";
    case "topic":
      return "专题上下文";
    default:
      return String(value || "自动路由");
  }
}

function formatAccessModeLabel(value) {
  return String(value || "").trim() === "full" ? "完全访问" : "默认权限";
}

function availableCodexModelChoices(metadata) {
  const rows = Array.isArray(metadata?.codex_model_settings?.choices) ? metadata.codex_model_settings.choices : [];
  return rows
    .map((item) => {
      const id = String(item?.id || "").trim();
      if (!id) return null;
      return {
        id,
        label: String(item?.label || id).trim() || id,
        note: String(item?.note || "").trim(),
      };
    })
    .filter(Boolean);
}

function resolveDefaultCodexModel(metadata, entrypoint = "electron") {
  const defaults = metadata?.codex_model_settings?.defaults || {};
  const preferred = String(defaults?.[entrypoint] || "").trim();
  if (preferred) return preferred;
  return String(metadata?.codex_model_settings?.cli_default_model || "").trim();
}

function formatModelLabel(modelId, metadata) {
  const selected = String(modelId || "").trim();
  if (!selected) return "继承 CLI 默认";
  const match = availableCodexModelChoices(metadata).find((item) => item.id === selected);
  return match?.label || selected;
}

function formatRetrievalStepLabel(value) {
  switch (String(value || "").trim()) {
    case "search":
      return "搜索候选";
    case "timeline":
      return "时间线浏览";
    case "detail":
      return "细读详情";
    default:
      return String(value || "未指定");
  }
}

function compactPathLabel(value) {
  const path = String(value || "").trim();
  if (!path) return "";
  const parts = path.split("/");
  return parts.slice(-2).join("/") || path;
}

function inferFilterForConversation(row) {
  if (!row) return "all";
  if (row.approval_pending) return "approval";
  if (row.needs_attention) return "attention";
  const executionState = String(row.execution_state || "").trim();
  if (executionState === "running" || row.awaiting_report || row.ack_pending) {
    return "running";
  }
  return "all";
}

function summarizeCommandCenterResponse(response) {
  const data = response?.data || {};
  if (response?.ok) {
    const stdout = String(data.stdout || "").trim();
    const stderr = String(data.stderr || "").trim();
    if (stdout) return truncateText(stdout, 320);
    if (stderr) return truncateText(stderr, 320);
    return "任务已执行完成。";
  }
  return truncateText(response?.stderr || response?.error || "任务执行失败。", 320);
}

function formatRecoveryReason(value) {
  switch (String(value || "").trim()) {
    case "event_stalled":
      return "事件流停滞";
    case "stale":
      return "心跳过期";
    case "disconnected":
      return "桥接断开";
    default:
      return String(value || "尚无记录");
  }
}

function formatRelativeTimestamp(value) {
  if (!value) return "";
  const parsed = Date.parse(value);
  if (Number.isNaN(parsed)) return "";
  const diff = Date.now() - parsed;
  const minutes = Math.round(diff / 60000);
  if (minutes < 2) return "刚刚";
  if (minutes < 60) return `${minutes} 分钟前`;
  const hours = Math.round(minutes / 60);
  if (hours < 24) return `${hours} 小时前`;
  const days = Math.round(hours / 24);
  return `${days} 天前`;
}

function buildCodexThread({ projectName = "", prompt = "", action = "codex-exec", sessionId = "", accessMode = "default", model = "" } = {}) {
  const titleSeed = String(prompt || "").trim() || `${projectName || "工作区"} ${action === "codex-resume" ? "续接会话" : "新任务"}`;
  return {
    id: `codex-thread-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`,
    source: "desktop",
    title: truncateText(titleSeed, 28),
    project_name: projectName,
    session_id: sessionId,
    access_mode: accessMode,
    model_name: model,
    status: "draft",
    updated_at: new Date().toISOString(),
    last_summary: "",
    entries: [],
  };
}

function RailIcon({ kind, active = false }) {
  const stroke = active ? "currentColor" : "rgba(22, 32, 51, 0.78)";
  const common = { fill: "none", stroke, strokeWidth: "1.8", strokeLinecap: "round", strokeLinejoin: "round" };

  if (kind === "projects") {
    return (
      <svg viewBox="0 0 24 24" aria-hidden="true">
        <path d="M4 7.5h6l1.5 2H20v8.5a2 2 0 0 1-2 2H6a2 2 0 0 1-2-2Z" {...common} />
        <path d="M4 7.5A1.5 1.5 0 0 1 5.5 6H9l1 1.5" {...common} />
      </svg>
    );
  }
  if (kind === "ops") {
    return (
      <svg viewBox="0 0 24 24" aria-hidden="true">
        <circle cx="12" cy="12" r="3.2" {...common} />
        <path d="M12 3.75v2.4M12 17.85v2.4M20.25 12h-2.4M6.15 12h-2.4M17.84 6.16l-1.7 1.7M7.86 16.14l-1.7 1.7M17.84 17.84l-1.7-1.7M7.86 7.86l-1.7-1.7" {...common} />
      </svg>
    );
  }
  return (
    <svg viewBox="0 0 24 24" aria-hidden="true">
      <path d="M6 18.25c0-2.35 2.3-4.25 6-4.25s6 1.9 6 4.25" {...common} />
      <path d="M8.75 10.25c0-2.2 1.45-3.75 3.25-3.75s3.25 1.55 3.25 3.75-1.45 3.75-3.25 3.75-3.25-1.55-3.25-3.75Z" {...common} />
      <path d="M5 6.5h.01M19 6.5h.01" {...common} />
    </svg>
  );
}

export default function HomePage() {
  const [activeView, setActiveView] = useState("codex");
  const [metadata, setMetadata] = useState(null);
  const [overview, setOverview] = useState(null);
  const [projects, setProjects] = useState([]);
  const [reviewRows, setReviewRows] = useState([]);
  const [coordinationRows, setCoordinationRows] = useState([]);
  const [healthRows, setHealthRows] = useState([]);
  const [conversationRows, setConversationRows] = useState([]);
  const [selectedConversation, setSelectedConversation] = useState("");
  const [conversationFilter, setConversationFilter] = useState("all");
  const [feishuSearch, setFeishuSearch] = useState("");
  const [messageRows, setMessageRows] = useState([]);
  const [bridgeStatus, setBridgeStatus] = useState(null);
  const [serviceStatus, setServiceStatus] = useState(null);
  const [launcherStatus, setLauncherStatus] = useState(null);
  const [bridgeSettings, setBridgeSettings] = useState(DEFAULT_BRIDGE_SETTINGS);
  const [error, setError] = useState("");
  const [saveState, setSaveState] = useState("");
  const [bridgeActionState, setBridgeActionState] = useState("");
  const [serviceActionState, setServiceActionState] = useState("");
  const [launcherActionState, setLauncherActionState] = useState("");
  const [userProfile, setUserProfile] = useState(null);
  const [profileStatus, setProfileStatus] = useState("idle");
  const [profileError, setProfileError] = useState("");
  const [preferredNameInput, setPreferredNameInput] = useState("");
  const [profileMessage, setProfileMessage] = useState("");
  const [profileSaving, setProfileSaving] = useState(false);
  const [onboardingVisible, setOnboardingVisible] = useState(false);
  const [onboardingSkipped, setOnboardingSkipped] = useState(false);
  const [conversationFilterPinned, setConversationFilterPinned] = useState(false);
  const [serviceVerification, setServiceVerification] = useState(null);
  const [commandAction, setCommandAction] = useState("codex-exec");
  const [commandProject, setCommandProject] = useState("");
  const [commandSessionId, setCommandSessionId] = useState("");
  const [commandPrompt, setCommandPrompt] = useState("");
  const [commandAccessMode, setCommandAccessMode] = useState("default");
  const [commandModel, setCommandModel] = useState("");
  const [materialContext, setMaterialContext] = useState(null);
  const [codexThreads, setCodexThreads] = useState([]);
  const [activeCodexThreadId, setActiveCodexThreadId] = useState("");
  const [codexSurfaceMode, setCodexSurfaceMode] = useState("desktop");
  const [codexThreadSearch, setCodexThreadSearch] = useState("");
  const [codexThreadFilter, setCodexThreadFilter] = useState("all");
  const [commandRunning, setCommandRunning] = useState(false);
  const [commandStatus, setCommandStatus] = useState("");
  const [showCodexAdvanced, setShowCodexAdvanced] = useState(false);
  const [showCodexInspector, setShowCodexInspector] = useState(false);
  const [showFeishuInspector, setShowFeishuInspector] = useState(false);
  const [codexModelDefaults, setCodexModelDefaults] = useState({
    workspace: "",
    feishu: "",
    electron: "",
  });
  const [codexModelSaveState, setCodexModelSaveState] = useState("");
  const [selectedProjectName, setSelectedProjectName] = useState("");
  const [projectWorkspaceView, setProjectWorkspaceView] = useState("overview");

  useEffect(() => {
    let cancelled = false;
    if (typeof window !== "undefined") {
      try {
        const storedView = window.localStorage?.getItem(ACTIVE_VIEW_STORAGE_KEY) || "";
        if (PRIMARY_VIEWS.some((item) => item.id === storedView)) {
          setActiveView(storedView);
        }
        const storedThreads = JSON.parse(window.localStorage?.getItem(CODEX_THREADS_STORAGE_KEY) || "[]");
        if (Array.isArray(storedThreads) && storedThreads.length) {
          setCodexThreads(storedThreads);
        }
        const storedThreadId = window.localStorage?.getItem(ACTIVE_CODEX_THREAD_STORAGE_KEY) || "";
        if (storedThreadId) {
          setActiveCodexThreadId(storedThreadId);
        }
        const storedAccessMode = window.localStorage?.getItem(CODEX_ACCESS_MODE_STORAGE_KEY) || "";
        if (storedAccessMode === "default" || storedAccessMode === "full") {
          setCommandAccessMode(storedAccessMode);
        }
        const storedModel = window.localStorage?.getItem(CODEX_MODEL_STORAGE_KEY) || "";
        if (storedModel) {
          setCommandModel(storedModel);
        }
      } catch (_error) {
        // ignore malformed local cache
      }
    }
    async function load() {
      try {
        const api = window.workspaceHubAPI;
        if (!api) {
          if (cancelled) return;
          setMetadata({
            broker_mode: "浏览器预览",
            broker_workspace_root: "未接入 Electron preload",
          });
          setError(null);
          return;
        }
        const [
          metaPayload,
          overviewPayload,
          projectsPayload,
          reviewPayload,
          coordinationPayload,
          healthPayload,
          bridgePayload,
          servicePayload,
          launcherPayload,
          bridgeSettingsPayload,
          conversationsPayload,
        ] = await Promise.all([
          api.getMetadata(),
          api.getPanel({ panelName: "overview" }),
          api.getPanel({ panelName: "projects" }),
          api.getPanel({ panelName: "review" }),
          api.getPanel({ panelName: "coordination" }),
          api.getPanel({ panelName: "health" }),
          api.getBridgeStatus("feishu"),
          api.getCoCoServiceStatus(),
          typeof api.getLauncherStatus === "function" ? api.getLauncherStatus() : Promise.resolve(null),
          api.getBridgeSettings("feishu"),
          api.getBridgeConversations("feishu", 50),
        ]);
        if (cancelled) return;
        setMetadata(metaPayload || null);
        const defaultElectronModel = resolveDefaultCodexModel(metaPayload, "electron");
        setCommandModel((current) => current || defaultElectronModel);
        setOverview(overviewPayload?.data || null);
        setProjects(projectsPayload?.data?.rows || []);
        setReviewRows(reviewPayload?.data?.rows || []);
        setCoordinationRows(coordinationPayload?.data?.rows || []);
        setHealthRows(healthPayload?.data?.rows || []);
        setBridgeStatus(bridgePayload?.data || null);
        setServiceStatus(servicePayload?.data || servicePayload || null);
        setLauncherStatus(launcherPayload?.data || launcherPayload || null);
        setBridgeSettings({ ...DEFAULT_BRIDGE_SETTINGS, ...(bridgeSettingsPayload?.data?.settings || {}) });
        const nextConversations = conversationsPayload?.data?.rows || [];
        setConversationRows(nextConversations);
        setSelectedConversation(nextConversations[0]?.chat_ref || "");
      } catch (loadError) {
        if (!cancelled) {
          setError(String(loadError?.message || loadError || "加载桌面工作台失败"));
        }
      }
    }
    load();
    return () => {
      cancelled = true;
    };
  }, []);

  useEffect(() => {
    if (typeof window === "undefined") return;
    window.localStorage?.setItem(ACTIVE_VIEW_STORAGE_KEY, activeView);
  }, [activeView]);

  useEffect(() => {
    if (typeof window === "undefined") return;
    window.localStorage?.setItem(CODEX_THREADS_STORAGE_KEY, JSON.stringify(codexThreads));
  }, [codexThreads]);

  useEffect(() => {
    if (typeof window === "undefined") return;
    if (activeCodexThreadId) {
      window.localStorage?.setItem(ACTIVE_CODEX_THREAD_STORAGE_KEY, activeCodexThreadId);
    } else {
      window.localStorage?.removeItem(ACTIVE_CODEX_THREAD_STORAGE_KEY);
    }
  }, [activeCodexThreadId]);

  useEffect(() => {
    if (typeof window === "undefined") return;
    window.localStorage?.setItem(CODEX_ACCESS_MODE_STORAGE_KEY, commandAccessMode);
  }, [commandAccessMode]);

  useEffect(() => {
    if (typeof window === "undefined") return;
    if (commandModel) {
      window.localStorage?.setItem(CODEX_MODEL_STORAGE_KEY, commandModel);
    } else {
      window.localStorage?.removeItem(CODEX_MODEL_STORAGE_KEY);
    }
  }, [commandModel]);

  useEffect(() => {
    let cancelled = false;
    if (typeof window !== "undefined") {
      if (window.localStorage?.getItem(ONBOARDING_STORAGE_KEY) === "1") {
        setOnboardingSkipped(true);
      }
    }
    async function loadProfile() {
      if (typeof window?.workspaceHubAPI?.getUserProfile !== "function") {
        if (!cancelled) {
          setProfileStatus("unavailable");
          setProfileError("共享 broker 的用户画像接口暂时不可用。");
        }
        return;
      }
      setProfileStatus("loading");
      try {
        const response = await window.workspaceHubAPI.getUserProfile();
        if (cancelled) return;
        if (response?.ok) {
          const profile = response.data?.profile || null;
          setUserProfile(profile);
          setPreferredNameInput(profile?.preferred_name || "");
          setProfileStatus("ready");
          setProfileError("");
        } else {
          setUserProfile(response?.data?.profile || null);
          setProfileStatus("unavailable");
          setProfileError(response?.error || "User profile sync not enabled yet.");
        }
      } catch (profileError) {
        if (cancelled) return;
        setProfileStatus("unavailable");
        setProfileError(String(profileError?.message || profileError || "加载用户画像失败"));
        setUserProfile(null);
      }
    }
    loadProfile();
    return () => {
      cancelled = true;
    };
  }, []);

  useEffect(() => {
    if (userProfile && userProfile.preferred_name) {
      setOnboardingVisible(false);
      return;
    }
    if (!onboardingSkipped && userProfile && !userProfile.preferred_name) {
      setOnboardingVisible(true);
    }
  }, [userProfile, onboardingSkipped]);

  const cards = useMemo(() => overview?.cards || [], [overview]);
  const activeConversation = useMemo(
    () => conversationRows.find((row) => row.chat_ref === selectedConversation) || null,
    [conversationRows, selectedConversation],
  );
  const sortedCodexThreads = useMemo(
    () => codexThreads.slice().sort((left, right) => String(right.updated_at || "").localeCompare(String(left.updated_at || ""))),
    [codexThreads],
  );
  const codexThreadBuckets = useMemo(() => {
    const total = codexThreads.length;
    const workspace = codexThreads.filter((thread) => !String(thread.project_name || "").trim()).length;
    const project = codexThreads.filter((thread) => String(thread.project_name || "").trim()).length;
    const running = codexThreads.filter((thread) => String(thread.status || "").trim() === "running").length;
    const failed = codexThreads.filter((thread) => String(thread.status || "").trim() === "failed").length;
    return { total, workspace, project, running, failed };
  }, [codexThreads]);
  const filteredCodexThreads = useMemo(() => {
    const query = String(codexThreadSearch || "").trim().toLowerCase();
    const scopedThreads = sortedCodexThreads.filter((thread) => {
      switch (codexThreadFilter) {
        case "workspace":
          return !String(thread.project_name || "").trim();
        case "project":
          return Boolean(String(thread.project_name || "").trim());
        case "running":
          return String(thread.status || "").trim() === "running";
        case "failed":
          return String(thread.status || "").trim() === "failed";
        default:
          return true;
      }
    });
    if (!query) return scopedThreads;
    return scopedThreads.filter((thread) =>
      [thread.title, thread.project_name, thread.last_summary]
        .filter(Boolean)
        .join(" ")
        .toLowerCase()
        .includes(query),
    );
  }, [sortedCodexThreads, codexThreadSearch, codexThreadFilter]);
  const activeCodexThread = useMemo(
    () => codexThreads.find((thread) => thread.id === activeCodexThreadId) || null,
    [codexThreads, activeCodexThreadId],
  );
  const activeCodexEntries = activeCodexThread?.entries || [];
  const liveConversationRows = useMemo(
    () => conversationRows.filter((row) => !row.stale_thread),
    [conversationRows],
  );
  const staleConversationRows = useMemo(
    () => conversationRows.filter((row) => row.stale_thread),
    [conversationRows],
  );
  const conversationBuckets = useMemo(() => {
    const total = liveConversationRows.length;
    const archived = staleConversationRows.length;
    const approval = liveConversationRows.filter((row) => Boolean(row.approval_pending)).length;
    const attention = liveConversationRows.filter((row) => Boolean(row.needs_attention)).length;
    const running = liveConversationRows.filter((row) => {
      const executionState = String(row.execution_state || "").trim();
      return executionState === "running" || Boolean(row.awaiting_report) || Boolean(row.ack_pending);
    }).length;
    return { total, archived, approval, attention, running };
  }, [liveConversationRows, staleConversationRows]);
  const conversationDefaultFilter = useMemo(() => {
    if (conversationBuckets.approval) return "approval";
    if (conversationBuckets.attention) return "attention";
    if (conversationBuckets.running) return "running";
    return "all";
  }, [conversationBuckets]);
  const filteredConversationRows = useMemo(() => {
    switch (conversationFilter) {
      case "history":
        return staleConversationRows;
      case "approval":
        return liveConversationRows.filter((row) => Boolean(row.approval_pending));
      case "attention":
        return liveConversationRows.filter((row) => Boolean(row.needs_attention));
      case "running":
        return liveConversationRows.filter((row) => {
          const executionState = String(row.execution_state || "").trim();
          return executionState === "running" || Boolean(row.awaiting_report) || Boolean(row.ack_pending);
        });
      default:
        return liveConversationRows;
    }
  }, [liveConversationRows, staleConversationRows, conversationFilter]).filter((row) => {
    const query = String(feishuSearch || codexThreadSearch || "").trim().toLowerCase();
    if (!query) return true;
    return [
      formatThreadLabel(row),
      row.project_name,
      row.topic_name,
      row.last_user_request,
      row.last_report,
      row.last_error,
      row.chat_ref,
    ]
      .filter(Boolean)
      .join(" ")
      .toLowerCase()
      .includes(query);
  });
  const serviceState = serviceStatus?.service_state || null;
  const bridgeRuntimeSummary = useMemo(() => ({
    lastEventAt: bridgeStatus?.last_event_at || serviceState?.last_bridge_event_at || "",
    lastDeliveryAt: bridgeStatus?.last_delivery_at || serviceState?.last_bridge_delivery_at || "",
    lastDeliveryPhase: bridgeStatus?.last_delivery_phase || serviceState?.last_bridge_delivery_phase || "",
    lastMessagePreview: bridgeStatus?.last_message_preview || serviceState?.last_bridge_message_preview || "",
    lastSenderRef: bridgeStatus?.last_sender_ref || serviceState?.last_bridge_sender_ref || "",
    recentMessageCount: Number(bridgeStatus?.recent_message_count || serviceState?.last_bridge_recent_message_count || 0),
    recentReplyCount: Number(bridgeStatus?.recent_reply_count || serviceState?.last_bridge_recent_reply_count || 0),
  }), [bridgeStatus, serviceState]);
  const bridgeAlert = useMemo(() => {
    if (!bridgeStatus && !serviceStatus) return null;
    if (!serviceStatus?.installed || !serviceStatus?.loaded) {
      return {
        tone: "warning",
        title: "CoCo 服务需要关注",
        body: "LaunchAgent 尚未完全激活，飞书线程可能会在修复或重启前停止回复。",
      };
    }
    if (serviceState?.ack_stalled) {
      return {
        tone: "warning",
        title: "确认已发出，但结果仍未送达",
        body: "CoCo 已向飞书发出短确认，但这条线程后续结果迟迟没有送达。优先检查最近送达阶段、确认等待秒数和服务日志。",
      };
    }
    if (bridgeStatus?.stale) {
      return {
        tone: "warning",
        title: bridgeStatus?.event_stalled ? "事件流暂时停滞" : "心跳数据过期",
        body: bridgeStatus?.event_stalled
          ? "LaunchAgent 依旧在运行，但飞书最近没有新事件。请先让 CoCo 重连再处理待定线程。"
          : "CoCo 仍保留 runtime 状态，但心跳数据过期，请重启服务再继续信任正在运行的线程。",
      };
    }
    if (bridgeStatus?.connection_status && bridgeStatus.connection_status !== "connected") {
      return {
        tone: "warning",
        title: `桥接状态：${bridgeStatus.connection_status}`,
        body: "飞书 IM 尚未完全上线，新消息可能会排队，直到桥接重新建立连接。",
      };
    }
    if (bridgeStatus?.last_error) {
      return {
        tone: "error",
        title: "桥接报告了最近的错误",
        body: bridgeStatus.last_error,
      };
    }
    return null;
  }, [bridgeStatus, serviceState, serviceStatus]);
  const recoverySummary = useMemo(() => {
    if (!serviceState?.last_recovery_at) return null;
    const mismatchCount = Array.isArray(serviceState.last_recovery_mismatches)
      ? serviceState.last_recovery_mismatches.length
      : 0;
    return {
      ok: Boolean(serviceState.last_recovery_ok),
      label: serviceState.last_recovery_ok ? "自动恢复成功" : "自动恢复需关注",
      detail: `${formatRecoveryReason(serviceState.last_recovery_reason)} · ${mismatchCount ? `发现 ${mismatchCount} 处线程不一致` : `已核对 ${serviceState.last_recovery_compared_threads || 0} 条线程`}`,
      at: serviceState.last_recovery_at,
      error: serviceState.last_recovery_error || "",
    };
  }, [serviceState]);
  const persistenceSummary = useMemo(() => {
    if (!serviceState?.last_persistence_check_at) return null;
    const mismatchCount = Array.isArray(serviceState.last_persistence_mismatches)
      ? serviceState.last_persistence_mismatches.length
      : 0;
    return {
      ok: Boolean(serviceState.last_persistence_ok),
      label: serviceState.last_persistence_ok ? "持久化校验通过" : "持久化校验异常",
      detail: mismatchCount
        ? `发现 ${mismatchCount} 处线程不一致`
        : `已比对 ${serviceState.last_persistence_compared_threads || 0} 条线程`,
      at: serviceState.last_persistence_check_at,
    };
  }, [serviceState]);
  const operatorNextAction = useMemo(() => {
    if (!serviceStatus?.installed || !serviceStatus?.loaded) {
      return "请先修复 CoCo LaunchAgent，再继续处理飞书线程。";
    }
    if (serviceState?.ack_stalled) {
      return "先检查最近送达阶段和确认等待秒数，确认哪条线程已经 ack 但没有继续产出结果。";
    }
    if (bridgeStatus?.stale || bridgeStatus?.event_stalled) {
      return "等待 CoCo 重新连接并确认事件流后再派发工作。";
    }
    if (conversationBuckets.approval) {
      return "先处理待授权线程，让高风险动作可以继续。";
    }
    if (conversationBuckets.attention) {
      return "下一步清理 attention 队列，收敛延迟或失败的线程。";
    }
    if (conversationBuckets.running) {
      return "关注运行中线程直到它们产出报告。";
    }
    return "CoCo 状态良好。群聊会按消息内容自动路由到项目上下文，私聊继续承担工作区级协调和授权。";
  }, [bridgeStatus, conversationBuckets, serviceState, serviceStatus]);
  const projectConversationMap = useMemo(() => {
    const mapping = new Map();
    for (const row of liveConversationRows) {
      const projectName = String(row.project_name || "").trim();
      if (!projectName) continue;
      const current = mapping.get(projectName) || { count: 0, active: null, attention: 0 };
      current.count += 1;
      if (!current.active || String(row.last_message_at || "") > String(current.active.last_message_at || "")) {
        current.active = row;
      }
      if (row.needs_attention) {
        current.attention += 1;
      }
      mapping.set(projectName, current);
    }
    return mapping;
  }, [liveConversationRows]);
  const activeProject = useMemo(
    () => projects.find((row) => row.project_name === selectedProjectName) || projects[0] || null,
    [projects, selectedProjectName],
  );
  const activeProjectConversationState = useMemo(() => {
    if (!activeProject?.project_name) return null;
    return projectConversationMap.get(activeProject.project_name) || null;
  }, [activeProject, projectConversationMap]);
  const activeProjectReviewRows = useMemo(() => {
    if (!activeProject?.project_name) return [];
    return reviewRows.filter((row) => String(row.project_name || "").trim() === String(activeProject.project_name).trim());
  }, [reviewRows, activeProject]);
  const activeProjectCoordinationRows = useMemo(() => {
    if (!activeProject?.project_name) return [];
    const projectName = String(activeProject.project_name).trim();
    return coordinationRows.filter((row) => {
      const fromProject = String(row.from_project || "").trim();
      const toProject = String(row.to_project || "").trim();
      return fromProject === projectName || toProject === projectName;
    });
  }, [coordinationRows, activeProject]);
  const activeProjectHealthRows = useMemo(() => {
    if (!activeProject?.project_name) return [];
    const projectName = String(activeProject.project_name).trim().toLowerCase();
    return healthRows.filter((row) =>
      [row.project_name, row.title, row.summary]
        .filter(Boolean)
        .join(" ")
        .toLowerCase()
        .includes(projectName),
    );
  }, [healthRows, activeProject]);
  const activeProjectFocusSections = useMemo(
    () => ({
      overview: {
        title: "项目总览",
        description: "聚焦当前项目的下一步、线程状态和总控入口。",
      },
      review: {
        title: "审核事项",
        description: "只看当前项目的 review backlog 和最近一条重点事项。",
      },
      coordination: {
        title: "协同事项",
        description: "聚焦当前项目发出的或收到的跨项目协同任务。",
      },
      feishu: {
        title: "项目线程",
        description: "查看这个项目在 Feishu 中产生的主线程、最近请求和汇报。",
      },
      health: {
        title: "健康与风险",
        description: "只显示和当前项目相关的健康告警与跟进提示。",
      },
    }),
    [],
  );
  const commandDefinition = useMemo(
    () => COMMAND_ACTIONS[commandAction] || COMMAND_ACTIONS["codex-exec"],
    [commandAction],
  );
  const operatorDisplayName = String(userProfile?.preferred_name || "").trim() || "你";
  const codexModelChoices = useMemo(() => availableCodexModelChoices(metadata), [metadata]);
  const activeViewMeta = useMemo(
    () => PRIMARY_VIEWS.find((item) => item.id === activeView) || PRIMARY_VIEWS[0],
    [activeView],
  );

  useEffect(() => {
    const defaults = metadata?.codex_model_settings?.defaults || {};
    setCodexModelDefaults({
      workspace: String(defaults.workspace || "").trim(),
      feishu: String(defaults.feishu || "").trim(),
      electron: String(defaults.electron || "").trim(),
    });
  }, [metadata?.codex_model_settings?.defaults]);

  useEffect(() => {
    if (!commandProject && projects.length) {
      setCommandProject(projects[0]?.project_name || "");
    }
  }, [projects, commandProject]);

  useEffect(() => {
    if (!projects.length) {
      if (selectedProjectName) {
        setSelectedProjectName("");
      }
      return;
    }
    if (!selectedProjectName || !projects.find((row) => row.project_name === selectedProjectName)) {
      setSelectedProjectName(projects[0]?.project_name || "");
    }
  }, [projects, selectedProjectName]);

  useEffect(() => {
    if (codexSurfaceMode === "feishu") {
      return;
    }
    if (!activeCodexThreadId && sortedCodexThreads.length) {
      setActiveCodexThreadId(sortedCodexThreads[0].id);
    }
  }, [sortedCodexThreads, activeCodexThreadId, codexSurfaceMode]);

  function focusCodexThread(thread) {
    if (!thread) return;
    setCodexSurfaceMode("desktop");
    setActiveCodexThreadId(thread.id);
    setCommandProject(thread.project_name || "");
    setCommandSessionId(thread.session_id || "");
    setCommandAccessMode(thread.access_mode || "default");
    setCommandModel(thread.model_name || resolveDefaultCodexModel(metadata, "electron"));
    setShowCodexAdvanced(false);
    setCommandStatus(`已切换到 ${thread.title}。`);
    setActiveView("codex");
  }

  function createAndFocusCodexThread({ projectName = "", prompt = "", action = "codex-exec", sessionId = "", accessMode = "default", model = "" } = {}) {
    const thread = buildCodexThread({ projectName, prompt, action, sessionId, accessMode, model });
    setCodexThreads((rows) => [thread, ...rows]);
    focusCodexThread(thread);
    setCommandProject(projectName || "");
    setCommandPrompt(prompt || "");
    setCommandAction(action);
    setCommandSessionId(sessionId || "");
    setCommandAccessMode(accessMode);
    setCommandModel(model || resolveDefaultCodexModel(metadata, "electron"));
    setCommandStatus(projectName ? `已为 ${projectName} 创建新对话。` : "已创建新的桌面对话。");
    return thread;
  }

  function focusFeishuThread(row, requestedFilter = "") {
    if (!row) return;
    setCodexSurfaceMode("feishu");
    setActiveCodexThreadId("");
    setConversationFocus(row, requestedFilter);
    setCommandProject(row.project_name || "");
    setCommandSessionId(row.session_id || "");
    setShowCodexAdvanced(false);
    setCommandStatus(`已切换到 ${formatThreadLabel(row)}。`);
    setActiveView("codex");
  }

  function openCommandComposer({ projectName = "", prompt = "", action = "codex-exec", sessionId = "" } = {}) {
    setCodexSurfaceMode("desktop");
    const matchingThread = projectName
      ? sortedCodexThreads.find((thread) => String(thread.project_name || "").trim() === String(projectName || "").trim())
      : activeCodexThread;
    if (matchingThread) {
      focusCodexThread(matchingThread);
    }
    setCommandProject(projectName || "");
    setCommandPrompt(prompt || "");
    setCommandAction(action);
    setCommandSessionId(sessionId || "");
    setCommandAccessMode(matchingThread?.access_mode || commandAccessMode);
    setCommandModel(matchingThread?.model_name || commandModel || resolveDefaultCodexModel(metadata, "electron"));
    setShowCodexAdvanced(Boolean(sessionId) || action !== "codex-exec");
    setCommandStatus(projectName ? `已载入 ${projectName} 的上下文。` : "已切到 Codex 交互。");
    setActiveView("codex");
  }

  function focusProjectWorkspace(projectName, nextView = "overview") {
    if (!projectName) return;
    setSelectedProjectName(projectName);
    setProjectWorkspaceView(nextView);
    setActiveView("projects");
  }

  async function handleCommandSubmit(event) {
    event.preventDefault();
    if (commandRunning) return;
    const action = String(commandAction || "codex-exec");
    const prompt = String(commandPrompt || "").trim();
    const sessionId = String(commandSessionId || "").trim();
    const projectName = String(commandProject || "").trim();
    const definition = COMMAND_ACTIONS[action] || COMMAND_ACTIONS["codex-exec"];

    if (definition.promptRequired && !prompt) {
      setCommandStatus("请先输入要交给 Codex 的任务。");
      return;
    }
    if (definition.usesSession && !sessionId) {
      setCommandStatus("继续会话需要先填写 session ID。");
      return;
    }

    const submittedAt = new Date().toLocaleString();
    const requestEntry = {
      id: `request-${Date.now()}`,
      role: "user",
      title: action === "codex-resume" ? "继续会话" : action === "open-codex-app" ? "打开 Codex App" : "新任务",
      body: prompt || `执行动作：${action}`,
      meta: [
        projectName ? `项目 ${projectName}` : "工作区级",
        formatAccessModeLabel(commandAccessMode),
        formatModelLabel(commandModel, metadata),
        submittedAt,
      ].join(" · "),
      tone: "user",
    };

    let threadId = activeCodexThreadId;
    let thread = activeCodexThread;
    if (!threadId || !thread) {
      thread = createAndFocusCodexThread({
        projectName,
        prompt,
        action,
        sessionId,
        accessMode: commandAccessMode,
        model: commandModel,
      });
      threadId = thread.id;
    }
    setCodexThreads((rows) =>
      rows.map((item) =>
        item.id === threadId
          ? {
              ...item,
              title: item.entries.length ? item.title : truncateText(prompt || item.title || "新任务", 28),
              project_name: projectName || item.project_name,
              session_id: sessionId || item.session_id,
              access_mode: commandAccessMode,
              model_name: commandModel || item.model_name,
              status: "running",
              updated_at: new Date().toISOString(),
              last_summary: requestEntry.body,
              entries: [...(item.entries || []), requestEntry],
            }
          : item,
      ),
    );
    setCommandRunning(true);
    setCommandStatus("已提交给 Codex，正在等待结果。");

    try {
      const response = await window.workspaceHubAPI.runCommandCenter({
        action,
        project_name: projectName,
        session_id: sessionId,
        prompt,
        access_mode: commandAccessMode,
        model: commandModel,
        source: "electron",
        thread_name: prompt || activeCodexThread?.title || "",
        thread_label: activeCodexThread?.title || projectName || "桌面对话",
      });
      const ok = Boolean(response?.ok);
      const resultEntry = {
        id: `result-${Date.now()}`,
        role: "assistant",
        title: ok ? "Codex 已返回结果" : "Codex 返回错误",
        body: summarizeCommandCenterResponse(response),
        meta: [
          projectName ? `项目 ${projectName}` : "工作区级",
          formatAccessModeLabel(commandAccessMode),
          formatModelLabel(commandModel, metadata),
          response?.data?.delegated_broker_action || response?.data?.broker_action || action,
          submittedAt,
        ].join(" · "),
        tone: ok ? "assistant" : "warning",
      };
      setCodexThreads((rows) =>
        rows.map((item) =>
          item.id === threadId
            ? {
                ...item,
                project_name: projectName || item.project_name,
                session_id: sessionId || item.session_id,
                access_mode: commandAccessMode,
                model_name: commandModel || item.model_name,
                status: ok ? "reported" : "failed",
                updated_at: new Date().toISOString(),
                last_summary: resultEntry.body,
                entries: [...(item.entries || []), resultEntry],
              }
            : item,
        ),
      );
      setCommandStatus(ok ? "任务已完成，可继续发起新指令。" : "任务未成功，请检查返回结果。");
      if (action !== "codex-resume") {
        setCommandPrompt("");
      }
    } catch (commandError) {
      const errorEntry = {
        id: `error-${Date.now()}`,
        role: "assistant",
        title: "Codex 调用失败",
        body: truncateText(String(commandError?.message || commandError || "unknown_error"), 320),
        meta: [projectName ? `项目 ${projectName}` : "工作区级", formatAccessModeLabel(commandAccessMode), submittedAt].join(" · "),
        tone: "warning",
      };
      setCodexThreads((rows) =>
        rows.map((item) =>
          item.id === threadId
            ? {
                ...item,
                project_name: projectName || item.project_name,
                session_id: sessionId || item.session_id,
                access_mode: commandAccessMode,
                model_name: commandModel || item.model_name,
                status: "failed",
                updated_at: new Date().toISOString(),
                last_summary: errorEntry.body,
                entries: [...(item.entries || []), errorEntry],
              }
            : item,
        ),
      );
      setCommandStatus("调用 Codex 时出现错误，请查看返回内容。");
    } finally {
      setCommandRunning(false);
    }
  }

  function resetCodexComposer() {
    setCommandAction("codex-exec");
    setCommandPrompt("");
    setCommandSessionId(activeCodexThread?.session_id || "");
    setCommandProject(activeCodexThread?.project_name || "");
    setCommandAccessMode(activeCodexThread?.access_mode || "default");
    setCommandModel(activeCodexThread?.model_name || resolveDefaultCodexModel(metadata, "electron"));
    setShowCodexAdvanced(false);
    setCommandStatus(activeCodexThread ? `继续在 ${activeCodexThread.title} 中输入。` : "请先新建一个会话。");
  }
  const allowedUsersText = Array.isArray(bridgeSettings.allowed_users)
    ? bridgeSettings.allowed_users.join("\n")
    : "";
  const codexComposerPlaceholder = commandDefinition.usesPrompt
    ? commandDefinition.usesSession
      ? "补充这条会话下一步要继续推进的内容。"
      : "直接告诉 Codex 你要处理什么，例如：整理项目现状、继续推进某个任务、汇总当前工作区状态。"
    : commandDefinition.promptPlaceholder;
  const codexComposerContextSummary = [
    commandProject ? `项目：${commandProject}` : "工作区级总控",
    `权限：${formatAccessModeLabel(commandAccessMode)}`,
    `模型：${formatModelLabel(commandModel, metadata)}`,
    activeCodexThread?.session_id || commandSessionId ? `会话：${truncateText(activeCodexThread?.session_id || commandSessionId, 18)}` : "新会话",
    commandDefinition.label,
  ].join(" · ");
  const codexPrimaryActionLabel = commandRunning ? "处理中…" : commandDefinition.label;
  const codexSecondaryHint = commandDefinition.usesSession
    ? "继续已有会话时，可以在高级设置里调整 Session ID。"
    : "默认直接用聊天方式发起任务；只有切换动作、项目或 Session 时才展开高级设置。";
  const codexChatMode = useMemo(() => {
    if (codexSurfaceMode === "feishu" && activeConversation) {
      return "feishu";
    }
    return "desktop";
  }, [codexSurfaceMode, activeConversation]);
  const codexUnifiedThreads = useMemo(() => {
    const feishuRows = filteredConversationRows.map((row) => ({
      id: `feishu:${row.chat_ref}`,
      source: "feishu",
      updatedAt: String(row.last_message_at || row.updated_at || ""),
      data: row,
    }));
    const desktopRows = filteredCodexThreads.map((thread) => ({
      id: `desktop:${thread.id}`,
      source: "desktop",
      updatedAt: String(thread.updated_at || ""),
      data: thread,
    }));
    return [...feishuRows, ...desktopRows]
      .sort((left, right) => right.updatedAt.localeCompare(left.updatedAt))
      .slice(0, 16);
  }, [filteredConversationRows, filteredCodexThreads]);
  const codexUnifiedThreadGroups = useMemo(() => {
    const groups = new Map();
    codexUnifiedThreads.forEach((item) => {
      const groupKey =
        item.source === "feishu"
          ? isWorkspaceAdminThread(item.data)
            ? "CoCo 私聊总控"
            : item.data.project_name || "飞书来源"
          : item.data.project_name || "工作区对话";
      const existing = groups.get(groupKey);
      if (existing) {
        existing.items.push(item);
        if (item.updatedAt > existing.updatedAt) {
          existing.updatedAt = item.updatedAt;
        }
        return;
      }
      groups.set(groupKey, {
        key: groupKey,
        label: groupKey,
        updatedAt: item.updatedAt,
        items: [item],
      });
    });
    return Array.from(groups.values()).sort((left, right) => right.updatedAt.localeCompare(left.updatedAt));
  }, [codexUnifiedThreads]);
  const codexShellFilter = useMemo(() => {
    if (conversationFilter === "approval") return "approval";
    if (conversationFilter === "history") return "history";
    if (codexThreadFilter === "failed") return "failed";
    if (conversationFilter === "running" || codexThreadFilter === "running") return "running";
    return "all";
  }, [conversationFilter, codexThreadFilter]);

  function applyCodexShellFilter(filterId) {
    setConversationFilterPinned(true);
    switch (filterId) {
      case "approval":
        setConversationFilter("approval");
        setCodexThreadFilter("all");
        break;
      case "running":
        setConversationFilter("running");
        setCodexThreadFilter("running");
        break;
      case "failed":
        setConversationFilter("all");
        setCodexThreadFilter("failed");
        break;
      case "history":
        setConversationFilter("history");
        setCodexThreadFilter("all");
        break;
      default:
        setConversationFilter("all");
        setCodexThreadFilter("all");
        break;
    }
  }

  function renderUnifiedThreadCard(item) {
    if (item.source === "feishu") {
      const row = item.data;
      return (
        <button
          key={item.id}
          type="button"
          className={`list-card conversation-card compact-thread-card thread-list-item ${codexChatMode === "feishu" && selectedConversation === row.chat_ref ? "selected" : ""}`}
          onClick={() => focusFeishuThread(row)}
        >
          <div className="thread-list-item-head">
            <div className="thread-list-item-main">
              <p className="thread-list-item-kicker">
                <span className="thread-origin-pill">飞书</span> {formatChatTypeLabel(row.chat_type)}
              </p>
              <h3>{formatThreadLabel(row)}</h3>
            </div>
            <div className="thread-list-item-side">
              {row.last_message_at ? <span className="thread-list-item-time">{formatRelativeTimestamp(row.last_message_at)}</span> : null}
            </div>
          </div>
          <p className="thread-list-item-subline">{formatBindingDisplay(row)} · {formatChatSubtitle(row)}</p>
          <p className="thread-list-item-preview">{formatRecentRequest(row)}</p>
          <div className="thread-list-item-flags">
            <span className={`status-pill ${executionTone(row.execution_state)}`}>{formatExecutionStateLabel(row.execution_state)}</span>
            {row.approval_pending ? <span className="status-pill tone-warning">待授权</span> : null}
          </div>
        </button>
      );
    }
    const thread = item.data;
    return (
      <button
        key={item.id}
        type="button"
        className={`list-card conversation-card compact-thread-card thread-list-item ${codexChatMode === "desktop" && activeCodexThreadId === thread.id ? "selected" : ""}`}
        onClick={() => focusCodexThread(thread)}
      >
        <div className="thread-list-item-head">
          <div className="thread-list-item-main">
            <p className="thread-list-item-kicker">
              <span className="thread-origin-pill desktop">桌面</span> {thread.project_name ? `项目 · ${thread.project_name}` : "工作区"}
            </p>
            <h3>{thread.title}</h3>
          </div>
          <div className="thread-list-item-side">
            {thread.updated_at ? <span className="thread-list-item-time">{formatRelativeTimestamp(thread.updated_at)}</span> : null}
          </div>
        </div>
        <p className="thread-list-item-subline">
          {thread.session_id ? truncateText(thread.session_id, 18) : "新会话"} · {formatAccessModeLabel(thread.access_mode || "default")}
        </p>
        <p className="thread-list-item-preview">{formatRecentRequest(thread)}</p>
        <div className="thread-list-item-flags">
          <span className={`status-pill ${executionTone(thread.status)}`}>{formatExecutionStateLabel(thread.status || "idle")}</span>
        </div>
      </button>
    );
  }

  async function reloadBridgeState() {
    const api = window.workspaceHubAPI;
    const [bridgePayload, servicePayload, launcherPayload, bridgeSettingsPayload, conversationsPayload] = await Promise.all([
      api.getBridgeStatus("feishu"),
      api.getCoCoServiceStatus(),
      typeof api.getLauncherStatus === "function" ? api.getLauncherStatus() : Promise.resolve(null),
      api.getBridgeSettings("feishu"),
      api.getBridgeConversations("feishu", 50),
    ]);
    setBridgeStatus(bridgePayload?.data || null);
    setServiceStatus(servicePayload?.data || servicePayload || null);
    setLauncherStatus(launcherPayload?.data || launcherPayload || null);
    setBridgeSettings({ ...DEFAULT_BRIDGE_SETTINGS, ...(bridgeSettingsPayload?.data?.settings || {}) });
    const rows = conversationsPayload?.data?.rows || [];
    setConversationRows(rows);
    if (rows.length && !rows.find((item) => item.chat_ref === selectedConversation)) {
      setSelectedConversation(rows[0].chat_ref || "");
    }
  }

  useEffect(() => {
    if (!conversationRows.length) {
      if (!conversationFilterPinned) {
        setConversationFilter("all");
      }
      return;
    }
    if (!conversationFilterPinned) {
      setConversationFilter(conversationDefaultFilter);
    }
  }, [conversationRows.length, conversationDefaultFilter, conversationFilterPinned]);

  useEffect(() => {
    if (!filteredConversationRows.length) {
      if (selectedConversation) {
        setSelectedConversation("");
      }
      return;
    }
    if (!filteredConversationRows.find((row) => row.chat_ref === selectedConversation)) {
      setSelectedConversation(filteredConversationRows[0]?.chat_ref || "");
    }
  }, [filteredConversationRows, selectedConversation]);

  function setConversationFocus(row, requestedFilter = "") {
    if (!row) return;
    const nextFilter = requestedFilter || inferFilterForConversation(row);
    setConversationFilterPinned(true);
    setConversationFilter(nextFilter);
    setSelectedConversation(row.chat_ref || "");
  }

  function selectConversationFilter(filterId) {
    setConversationFilterPinned(true);
    setConversationFilter(filterId);
  }

  useEffect(() => {
    let cancelled = false;
    async function refreshConversations() {
      try {
        await reloadBridgeState();
        const activeChatRef = selectedConversation;
        if (activeChatRef) {
          const payload = await window.workspaceHubAPI.getBridgeMessages("feishu", activeChatRef, 100);
          if (!cancelled) {
            setMessageRows((payload?.data?.rows || []).slice().reverse());
          }
        }
      } catch (refreshError) {
        if (!cancelled) {
          setError(String(refreshError?.message || refreshError || "Failed to refresh bridge state"));
        }
      }
    }
    const timer = setInterval(() => {
      void refreshConversations();
    }, AUTO_REFRESH_INTERVAL_MS);
    return () => {
      cancelled = true;
      clearInterval(timer);
    };
  }, [selectedConversation]);

  async function handleProfileSave() {
    const trimmedName = String(preferredNameInput || "").trim();
    if (!trimmedName) {
      setProfileMessage("请先告诉 CoCo 希望它如何称呼你。");
      return;
    }
    if (typeof window?.workspaceHubAPI?.saveUserProfile !== "function") {
      setProfileStatus("unavailable");
      setProfileMessage("用户画像同步暂时还没有准备好。");
      return;
    }
    setProfileSaving(true);
    setProfileMessage("");
    try {
      const response = await window.workspaceHubAPI.saveUserProfile({
        preferred_name: trimmedName,
      });
      if (response?.ok) {
        const profile = response.data?.profile || {};
        setUserProfile(profile);
        setProfileStatus("ready");
          setProfileMessage("CoCo 之后会用这个称呼与你沟通。");
        if (typeof window !== "undefined") {
          window.localStorage?.setItem(ONBOARDING_STORAGE_KEY, "1");
        }
        setOnboardingSkipped(true);
        setOnboardingVisible(false);
      } else {
        setProfileMessage(response?.error || "更新称呼失败。");
      }
    } catch (saveError) {
      setProfileMessage(`保存失败：${String(saveError?.message || saveError)}`);
    } finally {
      setProfileSaving(false);
    }
  }

  function handleSkipOnboarding() {
    if (typeof window !== "undefined") {
      window.localStorage?.setItem(ONBOARDING_STORAGE_KEY, "1");
    }
    setOnboardingSkipped(true);
    setOnboardingVisible(false);
      setProfileMessage("你之后随时都可以再修改这个称呼。");
  }

  useEffect(() => {
    let cancelled = false;
    async function loadMessages() {
      if (!selectedConversation) {
        setMessageRows([]);
        return;
      }
      try {
        const payload = await window.workspaceHubAPI.getBridgeMessages("feishu", selectedConversation, 100);
        if (cancelled) return;
        setMessageRows((payload?.data?.rows || []).slice().reverse());
      } catch (messageError) {
        if (!cancelled) {
          setError(String(messageError?.message || messageError || "加载 Feishu 会话消息失败"));
        }
      }
    }
    loadMessages();
    return () => {
      cancelled = true;
    };
  }, [selectedConversation]);

  async function saveBridgeSettings() {
    setSaveState("saving");
    try {
      const api = window.workspaceHubAPI;
      const payload = {
        ...bridgeSettings,
        allowed_users: Array.isArray(bridgeSettings.allowed_users)
          ? bridgeSettings.allowed_users
          : String(bridgeSettings.allowed_users || "")
              .split("\n")
              .map((item) => item.trim())
              .filter(Boolean),
      };
      await api.updateBridgeSettings("feishu", payload);
      await reloadBridgeState();
      setSaveState("saved");
    } catch (saveError) {
      setSaveState(`save_failed:${String(saveError?.message || saveError || "unknown_error")}`);
    }
  }

  function formatTimestamp(value) {
    if (!value) return "unknown";
    const parsed = Date.parse(value);
    if (Number.isNaN(parsed)) return "unknown";
    return new Date(parsed).toLocaleString("zh-CN");
  }

  function translatePhaseLabel(value) {
    switch (String(value || "").trim()) {
      case "ack":
        return "已确认";
      case "reply":
        return "回复";
      case "final":
        return "终结";
      case "error":
        return "错误";
      case "running":
        return "进行中";
      case "incoming":
        return "收到";
      case "direct":
        return "直连";
      default:
        return String(value || "待命");
    }
  }

  function renderPhaseLabel(row) {
    const raw = row.phase || row.status || (row.direction === "outbound" ? "reply" : "incoming");
    return translatePhaseLabel(raw);
  }

  const timelineEntries = useMemo(
    () =>
      messageRows.map((row) => ({
        id: `${row.direction}-${row.message_id}`,
        timestamp: row.created_at || row.updated_at || "",
        sender: row.direction === "outbound" ? "CoCo" : operatorDisplayName,
        text: row.text || "",
        phase: renderPhaseLabel(row),
        sessionId: row.session_id,
        status: row.status,
        sourceId: row.source_message_id,
        direction: row.direction,
      })),
    [messageRows, operatorDisplayName],
  );

  const codexTimelineEntries = useMemo(
    () =>
      activeCodexEntries.map((entry) => ({
        ...entry,
        direction: entry.role === "user" ? "outbound" : "inbound",
        roleLabel: entry.role === "user" ? operatorDisplayName : "Codex",
      })),
    [activeCodexEntries, operatorDisplayName],
  );

  const conversationMetrics = useMemo(() => {
    const highlights = [...timelineEntries].reverse();
    const total = timelineEntries.length;
    const lastInbound = highlights.find((entry) => entry.direction === "inbound") || null;
    const lastOutbound = highlights.find((entry) => entry.direction === "outbound") || null;
    const lastAck = highlights.find((entry) => entry.direction === "outbound" && entry.phase === "ack") || null;
    const lastTerminal = highlights.find(
      (entry) => entry.direction === "outbound" && ["final", "reply", "direct", "error"].includes(entry.phase),
    ) || null;
    const inboundEntries = timelineEntries.filter((entry) => entry.direction === "inbound");
    const outboundEntries = timelineEntries.filter((entry) => entry.direction === "outbound");
    const phases = Array.from(
      new Set(
        timelineEntries
          .map((entry) => entry.phase)
          .filter((value) => Boolean(value))
        .map((value) => String(value)),
      ),
    );
    const ackTime = lastAck?.timestamp ? Date.parse(lastAck.timestamp) : Number.NaN;
    const terminalTime = lastTerminal?.timestamp ? Date.parse(lastTerminal.timestamp) : Number.NaN;
    const running = lastAck && (!Number.isFinite(terminalTime) || terminalTime < ackTime);
    const brokerExecutionState = String(activeConversation?.execution_state || "").trim();
    const executionState = brokerExecutionState || (
      running
        ? "running"
        : lastTerminal?.phase === "error"
          ? "failed"
          : lastTerminal
            ? "reported"
            : "idle"
    );
    const lastUserRequest = String(activeConversation?.last_user_request || "").trim() || lastInbound?.text || "";
    const lastReport = String(activeConversation?.last_report || "").trim()
      || lastOutbound?.text
      || activeConversation?.last_message_preview
      || "";
    const lastError = String(activeConversation?.last_error || "").trim()
      || (lastTerminal?.phase === "error" ? lastTerminal?.text || "" : "");
    const reportingStatus = String(activeConversation?.reporting_status || "").trim()
      || lastOutbound?.status
      || activeConversation?.last_delivery_phase
      || "pending";
    return {
      total,
      inbound: inboundEntries.length,
      outbound: outboundEntries.length,
      lastInbound,
      lastOutbound,
      lastAck,
      lastTerminal,
      phases,
      lastUserRequest,
      lastReport,
      lastError,
      reportingStatus,
      executionState,
      ackAt: activeConversation?.last_ack_at || lastAck?.timestamp || "",
      terminalAt: activeConversation?.last_terminal_at || lastTerminal?.timestamp || "",
    };
  }, [timelineEntries, activeConversation]);

  const bindingSummary = useMemo(() => {
    if (!activeConversation) return null;
    const projectName = activeConversation.project_name || "";
    const topicName = activeConversation.topic_name || "";
    const bindingScope = activeConversation.binding_scope || "chat";
    const bound = Boolean(projectName);
    const workspaceAdmin = isWorkspaceAdminThread(activeConversation);
    return {
      bound,
      workspaceAdmin,
      bindingScope,
      projectName,
      topicName,
      sessionId: activeConversation.session_id || conversationMetrics.lastOutbound?.sessionId || "",
    };
  }, [activeConversation, conversationMetrics.lastOutbound]);

  const activeMaterialProjectName = useMemo(() => {
    if (codexChatMode === "feishu") {
      return String(activeConversation?.project_name || "").trim();
    }
    return String(activeCodexThread?.project_name || commandProject || "").trim();
  }, [activeConversation, activeCodexThread, codexChatMode, commandProject]);

  const activeMaterialPrompt = useMemo(() => {
    if (codexChatMode === "feishu") {
      return String(conversationMetrics.lastUserRequest || "").trim();
    }
    return "";
  }, [codexChatMode, conversationMetrics.lastUserRequest]);

  useEffect(() => {
    let cancelled = false;
    async function loadMaterialContext() {
      if (!activeMaterialProjectName || typeof window?.workspaceHubAPI?.getMaterialSuggest !== "function") {
        if (!cancelled) {
          setMaterialContext(null);
        }
        return;
      }
      try {
        const payload = await window.workspaceHubAPI.getMaterialSuggest(activeMaterialProjectName, activeMaterialPrompt);
        if (cancelled) return;
        setMaterialContext(payload?.ok ? payload.data || null : null);
      } catch (_error) {
        if (!cancelled) {
          setMaterialContext(null);
        }
      }
    }
    void loadMaterialContext();
    return () => {
      cancelled = true;
    };
  }, [activeMaterialProjectName, activeMaterialPrompt]);

  const activeChatView = useMemo(() => {
    if (codexChatMode === "feishu" && activeConversation) {
      const contextRows = [
        {
          label: "项目路由",
          value: bindingSummary?.workspaceAdmin
            ? "CoCo 工作区管理员"
            : bindingSummary?.bound
            ? bindingSummary.projectName
            : "自动项目路由",
        },
        {
          label: "当前模型",
          value: formatModelLabel(resolveDefaultCodexModel(metadata, "feishu"), metadata),
        },
        {
          label: "执行状态",
          value: `${formatExecutionStateLabel(conversationMetrics.executionState)} · ${conversationMetrics.reportingStatus || "pending"}`,
        },
      ];
      const retrievalProtocol = materialContext?.retrieval_protocol || null;
      if (retrievalProtocol) {
        contextRows.push({
          label: "检索协议",
          value:
            `search → timeline → detail · 下一步：${formatRetrievalStepLabel(retrievalProtocol.next_step)} `
            + `(${retrievalProtocol.search_candidate_count || 0}/${retrievalProtocol.timeline_candidate_count || 0}/${retrievalProtocol.detail_candidate_count || 0})`,
        });
        if (Array.isArray(retrievalProtocol.timeline_paths) && retrievalProtocol.timeline_paths[0]) {
          contextRows.push({
            label: "时间线入口",
            value: compactPathLabel(retrievalProtocol.timeline_paths[0]),
          });
        }
        if (Array.isArray(retrievalProtocol.detail_paths) && retrievalProtocol.detail_paths[0]) {
          contextRows.push({
            label: "细读入口",
            value: compactPathLabel(retrievalProtocol.detail_paths[0]),
          });
        }
      }
      if (conversationMetrics.lastUserRequest) {
        contextRows.push({ label: "最近用户请求", value: conversationMetrics.lastUserRequest });
      }
      if (conversationMetrics.lastReport) {
        contextRows.push({ label: "最近汇报内容", value: conversationMetrics.lastReport });
      }
      if (conversationMetrics.lastError) {
        contextRows.push({ label: "最近错误", value: conversationMetrics.lastError });
      }
      if (conversationMetrics.ackAt) {
        contextRows.push({ label: "上次确认时间", value: formatTimestamp(conversationMetrics.ackAt) });
      }
      if (conversationMetrics.terminalAt) {
        contextRows.push({ label: "最近汇报时间", value: formatTimestamp(conversationMetrics.terminalAt) });
      }
      return {
        kind: "feishu",
        eyebrow: "飞书来源线程",
        title: formatThreadLabel(activeConversation),
        subtitle: `${formatChatSubtitle(activeConversation)} · 这条线程会继续把结果发回飞书客户端。`,
        primaryChip: {
          tone: bindingSummary?.workspaceAdmin || activeConversation.project_name ? "tone-info" : "tone-warning",
          label: formatBindingDisplay(activeConversation),
        },
        statusChip: {
          tone: executionTone(conversationMetrics.executionState),
          label: formatExecutionStateLabel(conversationMetrics.executionState),
        },
        sessionLabel: activeConversation.session_id
          ? truncateText(activeConversation.session_id, 18)
          : "尚未建立 Session",
        approvalLabel: activeConversation.approval_pending
          ? `待授权 ${activeConversation.pending_approval_token || "pending"}`
          : "",
        accessLabel: "Feishu 默认",
        modelLabel: formatModelLabel(resolveDefaultCodexModel(metadata, "feishu"), metadata),
        timeline: timelineEntries.map((entry) => ({
          id: entry.id,
          direction: entry.direction === "inbound" ? "outbound" : "inbound",
          label: entry.phase,
          title: entry.sender,
          meta: entry.timestamp ? formatTimestamp(entry.timestamp) : "",
          body: entry.text || "没有捕获到可显示文本。",
          tone: entry.status === "error" || entry.phase === "错误" ? "warning" : "default",
        })),
        emptyState: {
          mark: "飞",
          title: "暂无聊天时间线。",
          copy: "飞书里的消息和 CoCo 的回复，会继续在这里展开，不再跳到另一套页面。",
        },
        contextRows,
      };
    }

    const accessMode = activeCodexThread?.access_mode || commandAccessMode;
    const contextRows = [
      { label: "当前项目", value: activeCodexThread?.project_name || commandProject || "工作区级" },
      {
        label: "执行状态",
        value: commandRunning ? "正在处理中…" : formatExecutionStateLabel(activeCodexThread?.status || "idle"),
      },
      { label: "执行档位", value: formatAccessModeLabel(accessMode) },
      {
        label: "当前模型",
        value: formatModelLabel(activeCodexThread?.model_name || commandModel, metadata),
      },
      {
        label: "执行摘要",
        value: truncateText(commandStatus || commandDefinition.description, 96),
      },
    ];
    const retrievalProtocol = materialContext?.retrieval_protocol || null;
    if (retrievalProtocol) {
      contextRows.push({
        label: "检索协议",
        value:
          `search → timeline → detail · 下一步：${formatRetrievalStepLabel(retrievalProtocol.next_step)} `
          + `(${retrievalProtocol.search_candidate_count || 0}/${retrievalProtocol.timeline_candidate_count || 0}/${retrievalProtocol.detail_candidate_count || 0})`,
      });
      if (Array.isArray(retrievalProtocol.timeline_paths) && retrievalProtocol.timeline_paths[0]) {
        contextRows.push({
          label: "时间线入口",
          value: compactPathLabel(retrievalProtocol.timeline_paths[0]),
        });
      }
      if (Array.isArray(retrievalProtocol.detail_paths) && retrievalProtocol.detail_paths[0]) {
        contextRows.push({
          label: "细读入口",
          value: compactPathLabel(retrievalProtocol.detail_paths[0]),
        });
      }
    }
    if (activeCodexThread?.session_id || commandSessionId) {
      contextRows.push({
        label: "Session",
        value: truncateText(activeCodexThread?.session_id || commandSessionId, 18),
      });
    }
    return {
      kind: "desktop",
      eyebrow: "当前对话",
      title: activeCodexThread?.title || "开始一个新的 Codex 会话",
      subtitle: "像在 Codex App 里一样直接发任务，线程会通过 start-codex 拉起并持续续接。",
      primaryChip: {
        tone: "tone-info",
        label: activeCodexThread?.project_name || commandProject || "工作区级",
      },
      statusChip: {
        tone: executionTone(activeCodexThread?.status || "idle"),
        label: commandRunning ? "正在处理中…" : formatExecutionStateLabel(activeCodexThread?.status || "idle"),
      },
      sessionLabel: activeCodexThread?.session_id
        ? truncateText(activeCodexThread.session_id, 18)
        : "尚未建立 Session",
      approvalLabel: "",
      accessLabel: formatAccessModeLabel(accessMode),
      modelLabel: formatModelLabel(activeCodexThread?.model_name || commandModel, metadata),
      timeline: codexTimelineEntries.map((entry) => ({
        id: entry.id,
        direction: entry.direction,
        label: entry.roleLabel,
        title: entry.title,
        meta: entry.meta || "",
        body: entry.body,
        tone: entry.tone === "warning" ? "warning" : "default",
      })),
      emptyState: {
        mark: "聊",
        title: "从这里开始一条桌面主对话。",
        copy: "左侧选择会话或点击“新对话”，然后直接像聊天一样把任务发给 Codex。飞书线程也会作为来源线程出现在左侧。",
      },
      contextRows,
    };
  }, [
    activeConversation,
    activeCodexThread,
    bindingSummary,
    codexChatMode,
    codexTimelineEntries,
    commandAccessMode,
    commandDefinition.description,
    commandModel,
    commandProject,
    commandRunning,
    commandSessionId,
    commandStatus,
    conversationMetrics,
    materialContext,
    metadata,
    formatTimestamp,
    timelineEntries,
  ]);

  const showActiveContext = activeChatView.kind === "feishu" ? showFeishuInspector : showCodexInspector;

  async function runBridgeAction(action) {
    setBridgeActionState(`${action}:running`);
    try {
      const api = window.workspaceHubAPI;
      if (action === "connect") {
        await api.connectBridge("feishu");
      } else if (action === "disconnect") {
        await api.disconnectBridge("feishu");
      } else if (action === "reconnect") {
        await api.reconnectBridge("feishu");
      } else {
        throw new Error(`unsupported bridge action: ${action}`);
      }
      await reloadBridgeState();
      setBridgeActionState(`${action}:ok`);
    } catch (actionError) {
      setBridgeActionState(`${action}:failed:${String(actionError?.message || actionError || "unknown_error")}`);
    }
  }

  async function runServiceAction(action) {
    setServiceActionState(`${action}:running`);
    try {
      const api = window.workspaceHubAPI;
      if (action === "install") {
        await api.installCoCoService();
      } else if (action === "restart") {
        await api.restartCoCoService();
      } else if (action === "verify") {
        const result = await api.verifyCoCoServicePersistence();
        setServiceVerification(result?.data || null);
      } else if (action === "uninstall") {
        await api.uninstallCoCoService();
      } else {
        throw new Error(`unsupported service action: ${action}`);
      }
      await reloadBridgeState();
      setServiceActionState(`${action}:ok`);
    } catch (serviceError) {
      setServiceActionState(`${action}:failed:${String(serviceError?.message || serviceError || "unknown_error")}`);
    }
  }

  async function runLauncherAction(action) {
    if (!window?.workspaceHubAPI) return;
    setLauncherActionState(`${action}:running`);
    try {
      let result = null;
      if (action === "install") {
        result = await window.workspaceHubAPI.installLauncher();
      } else if (action === "uninstall") {
        result = await window.workspaceHubAPI.uninstallLauncher();
      } else {
        throw new Error(`unsupported launcher action: ${action}`);
      }
      setLauncherStatus(result?.data || result || null);
      setLauncherActionState(`${action}:ok`);
    } catch (launcherError) {
      setLauncherActionState(`${action}:failed:${String(launcherError?.message || launcherError || "unknown_error")}`);
    }
  }

  return (
    <main className="workspace-console">
      <aside className="workspace-sidebar workspace-rail">
        <div className="rail-brand" title="Codex Hub 工作台">
          <span>CH</span>
          <i className={`rail-bridge-dot tone-${bridgeStatus?.connection_status || "unknown"}`} />
        </div>
        <nav className="sidebar-nav">
          {PRIMARY_VIEWS.map((view) => (
            <button
              key={view.id}
              type="button"
              className={`sidebar-link ${activeView === view.id ? "active" : ""}`}
              title={view.label}
              onClick={() => setActiveView(view.id)}
            >
              <span className="sidebar-link-glyph">
                <RailIcon kind={view.icon} active={activeView === view.id} />
              </span>
            </button>
          ))}
        </nav>
        <div className="rail-footer">
          <button type="button" className="ghost-button rail-footer-button" onClick={() => setOnboardingVisible(true)}>
            设置称呼
          </button>
        </div>
      </aside>

      <section className={`workspace-main ${activeView === "codex" ? "workspace-main-codex" : ""}`}>
        {activeView !== "codex" ? (
          <header className="workspace-header panel">
            <div>
              <p className="eyebrow">Codex Hub · Electron 工作台</p>
              <h2>{activeViewMeta.label}</h2>
              <p className="lede">{activeViewMeta.subtitle}</p>
            </div>
            <div className="workspace-header-meta">
              <span className="status-pill tone-neutral">{userProfile?.preferred_name || "操作者"} · {userProfile?.relationship || "工作区负责人"}</span>
              <span className={`status-pill tone-${bridgeStatus?.connection_status || "unknown"}`}>
                飞书桥接 {bridgeStatus?.connection_status || "加载中"}
              </span>
              <span className={`status-pill ${(serviceStatus?.installed && serviceStatus?.loaded) ? "tone-primary" : "tone-warning"}`}>
                {(serviceStatus?.installed && serviceStatus?.loaded) ? "CoCo 服务在线" : "CoCo 服务需处理"}
              </span>
              <span className={`status-pill ${conversationBuckets.approval ? "tone-warning" : "tone-subtle"}`}>
                待授权 {conversationBuckets.approval}
              </span>
              <span className="status-pill tone-info">主干模式 {metadata?.broker_mode || "加载中"}</span>
              <span className="status-pill tone-neutral">{truncateText(metadata?.broker_workspace_root || "等待连接", 48)}</span>
            </div>
          </header>
        ) : null}

        {error ? (
          <section className="panel error-panel">
            <strong>{error}</strong>
            <p className="hint">刷新或切换视图可以帮助重新载入聊天状态，也可以先回到飞书端确认桥接状态。</p>
          </section>
        ) : null}

        {activeView === "projects" ? (
          <>
            <section className="panel">
              <h2>项目总览</h2>
              <div className="cards">
                {(cards.length ? cards : [
                  { label: "活跃项目", value: "加载中" },
                  { label: "待审核事项", value: "加载中" },
                  { label: "健康告警", value: "加载中" },
                ]).map((item) => (
                  <article key={item.label} className="metric-card">
                    <p>{item.label}</p>
                    <strong>{item.value}</strong>
                  </article>
                ))}
              </div>
            </section>

            <section className="panel-grid project-workspace-grid">
              <article className="panel project-master-panel">
                <div className="section-head">
                  <div>
                    <p className="eyebrow">项目管理</p>
                    <h2>工作区项目看板</h2>
                  </div>
                  <p className="hint">所有加入工作区的项目都在这里统一跟进，并可直接跳到 Codex 或 Feishu 线程。</p>
                </div>
                <div className="project-list">
                  {projects.length ? (
                    projects.map((row) => {
                      const projectThreadState = projectConversationMap.get(row.project_name) || null;
                      const selected = row.project_name === activeProject?.project_name;
                      return (
                        <button
                          key={row.project_name}
                          type="button"
                          className={`project-card project-card-button ${selected ? "selected" : ""}`}
                          onClick={() => setSelectedProjectName(row.project_name)}
                        >
                          <div className="section-head">
                            <div>
                              <p className="eyebrow">{row.priority || "优先级待定"}</p>
                              <h3>{row.project_name}</h3>
                            </div>
                            <span className={`status-pill ${pillTone(row.status)}`}>{row.status || "active"}</span>
                          </div>
                          <p>{row.next_action || "暂时还没有下一步动作。"}</p>
                          <div className="project-thread-row">
                            <span className={`status-pill ${executionTone(projectThreadState?.active?.execution_state)}`}>
                              {projectThreadState?.count || 0} 条飞书线程
                            </span>
                            {projectThreadState?.attention ? (
                              <span className="status-pill tone-warning">
                                {projectThreadState?.attention} 条需处理
                              </span>
                            ) : null}
                          </div>
                        </button>
                      );
                    })
                  ) : (
                    <p className="empty-copy">共享 broker 返回最新快照后，项目会显示在这里。</p>
                  )}
                </div>
              </article>

              <article className="panel project-detail-panel">
                <div className="section-head">
                  <div>
                    <p className="eyebrow">当前项目工作区视角</p>
                    <h2>{activeProject?.project_name || "请选择一个项目"}</h2>
                  </div>
                  <p className="hint">这里不混入系统运维信息，只聚焦当前项目的状态、下一步和操作入口。</p>
                </div>
                {activeProject ? (
                  <>
                    <div className="conversation-summary project-summary-grid">
                      <article className="summary-card">
                        <p className="eyebrow">当前状态</p>
                        <strong>{activeProject.status || "active"}</strong>
                        <p className="summary-note">优先级：{activeProject.priority || "待定"}</p>
                      </article>
                      <article className="summary-card">
                        <p className="eyebrow">飞书线程</p>
                        <strong>{activeProjectConversationState?.count || 0}</strong>
                        <p className="summary-note">
                          {activeProjectConversationState?.attention
                            ? `${activeProjectConversationState.attention} 条线程需要处理`
                            : "当前线程无额外告警"}
                        </p>
                      </article>
                      <article className="summary-card">
                        <p className="eyebrow">审核 / 协同</p>
                        <strong>{activeProjectReviewRows.length + activeProjectCoordinationRows.length}</strong>
                        <p className="summary-note">
                          审核 {activeProjectReviewRows.length} · 协同 {activeProjectCoordinationRows.length}
                        </p>
                      </article>
                      <article className="summary-card">
                        <p className="eyebrow">健康告警</p>
                        <strong>{activeProjectHealthRows.length}</strong>
                        <p className="summary-note">{activeProjectHealthRows.length ? "当前项目有待确认的健康项" : "当前未发现项目级健康告警"}</p>
                      </article>
                    </div>
                    <div className="binding-callout project-focus-callout">
                      <strong>下一步动作</strong>
                      <p>{activeProject.next_action || "当前项目暂时没有显式下一步。"}</p>
                    </div>
                    <div className="inline-actions">
                      <button
                        type="button"
                        onClick={() => openCommandComposer({
                          projectName: activeProject.project_name,
                          prompt: activeProject.next_action || `继续推进 ${activeProject.project_name}`,
                        })}
                      >
                        用 Codex 处理
                      </button>
                      {activeProjectConversationState?.active ? (
                        <button
                          type="button"
                          className="ghost-button"
                          onClick={() => focusFeishuThread(activeProjectConversationState.active)}
                        >
                          打开飞书线程
                        </button>
                      ) : null}
                      {activeProject.board_path ? (
                        <button type="button" className="ghost-button" onClick={() => window.workspaceHubAPI.openPath(activeProject.board_path)}>
                          打开项目板
                        </button>
                      ) : null}
                    </div>
                    <div className="conversation-filter-group project-workspace-tabs">
                      {[
                        { id: "overview", label: "总览" },
                        { id: "review", label: "审核" },
                        { id: "coordination", label: "协同" },
                        { id: "feishu", label: "线程" },
                        { id: "health", label: "健康" },
                      ].map((tab) => (
                        <button
                          key={tab.id}
                          type="button"
                          className={`filter-chip ${projectWorkspaceView === tab.id ? "selected" : ""}`}
                          onClick={() => setProjectWorkspaceView(tab.id)}
                        >
                          {tab.label}
                        </button>
                      ))}
                    </div>
                    <article className="summary-card detail-stack-card">
                      <p className="eyebrow">{activeProjectFocusSections[projectWorkspaceView]?.title || "项目总览"}</p>
                      <strong>{activeProjectFocusSections[projectWorkspaceView]?.description || "聚焦当前项目的主要工作上下文。"}</strong>
                      {projectWorkspaceView === "overview" ? (
                        <div className="detail-stack">
                          <article className="summary-card detail-card">
                            <p className="eyebrow">审核队列</p>
                            <strong>{activeProjectReviewRows.length ? activeProjectReviewRows[0].task_ref : "当前为空"}</strong>
                            <p className="summary-note">
                              {activeProjectReviewRows.length
                                ? `${activeProjectReviewRows.length} 条审核事项等待跟进`
                                : "当前项目没有审核积压。"}
                            </p>
                          </article>
                          <article className="summary-card detail-card">
                            <p className="eyebrow">协同队列</p>
                            <strong>{activeProjectCoordinationRows.length ? activeProjectCoordinationRows[0].coordination_id : "当前为空"}</strong>
                            <p className="summary-note">
                              {activeProjectCoordinationRows.length
                                ? `${activeProjectCoordinationRows.length} 条跨项目协同需要关注`
                                : "当前项目没有跨项目协同阻塞。"}
                            </p>
                          </article>
                          <article className="summary-card detail-card">
                            <p className="eyebrow">项目线程</p>
                            <strong>{activeProjectConversationState?.active ? formatThreadLabel(activeProjectConversationState.active) : "尚未建立线程"}</strong>
                            <p className="summary-note">
                              {activeProjectConversationState?.active
                                ? activeProjectConversationState.active.last_user_request || activeProjectConversationState.active.last_report || "线程已建立，等待下一条消息。"
                                : "当前项目还没有稳定的 Feishu 线程。"}
                            </p>
                          </article>
                        </div>
                      ) : null}
                      {projectWorkspaceView === "review" ? (
                        <div className="detail-list">
                          {activeProjectReviewRows.length ? activeProjectReviewRows.map((row) => (
                            <article key={`${row.project_name}-${row.task_ref}`} className="list-card mini-list-card">
                              <div>
                                <p className="eyebrow">{row.review_status || "pending"}</p>
                                <h3>{row.task_ref}</h3>
                              </div>
                              <p>{row.deliverable_ref || "尚未关联交付物。"}</p>
                            </article>
                          )) : (
                            <div className="empty-state-card">
                              <strong>当前项目没有 review backlog。</strong>
                              <p className="empty-copy">审核队列清空时，这里只保留项目级说明。</p>
                            </div>
                          )}
                        </div>
                      ) : null}
                      {projectWorkspaceView === "coordination" ? (
                        <div className="detail-list">
                          {activeProjectCoordinationRows.length ? activeProjectCoordinationRows.map((row) => (
                            <article key={row.coordination_id} className="list-card mini-list-card">
                              <div>
                                <p className="eyebrow">{row.status || "pending"}</p>
                                <h3>{row.coordination_id}</h3>
                              </div>
                              <p>{row.from_project} → {row.to_project}</p>
                              <p>{row.requested_action || "暂无请求动作。"}</p>
                            </article>
                          )) : (
                            <div className="empty-state-card">
                              <strong>当前项目没有跨项目协同阻塞。</strong>
                              <p className="empty-copy">后续如有跨项目请求，这里会单独呈现，不与系统运维混在一起。</p>
                            </div>
                          )}
                        </div>
                      ) : null}
                      {projectWorkspaceView === "feishu" ? (
                        activeProjectConversationState?.active ? (
                          <div className="detail-stack">
                            <article className="summary-card detail-card">
                              <p className="eyebrow">当前线程</p>
                              <strong>{formatThreadLabel(activeProjectConversationState.active)}</strong>
                              <p className="summary-note">{formatChatSubtitle(activeProjectConversationState.active)}</p>
                            </article>
                            <article className="summary-card detail-card">
                              <p className="eyebrow">最近用户请求</p>
                              <strong>{truncateText(activeProjectConversationState.active.last_user_request || "暂无", 48)}</strong>
                              <p className="summary-note">{formatExecutionStateLabel(activeProjectConversationState.active.execution_state)}</p>
                            </article>
                            <article className="summary-card detail-card">
                              <p className="eyebrow">最近汇报</p>
                              <strong>{truncateText(activeProjectConversationState.active.last_report || "暂无", 48)}</strong>
                              <p className="summary-note">{activeProjectConversationState.active.reporting_status || "pending"}</p>
                            </article>
                            <div className="inline-actions compact-actions">
                              <button type="button" onClick={() => focusFeishuThread(activeProjectConversationState.active)}>
                                打开聊天还原
                              </button>
                              <button type="button" className="ghost-button" onClick={() => openCommandComposer({
                                projectName: activeProject.project_name,
                                prompt: activeProjectConversationState.active.last_user_request || activeProject.next_action || `继续推进 ${activeProject.project_name}`,
                              })}>
                                转到 Codex 交互
                              </button>
                            </div>
                          </div>
                        ) : (
                          <div className="empty-state-card">
                            <strong>当前项目还没有稳定的 Feishu 线程。</strong>
                            <p className="empty-copy">一旦该项目在 Feishu 中建立线程，这里会还原主线程的请求、执行和汇报。</p>
                          </div>
                        )
                      ) : null}
                      {projectWorkspaceView === "health" ? (
                        <div className="detail-list">
                          {activeProjectHealthRows.length ? activeProjectHealthRows.map((row, index) => (
                            <article key={`${row.project_name || activeProject.project_name}-${index}`} className="list-card mini-list-card">
                              <div>
                                <p className="eyebrow">{row.severity || row.status || "info"}</p>
                                <h3>{row.title || row.project_name || "项目健康项"}</h3>
                              </div>
                              <p>{row.summary || row.detail || "暂无摘要。"}</p>
                            </article>
                          )) : (
                            <div className="empty-state-card">
                              <strong>当前项目没有健康告警。</strong>
                              <p className="empty-copy">健康视图只呈现项目级风险，不把系统级参数混进来。</p>
                            </div>
                          )}
                        </div>
                      ) : null}
                    </article>
                  </>
                ) : (
                  <div className="empty-state-card">
                    <strong>还没有项目数据。</strong>
                    <p className="empty-copy">一旦共享 broker 返回项目快照，这里会形成真正的项目工作区视角。</p>
                  </div>
                )}
              </article>
            </section>

            <section className="panel-grid">
              <article className="panel">
                <h2>审核队列</h2>
                <div className="list-stack">
                  {reviewRows.length ? (
                    reviewRows.slice(0, 5).map((row) => (
                      <article key={`${row.project_name}-${row.task_ref}`} className="list-card">
                        <div>
                          <p className="eyebrow">{row.review_status || "pending"}</p>
                          <h3>{row.task_ref}</h3>
                        </div>
                        <p>{row.project_name}</p>
                        <p>{row.deliverable_ref || "尚未关联交付物。"}</p>
                      </article>
                    ))
                  ) : (
                    <p className="empty-copy">共享 broker 返回非空数据后，审核事项会显示在这里。</p>
                  )}
                </div>
              </article>

              <article className="panel">
                <h2>协同队列</h2>
                <div className="list-stack">
                  {coordinationRows.length ? (
                    coordinationRows.slice(0, 5).map((row) => (
                      <article key={row.coordination_id} className="list-card">
                        <div>
                          <p className="eyebrow">{row.status || "pending"}</p>
                          <h3>{row.coordination_id}</h3>
                        </div>
                        <p>
                          {row.from_project} → {row.to_project}
                        </p>
                        <p>{row.requested_action || "暂无请求动作。"}</p>
                      </article>
                    ))
                  ) : (
                    <p className="empty-copy">共享工作流数据填充后，协同任务会显示在这里。</p>
                  )}
                </div>
              </article>
            </section>
          </>
        ) : null}

        {activeView === "codex" ? (
          <section className="panel-grid codex-console-grid">
            <article className="panel codex-thread-sidebar">
              <div className="chat-sidebar-shell codepilot-thread-shell">
                <div className="chat-sidebar-pill-row">
                  <span className={`status-pill ${bridgeStatus?.stale || bridgeStatus?.event_stalled ? "tone-warning" : "tone-success"}`}>
                    {bridgeStatus?.stale || bridgeStatus?.event_stalled ? "桥接需关注" : "已连接"}
                  </span>
                </div>
                <div className="chat-shell-heading">
                  <h2>会话</h2>
                  <p>桌面对话和飞书来源线程在同一个侧栏里显示，右侧始终只有一个主聊天区。</p>
                </div>
                <div className="thread-list-actions">
                  <button type="button" className="primary-inline-button" onClick={() => createAndFocusCodexThread({ projectName: commandProject })}>
                    + 新对话
                  </button>
                  <button
                    type="button"
                    className="ghost-button icon-square-button"
                    title="打开项目工作区"
                    onClick={() => {
                      const targetProject = activeProject?.project_name || commandProject || projects[0]?.project_name || "";
                      if (targetProject) {
                        focusProjectWorkspace(targetProject, "overview");
                      } else {
                        setActiveView("projects");
                      }
                    }}
                  >
                    <span aria-hidden="true">▣</span>
                  </button>
                </div>
                <label className="search-field chat-sidebar-search">
                  <span className="sr-only">搜索会话</span>
                  <input
                    value={codexThreadSearch}
                    onChange={(event) => setCodexThreadSearch(event.target.value)}
                    placeholder="搜索项目、群名、私聊名、标题或最近内容"
                  />
                </label>
                <div className="conversation-filter-group compact-thread-filters">
                  {[
                    { id: "all", label: "全部", count: codexUnifiedThreads.length },
                    { id: "approval", label: "待授权", count: conversationBuckets.approval },
                    { id: "running", label: "运行中", count: codexThreadBuckets.running + conversationBuckets.running },
                    { id: "failed", label: "失败", count: codexThreadBuckets.failed },
                    { id: "history", label: "历史", count: conversationBuckets.archived },
                  ].map((filter) => (
                    <button
                      key={filter.id}
                      type="button"
                      className={`filter-chip ${codexShellFilter === filter.id ? "selected" : ""}`}
                      onClick={() => applyCodexShellFilter(filter.id)}
                    >
                      {filter.label}
                      <span>{filter.count}</span>
                    </button>
                  ))}
                </div>
                <p className="sidebar-inline-note">飞书 {conversationBuckets.total} · 桌面 {codexThreadBuckets.total} · 需处理 {conversationBuckets.attention}</p>
              </div>
              {bridgeAlert ? (
                <div className={`bridge-alert tone-${bridgeAlert.tone}`}>
                  <strong>{bridgeAlert.title}</strong>
                  <p>{bridgeAlert.body}</p>
                  <div className="inline-actions compact-actions">
                    <button type="button" onClick={() => runServiceAction("restart")}>重启 CoCo</button>
                    <button type="button" onClick={() => reloadBridgeState()}>刷新状态</button>
                  </div>
                </div>
              ) : null}
              <div className="list-stack codex-thread-list">
                {codexUnifiedThreadGroups.length ? (
                  codexUnifiedThreadGroups.map((group) => (
                    <section key={group.key} className="thread-group-section">
                      <div className="thread-group-header">
                        <span>{group.label}</span>
                        <span>{group.items.length}</span>
                      </div>
                      <div className="thread-group-list">
                        {group.items.map((item) => renderUnifiedThreadCard(item))}
                      </div>
                    </section>
                  ))
                ) : (
                  <div className="empty-state-card">
                    <strong>还没有可显示的线程。</strong>
                    <p className="empty-copy">桌面对话和飞书来源线程都会直接出现在这里，不再分散到不同页面。</p>
                  </div>
                )}
              </div>
            </article>

            <article className="panel codex-chat-panel">
              <div className="chat-topbar unified-chat-topbar">
                <div>
                  <p className="eyebrow compact-eyebrow">{activeChatView.eyebrow}</p>
                  <h2>{activeChatView.title}</h2>
                  <p className="chat-panel-subtitle">{activeChatView.subtitle}</p>
                </div>
                <div className="chat-topbar-actions">
                  {activeChatView.kind === "feishu" ? (
                    <button
                      type="button"
                      className="ghost-button compact-topbar-button"
                      onClick={() => openCommandComposer({
                        projectName: activeConversation?.project_name || "",
                        prompt: activeConversation?.last_user_request || activeConversation?.last_report || "继续跟进这条线程",
                        sessionId: activeConversation?.session_id || "",
                      })}
                    >
                      在桌面续接
                    </button>
                  ) : null}
                  <button
                    type="button"
                    className="ghost-button compact-topbar-button"
                    onClick={() => {
                      if (activeChatView.kind === "feishu") {
                        setShowFeishuInspector((value) => !value);
                      } else {
                        setShowCodexInspector((value) => !value);
                      }
                    }}
                  >
                    {showActiveContext ? "收起上下文" : "更多上下文"}
                  </button>
                </div>
              </div>
              <div className="compact-chat-status chat-context-strip">
                <span className={`status-pill ${activeChatView.primaryChip.tone}`}>{activeChatView.primaryChip.label}</span>
                <span className={`status-pill ${activeChatView.statusChip.tone}`}>{activeChatView.statusChip.label}</span>
                {activeChatView.accessLabel ? <span className="status-pill tone-subtle">{activeChatView.accessLabel}</span> : null}
                <span className="session-pill">{activeChatView.sessionLabel}</span>
                {activeChatView.approvalLabel ? <span className="status-pill tone-warning">{activeChatView.approvalLabel}</span> : null}
              </div>
              {showActiveContext ? (
                <div className="chat-detail-sheet subtle-detail-sheet">
                  <div className="chat-detail-list">
                    {activeChatView.contextRows.map((detail) => (
                      <div key={detail.label} className="chat-detail-row">
                        <span>{detail.label}</span>
                        <strong>{truncateText(detail.value, 96)}</strong>
                      </div>
                    ))}
                  </div>
                  {activeChatView.kind === "feishu" && activeConversation?.approval_pending ? (
                    <p className="hint">批准命令：<span className="mono-inline">/approve {activeConversation.pending_approval_token}</span> · 拒绝命令：<span className="mono-inline">/deny {activeConversation.pending_approval_token}</span></p>
                  ) : null}
                </div>
              ) : null}
              <div className="timeline-panel">
                <div className={`chat-timeline ${activeChatView.kind === "desktop" ? "codex-chat-timeline" : ""}`}>
                  {activeChatView.timeline.length ? (
                    activeChatView.timeline.map((entry) => (
                      <article
                        key={entry.id}
                        className={`chat-bubble ${activeChatView.kind === "desktop" ? "codex-bubble" : ""} ${entry.direction} ${entry.tone === "warning" ? "tone-warning" : ""}`}
                      >
                        <div className="message-row">
                          <span className="phase-pill">{entry.label}</span>
                          <strong>{entry.title}</strong>
                          {entry.meta ? <span className="timestamp">{entry.meta}</span> : null}
                        </div>
                        <p className="timeline-text">{entry.body}</p>
                      </article>
                    ))
                  ) : (
                    <div className={`empty-state-card ${activeChatView.kind === "desktop" ? "codex-empty-stage" : ""}`}>
                      <span className="chat-empty-mark">{activeChatView.emptyState.mark}</span>
                      <strong>{activeChatView.emptyState.title}</strong>
                      <p className="empty-copy">{activeChatView.emptyState.copy}</p>
                    </div>
                  )}
                </div>
              </div>
              {activeChatView.kind === "desktop" ? (
                <form className="form-grid codex-composer codepilot-composer" onSubmit={handleCommandSubmit}>
                    <div className="composer-header-row">
                      <div className="composer-context-chips">
                        <span className="status-pill tone-info">{commandProject || "工作区级"}</span>
                        <span className={`status-pill ${commandAccessMode === "full" ? "tone-warning" : "tone-subtle"}`}>
                          {formatAccessModeLabel(commandAccessMode)}
                        </span>
                        {activeCodexThread?.session_id || commandSessionId ? (
                          <span className="session-pill">{truncateText(activeCodexThread?.session_id || commandSessionId, 18)}</span>
                        ) : null}
                      </div>
                    </div>
                    {commandDefinition.usesPrompt ? (
                      <label className="textarea-field codex-chat-input">
                        <span>对 Codex 说</span>
                        <textarea
                          rows={5}
                          value={commandPrompt}
                          onChange={(event) => setCommandPrompt(event.target.value)}
                          placeholder={codexComposerPlaceholder}
                        />
                      </label>
                    ) : null}
                    <div className="composer-action-bar">
                      <div className="composer-action-left">
                        <button type="button" className="ghost-button composer-icon-button" title="新对话" onClick={() => createAndFocusCodexThread({ projectName: commandProject })}>
                          <span aria-hidden="true">＋</span>
                        </button>
                        <button type="button" className="ghost-button composer-icon-button" title="重置输入" onClick={resetCodexComposer}>
                          <span aria-hidden="true">↺</span>
                        </button>
                        <button
                          type="button"
                          className="ghost-button composer-icon-button"
                          title={showCodexAdvanced ? "收起高级设置" : "展开高级设置"}
                          onClick={() => setShowCodexAdvanced((value) => !value)}
                        >
                          <span aria-hidden="true">⋯</span>
                        </button>
                        <label className="toolbar-select compact toolbar-select-inline">
                          <span className="sr-only">权限</span>
                          <select value={commandAccessMode} onChange={(event) => setCommandAccessMode(event.target.value)}>
                            <option value="default">默认权限</option>
                            <option value="full">完全访问</option>
                          </select>
                        </label>
                      </div>
                      <div className="composer-action-right">
                        <button type="button" className="ghost-button composer-pill-button" onClick={() => openCommandComposer({ action: "open-codex-app" })}>
                          原生 Codex
                        </button>
                        <button type="submit" disabled={commandRunning}>
                          {codexPrimaryActionLabel}
                        </button>
                      </div>
                    </div>
                    {showCodexAdvanced ? (
                      <div className="codex-composer-row codex-advanced-grid">
                        <label>
                          <span>动作</span>
                          <select value={commandAction} onChange={(event) => setCommandAction(event.target.value)}>
                            {Object.entries(COMMAND_ACTIONS).map(([value, item]) => (
                              <option key={value} value={value}>{item.label}</option>
                            ))}
                          </select>
                        </label>
                        <label>
                          <span>项目</span>
                          <select value={commandProject} onChange={(event) => setCommandProject(event.target.value)}>
                            <option value="">工作区级</option>
                            {projects.map((row) => (
                              <option key={row.project_name} value={row.project_name}>{row.project_name}</option>
                            ))}
                          </select>
                        </label>
                        <label>
                          <span>访问权限</span>
                          <select value={commandAccessMode} onChange={(event) => setCommandAccessMode(event.target.value)}>
                            <option value="default">默认权限（工作区写入）</option>
                            <option value="full">完全访问（danger-full-access）</option>
                          </select>
                        </label>
                        {commandDefinition.usesSession ? (
                          <label>
                            <span>Session ID</span>
                            <input
                              value={commandSessionId}
                              onChange={(event) => setCommandSessionId(event.target.value)}
                              placeholder="填入要续接的会话 ID"
                            />
                          </label>
                        ) : null}
                      </div>
                    ) : (
                      <p className="hint composer-inline-hint">像聊天一样直接输入任务；更多动作和 Session 续接收在高级设置里。</p>
                    )}
                    <div className="composer-footer-row">
                      <div className="composer-footer-copy">
                        <strong>{codexComposerContextSummary}</strong>
                        <p>{commandStatus || "直接像聊天一样输入任务；只有动作、项目或 Session 需要展开高级设置。"}</p>
                      </div>
                    </div>
                </form>
              ) : null}
            </article>
          </section>
        ) : null}

        {activeView === "ops" ? (
          <>
            <section className="panel-grid">
              <article className="panel">
                <h2>Feishu 桥接状态</h2>
                <div className="bridge-status">
                  <span className={`status-pill tone-${bridgeStatus?.connection_status || "unknown"}`}>{bridgeStatus?.connection_status || "加载中"}</span>
                  <p>宿主：{bridgeStatus?.host_mode || "electron"}</p>
                  <p>传输方式：{bridgeStatus?.transport || "sdk_websocket_plus_rest"}</p>
                  <p>最近错误：{bridgeStatus?.last_error || "无"}</p>
                  <p>最近消息：{bridgeStatus?.last_message_preview || "无"}</p>
                  <div className="inline-actions">
                    <button type="button" onClick={() => runBridgeAction("connect")}>启动桥接</button>
                    <button type="button" onClick={() => runBridgeAction("reconnect")}>重连桥接</button>
                    <button type="button" onClick={() => runBridgeAction("disconnect")}>断开桥接</button>
                    <button type="button" onClick={() => reloadBridgeState()}>刷新桥接状态</button>
                  </div>
                  <p className="hint">{bridgeActionState || "桥接生命周期由 broker 驱动，Electron 负责可视化呈现。"}</p>
                </div>
              </article>

              <article className="panel">
                <h2>CoCo 服务</h2>
                <div className="service-status">
                  <span className={`status-pill ${(serviceStatus?.installed && serviceStatus?.loaded) ? "tone-primary" : "tone-warning"}`}>
                    {(serviceStatus?.installed && serviceStatus?.loaded) ? "常驻服务就绪" : "服务需要关注"}
                  </span>
                  {serviceStatus?.service_state?.health_summary_label ? (
                    <div className="summary-card">
                      <p className="eyebrow">服务摘要</p>
                      <strong>{serviceStatus.service_state.health_summary_label}</strong>
                      <p className="hint">{serviceStatus.service_state.health_summary_detail || "无"}</p>
                      <p className="hint">下一步：{serviceStatus.service_state.health_next_action || "无"}</p>
                    </div>
                  ) : null}
                  <p>已安装：{String(Boolean(serviceStatus?.installed))}</p>
                  <p>已加载：{String(Boolean(serviceStatus?.loaded))}</p>
                  <p>最近健康探针：{serviceStatus?.service_state?.last_health_probe_at ? formatTimestamp(serviceStatus.service_state.last_health_probe_at) : "无"}</p>
                  <p>探针结论：{serviceStatus?.service_state?.last_health_probe_status || "尚未记录"}</p>
                  <p>桥接心跳：{bridgeStatus?.heartbeat_at ? formatTimestamp(bridgeStatus.heartbeat_at) : "无"}</p>
                  <p>最近事件：{bridgeRuntimeSummary.lastEventAt ? formatTimestamp(bridgeRuntimeSummary.lastEventAt) : "无"}</p>
                  <p>最近送达：{bridgeRuntimeSummary.lastDeliveryAt ? `${formatTimestamp(bridgeRuntimeSummary.lastDeliveryAt)} · ${bridgeRuntimeSummary.lastDeliveryPhase || "report"}` : "无"}</p>
                  <p>最近消息预览：{bridgeRuntimeSummary.lastMessagePreview || "无"}</p>
                  <p>最近发送人：{bridgeRuntimeSummary.lastSenderRef || "无"}</p>
                  <p>近期消息 / 回复：{bridgeRuntimeSummary.recentMessageCount} / {bridgeRuntimeSummary.recentReplyCount}</p>
                  <p>事件停滞：{String(Boolean(bridgeStatus?.event_stalled))}</p>
                  <p>确认等待中：{String(Boolean(serviceStatus?.service_state?.ack_pending))}</p>
                  <p>确认等待秒数：{serviceStatus?.service_state?.last_bridge_pending_ack_age_seconds || 0}</p>
                  <p>连续异常次数：{serviceStatus?.service_state?.consecutive_unhealthy_checks || 0}</p>
                  <p>最近异常时间：{serviceStatus?.service_state?.last_unhealthy_at ? formatTimestamp(serviceStatus.service_state.last_unhealthy_at) : "无"}</p>
                  <p>最近异常原因：{serviceStatus?.service_state?.last_unhealthy_reason || "无"}</p>
                  <p>事件流停滞次数：{serviceStatus?.service_state?.total_event_stalled_count || 0}</p>
                  <p>最近停滞时间：{serviceStatus?.service_state?.last_event_stalled_at ? formatTimestamp(serviceStatus.service_state.last_event_stalled_at) : "无"}</p>
                  <p>确认静默次数：{serviceStatus?.service_state?.total_ack_stalled_count || 0}</p>
                  <p>最近确认静默：{serviceStatus?.service_state?.last_ack_stalled_at ? formatTimestamp(serviceStatus.service_state.last_ack_stalled_at) : "无"}</p>
                  <p>最近重连尝试：{serviceStatus?.service_state?.last_reconnect_attempt_at ? formatTimestamp(serviceStatus.service_state.last_reconnect_attempt_at) : "无"}</p>
                  <p>重连尝试结果：{serviceStatus?.service_state?.last_reconnect_attempt_at ? (serviceStatus?.service_state?.last_reconnect_attempt_ok ? "成功" : "尚未成功") : "尚未尝试"}</p>
                  <p>累计重连次数：{serviceStatus?.service_state?.total_reconnect_attempts || 0}</p>
                  <p>累计重连成功：{serviceStatus?.service_state?.total_reconnect_successes || 0}</p>
                  <p>最近恢复耗时：{serviceStatus?.service_state?.last_recovery_duration_ms ? `${serviceStatus.service_state.last_recovery_duration_ms} ms` : "无"}</p>
                  <p>恢复后线程复核：{serviceStatus?.service_state?.last_recovery_followup_at ? (serviceStatus?.service_state?.last_recovery_followup_ok ? "通过" : "需关注") : "尚未执行"}</p>
                  <p>最近线程审计：{serviceStatus?.service_state?.last_thread_audit_at ? `${formatTimestamp(serviceStatus.service_state.last_thread_audit_at)} · ${serviceStatus?.service_state?.last_thread_audit_reason || "thread_audit"}` : "无"}</p>
                  <p>最近持久化校验耗时：{serviceStatus?.service_state?.last_persistence_duration_ms ? `${serviceStatus.service_state.last_persistence_duration_ms} ms` : "无"}</p>
                  <p>活跃线程数：{serviceStatus?.service_state?.active_threads || 0}</p>
                  <p>运行中线程：{serviceStatus?.service_state?.running_threads || 0}</p>
                  <p>待授权线程：{serviceStatus?.service_state?.approval_pending_threads || 0}</p>
                  <p>需处理线程：{serviceStatus?.service_state?.attention_threads || 0}</p>
                  <p>工作区管理线程：{serviceStatus?.service_state?.workspace_admin_threads || 0}</p>
                  <p>最近线程活动：{serviceStatus?.service_state?.last_thread_message_at ? formatTimestamp(serviceStatus.service_state.last_thread_message_at) : "无"}</p>
                  <p>最近活跃线程：{serviceStatus?.service_state?.last_thread_label || "无"}</p>
                  <div className="inline-actions">
                    <button type="button" onClick={() => runServiceAction("install")}>安装 / 修复服务</button>
                    <button type="button" onClick={() => runServiceAction("restart")}>重启服务</button>
                    <button type="button" onClick={() => runServiceAction("verify")}>验证线程持久化</button>
                    <button type="button" onClick={() => reloadBridgeState()}>刷新服务状态</button>
                    {serviceStatus?.logs?.stdout ? (
                      <button type="button" className="ghost-button" onClick={() => window.workspaceHubAPI.openPath(serviceStatus.logs.stdout)}>
                        打开 stdout 日志
                      </button>
                    ) : null}
                    {serviceStatus?.logs?.stderr ? (
                      <button type="button" className="ghost-button" onClick={() => window.workspaceHubAPI.openPath(serviceStatus.logs.stderr)}>
                        打开 stderr 日志
                      </button>
                    ) : null}
                  </div>
                  <p className="hint">{serviceActionState || "CoCo 需要作为本地常驻服务持续运行。"}</p>
                  {recoverySummary ? (
                    <div className="summary-card">
                      <p className="eyebrow">最近自动恢复</p>
                      <strong>{recoverySummary.label}</strong>
                      <p className="hint">{recoverySummary.detail}</p>
                      {serviceStatus?.service_state?.last_recovery_summary ? (
                        <p className="hint">{serviceStatus.service_state.last_recovery_summary}</p>
                      ) : null}
                      <p className="hint">时间：{formatTimestamp(recoverySummary.at)}</p>
                      {recoverySummary.error ? <p className="hint error-text">{recoverySummary.error}</p> : null}
                    </div>
                  ) : null}
                  {persistenceSummary ? (
                    <div className="summary-card">
                      <p className="eyebrow">最近持久化校验</p>
                      <strong>{persistenceSummary.label}</strong>
                      <p className="hint">{persistenceSummary.detail}</p>
                      {serviceStatus?.service_state?.last_persistence_summary ? (
                        <p className="hint">{serviceStatus.service_state.last_persistence_summary}</p>
                      ) : null}
                      <p className="hint">时间：{formatTimestamp(persistenceSummary.at)}</p>
                    </div>
                  ) : null}
                  {serviceStatus?.service_state?.latest_anomaly_summary ? (
                    <p className="hint">{serviceStatus.service_state.latest_anomaly_summary}</p>
                  ) : null}
                  {serviceVerification ? (
                    <p className={`hint ${serviceVerification.ok ? "" : "error-text"}`}>
                      {serviceVerification.ok ? `线程持久化校验通过，共验证 ${serviceVerification.compared_threads} 条线程。` : `线程持久化校验发现 ${serviceVerification.mismatches?.length || 0} 处不一致。`}
                    </p>
                  ) : null}
                </div>
              </article>
            </section>

            <section className="panel-grid">
              <article className="panel">
                <h2>桌面快捷启动</h2>
                <div className="service-status">
                  <span className={`status-pill ${launcherStatus?.installed ? "tone-primary" : "tone-warning"}`}>
                    {launcherStatus?.installed ? "图标入口已安装" : "尚未安装图标入口"}
                  </span>
                  <p>位置：{launcherStatus?.launcher_path || "等待检测"}</p>
                  <p>作用：把 `Codex Hub 工作台` 安装到 `~/Applications`，可双击或拖到 Dock 启动。</p>
                  <div className="inline-actions">
                    <button type="button" onClick={() => runLauncherAction("install")}>安装 / 重装快捷入口</button>
                    <button type="button" className="ghost-button" onClick={() => runLauncherAction("uninstall")}>移除快捷入口</button>
                    {launcherStatus?.launcher_path ? (
                      <button type="button" className="ghost-button" onClick={() => window.workspaceHubAPI.openPath(launcherStatus.launcher_path)}>
                        在 Finder 中打开
                      </button>
                    ) : null}
                  </div>
                  <p className="hint">{launcherActionState || "建议安装后将 `Codex Hub 工作台.app` 拖到 Dock，作为日常桌面入口。"}</p>
                </div>
              </article>

              <article className="panel">
                <h2>健康告警</h2>
                <div className="list-stack">
                  {healthRows.length ? (
                    healthRows.slice(0, 5).map((row) => (
                      <article key={`${row.title}-${row.report_path}`} className="list-card">
                        <div>
                          <p className="eyebrow">{row.severity || "info"}</p>
                          <h3>{row.title}</h3>
                        </div>
                        <p>{row.summary}</p>
                        <p className="mono">{row.report_path}</p>
                      </article>
                    ))
                  ) : (
                    <p className="empty-copy">当前没有活动中的健康告警。</p>
                  )}
                </div>
              </article>

              <article className="panel">
                <h2>Feishu 桥接设置</h2>
                <div className="form-grid">
                  <label>
                    <span>App ID</span>
                    <input value={bridgeSettings.app_id || ""} onChange={(event) => setBridgeSettings((prev) => ({ ...prev, app_id: event.target.value }))} />
                  </label>
                  <label>
                    <span>App Secret</span>
                    <input type="password" value={bridgeSettings.app_secret || ""} onChange={(event) => setBridgeSettings((prev) => ({ ...prev, app_secret: event.target.value }))} />
                  </label>
                  <label>
                    <span>域名</span>
                    <select value={bridgeSettings.domain || "feishu"} onChange={(event) => setBridgeSettings((prev) => ({ ...prev, domain: event.target.value }))}>
                      <option value="feishu">feishu</option>
                      <option value="lark">lark</option>
                    </select>
                  </label>
                  <label>
                    <span>群聊策略</span>
                    <select value={bridgeSettings.group_policy || "mentions_only"} onChange={(event) => setBridgeSettings((prev) => ({ ...prev, group_policy: event.target.value }))}>
                      <option value="mentions_only">mentions_only</option>
                      <option value="all_messages">all_messages</option>
                    </select>
                  </label>
                  <label className="checkbox">
                    <input
                      type="checkbox"
                      checked={Boolean(bridgeSettings.require_mention)}
                      onChange={(event) => setBridgeSettings((prev) => ({ ...prev, require_mention: event.target.checked }))}
                    />
                    <span>群聊中必须 @CoCo</span>
                  </label>
                  <label className="textarea-field">
                    <span>允许用户（每行一个 Open ID）</span>
                    <textarea
                      rows={5}
                      value={allowedUsersText}
                      onChange={(event) =>
                        setBridgeSettings((prev) => ({
                          ...prev,
                          allowed_users: event.target.value
                            .split("\n")
                            .map((item) => item.trim())
                            .filter(Boolean),
                        }))
                      }
                    />
                  </label>
                </div>
                <div className="inline-actions">
                  <button type="button" onClick={() => saveBridgeSettings()}>保存 Feishu 设置</button>
                  <button
                    type="button"
                    className="ghost-button"
                    onClick={() =>
                      window.workspaceHubAPI.openPath(
                        `${metadata?.broker_workspace_root || metadata?.workspace_root || ""}/apps/electron-console/README.md`
                      )
                    }
                  >
                    打开工作台指南
                  </button>
                  <span className="hint">{saveState || "设置会通过主干 runtime broker 持久化保存。"}</span>
                </div>
              </article>
            </section>
          </>
        ) : null}
      </section>
      {onboardingVisible ? (
        <div className="onboarding-backdrop">
          <section className="panel onboarding-modal">
            <h3>希望如何称呼你？</h3>
            <p className="hint">
              CoCo 会在消息和执行报告中使用这个称呼，让协作更自然。
            </p>
            <label>
              <span>希望的称呼</span>
              <input
                value={preferredNameInput}
                onChange={(event) => {
                  setPreferredNameInput(event.target.value);
                  setProfileMessage("");
                }}
                placeholder="例如 Frank"
              />
            </label>
            <div className="inline-actions">
              <button type="button" onClick={handleProfileSave} disabled={profileSaving}>
                {profileSaving ? "保存中…" : "保存并继续"}
              </button>
              <button type="button" className="ghost-button" onClick={handleSkipOnboarding}>
                稍后决定
              </button>
            </div>
            <p className="hint">{profileMessage || "之后也可以在用户画像卡片里重新设置。"}</p>
          </section>
        </div>
      ) : null}
    </main>
  );
}
