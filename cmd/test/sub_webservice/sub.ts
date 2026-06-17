/**
 * Binance aggregated kline data WebSocket subscriber
 *
 * Connects to a WebSocket server that streams aggregated kline data
 * for multiple symbols. Displays each symbol as a row in a table.
 *
 * Usage:
 *   tsc sub.ts --outDir .   (compile)
 *   Then open index.html in a browser
 */

// ─── Types ───────────────────────────────────────────────────────────

/** Data structure matching the server's AggregatedFutureKline */
interface AggregatedKline {
  symbol: string;
  period?: string;
  start_time: number;
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
  quote_asset_volume: number;
  trades: number;
  close_time: number;
  taker_buy_base_asset_volume: number;
  taker_buy_quote_asset_volume: number;
  count: number;
  vd: number;
  ma10: number;
  ratio: number;
  indicators?: Record<string, unknown>;
}

/** Ping message from the server */
interface PingMessage {
  ping: number;
}

/** Union type of all possible messages */
type ServerMessage = AggregatedKline | PingMessage;

// ─── Configuration ──────────────────────────────────────────────────

const DEFAULT_SERVER = "ws://154.86.24.27:8081";
const DEFAULT_PERIOD = "1m";
const DEFAULT_KIND = "volatility";

const ALL_SYMBOLS = [
  "BTCUSDT",
  "ETHUSDT",
  "SOLUSDT",
  "TRXUSDT",
  "DOGEUSDT",
  "XRPUSDT",
  "LTCUSDT",
  "SUIUSDT",
  "ZKUSDT",
  "AAVEUSDT",
  "AVAXUSDT",
  "ZECUSDT",
  "1000PEPEUSDT",
  "OPUSDT",
  "ADAUSDT",
  "LINKUSDT",
  "UNIUSDT",
  "TONUSDT",
];

/** Data columns in display order */
const TABLE_COLUMNS = [
  { key: "symbol", label: "Symbol", format: (v: unknown) => String(v) },
  {
    key: "start_time",
    label: "Start Time",
    format: (v: unknown) => new Date(v as number).toLocaleTimeString(),
  },
  {
    key: "close_time",
    label: "Close Time",
    format: (v: unknown) => new Date(v as number).toLocaleTimeString(),
  },
  { key: "open", label: "Open", format: (v: unknown) => Number(v).toFixed(2) },
  { key: "high", label: "High", format: (v: unknown) => Number(v).toFixed(2) },
  { key: "low", label: "Low", format: (v: unknown) => Number(v).toFixed(2) },
  {
    key: "close",
    label: "Close",
    format: (v: unknown) => Number(v).toFixed(2),
  },
  {
    key: "volume",
    label: "Volume",
    format: (v: unknown) => Number(v).toFixed(4),
  },
  {
    key: "quote_asset_volume",
    label: "Quote Vol",
    format: (v: unknown) => Number(v).toFixed(2),
  },
  {
    key: "trades",
    label: "Trades",
    format: (v: unknown) => Number(v).toLocaleString(),
  },
  {
    key: "count",
    label: "Count",
    format: (v: unknown) => Number(v).toLocaleString(),
  },
  { key: "vd", label: "VD", format: (v: unknown) => Number(v).toFixed(4) },
  { key: "ma10", label: "MA10", format: (v: unknown) => Number(v).toFixed(2) },
  {
    key: "ratio",
    label: "Ratio",
    format: (v: unknown) => Number(v).toFixed(4),
  },
] as const;

type ColumnKey = (typeof TABLE_COLUMNS)[number]["key"];

// ─── State ──────────────────────────────────────────────────────────

let ws: WebSocket | null = null;
let reconnectTimer: ReturnType<typeof setTimeout> | null = null;
let isConnected = false;

// Map from symbol -> latest data
const dataMap = new Map<string, AggregatedKline>();

// ─── DOM References ─────────────────────────────────────────────────

const $ = (id: string): HTMLElement => document.getElementById(id)!;

const dom = {
  serverUrl: $("server-url") as HTMLInputElement,
  kindSelect: $("kind-select") as HTMLSelectElement,
  symbolList: $("symbol-list") as HTMLElement,
  connectBtn: $("connect-btn") as HTMLButtonElement,
  disconnectBtn: $("disconnect-btn") as HTMLButtonElement,
  status: $("status") as HTMLElement,
  tableHead: $("table-head") as HTMLElement,
  tableBody: $("table-body") as HTMLElement,
  stats: $("stats") as HTMLElement,
} as const;

// ─── Symbol Checkboxes ──────────────────────────────────────────────

function buildSymbolCheckboxes(): void {
  dom.symbolList.innerHTML = "";
  for (const sym of ALL_SYMBOLS) {
    const label = document.createElement("label");
    label.className = "symbol-checkbox";
    const cb = document.createElement("input");
    cb.type = "checkbox";
    cb.value = sym;
    cb.checked = true;
    label.appendChild(cb);
    label.appendChild(document.createTextNode(" " + sym));
    dom.symbolList.appendChild(label);
  }
}

function getSelectedSymbols(): string[] {
  const inputs = dom.symbolList.querySelectorAll<HTMLInputElement>(
    "input[type=checkbox]:checked",
  );
  return Array.from(inputs).map((cb) => cb.value);
}

// ─── Table ──────────────────────────────────────────────────────────

function buildTableHeader(): void {
  const thead = dom.tableHead;
  thead.innerHTML = "";
  const tr = document.createElement("tr");
  for (const col of TABLE_COLUMNS) {
    const th = document.createElement("th");
    th.textContent = col.label;
    tr.appendChild(th);
  }
  thead.appendChild(tr);
}

function updateTable(): void {
  const tbody = dom.tableBody;
  // Get symbols in the order they are selected
  const selectedSymbols = getSelectedSymbols();

  // Remove rows for symbols no longer selected
  for (const tr of Array.from(tbody.children)) {
    const sym = tr.getAttribute("data-symbol");
    if (sym && !selectedSymbols.includes(sym)) {
      tr.remove();
    }
  }

  // Update or create rows
  for (const sym of selectedSymbols) {
    const data = dataMap.get(sym);
    let tr = tbody.querySelector<HTMLTableRowElement>(
      `tr[data-symbol="${sym}"]`,
    );

    if (!data) {
      // No data yet — show placeholder row
      if (!tr) {
        tr = document.createElement("tr");
        tr.setAttribute("data-symbol", sym);
        tr.innerHTML =
          `<td class="symbol-cell"><strong>${sym}</strong></td>` +
          `<td colspan="${TABLE_COLUMNS.length - 1}" class="pending">waiting for data...</td>`;
        tbody.appendChild(tr);
      }
      continue;
    }

    if (!tr) {
      tr = document.createElement("tr");
      tr.setAttribute("data-symbol", sym);
      tbody.appendChild(tr);
    }

    // Update cells
    const cells = tr.children;
    for (let i = 0; i < TABLE_COLUMNS.length; i++) {
      const col = TABLE_COLUMNS[i];
      const value = data[col.key as keyof AggregatedKline];
      const formatted = col.format(value);
      if (cells[i]) {
        (cells[i] as HTMLElement).textContent = formatted;
        // Add a flash animation class to highlight updated cells
        (cells[i] as HTMLElement).classList.remove("flash");
        // Force reflow to restart animation
        void (cells[i] as HTMLElement).offsetWidth;
        (cells[i] as HTMLElement).classList.add("flash");
      } else {
        const td = document.createElement("td");
        td.textContent = formatted;
        tr.appendChild(td);
      }
    }

    // Color the ratio cell
    if (data.ratio !== undefined) {
      const ratioCell = tr.children[TABLE_COLUMNS.length - 1] as HTMLElement;
      if (ratioCell) {
        ratioCell.style.color = data.ratio >= 0 ? "#e74c3c" : "#2ecc71";
      }
    }
  }
}

function updateStats(): void {
  const selected = getSelectedSymbols().length;
  const withData = dataMap.size;
  dom.stats.textContent = `Symbols: ${selected} selected, ${withData} with data`;
}

// ─── WebSocket ──────────────────────────────────────────────────────

function buildStreamUrl(): string {
  const server = dom.serverUrl.value.trim() || DEFAULT_SERVER;
  const kind = dom.kindSelect.value;
  const symbols = getSelectedSymbols();

  // Always include BTCUSDT first (matching Go server logic)
  const allSymbols = [...new Set(["BTCUSDT", ...symbols])];

  const suffix = `@${kind}_${DEFAULT_PERIOD}`;
  const streams = allSymbols.map((s) => s + suffix).join("/");
  return `${server}/stream?streams=${streams}`;
}

function connect(): void {
  if (ws) {
    disconnect();
  }

  const url = buildStreamUrl();
  if (getSelectedSymbols().length === 0) {
    setStatus("error", "Please select at least one symbol");
    return;
  }

  setStatus("connecting", `Connecting...`);

  try {
    ws = new WebSocket(url);
  } catch (err) {
    setStatus("error", `Failed to create WebSocket: ${err}`);
    return;
  }

  ws.onopen = () => {
    isConnected = true;
    setStatus(
      "connected",
      `Connected to ${dom.serverUrl.value || DEFAULT_SERVER}`,
    );
    dom.connectBtn.disabled = true;
    dom.disconnectBtn.disabled = false;
    updateStats();
  };

  ws.onmessage = (event: MessageEvent) => {
    try {
      const raw = JSON.parse(event.data) as ServerMessage;

      // Ignore ping messages
      if ("ping" in raw && Object.keys(raw).length === 1) {
        return;
      }

      const data = raw as AggregatedKline;
      if (data.symbol) {
        dataMap.set(data.symbol, data);
        updateTable();
        updateStats();
      }
    } catch {
      console.warn("Failed to parse message:", event.data);
    }
  };

  ws.onerror = (err: Event) => {
    console.error("WebSocket error:", err);
    setStatus("error", "WebSocket error occurred");
  };

  ws.onclose = (event: CloseEvent) => {
    isConnected = false;
    setStatus(
      event.code === 1000 ? "disconnected" : "error",
      `Disconnected (code: ${event.code})`,
    );
    dom.connectBtn.disabled = false;
    dom.disconnectBtn.disabled = true;
    ws = null;
    scheduleReconnect();
  };
}

function disconnect(): void {
  if (reconnectTimer) {
    clearTimeout(reconnectTimer);
    reconnectTimer = null;
  }
  if (ws) {
    ws.onclose = null; // prevent reconnect
    ws.close(1000, "user disconnect");
    ws = null;
  }
  isConnected = false;
  setStatus("disconnected", "Disconnected");
  dom.connectBtn.disabled = false;
  dom.disconnectBtn.disabled = true;
}

function scheduleReconnect(): void {
  if (reconnectTimer) return;
  reconnectTimer = setTimeout(() => {
    reconnectTimer = null;
    if (!isConnected) {
      setStatus("connecting", "Reconnecting...");
      connect();
    }
  }, 3000);
}

// ─── Status ─────────────────────────────────────────────────────────

function setStatus(type: string, message: string): void {
  dom.status.className = `status-${type}`;
  dom.status.textContent = message;
}

// ─── Initialization ─────────────────────────────────────────────────

function init(): void {
  // Build UI
  buildSymbolCheckboxes();
  buildTableHeader();
  dom.serverUrl.value = DEFAULT_SERVER;

  // Connect button
  dom.connectBtn.addEventListener("click", connect);

  // Disconnect button
  dom.disconnectBtn.addEventListener("click", disconnect);
  dom.disconnectBtn.disabled = true;

  // Rebuild connection when symbol selection changes
  dom.symbolList.addEventListener("change", () => {
    updateTable();
    updateStats();
  });

  // Initial stats
  updateStats();
}

// Wait for DOM to be ready
if (document.readyState === "loading") {
  document.addEventListener("DOMContentLoaded", init);
} else {
  init();
}
