import { VersalClient } from "./client.mjs";

const output = document.querySelector("#terminal-output");
const form = document.querySelector("#command-form");
const input = document.querySelector("#command-input");
const submit = document.querySelector("#command-submit");
const status = document.querySelector("#runtime-status");
const examples = document.querySelectorAll("[data-command]");

const history = [];
let historyIndex = 0;
let client = null;
let isExecuting = false;

function appendOutput(kind, text) {
  const block = document.createElement("div");
  block.className = `terminal-line terminal-line--${kind}`;
  block.textContent = text;
  output.append(block);
  output.scrollTop = output.scrollHeight;
}

function quote(value) {
  return JSON.stringify(value);
}

function indentLines(lines, prefix) {
  return lines.map((line, index) => `${index === 0 ? "" : prefix}${line}`);
}

function formatCollection(items, formatter, depth) {
  if (items.length === 0) return ["(empty array)"];
  const childIndent = "   ".repeat(depth + 1);
  return items.flatMap((item, index) => {
    const lines = formatter(item, depth + 1);
    const prefix = `${index + 1}) `;
    const continuation = `${" ".repeat(prefix.length)}${childIndent}`;
    const indented = indentLines(lines, continuation);
    indented[0] = `${prefix}${indented[0]}`;
    return indented;
  });
}

function formatReply(reply, depth = 0) {
  if (!reply || typeof reply !== "object") return [String(reply)];
  switch (reply.type) {
    case "simple":
      return [String(reply.value)];
    case "bulk":
      return [reply.value == null ? "(nil)" : quote(reply.value)];
    case "integer":
      return [`(integer) ${reply.value}`];
    case "double":
      return [`(double) ${reply.value}`];
    case "boolean":
      return [`(boolean) ${reply.value}`];
    case "array":
      return formatCollection(
        reply.value,
        (value) => [value == null ? "(nil)" : quote(value)],
        depth,
      );
    case "replies":
      return formatCollection(reply.value, formatReply, depth);
    case "map":
      return formatCollection(
        reply.value,
        ([key, value], itemDepth) => {
          const keyLines = formatReply(key, itemDepth);
          const valueLines = formatReply(value, itemDepth);
          return [
            `${keyLines.join(" ")} => ${valueLines[0]}`,
            ...valueLines.slice(1),
          ];
        },
        depth,
      );
    case "error":
      return [`(error) ${reply.value}`];
    default:
      return [JSON.stringify(reply)];
  }
}

function decodeEscape(character) {
  switch (character) {
    case "n":
      return "\n";
    case "r":
      return "\r";
    case "t":
      return "\t";
    default:
      return character;
  }
}

function parseCommand(line) {
  const argv = [];
  let current = "";
  let quoteCharacter = null;
  let escaped = false;
  let hasToken = false;

  for (const character of line) {
    if (escaped) {
      current += decodeEscape(character);
      escaped = false;
      hasToken = true;
    } else if (character === "\\") {
      escaped = true;
      hasToken = true;
    } else if (quoteCharacter !== null) {
      if (character === quoteCharacter) {
        quoteCharacter = null;
      } else {
        current += character;
      }
      hasToken = true;
    } else if (character === '"' || character === "'") {
      quoteCharacter = character;
      hasToken = true;
    } else if (/\s/u.test(character)) {
      if (hasToken) {
        argv.push(current);
        current = "";
        hasToken = false;
      }
    } else {
      current += character;
      hasToken = true;
    }
  }

  if (escaped) throw new Error("Command ends with an incomplete escape");
  if (quoteCharacter !== null) throw new Error("Command contains an unterminated quote");
  if (hasToken) argv.push(current);
  return argv;
}

function showHelp() {
  appendOutput(
    "system",
    [
      "Available examples:",
      "  PING | ECHO message | SET key value [PX milliseconds]",
      "  GET key | MGET key [key ...] | DEL key [key ...]",
      "  INCR key | EXPIRE key seconds | TTL key | SCAN 0",
      "  LPUSH/RPUSH/LRANGE | SADD/SMEMBERS | HSET/HGET",
      "  ZADD/ZRANGE | DBSIZE | FLUSHDB",
      "Local commands: help, clear",
      "Use quotes or backslash escapes for arguments containing spaces.",
    ].join("\n"),
  );
}

async function execute(line) {
  let argv;
  try {
    argv = parseCommand(line);
  } catch (error) {
    appendOutput("error", error.message);
    return;
  }
  if (argv.length === 0) return;

  const localCommand = argv.length === 1 ? argv[0].toLowerCase() : "";
  if (localCommand === "clear") {
    output.replaceChildren();
    return;
  }
  if (localCommand === "help") {
    showHelp();
    return;
  }

  appendOutput("command", `versal> ${line}`);
  isExecuting = true;
  submit.disabled = true;
  try {
    const reply = await client.execute(argv);
    appendOutput(reply.type === "error" ? "error" : "reply", formatReply(reply).join("\n"));
  } catch (error) {
    appendOutput("error", `(error) ${error.message}`);
  } finally {
    isExecuting = false;
    submit.disabled = false;
    input.focus();
  }
}

form.addEventListener("submit", async (event) => {
  event.preventDefault();
  const line = input.value.trim();
  if (!line || !client || isExecuting) return;
  if (history.at(-1) !== line) history.push(line);
  historyIndex = history.length;
  input.value = "";
  await execute(line);
});

input.addEventListener("keydown", (event) => {
  if (event.key === "ArrowUp" && historyIndex > 0) {
    event.preventDefault();
    historyIndex -= 1;
    input.value = history[historyIndex];
    input.setSelectionRange(input.value.length, input.value.length);
  } else if (event.key === "ArrowDown" && historyIndex < history.length) {
    event.preventDefault();
    historyIndex += 1;
    input.value = history[historyIndex] ?? "";
  } else if (event.key.toLowerCase() === "l" && event.ctrlKey) {
    event.preventDefault();
    output.replaceChildren();
  }
});

for (const example of examples) {
  example.addEventListener("click", () => {
    input.value = example.dataset.command;
    form.requestSubmit();
  });
}

async function start() {
  appendOutput("system", "Starting Versal WebAssembly runtime...");
  try {
    client = await VersalClient.create();
    status.textContent = "WASM ready";
    status.dataset.state = "ready";
    input.disabled = false;
    submit.disabled = false;
    appendOutput("system", "Ready. Type help to list commands, or run one of the examples below.");
    input.focus();
  } catch (error) {
    status.textContent = "WASM unavailable";
    status.dataset.state = "error";
    appendOutput(
      "error",
      [
        `Unable to start Versal: ${error.message}`,
        "Build the web package, then serve this directory over HTTP:",
        "  make wasm-web",
        "  python3 -m http.server 8000 --directory src/bin/wasm",
      ].join("\n"),
    );
  }
}

start();
