export class VersalClient {
  static async create(
    worker = new Worker(new URL("./worker-entry.mjs", import.meta.url), { type: "module" }),
    aofWorker = new Worker(new URL("./aof-worker-entry.mjs", import.meta.url), { type: "module" }),
  ) {
    const client = new VersalClient(worker, aofWorker);
    try {
      client.#attachAof();
      await client.#request({ kind: "create" });
      return client;
    } catch (error) {
      worker.terminate();
      aofWorker.terminate();
      throw error;
    }
  }

  #worker;
  #aofWorker;
  #nextId = 1;
  #pending = new Map();
  #failure = null;

  constructor(worker, aofWorker = null) {
    this.#worker = worker;
    this.#aofWorker = aofWorker;
    this.#worker.onmessage = (event) => this.#receive(event.data);
    this.#worker.onerror = (event) => {
      event.preventDefault();
      this.#fail(new Error(event.message || "Versal worker failed to start"));
    };
    this.#worker.onmessageerror = () => {
      this.#fail(new Error("Versal worker returned an unreadable response"));
    };
  }

  execute(argv) {
    return this.#request({ kind: "execute", argv });
  }

  async destroy() {
    try {
      await this.#request({ kind: "destroy" });
    } finally {
      this.#worker.terminate();
      if (this.#aofWorker) this.#aofWorker.terminate();
    }
  }

  // Link the runtime and AOF I/O workers with a direct MessageChannel so AOF
  // traffic never passes through the main thread (design §5).
  #attachAof() {
    if (!this.#aofWorker) return;
    const channel = new MessageChannel();
    this.#worker.postMessage({ kind: "attach_aof", port: channel.port1 }, [channel.port1]);
    this.#aofWorker.postMessage({ kind: "attach_runtime", port: channel.port2 }, [channel.port2]);
  }

  #request(message) {
    if (this.#failure) {
      return Promise.reject(this.#failure);
    }
    const id = this.#nextId++;
    return new Promise((resolve, reject) => {
      this.#pending.set(id, { resolve, reject });
      try {
        this.#worker.postMessage({ id, ...message });
      } catch (error) {
        this.#pending.delete(id);
        reject(error);
      }
    });
  }

  #receive(message) {
    const pending = this.#pending.get(message.id);
    if (!pending) return;
    this.#pending.delete(message.id);
    if (message.ok) {
      pending.resolve(message.value);
    } else {
      pending.reject(new Error(message.error ?? "Versal worker request failed"));
    }
  }

  #fail(error) {
    this.#failure = error;
    for (const pending of this.#pending.values()) {
      pending.reject(error);
    }
    this.#pending.clear();
  }
}
