export class VersalClient {
  static async create(worker = new Worker(new URL("../worker/entry.mjs", import.meta.url), { type: "module" })) {
    const client = new VersalClient(worker);
    try {
      await client.#request({ kind: "create" });
      return client;
    } catch (error) {
      worker.terminate();
      throw error;
    }
  }

  #worker;
  #nextId = 1;
  #pending = new Map();
  #failure = null;

  constructor(worker) {
    this.#worker = worker;
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
    }
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
