import init, { BrowserVersal } from "../pkg/versal.js";
import { installVersalWorker } from "./runtime.mjs";

const wasmReady = init();

installVersalWorker(self, async () => {
  await wasmReady;
  return new BrowserVersal();
});
