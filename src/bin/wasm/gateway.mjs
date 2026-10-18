export function attachRespGateway(peer, versal, now = () => Date.now()) {
  peer.addEventListener("message", (event) => {
    if (!(event.data instanceof ArrayBuffer) && !(event.data instanceof Uint8Array)) {
      peer.close(1003, "binary RESP frames only");
      return;
    }
    const bytes = event.data instanceof Uint8Array ? event.data : new Uint8Array(event.data);
    const reply = versal.feed_resp_at(bytes, now());
    if (reply.length > 0) {
      peer.send(reply);
    }
  });
}
