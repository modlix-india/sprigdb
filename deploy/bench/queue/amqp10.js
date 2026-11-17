// AMQP 1.0 checks with rhea: 20 messages sent to /queues/<queue> and settled
// (accepted), received with manual accepts (the order is recorded), application properties
// kept, and a released message delivered again. Prints one JSON object of
// observations; amqp10.sh runs it against a reference and the target and
// compares them. The queue must exist (amqp10.sh declares it over 0-9-1).
// Usage: NODE_PATH=<dir with rhea> node amqp10.js host port user pass vhost queue
const rhea = require('rhea');
const [host, port, user, pass, vhost, queue] = process.argv.slice(2);
const N = 20;
const out = {};
const c = rhea.create_container().connect({
  host, port: +port, username: user, password: pass, hostname: `vhost:${vhost}`, reconnect: false,
});
c.on('connection_error', (ctx) => { out.connection_error = String(ctx.connection.error && ctx.connection.error.condition); });
c.on('disconnected', (ctx) => { if (!out.done) out.disconnected = String(ctx.error || 'closed'); });
const addr = `/queues/${queue}`;
const snd = c.open_sender({ target: { address: addr } });
let sent = 0, accepted = 0;
snd.on('sendable', () => {
  while (snd.sendable() && sent < N) { snd.send({ body: `m${sent}`, application_properties: { i: sent } }); sent++; }
});
snd.on('rejected', (ctx) => { out.send_rejected = JSON.stringify(ctx.delivery.remote_state); });
snd.on('accepted', () => { if (++accepted === N) { out.sent_accepted = accepted; receive(); } });
function receive() {
  const got = [], props = [];
  let released = false;
  const rcv = c.open_receiver({ source: { address: addr }, autoaccept: false, credit_window: 5 });
  rcv.on('message', (ctx) => {
    const b = typeof ctx.message.body === 'string' ? ctx.message.body : Buffer.from(ctx.message.body.content || ctx.message.body).toString();
    if (b === 'm3' && !released) { released = true; ctx.delivery.release({ delivery_failed: false }); return; }
    got.push(b); props.push((ctx.message.application_properties || {}).i);
    ctx.delivery.accept();
    if (got.length === N) {
      out.received = got.length;
      out.order = got.join(',');
      out.each_once = new Set(got).size === N;
      out.release_redelivered = got.filter((x) => x === 'm3').length === 1;
      out.properties_kept = props.every((p, k) => p === +got[k].slice(1));
      out.done = true;
      rcv.close(); c.close();
      console.log(JSON.stringify(out)); process.exit(0);
    }
  });
}
setTimeout(() => { out.timeout = { sent, accepted }; console.log(JSON.stringify(out)); process.exit(0); }, 30000);
