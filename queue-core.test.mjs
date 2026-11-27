import test from 'node:test';
import assert from 'node:assert/strict';
import { AnnotationQueue } from './queue-core.js';

const event = (type, seq, data) => ({ event: { type, seq, time: seq * 100, data } });
const history = (rpcId, reason = 'completed', turn = 'turn-1', before = false, base = 0) => [
  ...(before ? [event('user/message', base + 1, { source: { rpcId } }), event('turn/start', base + 2, { turn })]
    : [event('turn/start', base + 1, { turn }), event('user/message', base + 2, { source: { rpcId } })]),
  event('turn/end', base + 3, { turn, reason: typeof reason === 'string' ? { kind: reason } : reason }),
];
function harness(stored = null) {
  let durable = stored;
  let id = 0;
  const writes = [];
  const notices = [];
  const queue = new AnnotationQueue({ load: () => durable, save: value => {
    durable = structuredClone(value); writes.push(value);
  }, notify: value => notices.push(value), now: () => 123, idFactory: () => `id-${++id}` });
  return { queue, writes, notices, get durable() { return durable; } };
}
const enqueue = (queue, sessionId = 's') => queue.enqueue({ sessionId, path: 'sample.html', prompt: 'annotate', label: 'Label' });
const input = (extra = {}) => ({ sessionId: 's', inbox: { 'next-turn': [], 'next-step': [] }, events: [], ready: true, running: false, ...extra });

test('serial FIFO persists request id and intent before send; false running is not completion', async () => {
  const h = harness();
  const first = enqueue(h.queue);
  enqueue(h.queue);
  const sent = [];
  const send = async job => {
    assert.equal(h.durable.jobs.find(item => item.id === job.id).status, 'admitting');
    assert.equal(h.durable.jobs.find(item => item.id === job.id).requestId, job.requestId);
    sent.push(job);
  };
  await Promise.all([h.queue.reconcile(input({ send })), h.queue.reconcile(input({ send }))]);
  assert.equal(sent.length, 1);
  assert.equal(sent[0].id, first);
  await h.queue.reconcile(input({ send, running: false }));
  assert.equal(sent.length, 1, 'offline/native idle cannot release admission');
  await h.queue.reconcile(input({ send, events: history('unrelated') }));
  assert.equal(sent.length, 1);
  await h.queue.reconcile(input({ send, events: history(sent[0].requestId) }));
  assert.equal(sent.length, 2);
  assert.equal(h.queue.getSnapshot().jobs[0].status, 'done');
});

test('message before start correlates next start and exact turn end only', async () => {
  const { queue } = harness();
  enqueue(queue);
  enqueue(queue);
  const rpc = queue.getSnapshot().jobs[0].requestId;
  let sends = 0;
  const send = async () => { sends++; };
  const events = [event('turn/start', 1, { turn: 'old' }), event('turn/end', 2, { turn: 'old', reason: 'completed' }),
    event('user/message', 3, { source: { rpcId: rpc } }), event('turn/start', 4, { turn: 'mine' }),
    event('turn/end', 5, { turn: 'other', reason: 'completed' })];
  await queue.reconcile(input({ events, send }));
  assert.equal(sends, 0);
  assert.equal(queue.getSnapshot().jobs[0].status, 'running');
  events.push(event('turn/end', 6, { turn: 'mine', reason: 'completed' }));
  await queue.reconcile(input({ events, send }));
  assert.equal(sends, 1);
});

test('inbox admission blocks resend and cancellation, including next-step', async () => {
  for (const lane of ['next-turn', 'next-step']) {
    const { queue } = harness();
    const id = enqueue(queue);
    const rpc = queue.getSnapshot().jobs[0].requestId;
    let sends = 0;
    await queue.reconcile(input({ inbox: { [lane]: [{ id: 'msg', source: { rpcId: rpc }, content: [] }] }, send: async () => sends++ }));
    assert.equal(sends, 0);
    assert.equal(queue.cancel(id), false);
    await queue.reconcile(input({ send: async () => sends++ }));
    assert.equal(sends, 0, 'disappearing inbox entry does not prove a turn ended');
  }
});

test('refresh of admitting is uncertain and never automatically resends, even with full unrelated history', async () => {
  const h = harness();
  enqueue(h.queue);
  const stored = h.queue.getSnapshot();
  stored.jobs[0].status = 'admitting';
  stored.jobs[0].attempts = 1;
  const restored = harness(stored);
  const { queue } = restored;
  assert.equal(queue.getSnapshot().jobs[0].status, 'uncertain');
  assert.equal(queue.getSnapshot().sessions.s.paused, true);
  let sends = 0;
  queue.resume('s');
  await queue.reconcile(input({ events: history('someone-else'), send: async () => sends++ }));
  assert.equal(sends, 0);
  const rpc = stored.jobs[0].requestId;
  assert.equal(queue.retry(stored.jobs[0].id), true);
  await queue.reconcile(input({ send: async job => { sends++; assert.equal(job.requestId, rpc); } }));
  assert.equal(sends, 1);
});

test('send failure is uncertain, pauses session, and refresh preserves request id', async () => {
  const h = harness();
  const id = enqueue(h.queue);
  enqueue(h.queue);
  const rpc = h.queue.getSnapshot().jobs[0].requestId;
  await h.queue.reconcile(input({ send: async () => { throw new Error('response lost'); } }));
  assert.equal(h.queue.getSnapshot().jobs[0].status, 'uncertain');
  assert.equal(h.queue.getSnapshot().sessions.s.paused, true);
  const restored = harness(h.durable).queue;
  assert.equal(restored.retry(id), true);
  await restored.reconcile(input({ send: async job => assert.equal(job.requestId, rpc) }));
  assert.equal(restored.getSnapshot().jobs[0].status, 'waiting');
});

test('failed retry uses fresh RPC identity so Host dedup admits a new turn', async () => {
  const h = harness();
  const { queue } = h;
  const id = enqueue(queue);
  enqueue(queue);
  const rpc = queue.getSnapshot().jobs[0].requestId;
  const events = history(rpc, 'interrupted');
  let sends = 0;
  await queue.reconcile(input({ events, send: async () => sends++ }));
  assert.equal(queue.getSnapshot().jobs[0].status, 'failed');
  assert.equal(queue.getSnapshot().sessions.s.paused, true);
  assert.equal(sends, 0);
  assert.equal(queue.retry(id), true);
  const retried = queue.getSnapshot().jobs[0];
  assert.notEqual(retried.requestId, rpc);
  assert.deepEqual(retried.previousRequestIds, [rpc]);
  assert.equal(retried.lastEvidenceSeq, undefined);
  assert.equal(retried.turn, undefined);
  assert.equal(retried.retryAfterSeq, undefined);
  const hostSeen = new Set([rpc]);
  await queue.reconcile(input({ events, send: async job => {
    assert.equal(h.durable.jobs[0].requestId, job.requestId, 'new identity must persist before send');
    if (hostSeen.has(job.requestId)) return;
    hostSeen.add(job.requestId);
    sends++;
  } }));
  assert.equal(sends, 1, 'fresh failed retry must bypass old durable RPC dedup');
  await queue.reconcile(input({ events: [...events, ...history(retried.requestId, 'completed', 'retry-turn', true, 3)], send: async () => sends++ }));
  assert.equal(queue.getSnapshot().jobs[0].status, 'done');
  assert.equal(sends, 2);
});

test('uncertain retry retains RPC identity and reconciles full old completion before send', async () => {
  const { queue } = harness();
  const id = enqueue(queue);
  const rpc = queue.getSnapshot().jobs[0].requestId;
  await queue.reconcile(input({ send: async () => { throw new Error('response lost'); } }));
  assert.equal(queue.retry(id), true);
  assert.equal(queue.getSnapshot().jobs[0].requestId, rpc);
  assert.equal(queue.getSnapshot().jobs[0].previousRequestIds, undefined);
  await queue.reconcile(input({ events: history(rpc), send: async () => assert.fail('durable old completion resolves uncertainty') }));
  assert.equal(queue.getSnapshot().jobs[0].status, 'done');
});

test('resume advances past failed job; waiting unsent work cancels but active work cannot', async () => {
  const { queue } = harness();
  const first = enqueue(queue);
  const second = enqueue(queue);
  await queue.reconcile(input({ ready: false }));
  assert.equal(queue.getSnapshot().jobs[0].status, 'waiting');
  assert.equal(queue.cancel(first), true);
  await queue.reconcile(input({ send: async () => {} }));
  assert.equal(queue.cancel(second), false);
  const rpc = queue.getSnapshot().jobs[1].requestId;
  enqueue(queue);
  await queue.reconcile(input({ events: history(rpc, 'error'), send: async () => assert.fail('paused') }));
  queue.resume('s');
  let sends = 0;
  await queue.reconcile(input({ events: history(rpc, 'error'), send: async () => sends++ }));
  assert.equal(sends, 1);
});

test('incomplete and duplicate history never borrows a completion or declares all occurrences done', async () => {
  const { queue } = harness();
  enqueue(queue);
  const rpc = queue.getSnapshot().jobs[0].requestId;
  const events = [...history(rpc), event('user/message', 4, { source: { rpcId: rpc } }),
    event('turn/end', 5, { turn: 'missing-start', reason: 'completed' })];
  await queue.reconcile(input({ events, send: async () => assert.fail('unresolved history') }));
  assert.notEqual(queue.getSnapshot().jobs[0].status, 'done');
  const onlyEnd = harness();
  enqueue(onlyEnd.queue);
  await onlyEnd.queue.reconcile(input({ events: [event('turn/end', 1, { turn: 'x', reason: 'completed' })], ready: false }));
  assert.equal(onlyEnd.queue.getSnapshot().jobs[0].status, 'waiting');
});

test('unrelated session can progress and snapshots cannot mutate live state', async () => {
  const { queue } = harness();
  enqueue(queue, 'a');
  enqueue(queue, 'b');
  let sends = 0;
  await queue.reconcile(input({ sessionId: 'a', send: async () => { throw new Error('offline'); } }));
  await queue.reconcile(input({ sessionId: 'b', send: async () => sends++ }));
  assert.equal(sends, 1);
  const snapshot = queue.getSnapshot();
  snapshot.jobs[0].prompt = 'mutated';
  assert.equal(queue.getSnapshot().jobs[0].prompt, 'annotate');
});

test('SDK structured end reasons handle zero turn and only completed succeeds', async () => {
  for (const reason of [{ kind: 'completed' }, { kind: 'error', error: { message: 'provider failed', code: 'NETWORK' } },
    { kind: 'aborted', reason: 'user stop' }, { kind: 'blocked' }, { kind: 'max-tokens' }, { kind: 'future-kind' }, 'stop']) {
    const { queue } = harness();
    enqueue(queue);
    enqueue(queue);
    const rpc = queue.getSnapshot().jobs[0].requestId;
    let sends = 0;
    await queue.reconcile(input({ events: history(rpc, reason, 0), send: async () => sends++ }));
    const state = queue.getSnapshot();
    if (reason.kind === 'completed') {
      assert.equal(state.jobs[0].status, 'done');
      assert.equal(sends, 1);
    } else {
      assert.equal(sends, 0);
      assert.equal(state.sessions.s.paused, true);
      assert.equal(state.jobs[0].status, reason.kind === 'future-kind' || reason === 'stop' ? 'uncertain' : 'failed');
      if (reason.kind === 'error') assert.equal(state.jobs[0].error, 'provider failed');
    }
  }
});

test('durable persistence failure prevents send', async () => {
  let fail = false;
  const queue = new AnnotationQueue({ idFactory: (() => { let i = 0; return () => String(++i); })(),
    save: snapshot => { if (fail && snapshot.jobs.some(job => job.status === 'admitting')) throw new Error('disk full'); } });
  enqueue(queue);
  fail = true;
  await assert.rejects(queue.reconcile(input({ send: async () => assert.fail('must persist first') })), /disk full/);
});
