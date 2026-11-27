// The adapter owns cross-tab locking. This module owns admission and durable FIFO.
const TERMINAL = new Set(['done', 'failed', 'cancelled']);
const FAILURE = new Set(['error', 'failed', 'failure', 'interrupted', 'cancelled', 'canceled', 'aborted', 'blocked', 'max-tokens']);
const reasonKind = reason => typeof reason === 'string' ? reason : reason?.kind;
const reasonText = reason => typeof reason === 'string' ? reason
  : reason?.error?.message || (typeof reason?.reason === 'string' ? reason.reason : reason?.kind) || 'Unknown turn/end reason';
const clone = value => JSON.parse(JSON.stringify(value));

export class AnnotationQueue {
  constructor({ load = () => null, save = () => {}, notify = () => {}, now = Date.now,
    idFactory = () => globalThis.crypto.randomUUID() } = {}) {
    this.save = save;
    this.notify = notify;
    this.now = now;
    this.idFactory = idFactory;
    this.locks = new Map();
    const stored = load();
    if (stored && typeof stored.then === 'function') throw new TypeError('load must be synchronous');
    this.state = stored ? clone(stored) : { version: 1, jobs: [], sessions: {} };
    this.state.sessions ||= {};
    this.state.jobs ||= [];
    let restored = false;
    for (const job of this.state.jobs) {
      if (job.status === 'admitting') {
        job.status = 'uncertain';
        job.error = 'Admission was interrupted; inspect history or explicitly retry.';
        this._session(job.sessionId).paused = true;
        this._session(job.sessionId).reason = job.error;
        restored = true;
      }
    }
    if (restored) this._commit();
  }

  _session(id) {
    return this.state.sessions[id] ||= { paused: false, reason: null };
  }

  _commit() {
    // Synchronous durable storage is intentional: send may only follow this write.
    const result = this.save(this.getSnapshot());
    if (result && typeof result.then === 'function') throw new TypeError('save must be synchronous');
    this.notify(this.getSnapshot());
  }

  getSnapshot() { return clone(this.state); }

  enqueue({ sessionId, path, prompt, label = '' }) {
    if (!sessionId || typeof prompt !== 'string') throw new TypeError('sessionId and prompt are required');
    const id = this.idFactory();
    this.state.jobs.push({ id, requestId: this.idFactory(), sessionId, path, prompt, label,
      status: 'queued', createdAt: this.now(), attempts: 0, admitted: false });
    this._session(sessionId);
    this._commit();
    return id;
  }

  cancel(id) {
    const job = this.state.jobs.find(item => item.id === id);
    if (!job || !['queued', 'waiting'].includes(job.status) || job.admitted || job.attempts > 0) return false;
    job.status = 'cancelled';
    job.updatedAt = this.now();
    this._commit();
    return true;
  }

  retry(id) {
    const job = this.state.jobs.find(item => item.id === id);
    if (!job || !['failed', 'uncertain'].includes(job.status)) return false;
    if (job.status === 'failed') {
      // Host deduplicates durable messages by RPC id. A known failed turn needs
      // a new admission; an uncertain admission must retain its dedup identity.
      (job.previousRequestIds ||= []).push(job.requestId);
      job.requestId = this.idFactory();
      delete job.lastEvidenceSeq;
    }
    job.status = 'queued';
    delete job.retryAfterSeq;
    delete job.finishedAt;
    job.admitted = false;
    job.attempts = 0;
    delete job.error;
    delete job.turn;
    this._session(job.sessionId).paused = false;
    this._session(job.sessionId).reason = null;
    this._commit();
    return true;
  }

  resume(sessionId) {
    const session = this._session(sessionId);
    session.paused = false;
    session.reason = null;
    this._commit();
    return this.getSnapshot();
  }

  reconcile(input) {
    const { sessionId } = input;
    // Concurrent calls for a session must not both decide to admit its head.
    const previous = this.locks.get(sessionId) || Promise.resolve();
    const next = previous.catch(() => {}).then(() => this._reconcile(input));
    this.locks.set(sessionId, next);
    return next.finally(() => { if (this.locks.get(sessionId) === next) this.locks.delete(sessionId); });
  }

  async _reconcile({ sessionId, inbox = {}, events = [], ready = false, running = false, send }) {
    const session = this._session(sessionId);
    const history = events.map(entry => entry?.event).filter(Boolean)
      .sort((a, b) => a.seq - b.seq);
    const messages = [...(inbox['next-turn'] || []), ...(inbox['next-step'] || [])];
    const jobs = this.state.jobs.filter(job => job.sessionId === sessionId);
    for (const job of jobs) {
      if (TERMINAL.has(job.status)) continue;
      const evidence = this._evidence(job, history, messages);
      if (!evidence) continue;
      job.lastEvidenceSeq = Math.max(job.lastEvidenceSeq ?? -1, evidence.seq ?? -1);
      job.admitted = true;
      if (evidence.turn !== undefined) job.turn = evidence.turn;
      if (evidence.status === 'done') {
        job.status = 'done';
        job.finishedAt = this.now();
        delete job.error;
      } else if (evidence.status === 'failed' || evidence.status === 'uncertain') {
        job.status = evidence.status;
        job.error = evidence.reason;
        session.paused = true;
        session.reason = evidence.reason;
      } else {
        job.status = evidence.status;
      }
    }
    // Persist evidence before any subsequent admission, including completed heads.
    this._commit();
    const head = jobs.find(job => !TERMINAL.has(job.status));
    if (!head || session.paused || head.status === 'uncertain') return this.getSnapshot();
    if (jobs.some(job => job !== head && !TERMINAL.has(job.status) &&
      (job.admitted || job.attempts > 0 || ['admitting', 'running', 'uncertain'].includes(job.status)))) return this.getSnapshot();
    if (head.admitted || head.attempts > 0 || head.status === 'admitting' || head.status === 'running') return this.getSnapshot();
    if (!ready || running || typeof send !== 'function') {
      head.status = 'waiting';
      this._commit();
      return this.getSnapshot();
    }
    head.status = 'admitting';
    head.attempts += 1;
    head.updatedAt = this.now();
    // Both the stable request ID and admission intent are durable before RPC.
    this._commit();
    try {
      await send(clone(head));
      head.status = 'waiting';
      head.admitted = true;
    } catch (error) {
      // A lost response cannot prove that the host did not durably admit the RPC.
      head.status = 'uncertain';
      head.error = error instanceof Error ? error.message : String(error);
      session.paused = true;
      session.reason = head.error;
    }
    this._commit();
    return this.getSnapshot();
  }

  _evidence(job, events, inbox) {
    const occurrences = [];
    let openTurn;
    const pending = [];
    const starts = new Map();
    const ends = new Map();
    for (const event of events) {
      const data = event.data || {};
      if (event.type === 'turn/start') {
        openTurn = data.turn;
        starts.set(data.turn, event);
        for (const occurrence of pending.splice(0)) occurrence.turn = data.turn;
      } else if (event.type === 'turn/end') {
        ends.set(data.turn, event);
        if (openTurn === data.turn) openTurn = undefined;
      } else if (event.type === 'user/message') {
        // Messages arriving before start belong to the next turn, not the last
        // completed turn. Never borrow an unrelated end from truncated history.
        const occurrence = { seq: event.seq, turn: openTurn, matches: data.source?.rpcId === job.requestId };
        if (occurrence.matches) occurrences.push(occurrence);
        if (openTurn === undefined) pending.push(occurrence);
      }
    }
    const relevant = occurrences;
    const inInbox = inbox.some(message => message?.source?.rpcId === job.requestId);
    if (!relevant.length) return inInbox ? { status: 'waiting' } : null;
    const seq = Math.max(...relevant.map(item => ends.get(item.turn)?.seq ?? item.seq));
    // Any occurrence lacking its own start/end prevents declaring completion.
    const incomplete = relevant.find(item => item.turn === undefined || !starts.has(item.turn) || !ends.has(item.turn));
    if (incomplete || inInbox) {
      return { status: incomplete?.turn !== undefined ? 'running' : 'waiting', seq, turn: incomplete?.turn };
    }
    const terminal = relevant.map(item => ends.get(item.turn));
    const bad = terminal.find(event => reasonKind(event.data.reason) !== 'completed');
    if (bad) return { status: FAILURE.has(reasonKind(bad.data.reason)) ? 'failed' : 'uncertain',
      reason: reasonText(bad.data.reason), seq, turn: bad.data.turn };
    return { status: 'done', seq, turn: terminal.at(-1).data.turn };
  }
}

export default AnnotationQueue;
