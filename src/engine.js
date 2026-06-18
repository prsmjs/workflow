import { EventEmitter } from 'events'
import { randomUUID } from 'crypto'
import ms from '@prsm/ms'
import { memoryDriver } from './memoryDriver.js'
import { clone } from './util.js'

/**
 * @typedef {Object} WorkflowEngineOptions
 * @property {object} [storage] - persistence adapter that stores executions and coordinates workers (default an in-memory driver). Use the in-memory driver for tests and prototypes, and the SQLite or Postgres driver for durable, crash-recoverable state. Postgres is required when more than one worker process shares the same executions.
 * @property {string|number} [leaseMs] - how long a worker owns a claimed execution before another worker may reclaim it, as a duration string ("5m", "30s") or milliseconds (default "5m"). A worker that crashes mid-step holds its lease only until this window expires, after which the execution becomes available again.
 * @property {string|number} [leaseRenewInterval] - how often a running step renews its lease so a long step does not lose ownership, as a duration string or milliseconds (default leaseMs / 3, with a floor of 10ms). Keep this comfortably below leaseMs so a renewal can land before the lease expires.
 * @property {string|number} [defaultActivityTimeout] - timeout applied to activity steps that do not set their own timeout, as a duration string or milliseconds (default "30s"). Decision and terminal steps have no default timeout; only activities inherit this value.
 * @property {string} [owner] - identity written into claims and checked on every owner-guarded save (default a random "workflow-engine:<uuid>"). Set a stable, per-process value (for example "worker-<pid>") so claims and saves are attributable across restarts.
 * @property {number} [batchSize] - maximum number of ready executions to claim and process concurrently per polling cycle (default 10). Higher values raise throughput and concurrent load; this is the per-cycle ceiling, not a global limit.
 * @property {number} [maxJournalEntries] - cap on the number of journal entries retained per execution, where 0 means unbounded (default 0). When the cap is exceeded the oldest entries are dropped, keeping only the most recent ones.
 * @property {object} [tracer] - optional tracer (for example a prsm/trace tracer) used to wrap start, signal, cancel, pause, resume, and each step in spans. When omitted, no tracing is performed.
 * @property {object} [pubsub] - optional Redis connection (a node-redis client or a client-options object) that broadcasts execution and step events across engine instances and powers cross-instance workflow discovery. When omitted, events stay local to this process.
 * @property {string} [pubsubPrefix] - key and channel prefix for the cross-instance event channel and instance registry when pubsub is enabled (default "workflow:"). All instances that should see each other must share the same prefix.
 */

/**
 * @typedef {Object} StartOptions
 * @property {string} [version] - explicit workflow version to start. Required only when more than one version of the same workflow name is registered; otherwise the single registered version is used.
 * @property {object} [data] - initial mutable working state for the execution, merged into by activity steps that return plain objects (default {}). Available to every step as context.data.
 * @property {object} [metadata] - arbitrary read-only metadata attached to the execution for your own bookkeeping, never modified by the engine (default {}).
 * @property {Array} [tags] - free-form tags attached to the execution for filtering and grouping in your own tooling (default []).
 * @property {string} [id] - explicit execution id to assign instead of a generated UUID. Useful for deterministic ids or idempotent creation; the storage adapter rejects a duplicate id.
 */

/**
 * @typedef {Object} RunDueOptions
 * @property {number} [limit] - maximum number of ready executions to claim and process in this single pass (default the engine's batchSize).
 */

/**
 * @typedef {Object} RunUntilIdleOptions
 * @property {number} [limit] - maximum number of executions to claim per internal runDue pass (default the engine's batchSize).
 * @property {number} [maxPasses] - safety cap on how many runDue passes to make before giving up (default 100). Exceeding it throws, which signals work that never drains (for example a step that immediately re-queues itself).
 */

/**
 * @typedef {Object} StartWorkerOptions
 * @property {string|number} [interval] - how often the worker polls for ready executions, as a duration string ("1s", "100ms") or milliseconds (default "1s"). Ticks never overlap: a slow cycle delays the next poll rather than running two at once.
 * @property {number} [batchSize] - maximum executions claimed and processed per poll, overriding the engine's batchSize for this worker only (default the engine's batchSize).
 */

/**
 * @typedef {Object} RestartUnderOptions
 * @property {string} [version] - workflow version to restart the execution under (default the same version the execution already ran on). The new execution starts from the workflow's start step; completed work is not replayed.
 * @property {*} [input] - input to pass to the new execution (default the previous execution's input).
 * @property {object} [data] - working state to carry into the new execution (default the previous execution's data).
 * @property {object} [metadata] - metadata to carry into the new execution (default the previous execution's metadata).
 * @property {Array} [tags] - tags to carry into the new execution (default the previous execution's tags).
 */

/**
 * @typedef {Object} ListExecutionsFilter
 * @property {string} [workflow] - only return executions of this workflow name.
 * @property {string} [status] - only return executions in this status (one of "queued", "waiting", "running", "suspended", "paused", "succeeded", "failed", "canceled").
 * @property {number} [limit] - maximum number of executions to return, most recently created first.
 */

const DISTRIBUTED_EVENTS = new Set([
  'execution:queued',
  'execution:succeeded',
  'execution:failed',
  'execution:canceled',
  'execution:paused',
  'execution:lease-lost',
  'step:started',
  'step:succeeded',
  'step:routed',
  'step:retry',
  'step:failed',
  'step:suspended',
  'step:resumed',
])

const DEFAULT_RETRY = { maxAttempts: 1, backoff: 0 }
const TERMINAL_EXECUTION_STATUSES = new Set(['succeeded', 'failed', 'canceled'])
const PROCESSABLE_EXECUTION_STATUSES = new Set(['queued', 'waiting'])

/**
 * Thrown when signaling, or resolving the wait step of, an execution that is no longer suspended (it was already signaled, canceled, or otherwise moved on). The offending execution id is exposed as `executionId`.
 */
export class AlreadySignaledError extends Error {
  /**
   * @param {string} executionId - the id of the execution that was no longer suspended.
   */
  constructor(executionId) {
    super(`execution ${executionId} is no longer suspended`)
    this.name = 'AlreadySignaledError'
    this.executionId = executionId
  }
}

function serializeError(error) {
  if (!error) return { name: 'Error', message: 'Unknown error' }

  return {
    name: error.name ?? 'Error',
    message: error.message ?? String(error),
    code: error.code,
    stack: error.stack,
  }
}

function withTimeout(promise, timeoutMs, message, onTimeout) {
  if (!timeoutMs || timeoutMs <= 0) return promise

  let timer = null

  return Promise.race([
    promise,
    new Promise((_, reject) => {
      timer = setTimeout(() => {
        onTimeout?.()
        reject(new Error(message))
      }, timeoutMs)
      timer.unref?.()
    }),
  ]).finally(() => {
    if (timer) clearTimeout(timer)
  })
}

function normalizeMs(value, fallback = 0) {
  if (value == null) return ms(fallback)
  return ms(value)
}

function freshStepState(execution, stepName, pass = 1) {
  return {
    status: 'pending',
    attempts: 0,
    output: null,
    error: null,
    startedAt: null,
    endedAt: null,
    route: null,
    pass,
    idempotencyKey: pass > 1 ? `${execution.id}:${stepName}:${pass}` : `${execution.id}:${stepName}`,
  }
}

function ensureStepState(execution, stepName) {
  execution.steps[stepName] = execution.steps[stepName] ?? freshStepState(execution, stepName)
  return execution.steps[stepName]
}

function stepContext(execution, workflow, stepName) {
  const currentStep = workflow.steps[stepName]
  const stepState = execution.steps[stepName]

  return {
    execution: clone(execution),
    workflow: {
      name: workflow.name,
      version: workflow.version,
    },
    input: clone(execution.input),
    data: clone(execution.data),
    metadata: clone(execution.metadata),
    steps: clone(execution.steps),
    params: clone(currentStep.params ?? {}),
    step: {
      name: stepName,
      type: currentStep.type,
      attempt: stepState?.attempts ?? 0,
      pass: stepState?.pass ?? 1,
      idempotencyKey: stepState?.idempotencyKey ?? `${execution.id}:${stepName}`,
    },
    getStep(name) {
      return clone(execution.steps[name] ?? null)
    },
  }
}

function terminalState(type) {
  return type === 'succeed' ? 'succeeded' : 'failed'
}

class LeaseLostError extends Error {
  constructor(executionId, stepName) {
    super(`workflow execution lease lost: ${executionId}:${stepName}`)
    this.name = 'LeaseLostError'
    this.executionId = executionId
    this.stepName = stepName
  }
}

export class WorkflowEngine extends EventEmitter {
  /**
   * @param {WorkflowEngineOptions} [options] - engine configuration controlling storage, leasing, concurrency, tracing, and cross-instance events.
   */
  constructor(options = {}) {
    super()

    this._tracer = options.tracer ?? null
    this._storage = options.storage ?? memoryDriver()
    this._workflows = new Map()
    this._owner = options.owner ?? `workflow-engine:${randomUUID()}`
    this._instanceId = randomUUID()
    this._leaseMs = normalizeMs(options.leaseMs ?? '5m')
    this._defaultActivityTimeout = normalizeMs(options.defaultActivityTimeout ?? '30s')
    this._leaseRenewInterval = normalizeMs(options.leaseRenewInterval ?? Math.max(10, Math.floor(this._leaseMs / 3)))
    this._pollTimer = null
    this._started = false
    this._activePoll = null
    this._batchSize = options.batchSize ?? 10
    this._maxJournalEntries = options.maxJournalEntries ?? 0

    this._pubsubConfig = options.pubsub ?? null
    this._pubsubPrefix = options.pubsubPrefix ?? 'workflow:'
    this._eventsChannel = `${this._pubsubPrefix}events`
    this._pubClient = null
    this._subClient = null

    if (this._pubsubConfig) {
      const baseEmit = super.emit.bind(this)
      this.emit = (event, payload) => {
        const result = baseEmit(event, payload)
        if (DISTRIBUTED_EVENTS.has(event) && this._pubClient?.isOpen) {
          let body
          try { body = JSON.stringify({ event, payload, from: this._instanceId }) } catch { return result }
          this._pubClient.publish(this._eventsChannel, body).catch(() => {})
        }
        return result
      }
    }
  }

  /**
   * Initialize storage and, if pubsub is configured, the cross-instance event channel and registry. Called automatically by start, signal, and the run methods, so explicit calls are optional and idempotent.
   * @returns {Promise<void>} resolves once the engine is ready to process work.
   */
  async ready() {
    if (this._started) return
    this._started = true
    if (this._storage.init) await this._storage.init()
    if (this._pubsubConfig) {
      const baseRedis = this._pubsubConfig.redis ?? this._pubsubConfig
      if (typeof baseRedis.duplicate === 'function') {
        this._pubClient = baseRedis
      } else {
        const { createClient } = await import('redis')
        this._pubClient = createClient(baseRedis)
      }
      this._pubClient.on?.('error', () => {})
      this._subClient = this._pubClient.duplicate()
      this._subClient.on?.('error', () => {})

      if (!this._pubClient.isOpen) await this._pubClient.connect()
      if (!this._subClient.isOpen) await this._subClient.connect()
      await this._subClient.subscribe(this._eventsChannel, (message) => {
        try {
          const { event, payload, from } = JSON.parse(message)
          if (from === this._instanceId) return
          super.emit(event, payload)
        } catch {}
      })

      this._registryKey = `${this._pubsubPrefix}registry:${this._instanceId}`
      this._registryTtlSec = 60
      this._registryHeartbeatMs = 15000
      await this._writeRegistry()
      this._registryTimer = setInterval(() => { this._writeRegistry() }, this._registryHeartbeatMs)
      this._registryTimer.unref?.()
    }
  }

  async _writeRegistry() {
    if (!this._pubClient?.isOpen) return
    const snapshot = {
      instanceId: this._instanceId,
      workflows: Array.from(this._workflows.values()).map((wf) => ({
        name: wf.name,
        version: wf.version,
        description: wf.description,
        graph: wf.graph,
      })),
    }
    try {
      await this._pubClient.set(this._registryKey, JSON.stringify(snapshot), { EX: this._registryTtlSec })
    } catch {}
  }

  /**
   * Discover workflows registered on every engine instance sharing this pubsub prefix, keyed by instance id. Without pubsub configured this returns only the local instance's workflows.
   * @returns {Promise<Object<string, Array<{name: string, version: string, description: string}>>>} a map of instance id to its registered workflows.
   */
  async listWorkflowsAcrossInstances() {
    const local = this.listWorkflows().map((w) => ({ ...w, instanceId: this._instanceId }))
    if (!this._pubClient?.isOpen) return { [this._instanceId]: this.listWorkflows() }
    const out = { [this._instanceId]: this.listWorkflows() }
    try {
      const keys = []
      for await (const batch of this._pubClient.scanIterator({ MATCH: `${this._pubsubPrefix}registry:*`, COUNT: 100 })) {
        if (Array.isArray(batch)) keys.push(...batch)
        else keys.push(batch)
      }
      if (keys.length) {
        const values = await this._pubClient.mGet(keys)
        keys.forEach((key, i) => {
          const id = key.slice(`${this._pubsubPrefix}registry:`.length)
          if (id === this._instanceId) return
          if (!values[i]) return
          try {
            const snap = JSON.parse(values[i])
            out[id] = (snap.workflows ?? []).map((w) => ({ name: w.name, version: w.version, description: w.description }))
          } catch {}
        })
      }
    } catch {}
    return out
  }

  /**
   * Register a workflow definition so executions of it can be started and processed. Works at any time, not just at startup, so versions can be added while workers run.
   * @param {object} workflow - a frozen definition produced by defineWorkflow.
   * @returns {this} the engine, for chaining.
   */
  register(workflow) {
    const key = `${workflow.name}@${workflow.version}`
    if (this._workflows.has(key)) throw new Error(`workflow already registered: ${key}`)
    this._workflows.set(key, workflow)
    if (this._started && this._pubClient?.isOpen) this._writeRegistry().catch(() => {})
    return this
  }

  /**
   * Remove a registered workflow version. In-flight executions of that version fail to process until it is registered again, so only unregister after its executions have drained or been restarted under a newer version.
   * @param {string} name - the workflow name.
   * @param {string} version - the exact version to remove (required; there is no implicit version here).
   * @returns {this} the engine, for chaining.
   */
  unregister(name, version) {
    if (!version) throw new Error('unregister requires an explicit version')
    const key = `${name}@${version}`
    if (!this._workflows.delete(key)) throw new Error(`workflow not registered: ${key}`)
    if (this._started && this._pubClient?.isOpen) this._writeRegistry().catch(() => {})
    return this
  }

  /**
   * List the workflows registered on this engine instance.
   * @returns {Array<{name: string, version: string, description: string}>} one entry per registered workflow.
   */
  listWorkflows() {
    return Array.from(this._workflows.values()).map((workflow) => ({
      name: workflow.name,
      version: workflow.version,
      description: workflow.description,
    }))
  }

  /**
   * Return a serializable description of a registered workflow, including its graph of nodes and edges for introspection and visualization.
   * @param {string} name - the workflow name.
   * @param {string} [version] - the version to describe; required only when more than one version of the name is registered.
   * @returns {{name: string, version: string, description: string, graph: object}} the workflow's metadata and graph.
   */
  describe(name, version) {
    const workflow = this._resolveWorkflow(name, version)
    return clone({
      name: workflow.name,
      version: workflow.version,
      description: workflow.description,
      graph: workflow.graph,
    })
  }

  /**
   * Create and persist a new execution of a workflow, queued to run on the next worker poll. Returns immediately after persisting; it does not run any steps.
   * @param {string} name - the workflow name to start.
   * @param {*} input - the input payload, available to steps as context.input.
   * @param {StartOptions} [options] - version selection and initial data, metadata, tags, and id.
   * @returns {Promise<object>} the created execution record in "queued" status.
   */
  async start(name, input, options = {}) {
    await this.ready()

    const workflow = this._resolveWorkflow(name, options.version)
    const doStart = async () => {
      const execution = this._createExecution(workflow, input, options)
      if (options.parent) {
        execution.parent = clone(options.parent)
      }
      if (this._tracer) {
        const traceparent = this._tracer.toTraceparent()
        if (traceparent) execution.traceparent = traceparent
      }

      await this._storage.createExecution(execution)
      this.emit('execution:queued', { execution: clone(execution) })
      return clone(execution)
    }

    if (!this._tracer) return await doStart()
    return await this._tracer.span(`workflow.start:${name}`, {
      'workflow.name': workflow.name,
      'workflow.version': workflow.version,
    }, doStart)
  }

  /**
   * Deliver an external signal to an execution suspended at a wait step, resolving it to one of the step's routes. Idempotent: a second call for an already-resolved execution throws AlreadySignaledError and leaves it unchanged.
   * @param {string} id - the execution id.
   * @param {*} payload - the signal payload, passed to the wait step's resolve function and recorded as the step's output. If the step has no resolve function, the payload must include a string `route`.
   * @returns {Promise<object>} the updated execution after it advances past the wait step.
   */
  async signal(id, payload) {
    await this.ready()
    if (!this._tracer) return this._signal(id, payload)
    return this._tracer.span('workflow.signal', { 'workflow.execution': id }, () => this._signal(id, payload))
  }

  async _signal(id, payload) {
    const execution = await this._storage.getExecution(id)
    if (!execution) throw new Error(`execution not found: ${id}`)
    if (execution.status !== 'suspended') {
      throw new AlreadySignaledError(id)
    }

    const workflow = this._resolveWorkflow(execution.workflow, execution.workflowVersion)
    const stepName = execution.currentStep
    const definition = workflow.steps[stepName]
    if (!definition || definition.type !== 'wait') {
      throw new Error(`execution ${id} is not at a wait step`)
    }

    let route
    if (typeof definition.resolve === 'function') {
      const context = stepContext(execution, workflow, stepName)
      route = await Promise.resolve(definition.resolve({ ...context, signal: clone(payload) }))
    } else if (payload && typeof payload === 'object' && typeof payload.route === 'string') {
      route = payload.route
    } else {
      throw new Error(`wait step "${stepName}" has no resolve function and signal payload did not include { route }`)
    }

    if (!Object.hasOwn(definition.transitions, route)) {
      throw new Error(`wait step "${stepName}" returned unknown route "${route}"`)
    }

    return await this._resolveSuspendedStep(execution, definition, stepName, {
      route,
      output: clone(payload) ?? null,
      journalEntry: { type: 'step.signaled', step: stepName, route, payload: clone(payload) ?? null },
    })
  }

  /**
   * Fetch the full persisted state of a single execution, including its current status, step states, journal, and final output or error.
   * @param {string} id - the execution id.
   * @returns {Promise<object|null>} the execution record, or null if no execution has that id.
   */
  async getExecution(id) {
    await this.ready()
    return await this._storage.getExecution(id)
  }

  /**
   * List executions, most recently created first, optionally filtered by workflow, status, or count.
   * @param {ListExecutionsFilter} [filter] - narrowing criteria.
   * @returns {Promise<Array<object>>} matching execution records.
   */
  async listExecutions(filter = {}) {
    await this.ready()
    return await this._storage.listExecutions(filter)
  }

  /**
   * Cancel a non-terminal execution and cascade the cancellation to any suspended child executions. Already-terminal executions are returned unchanged.
   * @param {string} id - the execution id.
   * @param {string} [reason] - human-readable reason recorded in the journal and the execution error (default "Canceled").
   * @returns {Promise<object>} the canceled execution.
   */
  async cancel(id, reason = 'Canceled') {
    await this.ready()
    if (!this._tracer) return this._cancel(id, reason)
    return this._tracer.span('workflow.cancel', { 'workflow.execution': id }, () => this._cancel(id, reason))
  }

  async _cancel(id, reason) {
    const execution = await this._storage.getExecution(id)
    if (!execution) throw new Error(`execution not found: ${id}`)
    if (TERMINAL_EXECUTION_STATUSES.has(execution.status)) return clone(execution)

    const completedAt = Date.now()
    execution.status = 'canceled'
    execution.error = { name: 'Canceled', message: reason }
    execution.completedAt = completedAt
    execution.updatedAt = completedAt
    execution.currentStep = null
    this._releaseLock(execution)
    execution.journal.push({
      type: 'execution.canceled',
      at: completedAt,
      reason,
    })

    this._trimJournal(execution)
    await this._storage.saveExecution(execution)
    this.emit('execution:canceled', { execution: clone(execution) })

    await this._cancelChildren(execution.id, `parent ${execution.id} canceled`)

    if (execution.parent) {
      await this._resolveSubworkflowParent(execution)
    }

    return clone(execution)
  }

  async _cancelChildren(parentId, reason) {
    if (!this._storage.findChildren) return
    const children = await this._storage.findChildren(parentId)
    for (const child of children) {
      if (TERMINAL_EXECUTION_STATUSES.has(child.status)) continue
      try {
        await this.cancel(child.id, reason)
      } catch {}
    }
  }

  /**
   * Halt a non-terminal execution where it stands. Paused executions are never claimed by workers and cannot be signaled; pause records the status it interrupted so resume can restore it. Throws if the execution is already terminal.
   * @param {string} id - the execution id.
   * @param {string} [reason] - human-readable reason recorded in the journal (default "Paused").
   * @returns {Promise<object>} the paused execution.
   */
  async pause(id, reason = 'Paused') {
    await this.ready()
    if (!this._tracer) return this._pause(id, reason)
    return this._tracer.span('workflow.pause', { 'workflow.execution': id }, () => this._pause(id, reason))
  }

  async _pause(id, reason) {
    const execution = await this._storage.getExecution(id)
    if (!execution) throw new Error(`execution not found: ${id}`)
    if (TERMINAL_EXECUTION_STATUSES.has(execution.status)) {
      throw new Error(`cannot pause ${execution.status} execution: ${id}`)
    }
    if (execution.status === 'paused') return clone(execution)

    const now = Date.now()
    execution.pausedFrom = execution.status
    execution.pausedAvailableAt = execution.availableAt
    execution.status = 'paused'
    execution.availableAt = null
    execution.updatedAt = now
    this._releaseLock(execution)
    execution.journal.push({
      type: 'execution.paused',
      at: now,
      reason,
      from: execution.pausedFrom,
    })

    this._trimJournal(execution)
    await this._storage.saveExecution(execution)
    this.emit('execution:paused', { execution: clone(execution) })
    return clone(execution)
  }

  /**
   * Resume a paused or failed execution. A paused execution returns to the exact status it was paused from (a suspended wait step keeps its original timeout deadline). A failed execution re-queues from the step that failed and gets exactly one more attempt; its attempt counter is not reset, so an exhausted retry budget means it fails again immediately.
   * @param {string} id - the execution id.
   * @returns {Promise<object>} the resumed execution.
   */
  async resume(id) {
    await this.ready()
    if (!this._tracer) return this._resume(id)
    return this._tracer.span('workflow.resume', { 'workflow.execution': id }, () => this._resume(id))
  }

  async _resume(id) {
    const execution = await this._storage.getExecution(id)
    if (!execution) throw new Error(`execution not found: ${id}`)
    if (execution.status === 'paused') return this._resumePaused(execution)
    if (execution.status !== 'failed') throw new Error('only failed or paused executions can be resumed')
    if (!execution.currentStep) throw new Error('failed execution has no current step to resume')

    const queuedAt = Date.now()
    execution.status = 'queued'
    execution.availableAt = queuedAt
    execution.updatedAt = queuedAt
    execution.error = null
    this._releaseLock(execution)
    execution.journal.push({
      type: 'execution.resumed',
      at: queuedAt,
      step: execution.currentStep,
    })

    this._trimJournal(execution)
    await this._storage.saveExecution(execution)
    this.emit('execution:queued', { execution: clone(execution) })
    return clone(execution)
  }

  async _resumePaused(execution) {
    const now = Date.now()
    const from = execution.pausedFrom ?? 'queued'

    if (from === 'running') {
      execution.status = 'queued'
      execution.availableAt = now
    } else {
      execution.status = from
      execution.availableAt = execution.pausedAvailableAt ?? (from === 'suspended' ? null : now)
    }

    execution.updatedAt = now
    delete execution.pausedFrom
    delete execution.pausedAvailableAt
    execution.journal.push({
      type: 'execution.resumed',
      at: now,
      step: execution.currentStep,
    })

    this._trimJournal(execution)
    await this._storage.saveExecution(execution)
    if (execution.status === 'queued') this.emit('execution:queued', { execution: clone(execution) })
    return clone(execution)
  }

  /**
   * Move an execution to a different workflow version by canceling the old one (if not already terminal) and starting a fresh execution under the target version, carrying over input, data, metadata, and tags and linking the two in both journals. The new execution starts from the workflow's start step, so completed work is not replayed; make step side effects idempotent if a restart may repeat them.
   * @param {string} id - the execution id to restart.
   * @param {RestartUnderOptions} [options] - target version and overrides for what is carried over.
   * @returns {Promise<object>} the new execution, whose restartedFrom points at the old id.
   */
  async restartUnder(id, options = {}) {
    await this.ready()
    if (!this._tracer) return this._restartUnder(id, options)
    return this._tracer.span('workflow.restart', { 'workflow.execution': id }, () => this._restartUnder(id, options))
  }

  async _restartUnder(id, options) {
    const previous = await this._storage.getExecution(id)
    if (!previous) throw new Error(`execution not found: ${id}`)

    const workflow = this._resolveWorkflow(previous.workflow, options.version)

    if (!TERMINAL_EXECUTION_STATUSES.has(previous.status)) {
      await this._cancel(previous.id, `restarted under ${workflow.name}@${workflow.version}`)
    }

    const next = await this.start(workflow.name, options.input ?? previous.input, {
      version: workflow.version,
      data: options.data ?? previous.data,
      metadata: options.metadata ?? previous.metadata,
      tags: options.tags ?? previous.tags,
      restartedFrom: previous.id,
    })

    const updated = await this._storage.getExecution(previous.id)
    if (updated) {
      const now = Date.now()
      updated.restartedTo = next.id
      updated.updatedAt = now
      updated.journal.push({
        type: 'execution.restarted',
        at: now,
        to: next.id,
        workflowVersion: workflow.version,
      })
      this._trimJournal(updated)
      await this._storage.saveExecution(updated)
    }

    return next
  }

  /**
   * Claim and process one batch of ready executions, then return. This is a single pass: it does not loop or wait for new work. Use startWorker for a continuous loop.
   * @param {RunDueOptions} [options] - per-pass claim limit.
   * @returns {Promise<number>} the number of executions processed in this pass.
   */
  async runDue(options = {}) {
    await this.ready()

    const now = Date.now()
    const claimed = await this._storage.claimAvailable({
      now,
      owner: this._owner,
      limit: options.limit ?? this._batchSize,
      leaseMs: this._leaseMs,
    })

    const results = await Promise.allSettled(claimed.map(({ id }) => this._processExecution(id)))
    return results.length
  }

  /**
   * Repeatedly call runDue until a pass processes nothing, draining all immediately runnable work. Useful in tests and scripts; production should use startWorker. Note that work scheduled for the future (a retry backoff, a wait timeout) is not "immediately runnable" and will not be waited for.
   * @param {RunUntilIdleOptions} [options] - per-pass limit and the maxPasses safety cap.
   * @returns {Promise<number>} the number of passes it took to reach idle. Throws if maxPasses is exceeded.
   */
  async runUntilIdle(options = {}) {
    const maxPasses = options.maxPasses ?? 100

    for (let pass = 0; pass < maxPasses; pass++) {
      const processed = await this.runDue({ limit: options.limit })
      if (processed === 0) return pass
    }

    throw new Error(`runUntilIdle exceeded maxPasses=${maxPasses}`)
  }

  /**
   * Start the background polling loop that calls runDue on a timer for the life of the process. This is the production entry point for processing executions. Throws if a worker is already running on this engine.
   * @param {StartWorkerOptions} [options] - poll interval and per-poll batch size.
   * @returns {Promise<void>} resolves after the first poll cycle completes; the loop continues in the background until close is called.
   */
  async startWorker(options = {}) {
    await this.ready()
    if (this._pollTimer) throw new Error('worker already started')

    const interval = normalizeMs(options.interval ?? '1s')
    const batchSize = options.batchSize ?? this._batchSize

    const tick = async () => {
      if (this._activePoll) return this._activePoll

      this._activePoll = this.runDue({ limit: batchSize }).finally(() => {
        this._activePoll = null
      })

      return this._activePoll
    }

    this._pollTimer = setInterval(() => {
      tick().catch((error) => this.emit('worker:error', { error }))
    }, interval)
    this._pollTimer.unref?.()

    await tick()
  }

  /**
   * Stop the worker loop, let any in-flight poll settle, and close storage and pubsub connections. Call this on shutdown so timers and connections do not keep the process alive.
   * @returns {Promise<void>} resolves once everything is torn down.
   */
  async close() {
    if (this._pollTimer) clearInterval(this._pollTimer)
    this._pollTimer = null
    if (this._registryTimer) clearInterval(this._registryTimer)
    this._registryTimer = null
    await this._activePoll
    if (this._storage.close) await this._storage.close()
    if (this._pubClient?.isOpen && this._registryKey) {
      await this._pubClient.del(this._registryKey).catch(() => {})
    }
    if (this._subClient?.isOpen) await this._subClient.unsubscribe().catch(() => {})
    if (this._subClient?.isOpen) await this._subClient.quit().catch(() => {})
    if (this._pubClient?.isOpen) await this._pubClient.quit().catch(() => {})
  }

  _createExecution(workflow, input, options) {
    const now = Date.now()
    const execution = {
      id: options.id ?? randomUUID(),
      workflow: workflow.name,
      workflowVersion: workflow.version,
      status: 'queued',
      currentStep: workflow.start,
      input: clone(input),
      data: clone(options.data ?? {}),
      metadata: clone(options.metadata ?? {}),
      tags: clone(options.tags ?? []),
      createdAt: now,
      updatedAt: now,
      availableAt: now,
      completedAt: null,
      output: null,
      error: null,
      lockOwner: null,
      lockExpiresAt: null,
      journal: [
        {
          type: 'execution.created',
          at: now,
          step: workflow.start,
        },
      ],
      steps: {},
    }

    if (options.restartedFrom) {
      execution.restartedFrom = options.restartedFrom
      execution.journal.push({
        type: 'execution.restarted-from',
        at: now,
        from: options.restartedFrom,
      })
    }

    ensureStepState(execution, workflow.start)
    return execution
  }

  _resolveWorkflow(name, version) {
    if (version) {
      const workflow = this._workflows.get(`${name}@${version}`)
      if (!workflow) throw new Error(`workflow not registered: ${name}@${version}`)
      return workflow
    }

    const matches = Array.from(this._workflows.values()).filter((workflow) => workflow.name === name)
    if (matches.length === 0) throw new Error(`workflow not registered: ${name}`)
    if (matches.length > 1) throw new Error(`workflow version required for "${name}"`)
    return matches[0]
  }

  _trimJournal(execution) {
    if (this._maxJournalEntries > 0 && execution.journal.length > this._maxJournalEntries) {
      execution.journal = execution.journal.slice(-this._maxJournalEntries)
    }
  }

  _releaseLock(execution) {
    execution.lockOwner = null
    execution.lockExpiresAt = null
  }

  _setNextQueuedStep(execution, nextStep, at) {
    execution.status = 'queued'
    execution.currentStep = nextStep
    execution.availableAt = at
    execution.updatedAt = at
    this._releaseLock(execution)
    const state = ensureStepState(execution, nextStep)
    if (state.status === 'succeeded') this._resetStepForReentry(execution, nextStep, state, at)
  }

  _resetStepForReentry(execution, stepName, state, at) {
    const workflow = this._resolveWorkflow(execution.workflow, execution.workflowVersion)
    const definition = workflow.steps[stepName]
    const nextPass = (state.pass ?? 1) + 1
    const maxPasses = definition?.maxPasses ?? 0

    if (maxPasses > 0 && nextPass > maxPasses) {
      throw new Error(`step "${stepName}" exceeded maxPasses=${maxPasses}`)
    }

    execution.steps[stepName] = freshStepState(execution, stepName, nextPass)
    execution.journal.push({
      type: 'step.reentered',
      at,
      step: stepName,
      pass: nextPass,
    })
  }

  _beginStepRun(execution, stepName, stepState) {
    const now = Date.now()

    stepState.status = 'running'
    stepState.attempts += 1
    stepState.startedAt = now
    stepState.endedAt = null
    stepState.error = null

    execution.status = 'running'
    execution.updatedAt = now
    execution.journal.push({
      type: 'step.started',
      at: now,
      step: stepName,
      attempt: stepState.attempts,
    })
  }

  async _skipSucceededStep(execution, definition, stepName, stepState) {
    let nextStep
    if (definition.type === 'activity') nextStep = definition.next
    else if (definition.type === 'decision') nextStep = definition.transitions[stepState.route]

    if (!nextStep) {
      throw new Error(`cannot skip step "${stepName}" (type=${definition.type}): no next step resolvable`)
    }

    const now = Date.now()
    execution.updatedAt = now
    execution.journal.push({
      type: 'step.skipped',
      at: now,
      step: stepName,
      reason: 'already succeeded',
    })
    this._setNextQueuedStep(execution, nextStep, now)

    this._trimJournal(execution)
    await this._storage.saveExecution(execution, { expectedLockOwner: this._owner })
  }

  async _completeActivityStep(execution, definition, stepName, stepState, output, heartbeat) {
    if (output != null && typeof output === 'object' && !Array.isArray(output)) {
      Object.assign(execution.data, output)
    }

    const endedAt = Date.now()
    stepState.status = 'succeeded'
    const clonedOutput = clone(output) ?? null
    stepState.output = clonedOutput
    stepState.endedAt = endedAt

    this._setNextQueuedStep(execution, definition.next, endedAt)
    execution.journal.push({
      type: 'step.succeeded',
      at: endedAt,
      step: stepName,
    })

    heartbeat.assertHeld()
    this._trimJournal(execution)
    const saved = await this._storage.saveExecution(execution, { expectedLockOwner: this._owner })
    if (!saved) throw new LeaseLostError(execution.id, stepName)

    this.emit('step:succeeded', { execution: clone(execution), step: stepName, output: clonedOutput })
  }

  async _completeDecisionStep(execution, definition, stepName, stepState, route, heartbeat) {
    if (!Object.hasOwn(definition.transitions, route)) {
      throw new Error(`decision step "${stepName}" returned unknown route "${route}"`)
    }

    const endedAt = Date.now()
    stepState.status = 'succeeded'
    stepState.route = route
    stepState.output = route
    stepState.endedAt = endedAt

    const nextStep = definition.transitions[route]
    this._setNextQueuedStep(execution, nextStep, endedAt)
    execution.journal.push({
      type: 'step.routed',
      at: endedAt,
      step: stepName,
      route,
      to: nextStep,
    })

    heartbeat.assertHeld()
    this._trimJournal(execution)
    const saved = await this._storage.saveExecution(execution, { expectedLockOwner: this._owner })
    if (!saved) throw new LeaseLostError(execution.id, stepName)

    this.emit('step:routed', {
      execution: clone(execution),
      step: stepName,
      route,
      to: execution.currentStep,
    })
  }

  async _spawnSubworkflowStep(execution, definition, stepName, stepState, context, heartbeat) {
    const childInput = typeof definition.input === 'function'
      ? await Promise.resolve(definition.input(context))
      : {}

    const child = await this.start(definition.workflow, childInput, {
      version: definition.version,
      parent: { executionId: execution.id, step: stepName },
    })

    const now = Date.now()
    stepState.status = 'awaiting'
    stepState.endedAt = null
    stepState.childExecutionId = child.id

    execution.status = 'suspended'
    execution.availableAt = null
    execution.updatedAt = now
    this._releaseLock(execution)
    execution.journal.push({
      type: 'step.subworkflow-started',
      at: now,
      step: stepName,
      childExecutionId: child.id,
      childWorkflow: definition.workflow,
      childWorkflowVersion: child.workflowVersion,
    })

    heartbeat.assertHeld()
    this._trimJournal(execution)
    const saved = await this._storage.saveExecution(execution, { expectedLockOwner: this._owner })
    if (!saved) throw new LeaseLostError(execution.id, stepName)

    this.emit('step:suspended', { execution: clone(execution), step: stepName, childExecutionId: child.id })
  }

  async _resolveSubworkflowParent(child) {
    if (!child.parent) return
    const parent = await this._storage.getExecution(child.parent.executionId)
    if (!parent) return
    if (parent.status !== 'suspended') return
    if (parent.currentStep !== child.parent.step) return

    const workflow = this._resolveWorkflow(parent.workflow, parent.workflowVersion)
    const definition = workflow.steps[child.parent.step]
    if (!definition || definition.type !== 'subworkflow') return

    const route = child.status
    if (!Object.hasOwn(definition.transitions, route)) return

    const childOutcome = {
      executionId: child.id,
      status: child.status,
      output: clone(child.output ?? null),
      error: clone(child.error ?? null),
    }

    try {
      await this._resolveSuspendedStep(parent, definition, child.parent.step, {
        route,
        output: childOutcome,
        journalEntry: {
          type: 'step.subworkflow-resolved',
          step: child.parent.step,
          childExecutionId: child.id,
          status: child.status,
          route,
        },
      })
    } catch (error) {
      if (error instanceof AlreadySignaledError) return
      throw error
    }
  }

  async _suspendWaitStep(execution, definition, stepName, stepState, heartbeat) {
    const now = Date.now()
    const timeoutMs = definition.timeout != null ? normalizeMs(definition.timeout) : 0
    const timeoutAt = timeoutMs > 0 ? now + timeoutMs : null

    stepState.status = 'awaiting'
    stepState.endedAt = null

    execution.status = 'suspended'
    execution.availableAt = timeoutAt
    execution.updatedAt = now
    this._releaseLock(execution)
    execution.journal.push({
      type: 'step.suspended',
      at: now,
      step: stepName,
      timeoutAt,
    })

    heartbeat.assertHeld()
    this._trimJournal(execution)
    const saved = await this._storage.saveExecution(execution, { expectedLockOwner: this._owner })
    if (!saved) throw new LeaseLostError(execution.id, stepName)

    this.emit('step:suspended', { execution: clone(execution), step: stepName, timeoutAt })
  }

  async _fireWaitTimeout(execution, definition, stepName) {
    const stepState = execution.steps[stepName]
    if (!stepState) return null
    if (!Object.hasOwn(definition.transitions, 'timeout')) {
      throw new Error(`wait step "${stepName}" timed out but defines no "timeout" transition`)
    }

    return await this._resolveSuspendedStep(execution, definition, stepName, {
      route: 'timeout',
      output: null,
      journalEntry: { type: 'step.timed-out', step: stepName },
    })
  }

  async _resolveSuspendedStep(execution, definition, stepName, { route, output, journalEntry }) {
    const stepState = execution.steps[stepName]
    if (!stepState) throw new Error(`execution ${execution.id} has no state for step "${stepName}"`)

    const now = Date.now()
    stepState.status = 'succeeded'
    stepState.output = output
    stepState.route = route
    stepState.endedAt = now

    const nextStep = definition.transitions[route]
    execution.journal.push({ at: now, ...journalEntry })
    execution.journal.push({
      type: 'step.routed',
      at: now,
      step: stepName,
      route,
      to: nextStep,
    })

    this._setNextQueuedStep(execution, nextStep, now)
    this._trimJournal(execution)

    const saved = await this._storage.saveExecution(execution, { expectedStatus: 'suspended' })
    if (!saved) throw new AlreadySignaledError(execution.id)

    this.emit('step:resumed', { execution: clone(execution), step: stepName, route })
    this.emit('step:routed', {
      execution: clone(execution),
      step: stepName,
      route,
      to: nextStep,
    })
    return clone(execution)
  }

  async _completeTerminalStep(execution, definition, stepName, stepState, output, heartbeat) {
    const endedAt = Date.now()
    const status = terminalState(definition.type)
    const error =
      definition.type === 'fail' ? clone(output ?? { name: 'WorkflowFailed', message: 'Workflow failed' }) : null

    stepState.status = definition.type === 'succeed' ? 'succeeded' : 'failed'
    stepState.output = definition.type === 'succeed' ? output : null
    stepState.error = error
    stepState.endedAt = endedAt

    execution.updatedAt = endedAt
    execution.status = status
    execution.completedAt = endedAt
    execution.output = definition.type === 'succeed' ? clone(output) : null
    execution.error = error
    execution.currentStep = null
    execution.availableAt = null
    this._releaseLock(execution)
    execution.journal.push({
      type: `execution.${status}`,
      at: endedAt,
      step: stepName,
    })

    heartbeat.assertHeld()
    this._trimJournal(execution)
    const saved = await this._storage.saveExecution(execution, { expectedLockOwner: this._owner })
    if (!saved) throw new LeaseLostError(execution.id, stepName)

    this.emit(`execution:${status}`, { execution: clone(execution) })

    if (execution.parent) {
      await this._resolveSubworkflowParent(execution)
    }
  }

  async _handleStepFailure(execution, definition, stepName, stepState, error, heartbeat) {
    if (error instanceof LeaseLostError || heartbeat.lost) {
      this.emit('execution:lease-lost', { executionId: execution.id, step: stepName })
      return
    }

    const serialized = serializeError(error)
    const retry = definition.retry ?? DEFAULT_RETRY
    const endedAt = Date.now()

    stepState.status = 'failed'
    stepState.error = serialized
    stepState.endedAt = endedAt

    execution.updatedAt = endedAt
    this._releaseLock(execution)

    if (stepState.attempts < retry.maxAttempts) {
      execution.status = 'waiting'
      execution.availableAt = endedAt + normalizeMs(retry.backoff ?? 0)
      execution.journal.push({
        type: 'step.retry-scheduled',
        at: endedAt,
        step: stepName,
        attempt: stepState.attempts,
        availableAt: execution.availableAt,
        error: serialized,
      })

      this._trimJournal(execution)
    const saved = await this._storage.saveExecution(execution, { expectedLockOwner: this._owner })
      if (!saved) {
        this.emit('execution:lease-lost', { executionId: execution.id, step: stepName })
        return
      }

      this.emit('step:retry', {
        execution: clone(execution),
        step: stepName,
        attempt: stepState.attempts,
        error: serialized,
        availableAt: execution.availableAt,
      })
      return
    }

    execution.status = 'failed'
    execution.error = serialized
    execution.completedAt = endedAt
    execution.availableAt = null
    execution.journal.push({
      type: 'execution.failed',
      at: endedAt,
      step: stepName,
      error: serialized,
    })

    this._trimJournal(execution)
    const saved = await this._storage.saveExecution(execution, { expectedLockOwner: this._owner })
    if (!saved) {
      this.emit('execution:lease-lost', { executionId: execution.id, step: stepName })
      return
    }

    this.emit('step:failed', {
      execution: clone(execution),
      step: stepName,
      attempt: stepState.attempts,
      error: serialized,
    })
    this.emit('execution:failed', { execution: clone(execution) })

    if (execution.parent) {
      await this._resolveSubworkflowParent(execution)
    }
  }

  async _processExecution(id) {
    const execution = await this._storage.getExecution(id)
    if (!execution) return
    if (execution.lockOwner !== this._owner) return

    const workflow = this._resolveWorkflow(execution.workflow, execution.workflowVersion)
    const stepName = execution.currentStep
    const definition = workflow.steps[stepName]
    if (!definition) throw new Error(`execution ${id} points to unknown step "${stepName}"`)

    if (execution.status === 'suspended') {
      if (definition.type !== 'wait') return
      await this._fireWaitTimeout(execution, definition, stepName)
      return
    }

    if (!PROCESSABLE_EXECUTION_STATUSES.has(execution.status)) return

    const stepState = ensureStepState(execution, stepName)
    if (stepState.status === 'succeeded') {
      await this._skipSucceededStep(execution, definition, stepName, stepState)
      return
    }

    this._beginStepRun(execution, stepName, stepState)
    this._trimJournal(execution)
    const started = await this._storage.saveExecution(execution, { expectedLockOwner: this._owner })
    if (!started) {
      this.emit('execution:lease-lost', { executionId: execution.id, step: stepName })
      return
    }

    this.emit('step:started', { execution: clone(execution), step: stepName, attempt: stepState.attempts })

    const abort = new AbortController()
    const heartbeat = this._startLeaseHeartbeat(execution.id, stepName, () => {
      abort.abort(new LeaseLostError(execution.id, stepName))
    })

    const traceParent = this._tracer && execution.traceparent ? this._tracer.fromTraceparent(execution.traceparent) : null
    const stepSpan = this._tracer?.startSpan(`workflow.step:${stepName}`, {
      'workflow.name': workflow.name,
      'workflow.version': workflow.version,
      'workflow.execution': execution.id,
      'workflow.step': stepName,
      'workflow.step.type': definition.type,
      'workflow.step.attempt': stepState.attempts,
    }, {
      kind: 'internal',
      parent: traceParent ? { traceId: traceParent.traceId, spanId: traceParent.parentSpanId, sampled: traceParent.sampled } : null,
    })

    const runStep = async () => {
      const context = stepContext(execution, workflow, stepName)
      context.signal = abort.signal
      const timeoutMs = normalizeMs(definition.timeout ?? (definition.type === 'activity' ? this._defaultActivityTimeout : 0))
      const timeoutMessage = `Step "${stepName}" timed out after ${timeoutMs}ms`
      const abortOnTimeout = () => abort.abort(new Error(timeoutMessage))

      if (definition.type === 'activity') {
        const output = await withTimeout(
          Promise.resolve(definition.run(context)),
          timeoutMs,
          timeoutMessage,
          abortOnTimeout,
        )

        await this._completeActivityStep(execution, definition, stepName, stepState, output, heartbeat)
        return
      }

      if (definition.type === 'decision') {
        const route = await withTimeout(
          Promise.resolve(definition.decide(context)),
          timeoutMs,
          timeoutMessage,
          abortOnTimeout,
        )

        await this._completeDecisionStep(execution, definition, stepName, stepState, route, heartbeat)
        return
      }

      if (definition.type === 'wait') {
        await this._suspendWaitStep(execution, definition, stepName, stepState, heartbeat)
        return
      }

      if (definition.type === 'subworkflow') {
        await this._spawnSubworkflowStep(execution, definition, stepName, stepState, context, heartbeat)
        return
      }

      if (definition.type === 'succeed' || definition.type === 'fail') {
        const output = typeof definition.result === 'function'
          ? await withTimeout(
              Promise.resolve(definition.result(context)),
              timeoutMs,
              timeoutMessage,
              abortOnTimeout,
            )
          : clone(definition.result ?? null)
        await this._completeTerminalStep(execution, definition, stepName, stepState, output, heartbeat)
      }
    }

    try {
      if (stepSpan) {
        await this._tracer.run(stepSpan.context, runStep)
      } else {
        await runStep()
      }
    } catch (error) {
      stepSpan?.setError(error)
      await this._handleStepFailure(execution, definition, stepName, stepState, error, heartbeat)
    } finally {
      stepSpan?.end()
      heartbeat.stop()
    }
  }

  _startLeaseHeartbeat(executionId, stepName, onLost) {
    if (!this._storage.renewLease) {
      return {
        lost: false,
        stop() {},
        assertHeld() {},
      }
    }

    let lost = false
    const markLost = () => {
      if (lost) return
      lost = true
      onLost?.()
    }
    const interval = setInterval(async () => {
      try {
        const renewed = await this._storage.renewLease(executionId, {
          owner: this._owner,
          now: Date.now(),
          leaseMs: this._leaseMs,
        })
        if (!renewed) markLost()
      } catch {
        markLost()
      }
    }, this._leaseRenewInterval)
    interval.unref?.()

    return {
      get lost() {
        return lost
      },
      stop() {
        clearInterval(interval)
      },
      assertHeld() {
        if (lost) throw new LeaseLostError(executionId, stepName)
      },
    }
  }
}
