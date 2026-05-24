import { afterEach, beforeAll, describe, expect, it } from 'vitest'
import WorkflowEngine, { defineWorkflow } from '../src/index.js'
import { createTracer } from '@prsm/trace'

let postgresDriver = null
let Pool = null
try {
  ;({ postgresDriver } = await import('../src/postgresDriver.js'))
  const pg = await import('pg')
  Pool = pg.default?.Pool || pg.Pool
} catch {}

const connectionString = process.env.WORKFLOW_TEST_POSTGRES_URL ?? 'postgres://workflow:workflow_password@127.0.0.1:5432/workflow_test'
const describeIfPostgres = postgresDriver && Pool ? describe : describe.skip

const sleep = (ms) => new Promise((r) => setTimeout(r, ms))

describeIfPostgres('cross-instance trace propagation', () => {
  let adminPool
  let postgresReady = false
  const engines = []

  beforeAll(async () => {
    adminPool = new Pool({ connectionString })
    try { await adminPool.query('SELECT 1'); postgresReady = true } catch { postgresReady = false }
  })

  afterEach(async () => {
    while (engines.length) {
      const e = engines.pop()
      try { await e.close() } catch {}
    }
    if (postgresReady) {
      try { await adminPool.query('TRUNCATE workflow_executions RESTART IDENTITY') } catch {}
    }
  })

  function makeEngine(opts) {
    const e = new WorkflowEngine({
      storage: postgresDriver({ connectionString }),
      ...opts,
    })
    engines.push(e)
    return e
  }

  it('step span on engine B is a child of workflow.start span on engine A', { timeout: 15000 }, async () => {
    if (!postgresReady) return
    const tracer = createTracer({ service: 'svc' })
    const spans = []
    tracer.onSpan((s) => spans.push(s))

    const wf = defineWorkflow({
      name: 'flow',
      version: '1',
      start: 'a',
      steps: {
        a: { type: 'activity', next: 'done', run: async () => ({ ok: 1 }) },
        done: { type: 'succeed', result: ({ steps }) => steps.a.output },
      },
    })

    const engineA = makeEngine({ tracer, owner: 'A' })
    engineA.register(wf)

    const engineB = makeEngine({ tracer, owner: 'B' })
    engineB.register(wf)

    // A starts the workflow (creates execution row with traceparent stored)
    let runTraceId = null
    await tracer.span('http.POST', async () => {
      runTraceId = tracer.current().traceId
      await engineA.start('flow', { hello: 'world' })
    })

    // B processes (starts its worker; A never starts a worker)
    await engineB.startWorker({ interval: '50ms' })

    // wait for completion
    let completed = false
    for (let i = 0; i < 120; i++) {
      await sleep(100)
      const list = await engineB.listExecutions({ status: 'succeeded' })
      if (list.length > 0) { completed = true; break }
    }
    expect(completed).toBe(true)

    // span topology:
    // http.POST (root)
    //   workflow.start:flow (engine A)
    //   workflow.step:a     (engine B, restored from traceparent)
    //   workflow.step:done  (engine B)
    const httpSpan = spans.find((s) => s.name === 'http.POST')
    const startSpan = spans.find((s) => s.name === 'workflow.start:flow')
    const stepA = spans.find((s) => s.name === 'workflow.step:a')
    const stepDone = spans.find((s) => s.name === 'workflow.step:done')

    expect(httpSpan).toBeTruthy()
    expect(startSpan).toBeTruthy()
    expect(stepA).toBeTruthy()
    expect(stepDone).toBeTruthy()

    expect(startSpan.traceId).toBe(runTraceId)
    expect(stepA.traceId).toBe(runTraceId)
    expect(stepDone.traceId).toBe(runTraceId)

    // workflow.start was inside http.POST
    expect(startSpan.parentSpanId).toBe(httpSpan.spanId)

    // step spans were processed by engine B but should still link back to the workflow.start chain
    // (parent is whatever startSpan's context carried into execution.traceparent)
    expect(stepA.parentSpanId).toBe(startSpan.spanId)
  })

  it('workflow started outside trace context produces a self-rooted trace covering all steps', { timeout: 15000 }, async () => {
    if (!postgresReady) return
    const tracer = createTracer({ service: 'svc' })
    const spans = []
    tracer.onSpan((s) => spans.push(s))

    const wf = defineWorkflow({
      name: 'standalone',
      version: '1',
      start: 's1',
      steps: {
        s1: { type: 'activity', next: 's2', run: async () => 1 },
        s2: { type: 'activity', next: 'done', run: async () => 2 },
        done: { type: 'succeed', result: () => 'ok' },
      },
    })

    const a = makeEngine({ tracer, owner: 'a' })
    a.register(wf)
    const b = makeEngine({ tracer, owner: 'b' })
    b.register(wf)

    // no surrounding tracer.span — workflow.start becomes the root
    await a.start('standalone', {})
    await b.startWorker({ interval: '50ms' })

    let done = false
    for (let i = 0; i < 120; i++) {
      await sleep(100)
      const list = await b.listExecutions({ status: 'succeeded' })
      if (list.length > 0) { done = true; break }
    }
    expect(done).toBe(true)

    const startSpan = spans.find((s) => s.name === 'workflow.start:standalone')
    const s1 = spans.find((s) => s.name === 'workflow.step:s1')
    const s2 = spans.find((s) => s.name === 'workflow.step:s2')
    const dn = spans.find((s) => s.name === 'workflow.step:done')

    expect(startSpan.parentSpanId).toBeNull()
    const tid = startSpan.traceId
    expect(s1.traceId).toBe(tid)
    expect(s2.traceId).toBe(tid)
    expect(dn.traceId).toBe(tid)
    expect(s1.parentSpanId).toBe(startSpan.spanId)
  })
})
