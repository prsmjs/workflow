import { describe, it, expect } from 'vitest'
import WorkflowEngine, { defineWorkflow } from '../src/index.js'

function makeEngine() {
  return new WorkflowEngine({ leaseRenewInterval: 60_000 })
}

function loopWorkflow({ until = 3, maxPasses } = {}) {
  return defineWorkflow({
    name: 'loop', version: '1', start: 'work', cycles: true,
    steps: {
      work: {
        type: 'activity',
        next: 'check',
        maxPasses,
        run: ({ data }) => ({ count: (data.count ?? 0) + 1 }),
      },
      check: {
        type: 'decision',
        transitions: { again: 'work', done: 'finish' },
        decide: ({ data }) => (data.count < until ? 'again' : 'done'),
      },
      finish: { type: 'succeed', result: ({ data }) => ({ count: data.count }) },
    },
  })
}

describe('cycle validation', () => {
  it('rejects cycles by default', () => {
    expect(() =>
      defineWorkflow({
        name: 'w', version: '1', start: 'a',
        steps: {
          a: { type: 'activity', next: 'b', run: () => ({}) },
          b: {
            type: 'decision',
            transitions: { back: 'a', out: 'done' },
            decide: () => 'out',
          },
          done: { type: 'succeed' },
        },
      }),
    ).toThrow(/must be acyclic/)
  })

  it('allows back-edges when cycles is true', () => {
    const workflow = loopWorkflow()
    expect(workflow.cycles).toBe(true)
    expect(workflow.graph.edges).toContainEqual({ from: 'check', to: 'work', label: 'again' })
  })

  it('rejects non-positive maxPasses', () => {
    expect(() =>
      defineWorkflow({
        name: 'w', version: '1', start: 'a',
        steps: { a: { type: 'succeed', maxPasses: 0 } },
      }),
    ).toThrow(/maxPasses must be a positive integer/)
  })
})

describe('cycle execution', () => {
  it('re-runs steps with reset state and a fresh pass counter', async () => {
    const engine = makeEngine()
    engine.register(loopWorkflow({ until: 3 }))

    const exec = await engine.start('loop', {})
    await engine.runUntilIdle()

    const final = await engine.getExecution(exec.id)
    expect(final.status).toBe('succeeded')
    expect(final.output).toEqual({ count: 3 })
    expect(final.steps.work.pass).toBe(3)
    expect(final.steps.work.attempts).toBe(1)
    expect(final.steps.work.idempotencyKey).toBe(`${exec.id}:work:3`)

    const reentries = final.journal.filter((e) => e.type === 'step.reentered')
    expect(reentries.map((e) => ({ step: e.step, pass: e.pass }))).toEqual([
      { step: 'work', pass: 2 },
      { step: 'check', pass: 2 },
      { step: 'work', pass: 3 },
      { step: 'check', pass: 3 },
    ])
  })

  it('keeps the legacy idempotency key on the first pass', async () => {
    const engine = makeEngine()
    engine.register(loopWorkflow({ until: 1 }))

    const exec = await engine.start('loop', {})
    await engine.runUntilIdle()

    const final = await engine.getExecution(exec.id)
    expect(final.steps.work.pass).toBe(1)
    expect(final.steps.work.idempotencyKey).toBe(`${exec.id}:work`)
  })

  it('exposes the pass number in step context', async () => {
    const passes = []
    const engine = makeEngine()
    engine.register(
      defineWorkflow({
        name: 'p', version: '1', start: 'work', cycles: true,
        steps: {
          work: {
            type: 'activity',
            next: 'check',
            run: ({ data, step }) => {
              passes.push(step.pass)
              return { count: (data.count ?? 0) + 1 }
            },
          },
          check: {
            type: 'decision',
            transitions: { again: 'work', done: 'finish' },
            decide: ({ data }) => (data.count < 2 ? 'again' : 'done'),
          },
          finish: { type: 'succeed' },
        },
      }),
    )

    const exec = await engine.start('p', {})
    await engine.runUntilIdle()

    expect(passes).toEqual([1, 2])
    expect((await engine.getExecution(exec.id)).status).toBe('succeeded')
  })

  it('fails the execution when maxPasses is exceeded', async () => {
    const engine = makeEngine()
    engine.register(loopWorkflow({ until: 100, maxPasses: 2 }))

    const exec = await engine.start('loop', {})
    await engine.runUntilIdle()

    const final = await engine.getExecution(exec.id)
    expect(final.status).toBe('failed')
    expect(final.error.message).toMatch(/step "work" exceeded maxPasses=2/)
  })

  it('re-suspends a wait step that is cycled back into', async () => {
    const engine = makeEngine()
    engine.register(
      defineWorkflow({
        name: 'gatekeeper', version: '1', start: 'gate', cycles: true,
        steps: {
          gate: {
            type: 'wait',
            transitions: { redo: 'gate', ok: 'done' },
            resolve: ({ signal }) => signal.decision,
          },
          done: { type: 'succeed', result: ({ steps }) => ({ pass: steps.gate.pass }) },
        },
      }),
    )

    const exec = await engine.start('gatekeeper', {})
    await engine.runUntilIdle()

    await engine.signal(exec.id, { decision: 'redo' })
    await engine.runUntilIdle()

    const mid = await engine.getExecution(exec.id)
    expect(mid.status).toBe('suspended')
    expect(mid.steps.gate.status).toBe('awaiting')
    expect(mid.steps.gate.pass).toBe(2)

    await engine.signal(exec.id, { decision: 'ok' })
    await engine.runUntilIdle()

    const final = await engine.getExecution(exec.id)
    expect(final.status).toBe('succeeded')
    expect(final.output).toEqual({ pass: 2 })
  })
})
