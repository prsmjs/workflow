import { describe, it, expect } from 'vitest'
import WorkflowEngine, { defineWorkflow } from '../src/index.js'

function makeEngine() {
  return new WorkflowEngine({ leaseRenewInterval: 60_000 })
}

function versionedWorkflows() {
  const v1 = defineWorkflow({
    name: 'job', version: '1', start: 'work',
    steps: {
      work: {
        type: 'activity',
        next: 'done',
        run: () => {
          throw new Error('broken in v1')
        },
      },
      done: { type: 'succeed' },
    },
  })
  const v2 = defineWorkflow({
    name: 'job', version: '2', start: 'work',
    steps: {
      work: { type: 'activity', next: 'done', run: ({ input, data }) => ({ sum: input.x + (data.carried ?? 0) }) },
      done: { type: 'succeed', result: ({ data }) => ({ sum: data.sum }) },
    },
  })
  return { v1, v2 }
}

describe('restartUnder', () => {
  it('restarts a failed execution under a new version with provenance', async () => {
    const engine = makeEngine()
    const { v1, v2 } = versionedWorkflows()
    engine.register(v1).register(v2)

    const exec = await engine.start('job', { x: 5 }, { version: '1' })
    await engine.runUntilIdle()
    expect((await engine.getExecution(exec.id)).status).toBe('failed')

    const next = await engine.restartUnder(exec.id, { version: '2' })
    expect(next.workflowVersion).toBe('2')
    expect(next.input).toEqual({ x: 5 })
    expect(next.restartedFrom).toBe(exec.id)
    expect(next.journal.map((e) => e.type)).toContain('execution.restarted-from')

    await engine.runUntilIdle()
    expect((await engine.getExecution(next.id)).status).toBe('succeeded')

    const old = await engine.getExecution(exec.id)
    expect(old.status).toBe('failed')
    expect(old.restartedTo).toBe(next.id)
    expect(old.journal.map((e) => e.type)).toContain('execution.restarted')
  })

  it('carries data forward and allows overrides', async () => {
    const engine = makeEngine()
    const { v1, v2 } = versionedWorkflows()
    engine.register(v1).register(v2)

    const exec = await engine.start('job', { x: 5 }, { version: '1', data: { carried: 10 } })
    await engine.runUntilIdle()

    const next = await engine.restartUnder(exec.id, { version: '2' })
    expect(next.data).toEqual({ carried: 10 })

    await engine.runUntilIdle()
    expect((await engine.getExecution(next.id)).output).toEqual({ sum: 15 })

    const overridden = await engine.restartUnder(exec.id, { version: '2', input: { x: 1 }, data: {} })
    await engine.runUntilIdle()
    expect((await engine.getExecution(overridden.id)).output).toEqual({ sum: 1 })
  })

  it('cancels a non-terminal execution before restarting it', async () => {
    const engine = makeEngine()
    engine.register(
      defineWorkflow({
        name: 'gated', version: '1', start: 'gate',
        steps: {
          gate: { type: 'wait', transitions: { ok: 'done' } },
          done: { type: 'succeed' },
        },
      }),
    )
    engine.register(
      defineWorkflow({
        name: 'gated', version: '2', start: 'gate',
        steps: {
          gate: { type: 'wait', transitions: { ok: 'done' } },
          done: { type: 'succeed' },
        },
      }),
    )

    const exec = await engine.start('gated', {}, { version: '1' })
    await engine.runUntilIdle()
    expect((await engine.getExecution(exec.id)).status).toBe('suspended')

    const next = await engine.restartUnder(exec.id, { version: '2' })

    const old = await engine.getExecution(exec.id)
    expect(old.status).toBe('canceled')
    expect(old.error.message).toMatch(/restarted under gated@2/)
    expect(old.restartedTo).toBe(next.id)

    await engine.runUntilIdle()
    expect((await engine.getExecution(next.id)).status).toBe('suspended')
  })

  it('rejects restart under an unregistered version', async () => {
    const engine = makeEngine()
    const { v1 } = versionedWorkflows()
    engine.register(v1)

    const exec = await engine.start('job', { x: 1 }, { version: '1' })
    await engine.runUntilIdle()

    await expect(engine.restartUnder(exec.id, { version: '9' })).rejects.toThrow(/not registered/)
  })

  it('rejects restart of an unknown execution', async () => {
    const engine = makeEngine()
    const { v1 } = versionedWorkflows()
    engine.register(v1)

    await expect(engine.restartUnder('nope', { version: '1' })).rejects.toThrow(/not found/)
  })
})

describe('unregister', () => {
  it('removes a workflow version at runtime', async () => {
    const engine = makeEngine()
    const { v1, v2 } = versionedWorkflows()
    engine.register(v1).register(v2)

    engine.unregister('job', '1')
    expect(engine.listWorkflows()).toEqual([{ name: 'job', version: '2', description: '' }])

    await expect(engine.start('job', { x: 1 }, { version: '1' })).rejects.toThrow(/not registered/)
    const exec = await engine.start('job', { x: 1 })
    expect(exec.workflowVersion).toBe('2')
  })

  it('requires an explicit version', () => {
    const engine = makeEngine()
    expect(() => engine.unregister('job')).toThrow(/explicit version/)
  })

  it('throws for an unknown registration', () => {
    const engine = makeEngine()
    expect(() => engine.unregister('job', '1')).toThrow(/not registered/)
  })
})
