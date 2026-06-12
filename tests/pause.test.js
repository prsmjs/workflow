import { describe, it, expect } from 'vitest'
import WorkflowEngine, { defineWorkflow, AlreadySignaledError } from '../src/index.js'

function makeEngine() {
  return new WorkflowEngine({ leaseRenewInterval: 60_000 })
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms))
}

function simpleWorkflow() {
  return defineWorkflow({
    name: 's', version: '1', start: 'work',
    steps: {
      work: { type: 'activity', next: 'done', run: ({ input }) => ({ value: input.value }) },
      done: { type: 'succeed', result: ({ data }) => ({ value: data.value }) },
    },
  })
}

function waitWorkflow(timeout) {
  return defineWorkflow({
    name: 'w', version: '1', start: 'gate',
    steps: {
      gate: {
        type: 'wait',
        timeout,
        transitions: timeout
          ? { approved: 'done', timeout: 'late' }
          : { approved: 'done' },
        resolve: ({ signal }) => signal.decision,
      },
      done: { type: 'succeed', result: () => ({ via: 'signal' }) },
      ...(timeout ? { late: { type: 'succeed', result: () => ({ via: 'timeout' }) } } : {}),
    },
  })
}

describe('pause and resume', () => {
  it('pauses a queued execution so workers do not pick it up', async () => {
    const engine = makeEngine()
    engine.register(simpleWorkflow())

    const exec = await engine.start('s', { value: 1 })
    const paused = await engine.pause(exec.id, 'human review')
    expect(paused.status).toBe('paused')
    expect(paused.pausedFrom).toBe('queued')

    await engine.runUntilIdle()
    const after = await engine.getExecution(exec.id)
    expect(after.status).toBe('paused')
    expect(after.journal.map((e) => e.type)).not.toContain('step.started')

    await engine.resume(exec.id)
    await engine.runUntilIdle()

    const final = await engine.getExecution(exec.id)
    expect(final.status).toBe('succeeded')
    expect(final.output).toEqual({ value: 1 })
    expect(final.pausedFrom).toBeUndefined()

    const types = final.journal.map((e) => e.type)
    expect(types).toContain('execution.paused')
    expect(types).toContain('execution.resumed')
  })

  it('emits execution:paused', async () => {
    const engine = makeEngine()
    engine.register(simpleWorkflow())
    const events = []
    engine.on('execution:paused', (e) => events.push(e))

    const exec = await engine.start('s', { value: 1 })
    await engine.pause(exec.id)

    expect(events).toHaveLength(1)
    expect(events[0].execution.id).toBe(exec.id)
  })

  it('pausing an already paused execution is a no-op', async () => {
    const engine = makeEngine()
    engine.register(simpleWorkflow())

    const exec = await engine.start('s', { value: 1 })
    await engine.pause(exec.id)
    const again = await engine.pause(exec.id)
    expect(again.status).toBe('paused')
    expect(again.journal.filter((e) => e.type === 'execution.paused')).toHaveLength(1)
  })

  it('rejects pausing a terminal execution', async () => {
    const engine = makeEngine()
    engine.register(simpleWorkflow())

    const exec = await engine.start('s', { value: 1 })
    await engine.runUntilIdle()

    await expect(engine.pause(exec.id)).rejects.toThrow(/cannot pause succeeded/)
  })

  it('pauses a suspended wait step and blocks signals until resumed', async () => {
    const engine = makeEngine()
    engine.register(waitWorkflow())

    const exec = await engine.start('w', {})
    await engine.runUntilIdle()

    await engine.pause(exec.id)
    await expect(engine.signal(exec.id, { decision: 'approved' })).rejects.toThrow(AlreadySignaledError)

    const resumed = await engine.resume(exec.id)
    expect(resumed.status).toBe('suspended')
    expect(resumed.availableAt).toBeNull()

    await engine.signal(exec.id, { decision: 'approved' })
    await engine.runUntilIdle()

    expect((await engine.getExecution(exec.id)).status).toBe('succeeded')
  })

  it('restores a wait timeout deadline on resume', async () => {
    const engine = makeEngine()
    engine.register(waitWorkflow(10))

    const exec = await engine.start('w', {})
    await engine.runUntilIdle()

    await engine.pause(exec.id)
    await sleep(30)
    await engine.runUntilIdle()
    expect((await engine.getExecution(exec.id)).status).toBe('paused')

    await engine.resume(exec.id)
    await engine.runUntilIdle()

    const final = await engine.getExecution(exec.id)
    expect(final.status).toBe('succeeded')
    expect(final.output).toEqual({ via: 'timeout' })
  })

  it('discards the in-flight result when paused mid-step and re-runs after resume', async () => {
    const engine = makeEngine()
    let runs = 0
    engine.register(
      defineWorkflow({
        name: 'slow', version: '1', start: 'work',
        steps: {
          work: {
            type: 'activity',
            next: 'done',
            run: async () => {
              runs += 1
              await sleep(120)
              return { runs }
            },
          },
          done: { type: 'succeed', result: ({ data }) => ({ runs: data.runs }) },
        },
      }),
    )

    const exec = await engine.start('slow', {})
    const leaseLost = []
    engine.on('execution:lease-lost', (e) => leaseLost.push(e))

    const inFlight = engine.runDue()
    await sleep(40)
    await engine.pause(exec.id)
    await inFlight

    const after = await engine.getExecution(exec.id)
    expect(after.status).toBe('paused')
    expect(after.pausedFrom).toBe('running')
    expect(after.steps.work.output).toBeNull()
    expect(leaseLost).toHaveLength(1)

    await engine.resume(exec.id)
    await engine.runUntilIdle()

    const final = await engine.getExecution(exec.id)
    expect(final.status).toBe('succeeded')
    expect(runs).toBe(2)
    expect(final.output).toEqual({ runs: 2 })
  })

  it('a paused execution can still be canceled', async () => {
    const engine = makeEngine()
    engine.register(simpleWorkflow())

    const exec = await engine.start('s', { value: 1 })
    await engine.pause(exec.id)
    await engine.cancel(exec.id, 'no longer needed')

    const final = await engine.getExecution(exec.id)
    expect(final.status).toBe('canceled')
  })
})
