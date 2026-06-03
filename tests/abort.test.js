import { describe, expect, it } from 'vitest'
import WorkflowEngine, { defineWorkflow, memoryDriver } from '../src/index.js'

function waitFor(predicate, timeoutMs = 2000) {
  return new Promise((resolve, reject) => {
    const start = Date.now()
    const tick = () => {
      if (predicate()) return resolve()
      if (Date.now() - start > timeoutMs) return reject(new Error('waitFor timed out'))
      setTimeout(tick, 5)
    }
    tick()
  })
}

describe('step abort signal', () => {
  it('exposes a non-aborted AbortSignal on the step context', async () => {
    let seen = null
    const engine = new WorkflowEngine()
    engine.register(
      defineWorkflow({
        name: 'capture',
        version: '1',
        start: 'work',
        steps: {
          work: {
            type: 'activity',
            next: 'done',
            run: ({ signal }) => {
              seen = signal
              return {}
            },
          },
          done: { type: 'succeed', result: () => ({}) },
        },
      }),
    )

    const execution = await engine.start('capture', {})
    await engine.runUntilIdle()

    expect(seen).toBeInstanceOf(AbortSignal)
    expect(seen.aborted).toBe(false)

    const finished = await engine.getExecution(execution.id)
    expect(finished.status).toBe('succeeded')
  })

  it('aborts the signal when an activity times out', async () => {
    let captured = null
    const engine = new WorkflowEngine()
    engine.register(
      defineWorkflow({
        name: 'slow',
        version: '1',
        start: 'hang',
        steps: {
          hang: {
            type: 'activity',
            next: 'done',
            timeout: '30ms',
            run: ({ signal }) =>
              new Promise((_, reject) => {
                captured = signal
                signal.addEventListener('abort', () => reject(signal.reason))
              }),
          },
          done: { type: 'succeed', result: () => ({}) },
        },
      }),
    )

    const execution = await engine.start('slow', {})
    await engine.runUntilIdle()

    expect(captured.aborted).toBe(true)
    expect(captured.reason).toBeInstanceOf(Error)

    const finished = await engine.getExecution(execution.id)
    expect(finished.status).toBe('failed')
    expect(finished.error.message).toContain('timed out')
  })

  it('aborts the signal when a running execution is canceled', async () => {
    const storage = memoryDriver()
    const engine = new WorkflowEngine({
      storage,
      owner: 'worker-a',
      leaseMs: '5m',
      leaseRenewInterval: '20ms',
    })

    let captured = null
    engine.register(
      defineWorkflow({
        name: 'long',
        version: '1',
        start: 'hang',
        steps: {
          hang: {
            type: 'activity',
            next: 'done',
            run: ({ signal }) =>
              new Promise((_, reject) => {
                captured = signal
                signal.addEventListener('abort', () => reject(signal.reason))
              }),
          },
          done: { type: 'succeed', result: () => ({}) },
        },
      }),
    )

    const execution = await engine.start('long', {})
    const running = engine.runDue()

    await waitFor(() => captured != null)
    await engine.cancel(execution.id, 'operator stop')
    await waitFor(() => captured.aborted)

    expect(captured.aborted).toBe(true)

    await running.catch(() => {})

    const finished = await engine.getExecution(execution.id)
    expect(finished.status).toBe('canceled')
  })
})
