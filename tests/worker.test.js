import { describe, it, expect } from 'vitest'
import WorkflowEngine, { defineWorkflow } from '../src/index.js'

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms))

const until = async (check, { timeoutMs = 5000 } = {}) => {
  const deadline = Date.now() + timeoutMs
  while (Date.now() < deadline) {
    if (await check()) return true
    await sleep(10)
  }
  return false
}

describe('worker loop', () => {
  it('lets a fast execution finish while a slow step is still running', async () => {
    const engine = new WorkflowEngine({ leaseRenewInterval: 60_000 })
    let releaseSlow
    const slowDone = new Promise((resolve) => { releaseSlow = resolve })
    engine.register(defineWorkflow({
      name: 'slow', version: '1', start: 'a',
      steps: { a: { type: 'activity', run: async () => { await slowDone; return {} }, next: 'done' }, done: { type: 'succeed' } },
    }))
    engine.register(defineWorkflow({
      name: 'fast', version: '1', start: 'a',
      steps: { a: { type: 'activity', run: async () => ({}), next: 'done' }, done: { type: 'succeed' } },
    }))

    const slow = await engine.start('slow', {})
    await engine.startWorker({ interval: '20ms', batchSize: 1 })
    const fast = await engine.start('fast', {})

    const fastFinished = await until(async () => (await engine.getExecution(fast.id)).status === 'succeeded')
    expect(fastFinished).toBe(true)
    expect((await engine.getExecution(slow.id)).status).toBe('running')

    releaseSlow()
    expect(await until(async () => (await engine.getExecution(slow.id)).status === 'succeeded')).toBe(true)
    await engine.close()
  })

  it('claims no more than the in-flight ceiling', async () => {
    const engine = new WorkflowEngine({ leaseRenewInterval: 60_000 })
    let release
    const gate = new Promise((resolve) => { release = resolve })
    let started = 0
    engine.register(defineWorkflow({
      name: 'held', version: '1', start: 'a',
      steps: { a: { type: 'activity', run: async () => { started++; await gate; return {} }, next: 'done' }, done: { type: 'succeed' } },
    }))
    for (let i = 0; i < 5; i++) await engine.start('held', {})
    await engine.startWorker({ interval: '20ms', batchSize: 10, maxInFlight: 2 })
    await sleep(150)
    expect(started).toBe(2)

    release()
    expect(await until(async () => (await engine.listExecutions({ status: 'succeeded' })).length === 5)).toBe(true)
    await engine.close()
  })
})
