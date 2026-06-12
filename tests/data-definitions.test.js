import { describe, it, expect } from 'vitest'
import WorkflowEngine, { defineWorkflow } from '../src/index.js'

function makeEngine() {
  return new WorkflowEngine({ leaseRenewInterval: 60_000 })
}

describe('handler binding validation', () => {
  it('rejects a handler that is not a string', () => {
    expect(() =>
      defineWorkflow(
        {
          name: 'w', version: '1', start: 'a',
          steps: { a: { type: 'succeed', handler: 42 } },
        },
        { handlers: {} },
      ),
    ).toThrow(/handler must be a string/)
  })

  it('rejects an unknown handler name', () => {
    expect(() =>
      defineWorkflow(
        {
          name: 'w', version: '1', start: 'a',
          steps: {
            a: { type: 'activity', handler: 'nope', next: 'done' },
            done: { type: 'succeed' },
          },
        },
        { handlers: { other: () => {} } },
      ),
    ).toThrow(/unknown handler "nope"/)
  })

  it('rejects a handler reference when no handlers map is provided', () => {
    expect(() =>
      defineWorkflow({
        name: 'w', version: '1', start: 'a',
        steps: {
          a: { type: 'activity', handler: 'work', next: 'done' },
          done: { type: 'succeed' },
        },
      }),
    ).toThrow(/unknown handler "work"/)
  })

  it('rejects a step defining both the function slot and a handler', () => {
    expect(() =>
      defineWorkflow(
        {
          name: 'w', version: '1', start: 'a',
          steps: {
            a: { type: 'activity', handler: 'work', run: () => ({}), next: 'done' },
            done: { type: 'succeed' },
          },
        },
        { handlers: { work: () => ({}) } },
      ),
    ).toThrow(/cannot define both "run" and handler/)
  })

  it('rejects params that are not a plain object', () => {
    expect(() =>
      defineWorkflow({
        name: 'w', version: '1', start: 'a',
        steps: { a: { type: 'succeed', params: ['nope'] } },
      }),
    ).toThrow(/params must be a plain object/)
  })
})

describe('data-only workflow definitions', () => {
  const handlers = {
    trim: ({ input, params }) => ({ message: `${input.message.trim()}${params.suffix ?? ''}` }),
    route: ({ data, params }) => (data.message.includes(params.word) ? 'spam' : 'ok'),
    approval: ({ signal }) => signal.decision,
    accept: ({ data }) => ({ outcome: 'sent', message: data.message }),
    reject: ({ data }) => ({ name: 'Spam', message: data.message }),
    childInput: ({ data }) => ({ message: data.message }),
  }

  const definition = {
    name: 'review',
    version: '1',
    start: 'clean',
    steps: {
      clean: { type: 'activity', handler: 'trim', params: { suffix: '!' }, next: 'route' },
      route: {
        type: 'decision',
        handler: 'route',
        params: { word: 'buy now' },
        transitions: { spam: 'rejected', ok: 'gate' },
      },
      gate: {
        type: 'wait',
        handler: 'approval',
        transitions: { approved: 'accepted', rejected: 'rejected' },
      },
      accepted: { type: 'succeed', handler: 'accept' },
      rejected: { type: 'fail', handler: 'reject' },
    },
  }

  it('survives a JSON round-trip and runs end-to-end', async () => {
    const parsed = JSON.parse(JSON.stringify(definition))
    const engine = makeEngine()
    engine.register(defineWorkflow(parsed, { handlers }))

    const exec = await engine.start('review', { message: '  hello  ' })
    await engine.runUntilIdle()

    expect((await engine.getExecution(exec.id)).status).toBe('suspended')
    await engine.signal(exec.id, { decision: 'approved' })
    await engine.runUntilIdle()

    const final = await engine.getExecution(exec.id)
    expect(final.status).toBe('succeeded')
    expect(final.output).toEqual({ outcome: 'sent', message: 'hello!' })
  })

  it('routes through decision handlers using params', async () => {
    const engine = makeEngine()
    engine.register(defineWorkflow(JSON.parse(JSON.stringify(definition)), { handlers }))

    const exec = await engine.start('review', { message: 'buy now cheap' })
    await engine.runUntilIdle()

    const final = await engine.getExecution(exec.id)
    expect(final.status).toBe('failed')
    expect(final.error.name).toBe('Spam')
    expect(final.steps.route.route).toBe('spam')
  })

  it('binds a subworkflow input handler', async () => {
    const engine = makeEngine()
    engine.register(
      defineWorkflow({
        name: 'child', version: '1', start: 'echo',
        steps: {
          echo: { type: 'activity', next: 'done', run: ({ input }) => ({ echoed: input.message }) },
          done: { type: 'succeed', result: ({ data }) => ({ echoed: data.echoed }) },
        },
      }),
    )
    engine.register(
      defineWorkflow(
        {
          name: 'parent', version: '1', start: 'prep',
          steps: {
            prep: { type: 'activity', handler: 'trim', params: {}, next: 'spawn' },
            spawn: {
              type: 'subworkflow',
              workflow: 'child',
              handler: 'childInput',
              transitions: { succeeded: 'done', failed: 'oops', canceled: 'oops' },
            },
            done: { type: 'succeed', result: ({ steps }) => steps.spawn.output.output },
            oops: { type: 'fail' },
          },
        },
        { handlers },
      ),
    )

    const exec = await engine.start('parent', { message: ' hi ' })
    await engine.runUntilIdle()

    const final = await engine.getExecution(exec.id)
    expect(final.status).toBe('succeeded')
    expect(final.output).toEqual({ echoed: 'hi' })
  })

  it('exposes params to inline function steps too', async () => {
    const engine = makeEngine()
    engine.register(
      defineWorkflow({
        name: 'inline', version: '1', start: 'work',
        steps: {
          work: {
            type: 'activity',
            params: { factor: 3 },
            next: 'done',
            run: ({ input, params }) => ({ result: input.value * params.factor }),
          },
          done: { type: 'succeed', result: ({ data }) => ({ result: data.result }) },
        },
      }),
    )

    const exec = await engine.start('inline', { value: 7 })
    await engine.runUntilIdle()

    expect((await engine.getExecution(exec.id)).output).toEqual({ result: 21 })
  })

  it('includes handler and params in graph nodes', () => {
    const workflow = defineWorkflow(JSON.parse(JSON.stringify(definition)), { handlers })
    const clean = workflow.graph.nodes.find((n) => n.name === 'clean')
    expect(clean.handler).toBe('trim')
    expect(clean.params).toEqual({ suffix: '!' })

    const accepted = workflow.graph.nodes.find((n) => n.name === 'accepted')
    expect(accepted.handler).toBe('accept')
    expect(accepted.params).toBeNull()
  })
})
