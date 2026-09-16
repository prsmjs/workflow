import { describe, expect, it } from 'vitest'
import WorkflowEngine, { defineWorkflow, memoryDriver } from '../src/index.js'

let sqliteDriver = null
let postgresDriver = null
try {
  ;({ sqliteDriver } = await import('../src/sqliteDriver.js'))
} catch {}
try {
  ;({ postgresDriver } = await import('../src/postgresDriver.js'))
} catch {}

const connectionString = process.env.WORKFLOW_TEST_POSTGRES_URL ?? 'postgres://workflow:workflow_password@127.0.0.1:5432/workflow_test'

const workflow = defineWorkflow({
  name: 'guarded',
  version: '1',
  start: 'hold',
  steps: {
    hold: { type: 'wait', transitions: { go: 'done' } },
    done: { type: 'succeed' },
  },
})

async function started(storage) {
  const engine = new WorkflowEngine({ storage })
  engine.register(workflow)
  const execution = await engine.start('guarded', {})
  return { engine, id: execution.id }
}

function itGuards(name, makeStorage) {
  it(`${name}: a save that expects the version it read loses to a newer write`, async () => {
    const storage = await makeStorage()
    const { id } = await started(storage)
    const first = await storage.getExecution(id)
    const second = await storage.getExecution(id)

    const firstSeen = first.updatedAt
    first.updatedAt = firstSeen + 1
    first.availableAt = 123
    expect(await storage.saveExecution(first, { expectedUpdatedAt: firstSeen })).not.toBeNull()

    second.updatedAt = firstSeen + 2
    second.availableAt = 456
    expect(await storage.saveExecution(second, { expectedUpdatedAt: firstSeen })).toBeNull()

    const stored = await storage.getExecution(id)
    expect(stored.availableAt).toBe(123)
  })
}

describe('saveExecution with expectedUpdatedAt', () => {
  itGuards('memory', async () => memoryDriver())
  if (sqliteDriver) {
    itGuards('sqlite', async () => {
      const file = `/tmp/prsm-workflow-guarded-${Date.now()}-${Math.random().toString(36).slice(2)}.sqlite`
      const driver = sqliteDriver({ filename: file })
      return driver
    })
  }
  if (postgresDriver) {
    itGuards('postgres', async () => {
      const driver = postgresDriver({ connectionString })
      await driver.init()
      return driver
    })
  }
})
