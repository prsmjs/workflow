import { afterEach, beforeEach, describe, expect, it } from "vitest"
import WorkflowEngine, { defineWorkflow } from "../src/index.js"
import { createClient } from "redis"

const redisOpts = { socket: { host: "127.0.0.1", port: 6379 } }

let redisReachable = false
try {
  const probe = createClient(redisOpts)
  probe.on("error", () => {})
  await probe.connect()
  await probe.ping()
  redisReachable = true
  await probe.quit()
} catch {}

const describeIfRedis = redisReachable ? describe : describe.skip

function defineSimple() {
  return defineWorkflow({
    name: "echo",
    version: "1",
    start: "first",
    steps: {
      first: {
        type: "activity",
        next: "done",
        run: ({ input }) => ({ message: input?.message ?? "ok" }),
      },
      done: {
        type: "succeed",
        result: ({ data }) => data,
      },
    },
  })
}

describeIfRedis("pubsub: cross-instance event fan-out", () => {
  let a, b, flushClient

  beforeEach(async () => {
    flushClient = createClient(redisOpts)
    flushClient.on("error", () => {})
    await flushClient.connect()
    await flushClient.flushAll()

    a = new WorkflowEngine({ pubsub: redisOpts })
    b = new WorkflowEngine({ pubsub: redisOpts })
    a.register(defineSimple())
    b.register(defineSimple())
    await a.ready()
    await b.ready()
  })

  afterEach(async () => {
    await a?.close()
    await b?.close()
    await flushClient?.quit()
  })

  it("execution events emitted on the running instance reach the other instance", async () => {
    const eventsOnB = []
    b.on("execution:succeeded", (data) => eventsOnB.push({ kind: "succeeded", id: data.execution.id }))

    const execution = await a.start("echo", { message: "hi" })
    await a.runUntilIdle()

    // give pubsub a moment to deliver
    await new Promise((r) => setTimeout(r, 100))

    expect(eventsOnB.some((e) => e.kind === "succeeded" && e.id === execution.id)).toBe(true)
  })

  it("step events reach the other instance", async () => {
    const stepEventsOnB = []
    b.on("step:succeeded", (data) => stepEventsOnB.push({ step: data.step, id: data.execution.id }))

    const execution = await a.start("echo", { message: "hi" })
    await a.runUntilIdle()

    await new Promise((r) => setTimeout(r, 100))

    expect(stepEventsOnB.some((e) => e.step === "first" && e.id === execution.id)).toBe(true)
  })

  it("instance does not double-fire its own events", async () => {
    let succeededCount = 0
    a.on("execution:succeeded", () => { succeededCount++ })

    await a.start("echo", { message: "hi" })
    await a.runUntilIdle()

    await new Promise((r) => setTimeout(r, 100))

    expect(succeededCount).toBe(1)
  })
})
