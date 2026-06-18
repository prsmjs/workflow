import { clone } from './util.js'

/**
 * @typedef {Object} RetryConfig
 * @property {number} [maxAttempts] - total number of times the step may run before its failure becomes the execution's failure (default 1, meaning no retry). Counts the first attempt, so 3 means one try plus two retries.
 * @property {string|number} [backoff] - delay before each retry, as a duration string ("5s", "1m") or milliseconds (default 0 for immediate retry). The execution sits in "waiting" status until the backoff elapses.
 */

/**
 * @typedef {Object} ActivityStep
 * @property {"activity"} type - runs application code, then moves to one explicit next step.
 * @property {string} next - name of the step to advance to after run succeeds (required).
 * @property {Function} [run] - async function receiving the step context and returning the step's output; a plain-object return is shallow-merged into execution.data. Provide this or `handler`, not both.
 * @property {string} [handler] - name of a function in the defineWorkflow handler catalog to use as `run`, for data-defined workflows. Mutually exclusive with `run`.
 * @property {string|number} [timeout] - per-step timeout as a duration string or milliseconds (default the engine's defaultActivityTimeout of "30s"). On timeout the step's AbortSignal fires and normal retry and failure rules apply.
 * @property {RetryConfig|number} [retry] - retry policy for this step (default no retry). A bare number is shorthand for maxAttempts with no backoff.
 * @property {object} [params] - plain JSON object passed to the handler as context.params, cloned per invocation (default none). Keeps workflow-specific configuration in the definition while the handler stays generic.
 * @property {number} [maxPasses] - in cyclic workflows, the maximum number of times this step may be re-entered before routing to it fails with an exceeded-maxPasses error (default unlimited).
 * @property {string} [label] - human-readable label shown in graph introspection (default the step's key).
 * @property {string} [description] - human-readable description shown in graph introspection (default "").
 */

/**
 * @typedef {Object} DecisionStep
 * @property {"decision"} type - chooses one named route from a fixed transitions map, then advances to that route's target step.
 * @property {Object<string, string>} transitions - map of route name to target step name; must define at least one route (required). A route returned by decide that is not present here fails the step.
 * @property {Function} [decide] - async function receiving the step context and returning one of the route names in transitions. Provide this or `handler`, not both.
 * @property {string} [handler] - name of a function in the handler catalog to use as `decide`, for data-defined workflows. Mutually exclusive with `decide`.
 * @property {string|number} [timeout] - per-step timeout as a duration string or milliseconds (default none). Decision steps have no default timeout, so set this when decide calls external services or a hanging call blocks the worker.
 * @property {RetryConfig|number} [retry] - retry policy for this step (default no retry).
 * @property {object} [params] - plain JSON object passed to the handler as context.params, cloned per invocation (default none).
 * @property {number} [maxPasses] - in cyclic workflows, the maximum number of re-entries before routing to this step fails (default unlimited).
 * @property {string} [label] - human-readable label shown in graph introspection (default the step's key).
 * @property {string} [description] - human-readable description shown in graph introspection (default "").
 */

/**
 * @typedef {Object} WaitStep
 * @property {"wait"} type - suspends the execution until an external signal arrives or, if set, a timeout fires. No handler runs while it waits.
 * @property {Object<string, string>} transitions - map of route name to target step name; must define at least one route (required). When `timeout` is set, a "timeout" route is required here.
 * @property {Function} [resolve] - async function mapping the signal payload to one of the routes in transitions. If omitted, the engine reads a string `route` from the signal payload instead. Provide this or `handler`, not both.
 * @property {string} [handler] - name of a function in the handler catalog to use as `resolve`, for data-defined workflows. Mutually exclusive with `resolve`.
 * @property {string|number} [timeout] - how long to wait before firing the "timeout" route automatically, as a duration string ("7d") or milliseconds (default no timeout, meaning it waits indefinitely). Defining a timeout requires a matching "timeout" transition.
 * @property {RetryConfig|number} [retry] - retry policy applied if resolution throws (default no retry).
 * @property {object} [params] - plain JSON object passed to the handler as context.params, cloned per invocation (default none).
 * @property {number} [maxPasses] - in cyclic workflows, the maximum number of re-entries before routing to this step fails (default unlimited).
 * @property {string} [label] - human-readable label shown in graph introspection (default the step's key).
 * @property {string} [description] - human-readable description shown in graph introspection (default "").
 */

/**
 * @typedef {Object} SubworkflowStep
 * @property {"subworkflow"} type - starts a child workflow and suspends until the child reaches a terminal status.
 * @property {string} workflow - name of the child workflow to start (required).
 * @property {string} [version] - version of the child workflow to start (default the single registered version of that name).
 * @property {Object<string, string>} transitions - map of route to target step; the routes "succeeded", "failed", and "canceled" are all required, matching the child's terminal statuses.
 * @property {Function} [input] - async function receiving the step context and returning the child's input (default an empty object). Provide this or `handler`, not both.
 * @property {string} [handler] - name of a function in the handler catalog to use as `input`, for data-defined workflows. Mutually exclusive with `input`.
 * @property {RetryConfig|number} [retry] - retry policy applied if spawning the child throws (default no retry).
 * @property {object} [params] - plain JSON object passed to the handler as context.params, cloned per invocation (default none).
 * @property {number} [maxPasses] - in cyclic workflows, the maximum number of re-entries before routing to this step fails (default unlimited).
 * @property {string} [label] - human-readable label shown in graph introspection (default the step's key).
 * @property {string} [description] - human-readable description shown in graph introspection (default "").
 */

/**
 * @typedef {Object} TerminalStep
 * @property {"succeed"|"fail"} type - ends the execution. "succeed" completes it successfully with the result as output; "fail" completes it as failed with the result as the error.
 * @property {Function|object} [result] - the execution's final output (for "succeed") or error (for "fail"). May be a static value or an async function receiving the step context and returning the value (default null). Terminal steps cannot define `next`.
 * @property {string} [handler] - name of a function in the handler catalog to use as `result`, for data-defined workflows. Mutually exclusive with a function `result`.
 * @property {string} [label] - human-readable label shown in graph introspection (default the step's key).
 * @property {string} [description] - human-readable description shown in graph introspection (default "").
 */

/**
 * @typedef {ActivityStep|DecisionStep|WaitStep|SubworkflowStep|TerminalStep} StepDefinition
 */

/**
 * @typedef {Object} WorkflowDefinition
 * @property {string} name - the workflow name; combined with version it identifies a registered workflow (required).
 * @property {string} version - the workflow version as a string; in-flight executions stay pinned to the version they started on (required).
 * @property {string} start - the key of the step where every execution begins; must be one of the keys in steps (required).
 * @property {Object<string, StepDefinition>} steps - map of step name to step definition (required). Every step must be reachable from start, or definition fails.
 * @property {boolean} [cycles] - allow transitions that route back to an earlier step (default false, where a back-edge is a definition-time error). Enable this for processes that genuinely loop, such as a draft sent back for revision.
 * @property {string} [description] - human-readable description of the workflow, surfaced in describe and graph introspection (default "").
 */

/**
 * @typedef {Object} DefineWorkflowOptions
 * @property {Object<string, Function>} [handlers] - catalog of named functions that steps may reference via their `handler` field, enabling fully data-defined (JSON) workflows. A handler name not present here fails at definition time.
 */

const HANDLER_SLOTS = {
  activity: 'run',
  decision: 'decide',
  wait: 'resolve',
  subworkflow: 'input',
  succeed: 'result',
  fail: 'result',
}

function bindHandler(name, step, handlers) {
  if (step.handler == null) return
  if (typeof step.handler !== 'string') throw new Error(`step "${name}" handler must be a string`)

  const slot = HANDLER_SLOTS[step.type]
  if (!slot) throw new Error(`step "${name}" has unsupported type "${step.type}"`)
  if (step[slot] != null) throw new Error(`step "${name}" cannot define both "${slot}" and handler`)

  const fn = handlers?.[step.handler]
  if (typeof fn !== 'function') throw new Error(`step "${name}" references unknown handler "${step.handler}"`)
  step[slot] = fn
}

function normalizeRetry(retry) {
  if (retry == null) return { maxAttempts: 1, backoff: 0 }
  if (typeof retry === 'number') return { maxAttempts: retry, backoff: 0 }

  return {
    maxAttempts: retry.maxAttempts ?? 1,
    backoff: retry.backoff ?? 0,
  }
}

function inferEdgeLabels(step) {
  if (step.type === 'decision' || step.type === 'wait' || step.type === 'subworkflow') {
    return Object.entries(step.transitions).map(([label, to]) => ({
      from: step.name,
      to,
      label,
    }))
  }

  if (step.type === 'activity' && step.next) {
    return [{ from: step.name, to: step.next, label: 'next' }]
  }

  return []
}

function collectReachable(start, steps, allowCycles) {
  const seen = new Set()
  const visiting = new Set()

  function visit(stepName) {
    if (visiting.has(stepName)) {
      if (allowCycles) return
      throw new Error(`workflow graph must be acyclic; cycle detected at "${stepName}" (set cycles: true to allow back-edges)`)
    }
    if (seen.has(stepName)) return
    const step = steps[stepName]
    if (!step) throw new Error(`unknown step referenced: "${stepName}"`)

    visiting.add(stepName)
    for (const edge of inferEdgeLabels(step)) {
      visit(edge.to)
    }
    visiting.delete(stepName)
    seen.add(stepName)
  }

  visit(start)
  return seen
}

function validateStep(name, step, stepNames) {
  if (!step || typeof step !== 'object') throw new Error(`step "${name}" must be an object`)
  if (!step.type) throw new Error(`step "${name}" must declare a type`)

  if (step.type === 'activity') {
    if (typeof step.run !== 'function') throw new Error(`activity step "${name}" must define run(ctx)`)
    if (typeof step.next !== 'string') throw new Error(`activity step "${name}" must define next`)
    if (!stepNames.has(step.next)) throw new Error(`activity step "${name}" points to unknown step "${step.next}"`)
  } else if (step.type === 'decision') {
    if (typeof step.decide !== 'function') throw new Error(`decision step "${name}" must define decide(ctx)`)
    if (!step.transitions || typeof step.transitions !== 'object') {
      throw new Error(`decision step "${name}" must define transitions`)
    }
    if (Object.keys(step.transitions).length === 0) {
      throw new Error(`decision step "${name}" must define at least one transition`)
    }
    for (const [route, to] of Object.entries(step.transitions)) {
      if (!route) throw new Error(`decision step "${name}" has an empty route label`)
      if (!stepNames.has(to)) throw new Error(`decision step "${name}" route "${route}" points to unknown step "${to}"`)
    }
  } else if (step.type === 'wait') {
    if (!step.transitions || typeof step.transitions !== 'object') {
      throw new Error(`wait step "${name}" must define transitions`)
    }
    if (Object.keys(step.transitions).length === 0) {
      throw new Error(`wait step "${name}" must define at least one transition`)
    }
    for (const [route, to] of Object.entries(step.transitions)) {
      if (!route) throw new Error(`wait step "${name}" has an empty route label`)
      if (!stepNames.has(to)) throw new Error(`wait step "${name}" route "${route}" points to unknown step "${to}"`)
    }
    if (step.resolve != null && typeof step.resolve !== 'function') {
      throw new Error(`wait step "${name}" resolve must be a function if provided`)
    }
    if (step.timeout != null && !Object.hasOwn(step.transitions, 'timeout')) {
      throw new Error(`wait step "${name}" defines timeout but has no "timeout" transition`)
    }
  } else if (step.type === 'subworkflow') {
    if (typeof step.workflow !== 'string' || !step.workflow) {
      throw new Error(`subworkflow step "${name}" must define a workflow name`)
    }
    if (!step.transitions || typeof step.transitions !== 'object') {
      throw new Error(`subworkflow step "${name}" must define transitions`)
    }
    for (const route of ['succeeded', 'failed', 'canceled']) {
      if (!Object.hasOwn(step.transitions, route)) {
        throw new Error(`subworkflow step "${name}" missing required transition "${route}"`)
      }
    }
    for (const [route, to] of Object.entries(step.transitions)) {
      if (!route) throw new Error(`subworkflow step "${name}" has an empty route label`)
      if (!stepNames.has(to)) throw new Error(`subworkflow step "${name}" route "${route}" points to unknown step "${to}"`)
    }
    if (step.input != null && typeof step.input !== 'function') {
      throw new Error(`subworkflow step "${name}" input must be a function if provided`)
    }
  } else if (step.type === 'succeed' || step.type === 'fail') {
    if (step.next) throw new Error(`terminal step "${name}" cannot define next`)
  } else {
    throw new Error(`step "${name}" has unsupported type "${step.type}"`)
  }

  const retry = normalizeRetry(step.retry)
  if (!Number.isInteger(retry.maxAttempts) || retry.maxAttempts < 1) {
    throw new Error(`step "${name}" retry.maxAttempts must be a positive integer`)
  }

  if (step.params != null && (typeof step.params !== 'object' || Array.isArray(step.params))) {
    throw new Error(`step "${name}" params must be a plain object`)
  }

  if (step.maxPasses != null && (!Number.isInteger(step.maxPasses) || step.maxPasses < 1)) {
    throw new Error(`step "${name}" maxPasses must be a positive integer`)
  }
}

/**
 * Validate a workflow definition and compile it into a frozen, registerable workflow with a serializable graph. Checks that every step is well-formed, that all transitions point to known steps, that the graph is acyclic unless cycles is enabled, and that every step is reachable from start. Throws on the first problem it finds.
 * @param {WorkflowDefinition} definition - the workflow's name, version, start step, and steps map.
 * @param {DefineWorkflowOptions} [options] - the handler catalog for data-defined workflows.
 * @returns {Readonly<object>} a frozen workflow ready to pass to engine.register, exposing name, version, start, description, cycles, steps, and graph.
 */
export function defineWorkflow(definition, options = {}) {
  if (!definition || typeof definition !== 'object') throw new Error('workflow definition required')
  if (!definition.name || typeof definition.name !== 'string') throw new Error('workflow name is required')
  if (!definition.version || typeof definition.version !== 'string') throw new Error('workflow version is required')
  if (!definition.start || typeof definition.start !== 'string') throw new Error('workflow start step is required')
  if (!definition.steps || typeof definition.steps !== 'object') throw new Error('workflow steps are required')

  const allowCycles = definition.cycles === true
  const stepNames = new Set(Object.keys(definition.steps))
  if (!stepNames.has(definition.start)) throw new Error(`workflow start step "${definition.start}" does not exist`)

  const steps = {}
  for (const [name, rawStep] of Object.entries(definition.steps)) {
    const step = {
      name,
      label: rawStep.label ?? name,
      description: rawStep.description ?? '',
      ...rawStep,
      retry: normalizeRetry(rawStep.retry),
    }
    bindHandler(name, step, options.handlers)
    validateStep(name, step, stepNames)
    steps[name] = step
  }

  const reachable = collectReachable(definition.start, steps, allowCycles)
  for (const name of stepNames) {
    if (!reachable.has(name)) {
      throw new Error(`step "${name}" is unreachable from start step "${definition.start}"`)
    }
  }

  const nodes = Object.values(steps).map((step) => {
    const node = {
      name: step.name,
      label: step.label,
      type: step.type,
      description: step.description,
      retry: clone(step.retry),
      timeout: step.timeout ?? null,
      handler: step.handler ?? null,
      params: clone(step.params) ?? null,
      maxPasses: step.maxPasses ?? null,
    }
    if (step.type === 'subworkflow') {
      node.workflow = step.workflow
      node.version = step.version ?? null
    }
    return node
  })

  const edges = Object.values(steps).flatMap((step) => inferEdgeLabels(step))

  return Object.freeze({
    name: definition.name,
    version: definition.version,
    start: definition.start,
    description: definition.description ?? '',
    cycles: allowCycles,
    steps: Object.freeze(steps),
    graph: Object.freeze({
      start: definition.start,
      nodes: Object.freeze(nodes),
      edges: Object.freeze(edges),
    }),
  })
}
