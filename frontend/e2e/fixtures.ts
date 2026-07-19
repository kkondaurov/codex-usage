import { test as base } from '@playwright/test'
import { spawn, type ChildProcess } from 'node:child_process'
import { cp, mkdir, mkdtemp, rm, stat } from 'node:fs/promises'
import { createServer, type Server } from 'node:http'
import type { AddressInfo } from 'node:net'
import { tmpdir } from 'node:os'
import { dirname, join, resolve } from 'node:path'
import { env as processEnv } from 'node:process'
import { DatabaseSync } from 'node:sqlite'
import { setTimeout as delay } from 'node:timers/promises'
import { fileURLToPath } from 'node:url'

export { expect } from '@playwright/test'

export interface AppFixture {
  baseUrl: string
  tempRoot: string
  activeRoot: string
  archiveRoot: string
}

interface E2EWorkerOptions {
  app: AppFixture
  e2ePollSeconds: number
}

interface ExitResult {
  code: number | null
  signal: NodeJS.Signals | null
}

interface CapturedProcess {
  child: ChildProcess
  closed: Promise<ExitResult>
  logs: () => string
  spawnError: () => Error | null
}

const fixtureDirectory = dirname(fileURLToPath(import.meta.url))
const frontendRoot = resolve(fixtureDirectory, '..')
const repoRoot = resolve(frontendRoot, '..')
const binaryPath = join(repoRoot, 'target', 'debug', 'codex-usage')
const frontendDist = join(frontendRoot, 'dist')
const corpusRoot = join(repoRoot, 'tests', 'fixtures', 'corpus')
const corpusCases = ['replay_spike', 'rich_trace', 'legacy_v0', 'sparse_pricing']
const maximumLogLength = 200_000
const pricingPath = '/BerriAI/litellm/main/model_prices_and_context_window.json'

export const overviewScaleYear = Number(processEnv.CODEX_USAGE_E2E_YEAR ?? new Date().getFullYear())
export const overviewScaleMessagesPerMonth = 7_500
export const overviewScaleUsageFactsPerMonth = 18_000
export const overviewScaleEventsPerMonth = 60_000
export const overviewScaleThreadsPerMonth = 200
export const overviewScaleAdditionalGroupsPerMonth = 47
export const overviewScaleMessageCount = 12 * overviewScaleMessagesPerMonth
export const overviewScaleUsageFactCount = 12 * overviewScaleUsageFactsPerMonth
export const overviewScaleEventCount = 12 * overviewScaleEventsPerMonth

const pricingPayload = JSON.stringify({
  'openai/gpt-5.5': {
    input_cost_per_token: 0.000005,
    cache_read_input_token_cost: 0.0000005,
    output_cost_per_token: 0.00003,
  },
  'openai/gpt-5.6-sol': {
    input_cost_per_token: 0.000005,
    cache_read_input_token_cost: 0.0000005,
    output_cost_per_token: 0.00003,
  },
})

function appendLog(current: string, chunk: string) {
  const combined = current + chunk
  return combined.length > maximumLogLength
    ? combined.slice(combined.length - maximumLogLength)
    : combined
}

function captureProcess(command: string, args: string[], cwd: string, env: NodeJS.ProcessEnv): CapturedProcess {
  const child = spawn(command, args, {
    cwd,
    env,
    stdio: ['ignore', 'pipe', 'pipe'],
  })
  let stdout = ''
  let stderr = ''
  let processError: Error | null = null

  child.stdout?.setEncoding('utf8')
  child.stderr?.setEncoding('utf8')
  child.stdout?.on('data', (chunk: string) => { stdout = appendLog(stdout, chunk) })
  child.stderr?.on('data', (chunk: string) => { stderr = appendLog(stderr, chunk) })
  child.on('error', (error) => { processError = error })

  const closed = new Promise<ExitResult>((resolveClose) => {
    child.once('close', (code, signal) => resolveClose({ code, signal }))
  })

  return {
    child,
    closed,
    logs: () => [stdout && `stdout:\n${stdout}`, stderr && `stderr:\n${stderr}`]
      .filter(Boolean)
      .join('\n'),
    spawnError: () => processError,
  }
}

async function terminateProcess(process: CapturedProcess) {
  if (process.child.exitCode !== null || process.child.signalCode !== null) {
    await process.closed
    return
  }

  process.child.kill('SIGINT')
  const graceful = await Promise.race([
    process.closed.then(() => true),
    delay(5_000).then(() => false),
  ])
  if (graceful) return

  process.child.kill('SIGKILL')
  await Promise.race([process.closed, delay(2_000)])
}

async function runCommand(
  command: string,
  args: string[],
  cwd: string,
  env: NodeJS.ProcessEnv,
  timeoutMs = 60_000,
) {
  const process = captureProcess(command, args, cwd, env)
  const result = await Promise.race([
    process.closed.then(exit => ({ kind: 'exit' as const, exit })),
    delay(timeoutMs).then(() => ({ kind: 'timeout' as const })),
  ])

  if (result.kind === 'timeout') {
    await terminateProcess(process)
    throw new Error(`Command timed out: ${command} ${args.join(' ')}\n${process.logs()}`)
  }
  if (process.spawnError()) {
    throw new Error(`Could not start ${command}: ${process.spawnError()?.message}\n${process.logs()}`)
  }
  if (result.exit.code !== 0) {
    throw new Error(
      `Command failed (${result.exit.code ?? result.exit.signal}): ${command} ${args.join(' ')}\n${process.logs()}`,
    )
  }
}

async function listenOnLoopback(server: Server) {
  await new Promise<void>((resolveListen, rejectListen) => {
    const onError = (error: Error) => rejectListen(error)
    server.once('error', onError)
    server.listen(0, '127.0.0.1', () => {
      server.off('error', onError)
      resolveListen()
    })
  })
  const address = server.address()
  if (!address || typeof address === 'string') throw new Error('Loopback server did not expose a TCP address')
  return (address as AddressInfo).port
}

async function closeServer(server: Server | null) {
  if (!server?.listening) return
  await new Promise<void>((resolveClose, rejectClose) => {
    server.close((error) => error ? rejectClose(error) : resolveClose())
  })
}

async function startPricingServer() {
  const server = createServer((request, response) => {
    if (request.method !== 'GET' || request.url !== pricingPath) {
      response.writeHead(404).end()
      return
    }
    response.writeHead(200, {
      'cache-control': 'no-store',
      'content-type': 'application/json',
    })
    response.end(pricingPayload)
  })
  const port = await listenOnLoopback(server)
  return { server, url: `http://127.0.0.1:${port}${pricingPath}` }
}

async function reservePort() {
  const reservation = createServer()
  const port = await listenOnLoopback(reservation)
  await closeServer(reservation)
  return port
}

async function copyCorpus(activeRoot: string, archiveRoot: string) {
  await Promise.all([mkdir(activeRoot, { recursive: true }), mkdir(archiveRoot, { recursive: true })])
  for (const corpusCase of corpusCases) {
    for (const [kind, destinationRoot] of [['active', activeRoot], ['archived', archiveRoot]] as const) {
      const source = join(corpusRoot, corpusCase, kind)
      const sourceStats = await stat(source).catch(() => null)
      if (!sourceStats?.isDirectory()) continue
      await cp(source, join(destinationRoot, corpusCase), { recursive: true })
    }
  }
}

function seedOverviewScaleDatabase(dbPath: string) {
  // This is query-scale data, not an ingestion fixture. Seeding the disposable
  // projection directly keeps every E2E run fast while preserving the live
  // database shape that exposed the analytical latency regression.
  const database = new DatabaseSync(dbPath)
  try {
    database.exec(`
      PRAGMA foreign_keys = ON;
      BEGIN IMMEDIATE;

      WITH RECURSIVE
        months(month) AS (
          VALUES(1)
          UNION ALL SELECT month + 1 FROM months WHERE month < 12
        ),
        thread_numbers(value) AS (
          VALUES(1)
          UNION ALL SELECT value + 1 FROM thread_numbers WHERE value < ${overviewScaleThreadsPerMonth}
        )
      INSERT INTO threads(
        id,title,cwd,project,branch,source,thread_source,source_json,
        started_at,last_event_at,title_updated_at,root_metadata_seen
      )
      SELECT
        printf('scale-thread-%02d-%03d',month,value),
        printf('Overview scale session %02d-%03d',month,value),
        printf('/tmp/e2e-overview-scale/%02d/%03d',month,value),
        printf('overview-scale-%02d',month),
        'main','vscode','Codex Desktop',NULL,
        printf('${overviewScaleYear}-%02d-15T12:00:00.000Z',month),
        printf('${overviewScaleYear}-%02d-15T12:30:00.000Z',month),
        printf('${overviewScaleYear}-%02d-15T12:00:00.000Z',month),1
      FROM months CROSS JOIN thread_numbers;

      WITH RECURSIVE
        months(month) AS (
          VALUES(1)
          UNION ALL SELECT month + 1 FROM months WHERE month < 12
        ),
        thread_numbers(value) AS (
          VALUES(1)
          UNION ALL SELECT value + 1 FROM thread_numbers WHERE value < ${overviewScaleThreadsPerMonth}
        )
      INSERT INTO rollouts(
        id,thread_id,parent_rollout_id,parent_thread_id,agent_path,agent_nickname,
        cwd,started_at,last_event_at,archived
      )
      SELECT
        printf('scale-rollout-%02d-%03d',month,value),printf('scale-thread-%02d-%03d',month,value),
        NULL,NULL,NULL,NULL,printf('/tmp/e2e-overview-scale/%02d/%03d',month,value),
        printf('${overviewScaleYear}-%02d-15T12:00:00.000Z',month),
        printf('${overviewScaleYear}-%02d-15T12:30:00.000Z',month),0
      FROM months CROSS JOIN thread_numbers;

      WITH RECURSIVE
        months(month) AS (
          VALUES(1)
          UNION ALL SELECT month + 1 FROM months WHERE month < 12
        ),
        thread_numbers(value) AS (
          VALUES(1)
          UNION ALL SELECT value + 1 FROM thread_numbers WHERE value < ${overviewScaleThreadsPerMonth}
        )
      INSERT INTO turns(
        id,thread_id,rollout_id,agent_run_id,started_at,completed_at,status,
        model,effort,last_agent_message,duration_ms,time_to_first_token_ms
      )
      SELECT
        printf('scale-turn-%02d-%03d',month,value),printf('scale-thread-%02d-%03d',month,value),
        printf('scale-rollout-%02d-%03d',month,value),NULL,
        printf('${overviewScaleYear}-%02d-15T12:00:00.000Z',month),
        printf('${overviewScaleYear}-%02d-15T12:30:00.000Z',month),'completed',
        'gpt-5.6-sol','low',printf('Overview scale session %02d-%03d complete',month,value),1800000,10
      FROM months CROSS JOIN thread_numbers;

      WITH RECURSIVE
        months(month) AS (
          VALUES(1)
          UNION ALL SELECT month + 1 FROM months WHERE month < 12
        ),
        numbers(value) AS (
          VALUES(1)
          UNION ALL SELECT value + 1 FROM numbers WHERE value < ${overviewScaleMessagesPerMonth}
        )
      INSERT INTO messages(
        id,thread_id,rollout_id,turn_id,timestamp,role,content,source_line
      )
      SELECT
        printf('scale-message-%02d-%05d',month,value),
        printf('scale-thread-%02d-%03d',month,((value - 1) % ${overviewScaleThreadsPerMonth}) + 1),
        printf('scale-rollout-%02d-%03d',month,((value - 1) % ${overviewScaleThreadsPerMonth}) + 1),
        printf('scale-turn-%02d-%03d',month,((value - 1) % ${overviewScaleThreadsPerMonth}) + 1),
        printf('${overviewScaleYear}-%02d-15T12:05:00.000Z',month),
        CASE WHEN value % 2 = 1 THEN 'user' ELSE 'assistant' END,
        printf('Overview scale session %02d message %d',month,value),value
      FROM months CROSS JOIN numbers;

      WITH RECURSIVE
        months(month) AS (
          VALUES(1)
          UNION ALL SELECT month + 1 FROM months WHERE month < 12
        ),
        numbers(value) AS (
          VALUES(1)
          UNION ALL SELECT value + 1 FROM numbers WHERE value < ${overviewScaleUsageFactsPerMonth}
        )
      INSERT INTO usage_facts(
        id,thread_id,rollout_id,turn_id,agent_run_id,timestamp,source_line,
        model,effort,input_tokens,cached_input_tokens,output_tokens,
        reasoning_tokens,total_tokens,native
      )
      SELECT
        printf('scale-usage-%02d-%05d',month,value),
        printf('scale-thread-%02d-%03d',month,((value - 1) % ${overviewScaleThreadsPerMonth}) + 1),
        printf('scale-rollout-%02d-%03d',month,((value - 1) % ${overviewScaleThreadsPerMonth}) + 1),
        printf('scale-turn-%02d-%03d',month,((value - 1) % ${overviewScaleThreadsPerMonth}) + 1),NULL,
        printf('${overviewScaleYear}-%02d-15T12:10:00.000Z',month),
        ${overviewScaleMessagesPerMonth} + value,
        CASE WHEN value <= ${overviewScaleAdditionalGroupsPerMonth} THEN 'gpt-5.5' ELSE 'gpt-5.6-sol' END,
        'low',10,8,2,0,12,1
      FROM months CROSS JOIN numbers;

      WITH RECURSIVE
        months(month) AS (
          VALUES(1)
          UNION ALL SELECT month + 1 FROM months WHERE month < 12
        ),
        numbers(value) AS (
          VALUES(1)
          UNION ALL SELECT value + 1 FROM numbers WHERE value < ${overviewScaleEventsPerMonth}
        )
      INSERT INTO events(
        id,thread_id,rollout_id,turn_id,agent_run_id,timestamp,source_line,
        kind,role,label,body,status,tool_name,call_id,duration_ms,model,
        effort,payload_json,native
      )
      SELECT
        printf('scale-event-%02d-%05d',month,value),
        printf('scale-thread-%02d-%03d',month,((value - 1) % ${overviewScaleThreadsPerMonth}) + 1),
        printf('scale-rollout-%02d-%03d',month,((value - 1) % ${overviewScaleThreadsPerMonth}) + 1),
        printf('scale-turn-%02d-%03d',month,((value - 1) % ${overviewScaleThreadsPerMonth}) + 1),NULL,
        CASE
          WHEN value <= ${overviewScaleAdditionalGroupsPerMonth}
            THEN printf('${overviewScaleYear}-%02d-16T12:15:00.000Z',month)
          ELSE printf('${overviewScaleYear}-%02d-15T12:15:00.000Z',month)
        END,
        ${overviewScaleMessagesPerMonth} + ${overviewScaleUsageFactsPerMonth} + value,
        'assistant_message','assistant','Assistant message',
        printf('Overview scale event %02d-%d',month,value),NULL,NULL,NULL,NULL,
        'gpt-5.6-sol','low',NULL,1
      FROM months CROSS JOIN numbers;

      COMMIT;
    `)
  } finally {
    database.close()
  }
}

function isolatedEnvironment(tempRoot: string) {
  const env: NodeJS.ProcessEnv = { ...processEnv }
  for (const key of Object.keys(env)) {
    if (key.startsWith('CODEX_USAGE_')) delete env[key]
  }
  Object.assign(env, {
    HOME: tempRoot,
    USERPROFILE: tempRoot,
    XDG_CACHE_HOME: join(tempRoot, '.cache'),
    TZ: 'Europe/Amsterdam',
    NO_PROXY: '127.0.0.1,localhost',
    no_proxy: '127.0.0.1,localhost',
    HTTP_PROXY: 'http://127.0.0.1:1',
    HTTPS_PROXY: 'http://127.0.0.1:1',
    ALL_PROXY: 'http://127.0.0.1:1',
    http_proxy: 'http://127.0.0.1:1',
    https_proxy: 'http://127.0.0.1:1',
    all_proxy: 'http://127.0.0.1:1',
    RUST_LOG: 'info',
  })
  return env
}

async function waitUntilReady(baseUrl: string, process: CapturedProcess) {
  const deadline = Date.now() + 30_000
  let lastError: unknown = null

  while (Date.now() < deadline) {
    if (process.spawnError()) throw process.spawnError()
    if (process.child.exitCode !== null || process.child.signalCode !== null) {
      const exit = await process.closed
      throw new Error(`Codex Usage exited before readiness (${exit.code ?? exit.signal})\n${process.logs()}`)
    }
    try {
      const response = await fetch(`${baseUrl}/api/v1/status`, {
        signal: AbortSignal.timeout(1_000),
      })
      if (response.ok) {
        const status = await response.json() as { state?: unknown }
        if (status.state === 'idle') return
        lastError = new Error(`Readiness reported ingestion state ${String(status.state)}`)
      } else {
        lastError = new Error(`Readiness returned HTTP ${response.status}`)
      }
    } catch (error) {
      lastError = error
    }
    await delay(100)
  }

  throw new Error(
    `Codex Usage did not become ready: ${lastError instanceof Error ? lastError.message : String(lastError)}\n${process.logs()}`,
  )
}

export const test = base.extend<Record<never, never>, E2EWorkerOptions>({
  e2ePollSeconds: [1, { option: true, scope: 'worker' }],
  app: [async ({ browserName, e2ePollSeconds }, use, workerInfo) => {
    if (browserName !== 'chromium') {
      throw new Error(`The Codex Usage E2E fixture supports Chromium, received ${browserName}`)
    }
    if (workerInfo.parallelIndex !== 0) {
      throw new Error('The Codex Usage E2E fixture requires Playwright workers: 1')
    }

    const tempRoot = await mkdtemp(join(tmpdir(), 'codex-usage-e2e-'))
    const activeRoot = join(tempRoot, 'sessions')
    const archiveRoot = join(tempRoot, 'archived_sessions')
    const dbPath = join(tempRoot, 'data', 'codex-usage.db')
    const env = isolatedEnvironment(tempRoot)
    let pricingServer: Server | null = null
    let appProcess: CapturedProcess | null = null

    try {
      const [binaryStats, indexStats] = await Promise.all([
        stat(binaryPath).catch(() => null),
        stat(join(frontendDist, 'index.html')).catch(() => null),
      ])
      if (!binaryStats?.isFile()) throw new Error(`Missing E2E binary: ${binaryPath}`)
      if (!indexStats?.isFile()) throw new Error(`Missing production frontend: ${frontendDist}`)

      await copyCorpus(activeRoot, archiveRoot)
      const pricing = await startPricingServer()
      pricingServer = pricing.server

      await runCommand(binaryPath, [
        'ingest',
        '--db', dbPath,
        '--sessions', activeRoot,
        '--archive', archiveRoot,
        '--pricing-url', pricing.url,
        '--pricing-timeout-seconds', '2',
      ], repoRoot, env)
      seedOverviewScaleDatabase(dbPath)

      const appPort = await reservePort()
      const baseUrl = `http://127.0.0.1:${appPort}`
      appProcess = captureProcess(binaryPath, [
        'serve',
        '--db', dbPath,
        '--sessions', activeRoot,
        '--archive', archiveRoot,
        '--pricing-url', pricing.url,
        '--pricing-timeout-seconds', '2',
        '--frontend', frontendDist,
        '--host', '127.0.0.1',
        '--port', String(appPort),
        '--poll-seconds', String(e2ePollSeconds),
      ], repoRoot, env)
      await waitUntilReady(baseUrl, appProcess)

      await use({ baseUrl, tempRoot, activeRoot, archiveRoot })
    } finally {
      try {
        if (appProcess) await terminateProcess(appProcess)
      } finally {
        try {
          await closeServer(pricingServer)
        } finally {
          await rm(tempRoot, { recursive: true, force: true })
        }
      }
    }
  }, { scope: 'worker' }],
})
