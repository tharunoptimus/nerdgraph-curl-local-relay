import http from 'node:http'
import fs from 'node:fs'
import tty from 'node:tty'

const major = Number(process.versions.node.split('.')[0])
if (major < 18) {
	console.error(`node 18+ required, found ${process.versions.node}`)
	process.exit(1)
}

const args = process.argv.slice(2)
const flag = (name, fallback) => {
	const i = args.indexOf(`--${name}`)
	return i >= 0 ? args[i + 1] : fallback
}

const port = Number(flag('port', process.env.PORT ?? 8787))
const restrictedOrigins = String(flag('origin', '')).split(',').map((s) => s.trim()).filter(Boolean)
const maxConcurrent = Number(flag('concurrency', 6))
const maxAttempts = Number(flag('retries', 5))
const maxBodyBytes = 1024 * 1024
const upstreams = { us: 'https://api.newrelic.com/graphql', eu: 'https://api.eu.newrelic.com/graphql' }
const keyCheckQuery = 'SELECT count(*) AS events FROM MobileRequest SINCE 1 hour ago'

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms))

const createLimiter = (max) => {
	const state = { active: 0, queue: [] }
	const next = () => {
		if (state.active >= max || !state.queue.length) return
		const { task, resolve, reject } = state.queue.shift()
		state.active++
		task()
			.then(resolve, reject)
			.finally(() => {
				state.active--
				next()
			})
	}
	const limit = (task) =>
		new Promise((resolve, reject) => {
			state.queue.push({ task, resolve, reject })
			next()
		})
	limit.state = state
	return limit
}

const limit = createLimiter(maxConcurrent)

const backoff = (attempt, retryAfterSec) => {
	if (retryAfterSec) return retryAfterSec * 1000
	return Math.min(30000, 500 * 2 ** attempt) + Math.floor(Math.random() * 300)
}

const readBody = (req) =>
	new Promise((resolve, reject) => {
		const chunks = []
		let size = 0
		req.on('data', (chunk) => {
			size += chunk.length
			if (size > maxBodyBytes) {
				reject(Object.assign(new Error('body too large'), { status: 413 }))
				req.destroy()
			} else chunks.push(chunk)
		})
		req.on('end', () => resolve(Buffer.concat(chunks).toString('utf8')))
		req.on('error', reject)
	})

const graphqlQuery = (timeoutSec) =>
	`query($id:Int!,$nrql:Nrql!){actor{account(id:$id){nrql(query:$nrql,timeout:${timeoutSec}){results}}}}`

const isRateLimitMessage = (text) => /rate.?limit|too many requests|throttl|concurren/i.test(text)

const callNerdGraph = async ({ apiKey, accountId, region, query, timeoutSec, attempts = maxAttempts }) => {
	const url = process.env.NR_GRAPHQL_URL ?? upstreams[region] ?? upstreams.us
	const body = JSON.stringify({ query: graphqlQuery(timeoutSec), variables: { id: Number(accountId), nrql: query } })
	let rateLimited = false
	for (let attempt = 0; attempt < attempts; attempt++) {
		const last = attempt === attempts - 1
		try {
			const res = await fetch(url, {
				method: 'POST',
				headers: { 'Content-Type': 'application/json', 'API-Key': apiKey },
				body,
				signal: AbortSignal.timeout((timeoutSec + 10) * 1000)
			})
			const retryAfter = Number(res.headers.get('retry-after')) || 0
			if (res.status === 429 || res.status >= 500) {
				rateLimited = rateLimited || res.status === 429
				if (last) return { error: `upstream ${res.status}`, status: res.status, rateLimited, attempts: attempt + 1 }
				await sleep(backoff(attempt, retryAfter))
				continue
			}
			const json = await res.json().catch(() => null)
			if (res.status === 401 || res.status === 403) return { error: 'New Relic rejected the API key', status: res.status, rateLimited, attempts: attempt + 1 }
			const messages = (json?.errors ?? []).map((e) => e.message).join('; ')
			if (messages) {
				if (isRateLimitMessage(messages) && !last) {
					rateLimited = true
					await sleep(backoff(attempt, retryAfter))
					continue
				}
				return { error: messages, status: 502, rateLimited: rateLimited || isRateLimitMessage(messages), attempts: attempt + 1 }
			}
			const results = json?.data?.actor?.account?.nrql?.results
			if (!Array.isArray(results)) return { error: 'unexpected upstream payload', status: 502, rateLimited, attempts: attempt + 1 }
			return { results, rateLimited, attempts: attempt + 1 }
		} catch (err) {
			if (last) return { error: `network: ${err.message}`, status: 504, rateLimited, attempts: attempt + 1 }
			await sleep(backoff(attempt, 0))
		}
	}
	return { error: 'exhausted', status: 502, rateLimited, attempts }
}

const readLine = (input, label, { hidden = false, fallback = '' } = {}) =>
	new Promise((resolve) => {
		process.stdout.write(`${label}${fallback ? ` [${fallback}]` : ''}: `)
		let value = ''
		input.setRawMode(true)
		input.resume()
		const onData = (chunk) => {
			for (const ch of chunk.toString('utf8')) {
				if (ch === '\r' || ch === '\n') {
					input.setRawMode(false)
					input.pause()
					input.off('data', onData)
					process.stdout.write('\n')
					return resolve(value.trim() || fallback)
				}
				if (ch === '\u0003') {
					process.stdout.write('\n')
					process.exit(130)
				}
				if (ch === '\u007f' || ch === '\b') {
					if (value) {
						value = value.slice(0, -1)
						process.stdout.write('\b \b')
					}
					continue
				}
				if (ch < ' ') continue
				value += ch
				process.stdout.write(hidden ? '*' : ch)
			}
		}
		input.on('data', onData)
	})

const openTerminal = () => {
	try {
		return new tty.ReadStream(fs.openSync('/dev/tty', 'r+'))
	} catch {
		return null
	}
}

const promptCredentials = async (previous) => {
	const terminal = openTerminal()
	if (!terminal) throw new Error('no terminal available, set NR_API_KEY and NR_ACCOUNT_ID instead')
	try {
		const region = (await readLine(terminal, 'New Relic region (us or eu)', { fallback: previous?.region ?? 'us' })).toLowerCase()
		const accountId = await readLine(terminal, 'New Relic account id (3512946)', { fallback: previous?.accountId ?? '3512946' })
		const apiKey = await readLine(terminal, 'New Relic user API key (hidden)', { hidden: true })
		return { region, accountId, apiKey }
	} finally {
		terminal.destroy()
	}
}

const verifyCredentials = async (creds) => {
	if (!upstreams[creds.region]) return { error: 'region must be us or eu' }
	if (!/^\d+$/.test(creds.accountId)) return { error: 'account id must be a number' }
	if (!creds.apiKey) return { error: 'API key is empty' }
	const outcome = await callNerdGraph({ ...creds, query: keyCheckQuery, timeoutSec: 30, attempts: 2 })
	return outcome.error ? { error: outcome.error } : { events: outcome.results[0]?.events ?? 0 }
}

const obtainCredentials = async () => {
	const { NR_API_KEY, NR_ACCOUNT_ID, NR_REGION } = process.env
	if (NR_API_KEY && NR_ACCOUNT_ID) {
		const creds = { region: (NR_REGION ?? 'us').toLowerCase(), accountId: NR_ACCOUNT_ID, apiKey: NR_API_KEY }
		const check = await verifyCredentials(creds)
		if (check.error) throw new Error(`key check failed: ${check.error}`)
		return { creds, check }
	}
	let previous = null
	for (let attempt = 0; attempt < 3; attempt++) {
		const creds = await promptCredentials(previous)
		previous = creds
		const check = await verifyCredentials(creds)
		if (!check.error) return { creds, check }
		console.error(`key check failed: ${check.error}\n`)
	}
	throw new Error('giving up after 3 failed key checks')
}

const originAllowed = (origin) => !origin || !restrictedOrigins.length || restrictedOrigins.includes(origin)
const hostAllowed = (host) => /^(localhost|127\.0\.0\.1|\[::1\])(:\d+)?$/.test(host ?? '')

const corsHeaders = (req) => ({
	'Access-Control-Allow-Origin': req.headers.origin ?? '*',
	'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
	'Access-Control-Allow-Headers': req.headers['access-control-request-headers'] ?? 'Content-Type',
	'Access-Control-Allow-Private-Network': 'true',
	'Access-Control-Max-Age': '600',
	Vary: 'Origin'
})

const send = (req, res, status, payload) => {
	res.writeHead(status, { ...corsHeaders(req), 'Content-Type': 'application/json' })
	res.end(JSON.stringify(payload))
}

const validate = (input) => {
	if (!input || typeof input !== 'object') return 'body must be a JSON object'
	if (!input.query || typeof input.query !== 'string') return 'query required'
	return null
}

const createHandler = (creds) => async (req, res) => {
	if (!hostAllowed(req.headers.host)) return send(req, res, 403, { error: 'host not allowed' })
	if (!originAllowed(req.headers.origin)) return send(req, res, 403, { error: `origin not allowed, this relay was started with --origin ${restrictedOrigins.join(',')}` })
	if (req.method === 'OPTIONS') {
		res.writeHead(204, corsHeaders(req))
		return res.end()
	}
	if (req.method === 'GET' && req.url === '/health') {
		return send(req, res, 200, { ok: true, account: creds.accountId, region: creds.region, active: limit.state.active, queued: limit.state.queue.length, maxConcurrent })
	}
	if (req.method === 'POST' && req.url === '/nrql') {
		if (!String(req.headers['content-type'] ?? '').includes('application/json')) return send(req, res, 415, { error: 'content-type must be application/json' })
		const raw = await readBody(req)
		let input
		try {
			input = JSON.parse(raw)
		} catch {
			return send(req, res, 400, { error: 'invalid JSON' })
		}
		const problem = validate(input)
		if (problem) return send(req, res, 400, { error: problem })
		const outcome = await limit(() =>
			callNerdGraph({ ...creds, query: input.query, timeoutSec: Math.min(Number(input.timeoutSec) || 120, 120) })
		)
		return send(req, res, outcome.error ? outcome.status : 200, outcome)
	}
	return send(req, res, 404, { error: 'not found' })
}

const serve = (creds) =>
	new Promise((resolve, reject) => {
		const handler = createHandler(creds)
		const server = http.createServer((req, res) => {
			const started = Date.now()
			res.on('finish', () => console.log(`${new Date().toISOString()} ${req.method} ${req.url} ${res.statusCode} ${Date.now() - started}ms`))
			handler(req, res).catch((err) => {
				if (!res.headersSent) send(req, res, err.status ?? 500, { error: err.message })
				else res.end()
			})
		})
		server.on('error', (err) => reject(err.code === 'EADDRINUSE' ? new Error(`port ${port} is already in use, pick another with --port`) : err))
		server.listen(port, '127.0.0.1', () => resolve(server))
	})

const main = async () => {
	const { creds, check } = await obtainCredentials()
	console.log(`key OK for account ${creds.accountId} (${creds.region}), ${check.events} MobileRequest events in the last hour`)
	const server = await serve(creds)
	console.log(`nr-relay listening on http://127.0.0.1:${port}`)
	console.log(`enter port ${port} in the web app. allowed origins: ${restrictedOrigins.length ? restrictedOrigins.join(', ') : 'any (reflected from the request)'}`)
	const shutdown = () => server.close(() => process.exit(0))
	process.on('SIGINT', shutdown)
	process.on('SIGTERM', shutdown)
}

main().catch((err) => {
	console.error(err.message)
	process.exit(1)
})
