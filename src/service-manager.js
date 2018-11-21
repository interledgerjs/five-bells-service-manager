'use strict'

const assert = require('assert')
const path = require('path')
const util = require('./util')
const chalk = require('chalk')

const pgp = require('pg-promise')()
// Connection to the postgres master db shared by all ServiceManager instances
const masterdb = pgp({ database: 'postgres' })

const COMMON_ENV = Object.assign({}, {
  // Path is required for NPM to work properly
  PATH: process.env.PATH,
  // Print additional debug information from Five Bells and ILP modules, but
  // allow the user to override this setting.
  DEBUG: process.env.DEBUG || 'connector*,five-bells*,ilp*'
}, !require('supports-color') ? {} : {
  // Force colored output
  FORCE_COLOR: 1,
  DEBUG_COLORS: 1,
  npm_config_color: 'always'
})

const LEDGER_DEFAULT_SCALE = 4

class ServiceManager {
  /**
   * @param {String} depsDir
   * @param {Object} [_opts]
   * @param {String} [_opts.adminUser]
   * @param {String} [_opts.adminPass]
   */
  constructor (depsDir, _opts) {
    this.depsDir = depsDir
    const opts = _opts || {}
    this.adminUser = opts.adminUser || 'admin'
    this.adminPass = opts.adminPass || 'admin'

    this.dbUser = process.env.USER
    this.dbs = {} // stores the connection object for each database

    this.nodePath = process.env.npm_node_execpath
    this.npmPath = process.env.npm_execpath
    this.hasCustomNPM = this.nodePath && this.npmPath
    this.processes = []
    this.receivers = [] // [Plugin]

    this.Plugin = require(path.resolve(depsDir, 'ilp-plugin-btp'))

    // Load some dependencies from ILP module
    // const ilpModule = require.cache[require.resolve(path.resolve(depsDir, 'ilp'))]
    // this.ILDCP = ilpModule.require('ilp-protocol-ildcp')
    // this.ilpPacket = ilpModule.require('ilp-packet')
    // this.stream = ilpModule.require('ilp-protocol-stream')
    this.ILDCP = require(path.resolve(depsDir, 'ilp-protocol-ildcp'))
    this.ilpPacket = require(path.resolve(depsDir, 'ilp-packet'))
    this.stream = require(path.resolve(depsDir, 'ilp-protocol-stream'))

    process.on('exit', this.killAll.bind(this))
    process.on('SIGINT', this.killAll.bind(this))
    process.on('uncaughtException', this.killAll.bind(this))
  }

  _npm (args, prefix, options, waitFor) {
    return new Promise((resolve) => {
      let cmd = 'npm'
      if (this.hasCustomNPM) {
        cmd = this.nodePath
        args.unshift(this.npmPath)
      }
      options = Object.assign({
        detached: true,
        stdio: ['ignore', 'ignore', 'ignore']
      }, options)

      // Wait for result of process
      if (waitFor) {
        options.waitFor = {
          trigger: waitFor,
          callback: resolve
        }
      } else {
        resolve()
      }

      const formatter = this._getOutputFormatter(prefix)
      const proc = util.spawnParallel(cmd, args, options, formatter)

      // Keep track of processes so we can kill them later
      this.processes.push(proc)
    })
  }

  _getOutputFormatter (prefix) {
    return function (line, enc, callback) {
      this.push('' + chalk.dim(prefix) + ' ' + line.toString('utf-8') + '\n')
      callback()
    }
  }

  killAll (arg) {
    while (this.processes.length) {
      let pid
      try {
        pid = -this.processes.pop().pid
        if (pid) process.kill(pid)
      } catch (err) {
        console.error('could not kill pid ' + pid)
      }
    }
  }

  async startKit (kitName, config) {
    await this._createPostgresDb(config.LEDGER_ILP_PREFIX)
    const dbUri = this._getDbConnectionString(config.LEDGER_ILP_PREFIX)

    const ledgerUri = 'http://localhost:' + config.CLIENT_PORT + '/ledger'
    this.ledgers[config.LEDGER_ILP_PREFIX] = ledgerUri
    this.ledgerOptions[config.LEDGER_ILP_PREFIX] = config

    // this overwrites values set in the config file
    const customEnv = {
      API_CONFIG_FILE: config.apiConfigFile || '',
      CLIENT_PORT: config.CLIENT_PORT,
      DB_URI: dbUri,
      LEDGER_AMOUNT_SCALE: config.scale || String(LEDGER_DEFAULT_SCALE)
    }
    const env = { env: Object.assign(COMMON_ENV, customEnv) }

    const cwd = {
      cwd: path.resolve(this.depsDir, 'ilp-kit')
    }
    let npmOpts = Object.assign(cwd, env)
    let loggingPrefix = `ilp-kit[${kitName}]`

    // kit will start ledger and connector by itself
    return this._npm(['start'], loggingPrefix, npmOpts, 'Note that the development build is not optimized.')
  }

  startConnector (name, options) {
    return this._npm(['start'], name, {
      env: Object.assign({}, COMMON_ENV, omitUndefined({
        CONNECTOR_STORE: 'memdown',
        CONNECTOR_ILP_ADDRESS: options.ilpAddress,
        CONNECTOR_ACCOUNTS: JSON.stringify(options.accounts),
        CONNECTOR_ROUTES: JSON.stringify(options.routes || []),
        CONNECTOR_ROUTE_EXPIRY: 11 * 60 * 1000, // don't expire routes
        CONNECTOR_SPREAD: options.spread,
        CONNECTOR_SLIPPAGE: options.slippage,
        CONNECTOR_BACKEND: options.backend || 'one-to-one'
      })),
      cwd: path.resolve(this.depsDir, 'ilp-connector')
    }, 'connector ready')
  }

  async startSender (opts) {
    const plugin = new (this.Plugin)(opts)
    await plugin.connect()

    const connections = {}
    return {
      plugin,
      connections,
      getConnection: async (destinationAccount, sharedSecret) => {
        const key = `${destinationAccount}:${sharedSecret.toString('hex')}`
        if (connections[key]) {
          return connections[key]
        }
        const connection = await this.stream.createConnection({
          plugin,
          destinationAccount,
          sharedSecret
        })
        connections[key] = connection
        return connection
      },
      disconnect: async () => {
        plugin.disconnect()
      }
    }
  }

  async startReceiver (opts) {
    const plugin = new (this.Plugin)(opts)

    await plugin.connect()

    const server = await this.stream.createServer({
      plugin
    })

    const receiver = Object.assign({
      plugin,
      receivedMoney: 0,
      receivedData: Buffer.alloc(0),
      disconnect: async () => {
        plugin.disconnect()
      }
    }, server.generateAddressAndSecret())

    server.on('connection', (connection) => {
      connection.on('stream', (stream) => {
        // Set the maximum amount of money this stream can receive
        stream.setReceiveMax(Infinity)
        stream.on('money', (amount) => {
          receiver.receivedMoney += Number(amount)
        })
        stream.on('data', (chunk) => {
          receiver.receivedData = Buffer.concat(receiver.receivedData, chunk)
        })
      })
    })
    // await this.ilp.IPR.listen(receiver, {
    //   receiverSecret: Buffer.from('secret')
    // }, ({ destinationAmount, fulfill }) => {
    //   receiver.balance = new BigNumber(receiver.balance || 0).add(destinationAmount).toString()
    //   return fulfill()
    // })

    this.receivers.push(receiver)
    return receiver
  }

  assertBalance (receiver, expectedBalance) {
    assert.strictEqual(receiver.receivedMoney, expectedBalance,
      `Balance should be ${expectedBalance}, but is ${receiver.receivedMoney}`)
  }

  _getDbConnectionString (ledgerPrefix) {
    return 'postgres://' + this.dbUser + '@localhost/' + ledgerPrefix
  }

  async _createPostgresDb (dbName) {
    try {
      await masterdb.none('CREATE DATABASE "' + dbName + '"')
    } catch (e) {
      if (e.code && e.code === '42P04') { // 42P04 = db already exists
        await masterdb.none('DROP DATABASE "' + dbName + '"')
        await this._createPostgresDb(dbName)
      } else {
        throw new Error('Could not create test database: ' + e)
      }
    }
  }

  // TODO - Handle sendPaymentByDestinationAmount
  sendPayment (params) {
    return params.sourceAmount
      ? this.sendPaymentBySourceAmount(params)
      : this.sendPaymentByDestinationAmount(params)
  }

  async sendPaymentBySourceAmount (params) {
    const { sender, receiver, sourceAmount } = params
    const { destinationAccount, sharedSecret } = receiver

    const connection = await sender.getConnection(destinationAccount, sharedSecret)
    const stream = connection.createStream()
    await stream.sendTotal(sourceAmount)
    stream.end()
  }

  async sendPaymentByDestinationAmount (params) {
    // const { sender, receiver, sourceAmount } = params
    // const { destinationAccount, sharedSecret } = receiver
    throw new Error('Send by destination not implemented')
  }
}

function omitUndefined (src) {
  const dst = {}
  for (const key in src) {
    const val = src[key]
    if (val !== undefined) dst[key] = src[key]
  }
  return dst
}

module.exports = ServiceManager
