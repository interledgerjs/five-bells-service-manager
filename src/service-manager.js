'use strict'

const assert = require('assert')
const path = require('path')
const fs = require('fs')
const co = require('co')
const request = require('superagent')
const util = require('./util')
const chalk = require('chalk')

const COMMON_ENV = Object.assign({}, {
  // Path is required for NPM to work properly
  PATH: process.env.PATH,
  // Print additional debug information from Five Bells and ILP modules
  DEBUG: 'five-bells-*,ilp-*'
}, !require('supports-color') ? {} : {
  // Force colored output
  FORCE_COLOR: 1,
  DEBUG_COLORS: 1,
  npm_config_color: 'always'
})

class ServiceManager {
  /**
   * @param {String} depsDir
   * @param {String} dataDir
   * @param {Object} [_opts]
   * @param {String} [_opts.adminUser]
   * @param {String} [_opts.adminPass]
   */
  constructor (depsDir, dataDir, _opts) {
    this.depsDir = depsDir
    this.dataDir = dataDir
    try {
      fs.mkdirSync(dataDir)
    } catch (err) {
      if (err.code !== 'EEXIST') throw err
    }

    const opts = _opts || {}
    this.adminUser = opts.adminUser || 'admin'
    this.adminPass = opts.adminPass || 'admin'

    this.nodePath = process.env.npm_node_execpath
    this.npmPath = process.env.npm_execpath
    this.hasCustomNPM = this.nodePath && this.npmPath
    this.processes = []
    this.ledgers = {} // { name ⇒ host }
    this.connectors = [] // [host]
    this.receivers = {} // { name ⇒ Receiver }

    this.Client = require(path.resolve(depsDir, 'ilp-core')).Client
    this.FiveBellsLedger = require(path.resolve(depsDir, 'ilp-plugin-bells'))
    this.ilp = require(path.resolve(depsDir, 'ilp'))

    process.on('exit', this.killAll.bind(this))
    process.on('SIGINT', this.killAll.bind(this))
    process.on('uncaughtException', this.killAll.bind(this))
  }

  _npm (args, prefix, options, waitFor) {
    let cmd = 'npm'
    if (this.hasCustomNPM) {
      cmd = this.nodePath
      args.unshift(this.npmPath)
    }
    options = Object.assign({detached: true, stdio: ['ignore', 'ignore', 'ignore']}, options)
    const formatter = this._getOutputFormatter(prefix)
    const proc = util.spawnParallel(cmd, args, options, formatter)

    // Keep track of processes so we can kill them later
    this.processes.push(proc)

    if (!waitFor) return Promise.resolve(null)
    // Wait for result of process
    return util.wait(waitFor)
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

  startLedger (prefix, port, options) {
    const dbPath = path.resolve(this.dataDir, './' + prefix + 'sqlite')
    this.ledgers[prefix] = 'http://localhost:' + port
    return this._npm(['start'], 'ledger:' + port, {
      env: Object.assign({}, COMMON_ENV, {
        LEDGER_DB_URI: 'sqlite://' + dbPath,
        LEDGER_DB_SYNC: true,
        LEDGER_HOSTNAME: 'localhost',
        LEDGER_PORT: port,
        LEDGER_ILP_PREFIX: prefix,
        LEDGER_ADMIN_USER: this.adminUser,
        LEDGER_ADMIN_PASS: this.adminPass,
        LEDGER_AMOUNT_SCALE: options.scale || '4',
        LEDGER_SIGNING_PRIVATE_KEY: options.notificationPrivateKey || '',
        LEDGER_SIGNING_PUBLIC_KEY: options.notificationPublicKey || ''
      }),
      cwd: path.resolve(this.depsDir, 'five-bells-ledger')
    }, 'http://localhost:' + port + '/health')
  }

  startConnector (port, options) {
    this.connectors.push('http://localhost:' + port)
    return this._npm(['start'], 'connector:' + port, {
      env: Object.assign({}, COMMON_ENV, {
        CONNECTOR_CREDENTIALS: JSON.stringify(options.credentials),
        CONNECTOR_PAIRS: JSON.stringify(options.pairs),
        CONNECTOR_MAX_HOLD_TIME: 60,
        CONNECTOR_ROUTE_BROADCAST_ENABLED: options.routeBroadcastEnabled === undefined || options.routeBroadcastEnabled,
        CONNECTOR_ROUTE_BROADCAST_INTERVAL: 10 * 60 * 1000,
        CONNECTOR_ROUTE_EXPIRY: 11 * 60 * 1000, // don't expire routes
        CONNECTOR_AUTOLOAD_PEERS: true,
        CONNECTOR_FX_SPREAD: options.fxSpread || '',
        CONNECTOR_SLIPPAGE: options.slippage || '',
        CONNECTOR_HOSTNAME: 'localhost',
        CONNECTOR_PORT: port,
        CONNECTOR_ADMIN_USER: this.adminUser,
        CONNECTOR_ADMIN_PASS: this.adminPass,
        CONNECTOR_BACKEND: options.backend || 'one-to-one',
        CONNECTOR_NOTIFICATION_VERIFY: !!options.notificationKeys,
        CONNECTOR_NOTIFICATION_KEYS: options.notificationKeys ? JSON.stringify(options.notificationKeys) : ''
      }),
      cwd: path.resolve(this.depsDir, 'ilp-connector')
    }, 'http://localhost:' + port + '/health')
  }

  startNotary (port, options) {
    const dbPath = path.resolve(this.dataDir, './notary' + port + '.sqlite')
    return this._npm(['start'], 'notary:' + port, {
      env: Object.assign({}, COMMON_ENV, {
        NOTARY_DB_URI: 'sqlite://' + dbPath,
        NOTARY_DB_SYNC: true,
        NOTARY_HOSTNAME: 'localhost',
        NOTARY_PORT: port,
        NOTARY_ED25519_SECRET_KEY: options.secretKey,
        NOTARY_ED25519_PUBLIC_KEY: options.publicKey
      }),
      cwd: path.resolve(this.depsDir, 'five-bells-notary')
    }, 'http://localhost:' + port + '/health')
  }

  startVisualization (port) {
    return this._npm(['start'], 'visualization:' + port, {
      env: Object.assign({}, COMMON_ENV, {
        VISUALIZATION_LEDGERS: JSON.stringify(this.ledgers),
        VISUALIZATION_RECRAWL_INTERVAL: 30000,
        HOSTNAME: 'localhost',
        PORT: port,
        ADMIN_USER: this.adminUser,
        ADMIN_PASS: this.adminPass
      }),
      cwd: path.resolve(this.depsDir, 'five-bells-visualization')
    })
  }

  /**
   * @param {Object} credentials
   * @param {IlpAddress} credentials.prefix
   * @param {URI} credentials.account
   * @param {String} credentials.password
   * @param {Buffer} credentials.hmacKey
   * @returns {Promise}
   */
  startReceiver (credentials) {
    const receiver = this.receivers[credentials.prefix] =
      this.ilp.createReceiver(
        Object.assign({ _plugin: this.FiveBellsLedger }, credentials))
    return receiver.listen()
  }

  /**
   * @param {String} ledger
   * @param {String} name
   * @param {Object} options
   * @param {Amount} options.balance
   * @param {String} options.adminUser
   * @param {String} options.adminPass
   */
  * _updateAccount (ledger, name, options) {
    const accountURI = this.ledgers[ledger] + '/accounts/' + encodeURIComponent(name)
    const putAccountRes = yield request.put(accountURI)
      .auth(options.adminUser || this.adminUser, options.adminPass || this.adminPass)
      .send({
        name: name,
        password: name,
        balance: options.balance || '0'
      })
    if (putAccountRes.statusCode >= 400) {
      throw new Error('Unexpected status code ' + putAccountRes.statusCode)
    }
    return putAccountRes
  }

  updateAccount (ledger, name, options) {
    return co.wrap(this._updateAccount).call(this, ledger, name, options || {})
  }

  * _getBalance (ledger, name, options) {
    const accountURI = this.ledgers[ledger] + '/accounts/' + encodeURIComponent(name)
    const getAccountRes = yield request.get(accountURI)
      .auth(options.adminUser || this.adminUser, options.adminPass || this.adminPass)
    return getAccountRes.body && getAccountRes.body.balance
  }

  getBalance (ledger, name, options) {
    return co.wrap(this._getBalance).call(this, ledger, name, options || {})
  }

  sendPayment (params) {
    const sourceAddress = parseAddress(params.sourceAccount)
    const sourceLedgerHost = this.ledgers[sourceAddress.ledger]
    const clientOpts = {
      _plugin: this.FiveBellsLedger,
      prefix: sourceAddress.ledger,
      account: sourceLedgerHost + '/accounts/' + sourceAddress.username,
      password: params.sourcePassword
    }
    return params.sourceAmount
      ? this.sendPaymentBySourceAmount(clientOpts, params)
      : this.sendPaymentByDestinationAmount(clientOpts, params)
  }

  * sendPaymentBySourceAmount (clientOpts, params) {
    const client = new this.Client(clientOpts)
    yield client.connect()

    if (params.onOutgoingReject) {
      client.once('outgoing_reject', params.onOutgoingReject)
    }

    const quote = yield client.quote({
      sourceAmount: params.sourceAmount,
      destinationAddress: params.destinationAccount,
      destinationPrecision: params.destinationPrecision,
      destinationScale: params.destinationScale
    })
    const destinationLedger = parseAddress(params.destinationAccount).ledger
    const paymentRequest = this.receivers[destinationLedger].createRequest(
      {amount: quote.destinationAmount})
    const sourceExpiryDuration = quote.sourceExpiryDuration || 5
    const executionCondition = params.executionCondition || paymentRequest.condition

    return yield client.sendQuotedPayment(Object.assign({
      destinationAccount: paymentRequest.address,
      destinationLedger: destinationLedger,
      expiresAt: (new Date(Date.now() + sourceExpiryDuration * 1000)).toISOString(),
      destinationMemo: {
        expires_at: paymentRequest.expires_at,
        data: paymentRequest.data
      },
      executionCondition: params.unsafeOptimisticTransport ? undefined : executionCondition,
      unsafeOptimisticTransport: params.unsafeOptimisticTransport
    }, quote))
  }

  * sendPaymentByDestinationAmount (clientOpts, params) {
    if (params.unsafeOptimisticTransport) {
      throw new Error('ServiceManager#sendPaymentByDestinationAmount doesn\'t support unsafeOptimisticTransport')
    }

    const sender = this.ilp.createSender(clientOpts)
    const destinationLedger = parseAddress(params.destinationAccount).ledger
    const paymentRequest = this.receivers[destinationLedger].createRequest(
      {amount: params.destinationAmount})
    const paymentParams = yield sender.quoteRequest(paymentRequest)
    const result = yield sender.payRequest(paymentParams)
    return result
  }

  * sendRoutes (connectorHost, routes) {
    yield request
      .post(connectorHost + '/routes')
      .send(routes)
  }

  * assertBalance (ledger, name, expectedBalance) {
    const actualBalance = yield this.getBalance(ledger, name)
    assert.equal(actualBalance, expectedBalance,
      `Balance for ${ledger}${name} should be ${expectedBalance}, but is ${actualBalance}`)
  }
}

function parseAddress (address) {
  const addressParts = address.split('.')
  return {
    ledger: addressParts.slice(0, -1).join('.') + '.',
    username: addressParts[addressParts.length - 1]
  }
}

module.exports = ServiceManager
