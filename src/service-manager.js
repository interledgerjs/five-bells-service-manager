'use strict'

const assert = require('assert')
const path = require('path')
const fs = require('fs')
const co = require('co')
const request = require('superagent')
const util = require('./util')
const chalk = require('chalk')
const BigNumber = require('bignumber.js')
const hashPassword = require('five-bells-shared/utils/hashPassword')
const pgp = require('pg-promise')()
// Connection to the postgres master db shared by all ServiceManager instances
const masterdb = pgp({database: 'postgres'})
const uuid = require('uuid')
const rimraf = require('rimraf')

const COMMON_ENV = Object.assign({}, {
  // Path is required for NPM to work properly
  PATH: process.env.PATH,
  // Print additional debug information from Five Bells and ILP modules, but
  // allow the user to override this setting.
  DEBUG: process.env.DEBUG || 'ledger*,connector*,notary*,five-bells*,ilp*'
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
   * @param {String} dataDir
   * @param {Object} [_opts]
   * @param {String} [_opts.adminUser]
   * @param {String} [_opts.adminPass]
   */
  constructor (depsDir, dataDir, _opts) {
    this.depsDir = depsDir
    this.dataDir = dataDir
    try {
      rimraf.sync(dataDir, {disableGlob: true})
      fs.mkdirSync(dataDir)
    } catch (err) {
      if (err.code !== 'EEXIST') throw err
    }

    const opts = _opts || {}
    this.adminUser = opts.adminUser || 'admin'
    this.adminPass = opts.adminPass || 'admin'

    this.dbUser = process.env.USER
    this.dbs = {} // stores the connection object for each database

    this.nodePath = process.env.npm_node_execpath
    this.npmPath = process.env.npm_execpath
    this.hasCustomNPM = this.nodePath && this.npmPath
    this.processes = []
    this.ledgers = {} // { name ⇒ host }
    this.ledgerOptions = {} // { name ⇒ options }
    this.connectors = {} // { connectorAccount ⇒ [ ["source_asset@source_ledger", "destination_asset@destination_ledger"] ] }
    this.receivers = {} // { name ⇒ Receiver }

    this.FiveBellsLedger = require(path.resolve(depsDir, 'ilp-plugin-bells'))
    this.ilp = require(path.resolve(depsDir, 'ilp'))

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

  * _startKit (kitName, config) {
    yield this._createPostgresDb(config.LEDGER_ILP_PREFIX)
    const dbUri = this._getDbConnectionString(config.LEDGER_ILP_PREFIX)

    const ledgerUri = 'http://localhost:' + config.CLIENT_PORT + '/ledger'
    this.ledgers[config.LEDGER_ILP_PREFIX] = ledgerUri
    this.ledgerOptions[config.LEDGER_ILP_PREFIX] = config

    // this overwrites values set in the config file
    const customEnv = {
      API_CONFIG_FILE: config.apiConfigFile || '',
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
    return this._npm(['start'], loggingPrefix, npmOpts, 'wallet listening on 0.0.0.0:')
  }

  startKit (kitName, config) {
    return co.wrap(this._startKit).call(this, kitName, config)
  }

  * _startLedger (prefix, port, options) {
    yield this._createPostgresDb(prefix)
    const dbUri = this._getDbConnectionString(prefix)
    this.ledgers[prefix] = 'http://localhost:' + port
    this.ledgerOptions[prefix] = options
    return this._npm(['start'], 'ledger:' + port, {
      env: Object.assign({}, COMMON_ENV, {
        LEDGER_DB_URI: dbUri,
        LEDGER_DB_SYNC: true,
        LEDGER_HOSTNAME: 'localhost',
        LEDGER_PORT: port,
        LEDGER_ILP_PREFIX: prefix,
        LEDGER_ADMIN_USER: this.adminUser,
        LEDGER_ADMIN_PASS: this.adminPass,
        LEDGER_AMOUNT_SCALE: options.scale || String(LEDGER_DEFAULT_SCALE),
        LEDGER_SIGNING_PRIVATE_KEY: options.notificationPrivateKey || '',
        LEDGER_SIGNING_PUBLIC_KEY: options.notificationPublicKey || '',
        LEDGER_RECOMMENDED_CONNECTORS: options.recommendedConnectors || ''
      }),
      cwd: path.resolve(this.depsDir, 'five-bells-ledger')
    }, 'public at')
  }

  startLedger (prefix, port, options) {
    return co.wrap(this._startLedger).call(this, prefix, port, options)
  }

  startConnector (name, options) {
    this.connectors[name] = options.pairs
    return this._npm(['start'], name, {
      env: Object.assign({}, COMMON_ENV, {
        CONNECTOR_LEDGERS: JSON.stringify(options.credentials),
        CONNECTOR_PAIRS: JSON.stringify(options.pairs),
        CONNECTOR_MAX_HOLD_TIME: 600,
        CONNECTOR_ROUTES: JSON.stringify(options.routes || []),
        CONNECTOR_ROUTE_BROADCAST_ENABLED: options.routeBroadcastEnabled === undefined || options.routeBroadcastEnabled,
        CONNECTOR_ROUTE_BROADCAST_INTERVAL: 10 * 60 * 1000,
        CONNECTOR_ROUTE_EXPIRY: 11 * 60 * 1000, // don't expire routes
        CONNECTOR_AUTOLOAD_PEERS: true,
        CONNECTOR_FX_SPREAD: options.fxSpread || '',
        CONNECTOR_SLIPPAGE: options.slippage || '',
        CONNECTOR_ADMIN_USER: this.adminUser,
        CONNECTOR_ADMIN_PASS: this.adminPass,
        CONNECTOR_BACKEND: options.backend || 'one-to-one',
        CONNECTOR_NOTIFICATION_VERIFY: !!options.notificationKeys,
        CONNECTOR_NOTIFICATION_KEYS: options.notificationKeys ? JSON.stringify(options.notificationKeys) : ''
      }),
      cwd: path.resolve(this.depsDir, 'ilp-connector')
    }, 'connector ready')
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
    }, 'public at')
  }

  startVisualization (port) {
    return this._npm(['start'], 'visualization:' + port, {
      env: Object.assign({}, COMMON_ENV, {
        VISUALIZATION_LEDGERS: JSON.stringify(this.ledgers),
        VISUALIZATION_CONNECTORS: JSON.stringify(this.connectors),
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
      new (this.FiveBellsLedger)(credentials)
    return receiver.connect().then(() => {
      return this.ilp.IPR.listen(receiver, {
        receiverSecret: Buffer.from('secret')
      }, ({ fulfill }) => fulfill())
    })
  }

  /**
   * @param {String} ledger
   * @param {String} name
   * @param {Object} options
   * @param {Amount} options.balance
   * @param {String} options.adminUser
   * @param {String} options.adminPass
   * @param {String} options.connector
   */
  * _updateAccount (ledger, name, options) {
    const db = yield this._getLedgerDb(ledger)
    const password = (yield hashPassword(name)).toString('base64')
    yield db.none( // insert or update account
      'INSERT INTO "L_ACCOUNTS" ("NAME", "PASSWORD_HASH", "BALANCE") ' +
        'VALUES ($/name/, $/password/, $/balance/) ' +
        'ON CONFLICT ("NAME") DO UPDATE ' +
        'SET "PASSWORD_HASH"=$/password/, "BALANCE"=$/balance/',
      { // named parameters
        name: name,
        password: password,
        balance: options.balance || 0
      }
    )
  }

  /**
   * Updates an account specified by parameter name on a ledger specified by parameter ledger.
   * @param  {String} ledger  Ledger prefix identifying the ledger that holds the updated account.
   * @param  {String} name    Name of the updated account.
   * @param  {JSON}   options Used to specify the balance to which the account is updated. Use options.balance.
   * @return {[Promise]}
   */
  updateAccount (ledger, name, options) {
    return co.wrap(this._updateAccount).call(this, ledger, name, options || {})
  }

  * _updateKitAccount (ledgerPrefix, username) {
    const db = yield this._getLedgerDb(ledgerPrefix)
    yield db.none(
      'INSERT INTO "Users" (username, created_at, updated_at) VALUES ($1, $2, $3) ' +
      'ON CONFLICT DO NOTHING',
      [ username, '2017-01-10 16:12:17.039 +00:00', '2017-01-10 16:12:17.039 +00:00' ]
    )
  }

  updateKitAccount (ledgerPrefix, username) {
    return co.wrap(this._updateKitAccount).call(this, ledgerPrefix, username)
  }

  * _getPluginStoreTable (ledgerPrefix) {
    const db = yield this._getLedgerDb(ledgerPrefix)
    try {
      const pluginStore = yield db.one('SELECT table_name ' +
                                       'FROM information_schema.tables ' +
                                       'WHERE table_name ~ \'^plugin_store_\'')
      return pluginStore.table_name
    } catch (e) {
      if (e.code === pgp.errors.queryResultErrorCode.noData) {
        // sometimes the plugin store table does not exist yet,
        // because no transfer has yet been send
        return ''
      }
      throw e
    }
  }

  * _updateTrustlineBalance (ledgerPrefix, balance) {
    const db = yield this._getLedgerDb(ledgerPrefix)
    const pluginStore = yield this._getPluginStoreTable(ledgerPrefix)
    if (pluginStore) {
      yield db.none('UPDATE "' + pluginStore + '" ' +
                   'SET "value"=$/balance/ WHERE "key"=\'balance__\'',
                   { balance })
    }
  }

  updateTrustlineBalance (ledgerPrefix, balance) {
    return co.wrap(this._updateTrustlineBalance)
      .call(this, ledgerPrefix, balance)
  }

  _getDbConnectionString (ledgerPrefix) {
    return 'postgres://' + this.dbUser + '@localhost/' + ledgerPrefix
  }

  * _getLedgerDb (ledgerPrefix) {
    if (!this.dbs[ledgerPrefix]) {
      this.dbs[ledgerPrefix] = pgp({
        database: ledgerPrefix,
        user: this.dbUser
      })
    }
    return this.dbs[ledgerPrefix]
  }

  * _createPostgresDb (dbName) {
    try {
      yield masterdb.none('CREATE DATABASE "' + dbName + '"')
    } catch (e) {
      if (e.code && e.code === '42P04') { // 42P04 = db already exists
        yield masterdb.none('DROP DATABASE "' + dbName + '"')
        yield this._createPostgresDb(dbName)
      } else {
        throw new Error('Could not create test database: ' + e)
      }
    }
  }

  * _getBalance (ledger, name, options) {
    const db = yield this._getLedgerDb(ledger)
    const row = yield db.one('SELECT "BALANCE" FROM "L_ACCOUNTS" WHERE "NAME" = $1', name)
    const balance = parseFloat(row.BALANCE)
    const scale = (typeof this.ledgerOptions[ledger].scale !== 'undefined')
      ? this.ledgerOptions[ledger].scale
      : LEDGER_DEFAULT_SCALE

    return Number(balance.toFixed(scale))
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
    const sourceLedger = parseAddress(params.sourceAccount).ledger
    const destinationLedger = parseAddress(params.destinationAccount).ledger
    const sourceScale = this.ledgerOptions[sourceLedger].scale || LEDGER_DEFAULT_SCALE
    const sourceAmountInteger = (new BigNumber(params.sourceAmount)).shift(sourceScale)

    const sender = new (this.FiveBellsLedger)(clientOpts)
    yield sender.connect()

    const receiver = this.receivers[destinationLedger]

    const quote = yield this.ilp.ILQP.quote(sender, {
      sourceAmount: sourceAmountInteger.toString(),
      destinationAddress: params.destinationAccount
    })

    if (params.overrideMemoParams) {
      params.overrideMemoParams.expiresAt = params.overrideMemoParams.expires_at
    }

    const { packet, condition } = this.ilp.IPR.createPacketAndCondition(
      Object.assign({
        receiverSecret: Buffer.from('secret'),
        destinationAccount: receiver.getAccount(),
        destinationAmount: quote.destinationAmount
      }, params.overrideMemoParams))

    const executionCondition = params.executionCondition || condition

    const result = yield sender.sendTransfer({
      id: uuid(),
      account: quote.connectorAccount,
      amount: sourceAmountInteger.toString(),
      ilp: packet,
      expiresAt: params.unsafeOptimisticTransport ? undefined : quote.expiresAt,
      executionCondition: params.unsafeOptimisticTransport ? undefined : executionCondition
    })

    function cleanup () {
      sender.removeAllListeners()
      return sender.disconnect()
    }

    if (!params.unsafeOptimisticTransport) {
      yield new Promise((resolve, reject) => {
        const handleReject = (transfer, reason) => {
          try {
            if (params.onOutgoingReject) {
              params.onOutgoingReject(transfer, reason)
            }
            resolve()
          } catch (e) {
            cleanup().then(() => reject(e))
          }
        }

        sender.on('outgoing_fulfill', () => cleanup().then(resolve))
        sender.on('outgoing_reject', handleReject)
      })
    }

    yield cleanup()
    return result
  }

  * sendPaymentByDestinationAmount (clientOpts, params) {
    if (params.unsafeOptimisticTransport) {
      throw new Error('ServiceManager#sendPaymentByDestinationAmount doesn\'t support unsafeOptimisticTransport')
    }

    const sender = new (this.FiveBellsLedger)(clientOpts)
    const destinationLedger = parseAddress(params.destinationAccount).ledger
    const destinationScale = this.ledgerOptions[destinationLedger].scale || LEDGER_DEFAULT_SCALE
    const destinationAmountInteger = (new BigNumber(params.destinationAmount)).shift(destinationScale)
    const { packet, condition } = this.ilp.IPR.createPacketAndCondition({
      receiverSecret: Buffer.from('secret'),
      destinationAccount: this.receivers[destinationLedger].getAccount(),
      destinationAmount: destinationAmountInteger.toString()
    })
    const quote = yield this.ilp.ILQP.quoteByPacket(sender, packet)
    const result = yield sender.sendTransfer({
      id: uuid(),
      amount: quote.sourceAmount,
      account: quote.connectorAccount,
      ilp: packet,
      expiresAt: quote.expiresAt,
      executionCondition: condition
    })

    function cleanup () {
      sender.removeAllListeners()
      return sender.disconnect()
    }

    if (!params.unsafeOptimisticTransport) {
      yield new Promise((resolve, reject) => {
        const handleReject = (transfer, reason) => {
          try {
            if (params.onOutgoingReject) {
              params.onOutgoingReject(transfer, reason)
            }
            resolve()
          } catch (e) {
            cleanup().then(() => reject(e))
          }
        }

        sender.on('outgoing_fulfill', () => cleanup().then(resolve))
        sender.on('outgoing_reject', handleReject)
      })
    }

    yield cleanup()
    return result
  }

  * sendRoutes (routes, params) {
    const ledger = params.ledger
    yield request
      .post(this.ledgers[ledger] + '/messages')
      .auth(params.username, params.password)
      .send({
        ledger: this.ledgers[ledger],
        from: this.ledgers[ledger] + '/accounts/' + encodeURIComponent(params.username),
        to: this.ledgers[ledger] + '/accounts/' + encodeURIComponent(params.connectorName),
        data: {
          method: 'broadcast_routes',
          data: routes
        }
      })
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
