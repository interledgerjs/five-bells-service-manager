'use strict'
const childProcess = require('child_process')
const byline = require('byline')
const through2 = require('through2')

/**
 * Utility function for spawning processes.
 *
 * @return {Promise<ChildProcess>} Promise of the exit code of the process.
 */
function spawnParallel (cmd, args, opts, formatter) {
  const proc = childProcess.spawn(cmd, args,
    Object.assign({}, opts, { stdio: 'pipe' }))

  // Add prefix to output to distinguish processes
  if (typeof formatter === 'function') {
    const stdoutStream = byline(proc.stdout).pipe(through2(formatter))
    const stderrStream = byline(proc.stderr).pipe(through2(formatter))

    // Increase event listener limit to avoid memory leak warning
    process.stdout.setMaxListeners(process.stdout.getMaxListeners() + 1)
    process.stderr.setMaxListeners(process.stderr.getMaxListeners() + 1)

    if (opts.waitFor) {
      stdoutStream.pipe(through2(function (line, enc, callback) {
        this.push(line)
        if (line.toString('utf-8').indexOf(opts.waitFor.trigger) !== -1) {
          opts.waitFor.callback()
        }
        callback()
      })).pipe(process.stdout)
    } else {
      stdoutStream.pipe(process.stdout)
    }
    stderrStream.pipe(process.stderr)

    proc.on('exit', () => {
      // Disconnect pipes
      stdoutStream.unpipe(process.stdout)
      stderrStream.unpipe(process.stderr)

      // Return to previous event emitter limit
      process.stdout.setMaxListeners(process.stdout.getMaxListeners() - 1)
      process.stderr.setMaxListeners(process.stderr.getMaxListeners() - 1)
    })
  } else {
    proc.stdout.on('data', (data) => process.stdout.write(data.toString()))
    proc.stderr.on('data', (data) => process.stderr.write(data.toString()))
  }

  // When a process dies, we should abort
  proc.on('exit', (code) => {
    if (code) {
      console.error('child exited with code ' + code)
      process.exit(1)
    }
  })

  return proc
}

module.exports = {
  spawnParallel
}
