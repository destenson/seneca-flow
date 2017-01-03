/* Copyright (c) 2010-2016 Georgi Griffiths, MIT License */
'use strict'

var _ = require('lodash')
var Norma = require('norma')
var Zig = require('zig')
var Fs = require('fs')
var Lrucache = require('lru-cache')
var BSON = require('bson')
var bson = new BSON()
var cache = Lrucache()
var cache_name = process.cwd() + '/flow_cache.bson'
var deepExtend, clean, seneca
var main_options = {
  cache: {
    active: false,
    persist: false
  },
  logger: false,
  timeout: false,
  trace: false
}

function dump_cache (msg) {
  var dump = cache.dump()
  if (main_options.cache.persist) {
    Fs.writeFile(cache_name, bson.serialize(dump), () => {
      process.exit(0)
    })
  }
}

function flow (options) {
  main_options = deepExtend(main_options, options)
  if (main_options.cache.persist) {
    try {
      var cache_data = Fs.readFileSync(cache_name)
      var data = bson.deserialize(cache_data, {
        promoteBuffers: true,
        promoteValues: true
      })
      cache.load(data)
    }
    catch (e) {
      console.log('loadcache', e)
    }
    process.on('SIGINT', dump_cache)
  }
  if (main_options.logger) this.use(require('./flow_logger'))

  return {
    name: 'flow'
  }
}

function preload (plugin) {
  seneca = this


  deepExtend = seneca.util.deepextend
  clean = seneca.util.clean
  this.use('seneca-parambulator')
      .add({ flow: '*' }, flow_start)
      .add({ parallel: '*' }, parallel)
      .add({ sequence: '*' }, sequence)
      .add({ iterate: '*' }, iterate)
      .add({ waterfall: '*' }, waterfall)
      .add({ map: '*' }, map)
      .add({ _: '*' }, lodash)

  this.decorate('flow', start)
  this.decorate('flow_act', flow_act)
  this.decorate('parallel', parallel)
  this.decorate('sequence', sequence)
  this.decorate('iterate', iterate)
  this.decorate('waterfall', waterfall)
  this.decorate('map', map)
  this.decorate('lodash', lodash)


  return {
    name: 'flow',
    exportmap: {
      cache: cache
    }
  }
}

function flow_start (msg, done) {
  msg.flow.parent$ = _.get(msg, 'meta$.id')
  flow_act(_.cloneDeep(msg.flow), done)
}

function lodash (msg, done) {
  if (!msg.in) return done(null, null)
  var res
  var args = [msg.select ? _.get(msg.in, msg.select) : msg.in].concat(msg.args ? (!_.isArray(msg.args) ? [msg.args] : msg.args) : [])
  try {
    res = _[msg._].apply(this, args)
    return done(null, msg.select ? _.set(msg.in, msg.select, res) : res)
  }
  catch (e) {
    return done(e)
  }
}


map.validate = {
  required$: ['map', 'in'],
  map: {
    type$: 'object'
  },
  in: {
    type$: 'array'
  }
}
function map (msg, done) {
  flow_act({
    parent$: _.get(msg, 'meta$.id'),
    iterate: msg.map,
    with: msg.in
  }, done)
}

parallel.validate = {
  required$: ['parallel'],
  parallel: {
    type$: 'array'
  },
  merge: {
    type$: 'boolean'
  }
}
function parallel (msg, done) {
  var items = msg.parallel
  var merge = msg.merge
  var commands = start(done)
  _.each(items, function (item) {
    item.parent$ = _.get(msg, 'meta$.id')
    commands.run(item)
  })
  commands.wait(function (data, done) {
    if (merge) data = deepExtend.apply(this, data)
    done(null, data)
  }).end(done)
}


sequence.validate = {
  required$: ['sequence'],
  sequence: {
    type$: 'array'
  },
  extend: {
    type$: 'object'
  },
  data: {
    type$: 'object'
  },
  merge: {
    type$: 'boolean'
  },
  series: {
    type$: 'boolean'
  },
  results: {
    type$: 'boolean'
  },
  concurrency: {
    type$: 'integer'
  }
}
function sequence (msg, done) {
  var items = msg.sequence
  var merge = msg.merge
  var extend = msg.extend || {}
  var concurrency = msg.concurrency || 10
  var series = _.isUndefined(msg.series) ? true : msg.series
  var item_callback = msg.item_callback
  if (item_callback && !_.isFunction(item_callback)) return done('item_callback should be a function')
  var $ = msg.data || {}
  if (msg.results) $.results = []
  extend.parent$ = _.get(msg, 'meta$.id')
  if (msg.in) $.in = msg.in
  var total = items.length
  var finished = 0
  var processing = 0
  var started = 0
  var errored = false
  var ended = false

  run()

  function finish () {
    ended = true
    delete $.in
    done(null, $)
  }

  function run () {
    if (processing > concurrency) return
    if (finished === total) return finish()
    if (!items.length) return
    var item = deepExtend(items.shift(), extend)
    var index = started
    started++
    processing++
    if (!series) run()
    start(done)
    .step(function pass_data_to_sequence_act (data) {
      return $
    })
    .wait(item)
    .if(!!item_callback)
    .step(function pass_indexed_result (data) {
      return { index: index, event: item, result: data }
    })
    .wait(item_callback)
    .endif()
    .end(function sequence_act_result (err, data) {
      if (data === false && !ended) {
        return finish()
      }
      if (errored || ended) return
      processing--
      finished++
      if (err && item.catch && eval(item.catch)) { // eslint-disable-line
        $.in = {error: err}
        return run()
      }
      if (err) {
        errored = true
        return done(err)
      }
      data = data || null

      if (msg.results) $.results[index] = data
      if (item.key$ && data) {
        if (merge) {
          if (_.isPlainObject(data) && _.isPlainObject($[item.key$])) {
            $[item.key$] = deepExtend($[item.key$], data)
          }
          else if (_.isArray($[item.key$]) && _.isArray(data) && $[item.key$].length === data.length) {
            _.each(data, function (it, i) {
              $[item.key$][i] = deepExtend($[item.key$][i], it)
            })
          }
          else {
            $[item.key$] = data
          }
        }
        else {
          $[item.key$] = data
        }
      }

      var out = data // eslint-disable-line
      if (!item.exit$ ? false : eval(item.exit$)) return finish() // eslint-disable-line
      if (!item.error$ ? false : eval(item.error$)) return done(item.error_message$)  // eslint-disable-line
      $.in = data
      run()
    })
  }
}

iterate.validate = {
  exactlyone$: ['times', 'with'],
  required$: ['iterate'],
  iterate: {
    type$: 'object'
  },
  extend: {
    type$: 'object'
  },
  data: {
    type$: 'object'
  },
  series: {
    type$: 'boolean'
  },
  times: {
    type$: 'integer'
  },
  with: {
    type$: 'array'
  },
  exit: {
    type$: 'string'
  },
  concurrency: {
    type$: 'integer'
  },
  merge: {
    type$: 'boolean'
  },
  merge_key: {
    type$: 'string'
  },
  merge_select: {
    type$: 'string'
  }
}
function iterate (msg, done) {
  var res = msg.merge ? _.cloneDeep(msg.with) : []
  var pos = 0
  var iterator = msg.iterate
  var series = msg.exit ? true : !!msg.series
  var items = msg.with || _.range(0, msg.times)
  var exit = msg.exit
  var extend = msg.extend || {}
  var data = msg.data || {}
  var total = data.count = items.length
  var finished = 0
  var processing = 0
  var concurrency = msg.concurrency || 10
  var errored = false
  var until_success = msg.until_success
  var complete = false
  series = until_success === true ? true : series
  extend.parent$ = _.get(msg, 'meta$.id')
  if (!total) return finish()
  var until
  run()

  function finish () {
    complete = true
    if (until_success && !until) return done(msg.until_success_error)
    if (until) res = until
    done(null, res)
  }

  function run () {
    if (processing > concurrency) return
    if (finished === total) return finish()
    if (!items.length) return
    var item = items.shift()
    var slot = pos
    pos++
    processing++
    if (!series) run()
    var cmd = msg.with ? deepExtend({}, extend, iterator) : deepExtend({}, extend, iterator)
    var $ = msg.with ? deepExtend({in: item, index: slot}, data) : deepExtend({index: slot}, data)
    start(done)
    .step(function pass_data_to_iterate_act () {
      return $
    })
    .wait(cmd)
    .end(function iterate_act_result (err, data) {
      if (errored || complete) return
      processing--
      finished++
      if (err && until_success) return run()
      if (err) {
        errored = true
        return done(err)
      }
      if (msg.merge) {
        if (msg.merge_key) {
          res[slot][msg.merge_key] = msg.merge_select ? _.get(data, msg.merge_select) : data
        }
        else {
          res[slot] = deepExtend(res[slot], data)
        }
      }
      else {
        res[slot] = data
      }
      if (until_success) {
        until = data
        return finish()
      }
      var out = data // eslint-disable-line
      if (!exit ? false : eval(exit)) return finish() // eslint-disable-line
      // eslint-disable-line
      run()
    })
  }
}

waterfall.validate = {
  required$: ['waterfall'],
  waterfall: {
    type$: 'array'
  }
}
function waterfall (msg, done) {
  var items = msg.waterfall

  function run (err, result) {
    if (err) return done(err)
    if (!items.length) return done(null, result)
    var item = items.shift()
    item.parent$ = _.get(msg, 'meta$.id')
    item.in = result
    flow_act(item, run)
  }
  run(null, msg.in)
}


function flow_act (act, previous, done) {
  if (done) act.parent$ = _.get(previous, 'meta$.id')
  if (!done) {
    done = previous
  }
  start(done).wait(act).end(done)
}

function start () {
  var args = Norma('{ errhandler:f? options:o? }', arguments)
  var errhandler = args.errhandler
  var options = deepExtend({}, main_options, args.options)

  var sd = seneca.delegate()

  function make_fn (self, origargs) {
    var args = Norma('actargs:o? fn:f? name:s?', origargs)
    var actargs = args.actargs
    var fn
    if (args.fn) {
      fn = function (data, done) {
        return args.fn.call(self, data, done)
      }
      fn.nm = args.name || args.fn.name
    }
    else {
      fn = function (data, done) {
        _.set(actargs, 'meta$.id', seneca.idgen() + '/' + seneca.idgen())
        var $ = deepExtend({}, clean(actargs), data)
        if (!actargs.if$ ? false : !eval(actargs.if$)) return done() // eslint-disable-line

        start(done)
        .step(function pass_template_data () {
          return {
            source: actargs,
            $: $,
            remove: true
          }
        })
        .wait(exec_actions)
        .step(function pass_act_args (data) {
          return {
            source: actargs,
            $: $
          }
        })
        .wait(exec_actions)
        .step(function () {
          return actargs
        })
        .wait(run_act)
        .if(!!actargs.pause$, 'pause')
        .wait(function do_pause (data, done) {
          setTimeout(function () {
            done(null, data)
          }, actargs.pause$)
        })
        .endif('pause')
        .end(done)
        return true
      }
      fn.nm = JSON.stringify(actargs)
    }
    return fn
  }

  var dzig = Zig({
    timeout: options.timeout,
    trace: options.trace
  })
  dzig.start(function () {
    var self = this
    dzig.end(function () {
      if (errhandler) errhandler.apply(self, arguments)
    })
  })
  sd.end = function (cb) {
    var self = this
    dzig.end(function () {
      if (cb) return cb.apply(self, arguments)
      if (errhandler) return errhandler.apply(self, arguments)
    })
    return self
  }
  sd.wait = function () {
    dzig.wait(make_fn(this, arguments))
    return this
  }
  sd.step = function () {
    dzig.step(make_fn(this, arguments))
    return this
  }
  sd.run = function () {
    dzig.run(make_fn(this, arguments))
    return this
  }
  sd.if = function (cond, name) {
    dzig.if(cond, name)
    return this
  }
  sd.endif = function (name) {
    dzig.endif(name)
    return this
  }
  sd.fire = function () {
    dzig.step(make_fn(this, arguments))
    return this
  }
  return sd
}

function run_act (args, done) {
  var out // eslint-disable-line
  var wait = args.wait$ || 1000
  var data
  var cache_key
  if (main_options.cache.active) {
    cache_key = JSON.stringify(clean(args))
    if (args.cache$) {
      data = cache.get(cache_key)
    }
    if (data) {
      return done(null, data)
    }
  }
  start(done)
  .wait(function seneca_act (data, done) {
    var act
    try {
      act = seneca.has(args)
    }
    catch (e) {}
    if (act) {
      seneca.act(args, done)
    }
    else {
      done(null, clean(args))
    }
  })
  .step(function pass_data_to_format (data) {
    return {
      act: args,
      result: data
    }
  })
  .wait(format_act_result)
  .step(function (data) {
    out = data
    return data
  })
  .if(!!args.until$, 'until')
  .if(function check_until () {
    return !eval(args.until$) // eslint-disable-line
  })
  .wait(function until_wait (data, done) {
    setTimeout(function () {
      done(null, args)
    }, wait)
  })
  .wait(run_act)
  .endif('check_until')
  .endif('until')
  .if(!!args.act_result$, 'act_result')
  .wait(flow_act)
  .endif('act_result')
  .end((err, res) => {
    if (!err && res && main_options.cache.active && args.cache$) {
      cache.set(cache_key, res)
    }
    done(err, res)
  })
}

function run_template (msg) {
  var $ = msg.$
  var k = msg.key
  var v = msg.value
  var key
  if (_.isInteger(k) || _.startsWith(k, msg.prefix)) {
    key = _.isInteger(k) ? k : k.substr(msg.prefix.length)
    if (_.isString(v)) {
      if (v === '$*') {
        set_data(_.cloneDeep($))
      }
      else if (v === '$') {
        set_data($[key])
      }
      else if (~v.indexOf('<%=')) {
        var compiled = _.template(v, {
          variable: '$'
        })
        set_data(compiled($))
      }
      else if (~v.indexOf('$')) {
        set_data(eval(v)) // eslint-disable-line
      }
    }
    else if (_.isObject(v)) {
      set_data(v)
      var source = v
      _.each(source, function (v, k) {
        run_template(_.extend({}, msg, {
          source: source,
          value: v,
          key: k
        }))
      })
    }
  }
  function set_data (d) {
    _.unset(msg.source, k)
    if (!msg.remove) msg.source[key] = d
  }
}

function exec_action (msg, done) {
  run_template(msg)
  if (_.startsWith(msg.key, msg.prefix) && _.isPlainObject(msg.value)) {
    msg.value.parent$ = _.get(msg.source, 'parent$')
    flow_act(msg.value, function (err, result) {
      if (err) return done(err)
      _.unset(msg.source, msg.key)
      var key = msg.key.substr(msg.prefix.length)
      msg.target[key] = result
      done()
    })
  }
  else {
    setTimeout(done)
  }
}

function exec_actions (msg, done) {
  if (!msg.source) return done(null, msg)
  msg.target = msg.remove ? msg.$ : msg.source
  msg.prefix = msg.remove ? '$$' : '$'
  if (!_.isPlainObject(msg.source)) return done(null, msg)
  var execActions = start(done)
  _.each(msg.source, function (v, k) {
    execActions.step(function () {
      return _.extend({}, msg, {
        value: v,
        key: k
      })
    }).run(exec_action)
  })
  execActions.wait(function set_return_data_on_source (data, done) {
    done()
  })
  .end(done)
}

function format_act_result (msg, done) {
  var result = msg.result
  var act = msg.act
  var out = _.isArray(act.out$) ? act.out$ : (_.isPlainObject(act.out$) ? [act.out$] : null)
  if (out) {
    waterfall({
      waterfall: out,
      in: result,
      meta$: {id: _.get(act, 'parent$')}
    }, done)
  }
  else if (act.hasOwnProperty('out$')) {
    if (_.isString(act.out$)) {
      run_template({
        source: act,
        $: result,
        key: 'out$',
        value: act.out$
      })
    }
    done(null, act.out$)
  }
  else {
    done(null, msg.result)
  }
}
module.exports = flow
module.exports.preload = preload
