/* Copyright (c) 2010-2016 Georgi Griffiths, MIT License */
'use strict'


// External modules.
var _ = require('lodash')
var Norma = require('norma')
var Zig = require('zig')
var parambulator = require('parambulator')
var deepExtend, clean, seneca;

var main_options =  {
    timeout: false,
    trace: false
  }

function flow(options) {

  main_options = deepExtend(main_options, options);

  return {
    name: 'flow'
  }
}

function lodash(msg, done) {

  if (!msg.in) return done(null, null);

  var res, err
  var args = [msg.select ? _.get(msg.in, msg.select) : msg.in].concat(msg.args ? (!_.isArray(msg.args) ? [msg.args] : msg.args) : []);

  try {
    res = _[msg._].apply(this, args)
    return done(null, msg.select ? _.set(msg.in, msg.select, res) : res );
  } catch(e) {
    return done(e)
  }

}

function each(msg, done) {

  iterate({iterate:msg.each, with:msg.in}, done)

}

var parallel_validator = parambulator({
  parallel:   { type$:'array'   },
  merge:      { type$:'boolean' }
})

function parallel(msg, done) {

  parallel_validator.validate(msg, function (err) {
    var items = msg.parallel;
    var merge = msg.merge;

    var commands = start()

    _.each(items, function (item) {
      commands.run(item)
    })

    commands.wait(function (data, done) {
      if (merge) data = deepExtend.apply(this, data);
      done(null, data)
    })
    .end(done)
  })

}

var sequence_validator = parambulator({
  required$:  ['sequence'],
  sequence:   { type$:'array'   },
  extend:     { type$:'object'  },
  data:       { type$:'object'  },
  merge:      { type$:'boolean' },
  exit:       { type$:'string'  }
})

function sequence(msg, done) {

  sequence_validator.validate(msg, function (err) {

    if (err) return done(err);

    var items = msg.sequence
    var merge = msg.merge
    var extend = msg.extend || {}
    var $ = msg.data ? _.cloneDeep(msg.data) : {}
    if (msg.in) $.in = msg.in;

    function finish() {
      delete $.in
      done(null, $);
    }

    function run() {

      if (!items.length) {
        finish()
      } else {

        var item = deepExtend(items.shift(), extend);

        start()
        .step(function pass_data_to_sequence_act(data) {
          return $;
        })
        .wait(item)
        .end(function sequence_act_result(err, data) {

          data = data || null;

          if (item.key$) {
            if ( merge ) {
              if (_.isPlainObject(data) && _.isPlainObject($[item.key$])) {
                $[item.key$] = deepExtend($[item.key$], data);
              } else if(_.isArray($[item.key$]) && _.isArray(data) && $[item.key$].length == data.length) {
                _.each(data, function (it, i) {
                  $[item.key$][i] = deepExtend($[item.key$][i], it);
                })
              } else {
                $[item.key$] = data;
              }
            } else {
              $[item.key$] = data;
            }
          }
          var out = data;
          if (!item.exit$ ? false : eval(item.exit$)) return finish()
          $.in = data;
          run();
        })
      }
    }
    run();

  })

}

var iterate_validator = parambulator({
  exactlyone$:   ['times', 'with'],
  required$:    ['iterate'],
  iterate:  { type$:'object'  },
  extend:   { type$:'object'  },
  data:     { type$:'object'  },
  series:   { type$:'boolean' },
  times:    { type$:'integer' },
  with:     { type$:'array'   },
  exit:     { type$:'string'  }
})

function iterate(msg, done) {

  iterate_validator.validate(msg, function (err) {

    if(err) return done(err)

    var res = [];
    var pos = 0;
    var iterator = msg.iterate;
    var series = msg.exit ? true : !!msg.series;
    var items = msg.with || _.range(0,msg.times);
    var exit =  msg.exit
    var extend = msg.extend || {};
    var data = msg.data ? _.cloneDeep(msg.data) : {}
    var total = data.count = items.length;
    var finished = 0;

    if (!total) return finish();
    run();

    function finish() {
      done(null, res);
    }

    function run() {

      if (items.length) {

        var item = items.shift();
        var slot = pos;
        pos ++;
        var cmd = msg.with ? deepExtend({},extend,{in:item},iterator) : deepExtend({},extend,iterator);
        var $ = msg.with ? deepExtend({in:item,index:slot},data) : deepExtend({index:slot},data)

        start()
        .step(function pass_data_to_iterate_act() {
          return $;
        })
        .wait(cmd)
        .end(function iterate_act_result(err, data) {

          finished ++;
          //_.unset(data, 'in');
          res[slot] = data;
          var out = data;

          if(!exit ? false : eval(exit)) return finish()
          if (finished == total) return finish();
          if (series) run();

        })

        if (!series) run();
      }

    }

  })

}

var waterfall_validator = parambulator({
  required$: ['waterfall'],
  waterfall:   { type$:'array' }
})

function waterfall (msg, done) {

  waterfall_validator.validate(msg, function (err) {

    if(err) return done(err);

    var items = msg.waterfall;

    function run(err,result) {

      if (err) return done(err)
      if (!items.length) return done(null, result)
      var item = items.shift();
      item.in = result;
      flow_act(item, run)
    }

    run(null,msg.in)
  })

}



function preload () {

  seneca = this;

  deepExtend = seneca.util.deepextend;
  clean = seneca.util.clean;

  this.add({flow:'*'}, {
        flow: {
          required$: true,
          object$: true
        }
      }, function (msg, done) {
        flow_act(msg.flow, done);
      })
      .add({parallel:'*'},parallel)
      .add({sequence:'*'},sequence)
      .add({iterate:'*'},iterate)
      .add({waterfall:'*'},waterfall)
      .add({each:'*'},each)
      .add({_:'*'},lodash)


  this.decorate('flow', start);
  this.decorate('flow_act', flow_act)
  this.decorate('parallel',parallel)
  this.decorate('sequence:',sequence)
  this.decorate('iterate',iterate)
  this.decorate('waterfall',waterfall)
  this.decorate('each',each)
  this.decorate('lodash',lodash)

}

function flow_act(act, done){
  start().wait(act).end(done);
}

function start() {

  var args = Norma('{ errhandler:f? options:o? }', arguments)
  var errhandler = args.errhandler;
  var options = deepExtend({},main_options, args.options);

  var sd = {};

  function make_fn(self, origargs) {

    var args = Norma('actargs:o? fn:f? name:s?', origargs)

    var actargs = args.actargs;

    var fn
    if (args.fn) {
      fn = function (data, done) {
        return args.fn.call(self, data, done)
      }
      fn.nm = args.name || args.fn.name;
    } else {
      fn = function (data, done) {

        var $ = data || {};

        if (!actargs.if$ ? false : !eval(actargs.if$)) return done();

        var actions = start(options)
          .step(function pass_template_data() {
            return { source: actargs, $: $, options:options, remove:true }
          })
          .wait(exec_actions)
          .step(function pass_act_args(data) {
            return { source: actargs, $: $, options:options };
          })
          .wait(exec_actions)
          .step(function () {
            return actargs;
          })
          .wait(run_act)
          .if(!!actargs.pause$, 'pause')
          .wait(function do_pause(data, done) {
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

  return sd;
}


function run_act(args, done) {

  var out;
  var wait = args.wait$ || 1000;

  start()

    .wait(function seneca_act(data, done) {

      var act;
      try{
        act = seneca.has(args)
      } catch (e){
      }
      if (act) {
        seneca.act(args, done)
      } else {
        done(null, clean(args))
      }

    })

    .step(function pass_data_to_format(data) {
      return { act: args, result: data }
    })
    .wait(format_act_result)
    .step(function(data) {
        out = data;
        return data;
    })
    .if(!!args.until$, 'until')
    .if(function check_until() {
      return !eval(args.until$)
    })
    .wait(function until_wait(data, done) {
      setTimeout(function () {
        done(null, args)
      }, wait)
    })
    .wait(run_act)
    .endif('check_until')
    .endif('until')
    .end(done)


}

function run_template(msg) {

  var $ = msg.$;
  var k = msg.key;
  var v = msg.value;

  if (_.isInteger(k) || _.startsWith(k, msg.prefix)) {

    var key = _.isString(k) ? k.substr(msg.prefix.length) : k;

    if (_.isString(v)) {

      if (v == '$*') {
        set_data(_.cloneDeep($));
      } else if (v == '$') {
        set_data($[key]);
      } else if (~v.indexOf('<%=')) {
        var compiled = _.template(v, {
          variable: '$'
        });
        set_data(compiled($));
      } else if (~v.indexOf('$')) {
        set_data(eval(v));
      }

    } else if (_.isObject(v)) {

      set_data(v);
      var source = v;
      _.each(source, function (v, k) {
        run_template( _.extend({}, msg, {source:source, value:v, key:k}) )
      })
    }

    function set_data(d) {
      _.unset(msg.source, k);
      if (!msg.remove) msg.source[key] = d;
    }

  }




}

function exec_action(msg, done) {

  run_template(msg)

  if (_.startsWith(msg.key, msg.prefix) && _.isPlainObject(msg.value)) {

    flow_act(msg.value, function (err, result) {

      _.unset(msg.source, msg.key);

      var key = msg.key.substr(msg.prefix.length)
      msg.target[key] = result;

      if (_.isPlainObject(result)){
        exec_actions(_.extend({},msg,{source:result}), done)
      } else {
        done();
      }

    })

  } else {
    setTimeout(done);
  }


}

function exec_actions(msg, done) {

  if (!msg.source) return done(null, msg);
  msg.target = msg.remove ? msg.$ : msg.source;
  msg.prefix = msg.remove ? '$$' : '$';

  if (!_.isPlainObject(msg.source)) return done(null, msg);

  var execActions = start()

  _.each(msg.source, function (v, k) {

    execActions
      .step(function () {
        return _.extend({},msg,{value:v, key:k })
      })
      .run(exec_action)

  })

  execActions
    .wait(function set_return_data_on_source(data, done) {
      done();
    })
    .end(done);

}

function format_act_result(msg, done) {

  var result = msg.result
  var act = msg.act
  var out = _.isArray(act.out$) ? act.out$ : (_.isPlainObject(act.out$) ? [act.out$] : null);

  if (out) {
    waterfall({
      waterfall:out,
      in:result
    }, done)
  } else if (act.hasOwnProperty('out$')) {
    if (_.isString(act.out$)) run_template({source:act, $:result, key:'out$', value:act.out$})
    done(null, act.out$)
  } else {
    done(null,msg.result)
  }

}


module.exports = flow;
module.exports.preload = preload;
