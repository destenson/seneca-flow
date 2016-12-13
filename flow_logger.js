/* Copyright (c) 2016 Richard Rodger and other contributors, MIT License */
'use strict'
var _ = require('lodash')
require('colors')

var actions = {}

module.exports = flow_logger

function flow_logger (options) {
  // Everything is in preload as logging plugins are
  // a special case that need to be loaded asap.
}

flow_logger.preload = function () {
  var logger = function (seneca, data) {
    if (data.kind === 'act') {
      var item = _.cloneDeep(data)

      if (data.case === 'IN') {
        item.depth = actions[data.msg.parent$] && _.isInteger(actions[data.msg.parent$].depth) ? actions[data.msg.parent$].depth + 1 : 0
        actions[data.msg.meta$.id] = item
        print_log(data)
      }
      if (data.case === 'OUT') {
        delete actions[data.msg.meta$.id]
        print_log(data)
      }
    }

    function print_log (data) {
      var parent = actions[data.msg.parent$]
      var depth = parent && parent.depth ? parent.depth + 1 : 0
      console.log(' ')
      if (data.case === 'IN') {
        if (parent) {
          if (parent.msg.sequence) console_pad('SEQUENCE ITEM: ')
          if (parent.msg.iterate) console_pad('ITERATE ITEM: ')
          console_pad('Parent:' + data.msg.parent$)
        }

        if (data.msg.sequence) console_pad('SEQUENCE START: ')
        if (data.msg.iterate) console_pad('ITERATE START: ')
      }

      console_pad(data.case + '  ACTID:' + data.actid + '  ID:' + data.msg.meta$.id)

      _.each(seneca.util.clean(data.msg), function (v, k) {
        v = _.isObject(v) ? JSON.stringify(v) : v
        console_pad(k + ' : ' + v)
      })

      if (data.case === 'OUT') {
        var res = (data.result instanceof Buffer) ? 'BUFFER' : _.isObject(data.result) ? JSON.stringify(data.result, null, 2) : data.result
        console_pad(res)
      }

      function console_pad (msg) {
        var colors = ['black', 'red', 'green', 'yellow', 'blue', 'magenta', 'cyan']
        console.log((_.repeat('   ', depth) + msg)[colors[depth % 6]])
      }
    }
  }

  return {
    extend: {
      logger: logger
    }
  }
}
