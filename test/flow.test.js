'use strict'
var Seneca = require('seneca')
var Code = require('code')
var Lab = require('lab')
  // Test shortcuts
var lab = exports.lab = Lab.script()
var describe = lab.describe
var it = lab.it
var before = lab.before
var expect = Code.expect
var testopts = {
  log: 'silent',
  strict: {
    result: false
  }
}
describe('flow', function () {
  var si = Seneca(testopts).use(require('..')).add('cmd:echo', function (msg, done) {
    delete msg.cmd
    done(null, si.util.clean(msg))
  }).add('cmd:echodelayed', function (msg, done) {
    delete msg.cmd
    setTimeout(function () {
      done(null, si.util.clean(msg))
    }, 1)
  }).add('get:xyz', function (msg, done) {
    done(null, {
      x: 1,
      y: 2,
      z: 3
    })
  }).add('get:array', function (msg, done) {
    done(null, [{
      item: 1
    }, {
      item: 2
    }, {
      item: 3
    }, {
      item: 4
    }])
  }).add('get:array2', function (msg, done) {
    done(null, [{
      item: 4
    }, {
      item: 5
    }, {
      item: 6
    }, {
      item: 7
    }])
  }).add('get:array_complex', function (msg, done) {
    done(null, [{
      item: 1,
      ok: false,
      nested: {
        active: true
      }
    }, {
      item: 2,
      ok: true,
      nested: {
        active: true
      }
    }, {
      item: 3,
      ok: false,
      nested: {
        active: false
      }
    }, {
      item: 4,
      ok: false,
      nested: {
        active: true
      }
    }])
  }).add('get:x', function (msg, done) {
    done(null, {
      x: 10
    })
  })
  before(function (done) {
    si.ready(done)
  })
  it('set template data', function (done) {
    var act = {
      cmd: 'echo',
      $$item: {
        get: 'xyz'
      },
      $$hello: {
        get: 'xyz'
      },
      $item: '$',
      $item2: '$.hello.x',
      $item3: 'Hi <%= $.hello.x + $.hello.y %>',
      $item_sum: '$.hello.x * $.hello.y',
      $item_array: ['$.hello.x', '$.hello.y', '$.hello.z'],
      $all: '$*'
    }
    si.flow_act(act, function (err, out) {
      if (err) return done(err)
      expect(out).to.equal({
        item: {
          x: 1,
          y: 2,
          z: 3
        },
        item2: 1,
        item3: 'Hi 3',
        item_sum: 2,
        item_array: [1, 2, 3],
        all: {
          'item': {
            'x': 1,
            'y': 2,
            'z': 3
          },
          'hello': {
            'x': 1,
            'y': 2,
            'z': 3
          }
        }
      })
      done()
    })
  })
  it('act message input data', function (done) {
    var act = {
      cmd: 'echo',
      $item: {
        get: 'xyz'
      }
    }
    si.flow_act(act, function (err, out) {
      if (err) return done(err)
      expect(out).to.equal({
        item: {
          x: 1,
          y: 2,
          z: 3
        }
      })
      done()
    })
  })
  it('use action {flow:*}', function (done) {
    var act = {
      cmd: 'echo',
      $$item: {
        get: 'xyz'
      },
      $item: '$'
    }
    si.act({
      flow: act
    }, function (err, out) {
      if (err) return done(err)
      expect(out).to.equal({
        item: {
          x: 1,
          y: 2,
          z: 3
        }
      })
      done()
    })
  })
  it('parallel', function (done) {
    var act = {
      parallel: [{
        get: 'xyz'
      }, {
        get: 'x'
      }, {
        get: 'xyz'
      }, {
        get: 'x'
      }]
    }
    si.flow_act(act, function (err, out) {
      if (err) return done(err)
      expect(out).to.equal(
        [{
          x: 1,
          y: 2,
          z: 3
        }, {
          x: 10
        }, {
          x: 1,
          y: 2,
          z: 3
        }, {
          x: 10
        }])
      done()
    })
  })
  it('parallel, merge:true', function (done) {
    var act = {
      merge: true,
      parallel: [{
        get: 'xyz'
      }, {
        get: 'x'
      }, {
        get: 'xyz'
      }, {
        get: 'x'
      }]
    }
    si.flow_act(act, function (err, out) {
      if (err) return done(err)
      expect(out).to.equal({
        y: 2,
        z: 3,
        x: 10
      })
      done()
    })
  })
  it('iterate', function (done) {
    var act = {
      times: 5,
      iterate: {
        cmd: 'echo',
        $returnindex: '$.index'
      }
    }
    si.flow_act(act, function (err, out) {
      if (err) return done(err)
      expect(out).to.equal([{
        returnindex: 0
      }, {
        returnindex: 1
      }, {
        returnindex: 2
      }, {
        returnindex: 3
      }, {
        returnindex: 4
      }])
      done()
    })
  })
  it('iterate, exit', function (done) {
    var act = {
      times: 5,
      iterate: {
        cmd: 'echo',
        $returnindex: '$.index'
      },
      exit: 'out.returnindex == 2'
    }
    si.flow_act(act, function (err, out) {
      if (err) return done(err)
      expect(out).to.equal([{
        returnindex: 0
      }, {
        returnindex: 1
      }, {
        returnindex: 2
      }])
      done()
    })
  })
  it('iterate, parallel:true', function (done) {
    var act = {
      parallel: true,
      times: 5,
      iterate: {
        cmd: 'echodelayed',
        $returnindex: '$.index'
      }
    }
    si.flow_act(act, function (err, out) {
      if (err) return done(err)
      expect(out).to.equal([{
        returnindex: 0
      }, {
        returnindex: 1
      }, {
        returnindex: 2
      }, {
        returnindex: 3
      }, {
        returnindex: 4
      }])
      done()
    })
  })
  it('iterate with', function (done) {
    var act = {
      with: [0, 1, 2, 3, 4],
      iterate: {
        cmd: 'echodelayed',
        $returnindex: '$.index'
      }
    }
    si.flow_act(act, function (err, out) {
      if (err) return done(err)
      expect(out).to.equal([
      { in: 0,
        returnindex: 0
      }, { in: 1,
        returnindex: 1
      }, { in: 2,
        returnindex: 2
      }, { in: 3,
        returnindex: 3
      }, { in: 4,
        returnindex: 4
      }])
      done()
    })
  })
  it('sequence', function (done) {
    var act = {
      data: {
        test: true
      },
      sequence: [{
        get: 'xyz',
        key$: 'pos'
      }, {
        get: 'xyz',
        key$: 'pos2',
        out$: {
          _: 'pick',
          args: ['y', 'z']
        }
      }, {
        get: 'xyz',
        key$: 'pos3',
        out$: {
          _: 'get',
          args: 'z'
        }
      }, {
        get: 'xyz',
        key$: 'pos3',
        out$: {
          _: 'get',
          args: 'z'
        }
      }, {
        get: 'xyz',
        if$: '$.pos3 != 3',
        key$: 'pos3',
        out$: {
          _: 'get',
          args: 'z'
        }
      }]
    }
    si.flow_act(act, function (err, out) {
      if (err) return done(err)
      expect(out).to.equal({
        'test': true,
        'pos': {
          'x': 1,
          'y': 2,
          'z': 3
        },
        'pos2': {
          'y': 2,
          'z': 3
        },
        'pos3': null
      })
      done()
    })
  })
  it('sequence with exit', function (done) {
    var act = {
      data: {
        test: true
      },
      sequence: [{
        get: 'xyz',
        key$: 'pos',
        exit$: '$.pos.x == 1 && $.test'
      }, {
        get: 'xyz',
        key$: 'pos2',
        out$: {
          _: 'pick',
          args: ['y']
        }
      }]
    }
    si.flow_act(act, function (err, out) {
      if (err) return done(err)
      expect(out).to.equal({
        test: true,
        pos: {
          x: 1,
          y: 2,
          z: 3
        }
      })
      done()
    })
  })
  it('sequence with nested flows', function (done) {
    var act = {
      data: {
        test: true
      },
      sequence: [{
        parallel: true,
        times: 5,
        iterate: {
          cmd: 'echodelayed',
          $returnindex: '$.index'
        },
        key$: 'sequence_1'
      }, {
        sequence: [{
          get: 'xyz'
        }, {
          get: 'array'
        }, {
          get: 'xyz'
        }],
        key$: 'sequence_2'
      }, {
        get: 'xyz',
        key$: 'pos3',
        out$: {
          _: 'get',
          args: 'z'
        }
      }]
    }
    si.flow_act(act, function (err, out) {
      if (err) return done(err)
      expect(out).to.equal({
        'test': true,
        'sequence_1': [{
          'returnindex': 0
        }, {
          'returnindex': 1
        }, {
          'returnindex': 2
        }, {
          'returnindex': 3
        }, {
          'returnindex': 4
        }],
        'sequence_2': {},
        'pos3': 3
      })
      done()
    })
  })
  it('sequence with array output transform', function (done) {
    var act = {
      sequence: [{
        times: 5,
        iterate: {
          cmd: 'echodelayed',
          $returnindex: '$.index'
        },
        key$: 'sequence_1'
      }, {
        sequence: [{
          get: 'xyz'
        }, {
          get: 'array',
          out$: [{
            map: {
              _: 'padEnd',
              args: [3, '-'],
              select: 'item'
            }
          }, {
            _: 'find',
            args: {
              item: '1--'
            }
          }]
        }, {
          get: 'xyz'
        }],
        key$: 'sequence_2'
      }, {
        get: 'xyz',
        key$: 'pos3',
        out$: {
          _: 'get',
          args: 'z'
        }
      }]
    }
    si.flow_act(act, function (err, out) {
      if (err) return done(err)
      expect(out).to.equal({
        'sequence_1': [{
          'returnindex': 0
        }, {
          'returnindex': 1
        }, {
          'returnindex': 2
        }, {
          'returnindex': 3
        }, {
          'returnindex': 4
        }],
        'sequence_2': {},
        'pos3': 3
      })
      done()
    })
  })
  it('input transform pass through data', function (done) {
    var act = {
      sequence: [{
        get: 'array'
      }, {
        $transformed: {
          $in: '$.in',
          map: {
            _: 'padEnd',
            args: [3, '-'],
            select: 'item'
          }
        },
        key$: 'sequence_1'
      }]
    }
    si.flow_act(act, function (err, out) {
      if (err) return done(err)
      expect(out).to.equal({
        'sequence_1': {
          'transformed': [{
            'item': '1--'
          }, {
            'item': '2--'
          }, {
            'item': '3--'
          }, {
            'item': '4--'
          }]
        }
      })
      done()
    })
  })
  it('out$ output transform complex objects', function (done) {
    var act = {
      get: 'array_complex',
      out$: [{
        _: 'filter',
        args: 'nested.active'
      }]
    }
    si.flow_act(act, function (err, out) {
      if (err) return done(err)
      expect(out).to.equal(
        [{
          'item': 1,
          'ok': false,
          'nested': {
            'active': true
          }
        }, {
          'item': 2,
          'ok': true,
          'nested': {
            'active': true
          }
        }, {
          'item': 4,
          'ok': false,
          'nested': {
            'active': true
          }
        }])
      done()
    })
  })
  it('out$ set direct', function (done) {
    var act = {
      cmd: 'echo',
      out$: 'hello'
    }
    si.flow_act(act, function (err, out) {
      if (err) return done(err)
      expect(out).to.equal('hello')
      done()
    })
  })
  it('merge arrays in sequence', function (done) {
    var act = {
      merge: true,
      sequence: [{
        get: 'array_complex',
        key$: 'to_merge'
      }, {
        get: 'array2',
        key$: 'to_merge'
      }]
    }
    si.flow_act(act, function (err, out) {
      if (err) return done(err)
      expect(out).to.equal({
        'to_merge': [{
          'item': 4,
          'ok': false,
          'nested': {
            'active': true
          }
        }, {
          'item': 5,
          'ok': true,
          'nested': {
            'active': true
          }
        }, {
          'item': 6,
          'ok': false,
          'nested': {
            'active': false
          }
        }, {
          'item': 7,
          'ok': false,
          'nested': {
            'active': true
          }
        }]
      })
      done()
    })
  })
  it('merge objects in sequence', function (done) {
    var act = {
      merge: true,
      sequence: [{
        get: 'xyz',
        key$: 'to_merge'
      }, {
        get: 'x',
        key$: 'to_merge'
      }]
    }
    si.flow_act(act, function (err, out) {
      if (err) return done(err)
      expect(out).to.equal({
        'to_merge': {
          'x': 10,
          'y': 2,
          'z': 3
        }
      })
      done()
    })
  })
  it('No msg.in output transform', function (done) {
    var act = {
      merge: true,
      sequence: [{
        get: 'xyz',
        key$: 'res',
        out$: [{
          _: 'find',
          args: {
            x: 10
          }
        }, {
          _: 'find',
          args: {
            x: 10
          }
        }]
      }]
    }
    si.flow_act(act, function (err, out) {
      if (err) return done(err)
      expect(out).to.equal({
        'res': null
      })
      done()
    })
  })
  it('sequence extend', function (done) {
    var act = {
      extend: {
        user: {
          id: 0,
          name: 'bob'
        }
      },
      sequence: [{
        cmd: 'echo',
        key$: 'a'
      }, {
        cmd: 'echo',
        key$: 'b'
      }, {
        cmd: 'echo',
        key$: 'c'
      }]
    }
    si.flow_act(act, function (err, out) {
      if (err) return done(err)
      expect(out).to.equal({
        'a': {
          user: {
            id: 0,
            name: 'bob'
          }
        },
        'b': {
          user: {
            id: 0,
            name: 'bob'
          }
        },
        'c': {
          user: {
            id: 0,
            name: 'bob'
          }
        }
      })
      done()
    })
  })
  it('check until extend', function (done) {
    var act = {
      cmd: 'increment',
      until$: 'out == 5',
      wait$: 1
    }
    var counter = 0
    si.add('cmd:increment', function (msg, done) {
      counter++
      done(null, counter)
    }).flow_act(act, function (err, out) {
      if (err) return done(err)
      expect(counter).to.equal(5)
      done()
    })
  })
  it('iterate must have times or with', function (done) {
    var act = {
      iterate: {
        cmd: 'echo',
        hello: true
      }
    }
    si.flow_act(act, function (err, out) {
      expect(err.code).to.equal('act_invalid_msg')
      done()
    })
  })
  it('act pause$', function (done) {
    var timer = Date.now()
    var act = {
      cmd: 'echo',
      pause$: 50
    }
    si.flow_act(act, function (err, out) {
      if (err) return done(err)
      var diff = Date.now() - timer
      expect(diff).to.be.above(50)
      done()
    })
  })
  it('excecute nested act', function (done) {
    var act = {
      act_result$: true,
      cmd: 'nested'
    }
    si.add({
      cmd: 'nested'
    }, function (msg, done) {
      done(null, {
        get: 'array'
      })
    }).flow_act(act, function (err, out) {
      if (err) return done(err)
      expect(out).to.equal(
        [{
          item: 1
        }, {
          item: 2
        }, {
          item: 3
        }, {
          item: 4
        }])
      done()
    })
  })
})
