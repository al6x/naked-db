global.expect = require('chai').expect
global.p      = console.log.bind(console)

var _    = require('underscore')
var Db   = require('../naked-db')
var fs2  = require('../fs2')
var fork = fs2.fork

describe("Naked DB", function(){
  it("should update", function(next){
    fs2.getTemporarryDirectory(fork(next, function(tmpDir){
      var fname = tmpDir + '/db.json'
      var db = new Db(fname, {})
      // Updating.
      db.update([['set', 'version', 2]], fork(next, function(){
        // Checking data.
        expect(db.data).to.eql({version: 2})
        next()
      }))
    }))
  })

  it("should load", function(next){
    fs2.getTemporarryDirectory(fork(next, function(tmpDir){
      var data = [
        {},
        ['hSet', 2, 'version', 2]
      ].map(function(chunk){return JSON.stringify(chunk)}).join("\n\n")
      var fpath = tmpDir + '/db.json'
      fs2.writeFile(fpath, data, fork(next, function(){
        var db = new Db(fpath)
        db.load(fork(next, function(){
          expect(db.data).to.eql({version: 2})
          next()
        }))
      }))
    }))
  })

  it("should save transaction to file", function(next){
    fs2.getTemporarryDirectory(fork(next, function(tmpDir){
      var fpath = tmpDir + '/db.json'
      var db = new Db(fpath, {})
      db.update([['set', 'version', 2]], fork(next, function(){
        fs2.readFile(fpath, 'utf8', fork(next, function(content){
          var data = JSON.parse('[' + content.split("\n\n").join(', ') + ']')
          expect(data).to.eql([
            {},
            ['hSet', 2, 'version', 2]
          ])
          next()
        }))
      }))
    }))
  })

  it("should append transaction to file", function(next){
    fs2.getTemporarryDirectory(fork(next, function(tmpDir){
      var fpath = tmpDir + '/db.json'
      var db = new Db(fpath, {})
      db.update([['set', 'version', 2]], fork(next, function(){
        db.update([['set', 'version', 3]], fork(next, function(){
          fs2.readFile(fpath, 'utf8', fork(next, function(content){
            var data = JSON.parse('[' + content.split("\n\n").join(', ') + ']')
            expect(data).to.eql([
              {},
              ['hSet', 2, 'version', 2],
              ['hSet', 2, 'version', 3],
            ])
            next()
          }))
        }))
      }))
    }))
  })

  it("should rollback all changes if one is invalid", function(next){
    fs2.getTemporarryDirectory(fork(next, function(tmpDir){
      var fpath = tmpDir + '/db.json'
      var db = new Db(fpath, {})
      var transaction = [
        ['set', 'a', 'A'],
        ['set', 'b', 'B'],
        ['invalidCommand']
      ]
      db.update(transaction, function(err){
        expect(err).to.have.property('message').to.match(/no.*invalidCommand.*operatio/)
        expect(db.data).to.eql({})
        fs2.readFile(fpath, 'utf8', fork(next, function(content){
          var data = JSON.parse('[' + content.split("\n\n").join(', ') + ']')
          expect(data).to.eql([{}])
          next()
        }))
      })
    }))
  })

  it("should compact to other file")

  it("should compact to the same file")
})