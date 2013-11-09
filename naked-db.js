// Read-fast, transactional in-memory append only log file database.
//
// Designed to be used as a small (tens of megabytes) persistent data structures embedded
// within the application.
//
// ## Features:
//
// - High read speed, same as native objects (because it is native objects).
// - Parallel read requests and sequential write requests.
// - Transactions - all changes applied or none.
// - Isolation - database is consistent for each tick, for multiple ticks use RW
// lock (sort of simple transactional memory).
// - Durability - commited transaction always persisted (even if system suddently crushed).
// - Integrity - data never corrupted, transaction either commited as a whole or
// not (including cases of errors, simultaneous usage ans so on).
//
// ## Performance:
//
// - Append-only file used for persistency, sometimes it should be compacted.
// - Write requests should be sequential (if You try it in parallel it will be rejected).
// - Read speed is important, when there's a choice simplicity vs read performance, choice
// is read performance.
// - Write speed isn't very important, when there's a choice simplicity vs write performance
// choice is simplicity.
// - Data integrity and protection is very important, it's always protected, data can't
// be lost or corrupted with write operations, even by programmer mistake or worng API usage.
// - Data integrity for reader isn't very important, in case of programmer mistake or misuse
// of API wrong data may be readed.
//
// ## TODOs
//
// - add logging.
// - add compaction and auto-compaction (when size of changes > 80% of db or time of loading
// changes twice as bigger than time of loading db).
// - add indexes.
// - add REST interface (with multi-tenancy).

var _   = require('underscore')
var fs2 = require('./fs2')
var tm  = require('transactional-memory')

// Helpers.
var fork    = fs2.fork
var inspect = _(JSON.stringify).bind(JSON)

// # Database.
var Db = function(){this.initialize.apply(this, arguments)}
module.exports = Db

// Load database from file.
Db._load = function(filePath, cb){
  var data = null
  fs2.readFileSplittedBy(filePath, /\n\n/g, function(line){
    var content = null
    // Bad JSON means transaction hasn't been fully persisted, ingoring it.
    try{content = JSON.parse(line)}catch(err){}
    if(content){
      // In a db file the first line is the database, next are transactions.
      if(!data) data = content
      else
        try{tm.lowLevel.update(data, content)}catch(err){return cb(err)}
    }
  }, fork(cb, function(){cb(null, data)}))
}

// data oriented approach to work with Db.
Db.prototype = {
  // Initialize database with path to file.
  //   new Db('/my-db.json')
  initialize: function(filePath, defaultData, options){
    this.filePath = filePath
    this.options  = options || {}
    this.data     = defaultData || {}
  },

  // Guard aginst simultaneous usage.
  lock: function(finishCb, cb){
    var that = this

    if(that._updating)
      return originalCb(new Error("database '" + that.filePath + "' is locked!"))
    that.updating = true

    cb(function(){
      that._updating = false
      finishCb.apply(null, arguments)
    })
  },

  // Load database from file.
  load: function(cb){
    var that = this
    this.lock(cb, function(cb){
      that.log('loading...')
      // Loading.
      Db._load(that.filePath, fork(cb, function(data){
        that.data = data
        that.log('loaded')
        cb()
      }))
    })
  },

  // Save transaction to disk and update data.
  update: function(transaction, cb){
    var that = this
    for(var i = 0; i < transaction.length; i++)
      that.log('updating ' + inspect(transaction[i]))
    that.lock(cb, function(cb){
      that._ensureFileWithInitialDataExist(fork(cb, function(){
        // Updating data.
        var llTransaction = null
        try{
          llTransaction = tm.update(that.data, transaction)
        }catch(err){return cb(err)}

        // Saving.
        that._saveTransaction(llTransaction, fork(cb, function(){
          that.log('updated')
          cb(null, llTransaction)
        }))
      }))
    })
  },

  save: function(cb){
    this._ensureFileWithInitialDataExist(cb)
  },

  // Save transaction to disk.
  _saveTransaction: function(transaction, cb){
    if(transaction.length == 0) return cb()
    // Appending transaction to log file.
    var content = this.options.pretty ? tm.toPrettyJson(transaction) : JSON.stringify(transaction)
    fs2.appendFile(this.filePath, "\n\n" + content, cb)
  },

  _ensureFileWithInitialDataExist: function(cb){
    var that = this
    that.log('saving initial data...')
    if(that._fileExistenceChecked) return cb()
    else{
      fs2.exists(that.filePath, fork(cb, function(exists){
        if(exists){
          that._fileExistenceChecked = true
          cb()
        }else{
          // Writing initial data.
          var content = JSON.stringify(that.data)
          fs2.writeFile(that.filePath, content, fork(cb, function(){
            that._fileExistenceChecked = true
            that.log('initial data saved')
            cb()
          }))
        }
      }))
    }
  },

  // Override with Your implementation.
  log: function(msg){}
}