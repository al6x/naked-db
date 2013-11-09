var Db   = require('../naked-db')
var sync = require('synchronize')

sync(Db.prototype, 'load', 'save', 'update')

module.exports = Db