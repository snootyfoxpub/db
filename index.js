const adapter = require('./lib/adapter');
const dataset = require('./lib/dataset');
const migrate = require('./lib/migrate');

const {
  assignments,
  conditions,
  disconnect,
  escape,
  escapeId,
  execute,
  executeAndAffected,
  insertWithId,
  pluck,
  selectRow,
  selectRows,
  selectValue,
  transaction
} = adapter;

module.exports = {
  assignments,
  conditions,
  dataset,
  disconnect,
  escape,
  escapeId,
  execute,
  executeAndAffected,
  insertWithId,
  migrate,
  pluck,
  selectRow,
  selectRows,
  selectValue,
  transaction
};
