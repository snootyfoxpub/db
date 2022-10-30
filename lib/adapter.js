const mysql = require('mysql2/promise');
const logger = require('debug');

const { env } = require('@snooty/utils');

const { escape, escapeId } = mysql;

const debug = logger('db:sql');

const CONDITION_MODIFIERS = {
  'ne': '<>',
  'gt': '>',
  'lt': '<',
  'gte': '>=',
  'lte': '<='
};

module.exports = {
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
};

// Moved below exports to avoid circular require
const dataset = require('./dataset');

let pool = null;

/** @private */
function instantiatePool() {
  if (!pool) pool = mysql.createPool(env('DB'));

  return pool;
}

/** TODO: write doc comment */
async function execute(query, conn) {
  if (!conn) {
    instantiatePool();
    conn = pool;
  }
  const logger = conn.logger || debug;
  logger(query);

  return await conn.query(query);
}

/** TODO: write doc comment */
async function executeAndAffected(query, conn) {
  const [{ affectedRows }] = await execute(query, conn);

  return affectedRows;
}

/** TODO: write doc comment */
async function insertWithId(query, conn) {
  const [{ insertId }] = await execute(query, conn);

  return insertId;
}

/** TODO: write doc comment */
async function disconnect() {
  if (pool) await pool.end();
}

/** TODO: write doc comment */
async function pluck(query, conn) {
  const [rows, [{ name: firstCol }]] = await execute(query, conn);

  return rows.map(row => row[firstCol]);
}

/** TODO: write doc comment */
async function selectRow(query, conn) {
  const [row] = await selectRows(query, conn);

  return row;
}

/** TODO: write doc comment */
async function selectRows(query, conn) {
  const [rows] = await execute(query, conn);

  return rows;
}

/** TODO: write doc comment */
async function selectValue(query, conn) {
  const row = await selectRow(query, conn);
  if (!row) return null;

  return Object.values(row)[0];
}

/** TODO: write doc comment */
/* TODO: extract into separate file to avoid circular dependency with dataset */
async function transaction(perform) {
  instantiatePool();

  const conn = await pool.getConnection();
  conn.logger = debug.extend(`:tx@${conn.connection.connectionId}`);
  const handle = wrap(conn);

  try {
    await conn.beginTransaction();
    const result = await perform(handle);
    await conn.commit();

    return result;
  } catch (e) {
    await conn.rollback();

    throw e;
  } finally {
    delete conn.logger;
    conn.release();
  }

  function wrap(connection) {
    return {
      conditions,
      connection,
      dataset: tableName => dataset(tableName, connection),
      escape,
      escapeId,
      execute: q => execute(q, connection),
      executeAndAffected: q => executeAndAffected(q, connection),
      insertWithId: q => insertWithId(q, connection),
      pluck: q => pluck(q, connection),
      selectRow: q => selectRow(q, connection),
      selectRows: q => selectRows(q, connection),
      selectValue: q => selectValue(q, connection)
    };
  }
}

/** TODO: write doc comment */
function assignments(changes) {
  if (typeof changes === 'string') return changes;

  return Object.entries(changes).
    map(([col, value]) => `${escapeId(col)} = ${escape(value)}`).
    join(', ');
}

/** TODO: write doc comment */
function conditions(criteria) {
  if (typeof criteria === 'string') {
    return criteria;
  } else if (Array.isArray(criteria)) {
    return `(${criteria.map(conditions).join(') OR (')})`;
  }

  // XXX: no support for multiple alternatives at top level criteria yet
  const terms = [];

  Object.entries(criteria).map(([col, value]) => terms.push(term(col, value)));

  return terms.join(' AND ');

  function term(column, value) {
    const parts = column.split('.');
    let comparator = CONDITION_MODIFIERS[parts[parts.length - 1]];

    if (comparator) parts.pop(); else comparator = '=';

    column = escapeId(parts.join('.'));
    if (value === undefined) throw new Error('Can\'t filter by undefined');

    if (value === null)
      return `${column} IS ${comparator === '<>' ? 'NOT ': ''}NULL`;

    if (Array.isArray(value)) return inList(column, value);

    if (typeof value === 'string' && value.includes('%')) comparator = 'LIKE';

    return `${column} ${comparator} ${escape(value)}`;
  }

  function inList(column, value) {
    // XXX: Column is expected to be already escaped
    // XXX: No support for NULL checks

    // For empty set return predicate that is always false
    if (!value.length) return escape(false);

    const list = value.map(escape).join(', ');

    return `${column} IN (${list})`;
  }
}
