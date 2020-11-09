// TODO: destructure used methods from adapter into local consts
const db = require('./adapter');
const logger = require('debug');

module.exports = dataset;

// Limit to use when offset is given and limit is not set
const MAX_LIMIT = 0xFFFF_FFFF_FFFF_FFFF;

/** TODO: write doc comment */
class Dataset {
  /** TODO: write doc comment */
  constructor(name, conn = null) {
    this.tableName = name;
    this.quotedName = db.escapeId(name);
    this.conn = conn;
    this.criteria = null;
    this.sortings = [];
    this.selection = '*';

    this.logger = logger(`db:dataset:${name}`);
    if (conn) // we're in transaction
      this.logger =
        this.logger.extend(`tx@${conn.connection.connectionId}`);
  }

  /** TODO: write doc comment */
  async all(conditions) {
    // TODO: add support for string conditions

    let sql = `SELECT ${this.selection} FROM ${this.quotedName}`;
    sql += this.whereSql(conditions);
    sql += this.orderSql;
    sql += this.limitOffsetSql;

    this.logger(sql);
    return await db.selectRows(sql, this.conn);
  }

  /** TODO: write doc comment */
  async create(record) {
    const cols = [];
    const vals = [];

    Object.entries(record).
      forEach(([col, val]) => {
        cols.push(db.escapeId(col));
        vals.push(db.escape(val));
      });

    const sql = `
      INSERT INTO ${this.quotedName} (${cols.join(', ')})
      VALUES (${vals.join(', ')})
    `;

    const debug = this.logger.extend('create');
    debug(sql);

    const id = await db.insertWithId(sql, this.conn);
    debug(`DB returned id ${id}`);
    // FIXME: hardcoded `id` column
    // Also, this.get may fail due to inserting the record
    // that does not match the filters
    if (id) record = await this.get({ id });

    debug('-> %O', record);

    return record;
  }

  /** TODO: write doc comment */
  async delete(conditions) {
    let sql = `DELETE FROM ${this.quotedName}`;

    sql += this.whereSql(conditions);
    sql += this.orderSql;
    sql += this.limitOffsetSql;

    return await this.executeAndAffected(sql);
  }

  /** TODO: write doc comment */
  async get(conditions) {
    // FIXME: perhaps use this.limit(1) somehow?
    let sql = `SELECT ${this.selection} FROM ${this.quotedName}`;

    sql += this.whereSql(conditions);
    sql += this.orderSql;
    sql += ' LIMIT 1';

    if (this.startRow) sql += ` OFFSET ${db.escape(this.startRow)}`;

    this.logger(sql);
    return await db.selectRow(sql, this.conn);
  }

  /** TODO: write doc comment */
  async count(what) {
    let sql = `SELECT count(${what || this.selection}) FROM ${this.quotedName}`;

    sql += this.whereSql();

    this.logger.extend('count')(sql);
    return await db.selectValue(sql);
  }

  /** TODO: write doc comment */
  async pluck(column, conditions) {
    let sql = `SELECT ${db.escapeId(column)} FROM ${this.quotedName}`;

    sql += this.whereSql(conditions);
    sql += this.orderSql;
    sql += this.limitOffsetSql;

    this.logger.extend('pluck')(sql);
    return await db.pluck(sql, this.conn);
  }

  /** TODO: write doc comment */
  async update(assignments, conditions) {
    let sql = `UPDATE ${this.quotedName} SET ${db.assignments(assignments)}`;

    sql += this.whereSql(conditions);
    sql += this.orderSql;
    sql += this.limitOffsetSql;

    return await this.executeAndAffected(sql);
  }

  /** TODO: write doc comment */
  restrictCriteria(additional) {
    let existing = this.criteria;

    if (!isPresent(additional)) return existing;
    if (!isPresent(existing)) return additional;

    const existingIsString = typeof existing === 'string';
    const additionalIsString = typeof additional === 'string';

    if (!(existingIsString || additionalIsString))
      // This way to disallow additional to loosen existing criteria
      return merge(additional, existing);

    if (!existingIsString) existing = db.conditions(existing);
    if (!additionalIsString) additional = db.conditions(additional);

    return `(${existing}) AND (${additional})`;

    function merge(one, other) {
      const res = { ...one };

      const otherKeys = Object.keys(other);
      for (let i = 0, length = otherKeys.length; i < length; i++) {
        const col = otherKeys[i];
        let val = other[col];

        if (! (col in res)) {
          res[col] = val;

          continue;
        }

        const theirs = res[col];
        const theirsArray = Array.isArray(theirs);
        const valArray = Array.isArray(val);

        if (!(theirsArray || valArray)) {
          if (theirs !== val) return '1 = 0';
        } else {
          if (!valArray) {
            if (!theirs.includes(val)) return '1 = 0';

            res[col] = val;
          } else if (!theirsArray) {
            if (!val.includes(theirs)) return '1 = 0';
            // res[col] is already scalar of `theirs`
          } else {
            const theirsSet = new Set(...theirs);

            val = val.filter(v => theirsSet.has(v));

            if (!val.length) return '1 = 0'; // No intersection

            res[col] = val;
          }
        }
      }
      return res;
    }
  }

  /** @private */
  clone(changes = {}) {
    const copy = new this.constructor(this.tableName, this.conn);

    Object.assign(copy, {
      criteria: this.criteria,
      sortings: this.sortings,
      maxRows: this.maxRows,
      startRow: this.startRow,
      selection: this.selection,
      ...changes
    });

    return copy;
  }

  /** TODO: write doc comment */
  select(str) {
    return this.clone({ selection: str });
  }

  /** TODO: write doc comment */
  order(...cols) {
    return this.clone({ sortings: [...this.sortings, ...cols] });
  }

  /** TODO: write doc comment */
  where(criteria) {
    return this.clone({ criteria: this.restrictCriteria(criteria) });
  }

  /** TODO: write doc comment */
  limit(limit, offset) {
    const changes = { maxRows: limit };
    if (offset) changes.startRow = offset;

    return this.clone(changes);
  }

  /** TODO: write doc comment */
  offset(offset) {
    return this.limit(this.maxRows, offset);
  }

  /** @private */
  get orderSql() {
    // FIXME: sanitize input
    // TODO: memoize
    if (this.sortings.length) return ` ORDER BY ${this.sortings.join(', ')}`;

    return '';
  }

  /** @private */
  get limitOffsetSql() {
    // FIXME: sanitize input
    // TODO: memoize
    if (!(this.maxRows || this.startRow)) return '';

    let str = ` LIMIT ${db.escape(this.maxRows || MAX_LIMIT) }`;

    if (this.startRow) str += ` OFFSET ${db.escape(this.startRow)}`;

    return str;
  }

  /** @private */
  whereSql(conditions) {
    conditions = this.restrictCriteria(conditions);

    if (isPresent(conditions)) return ` WHERE ${db.conditions(conditions)}`;

    return '';
  }

  /** @private */
  async executeAndAffected(sql) {
    this.logger(sql);
    const res = await db.executeAndAffected(sql, this.conn);
    this.logger(`-> ${res}`);

    return res;
  }
}

/** TODO: write doc comment */
// TODO: add caching when no connection/transaction given
function dataset(name, conn) {
  return new Dataset(name, conn);
}

/** @private */
function isPresent(criteria) {
  if (!criteria) return false;

  if (typeof criteria !== 'object') return Boolean(criteria);

  return Boolean(Object.keys(criteria).length);
}
