// TODO: destructure used methods from adapter into local consts
const db = require('./adapter');

module.exports = dataset;

/** TODO: write doc comment */
class Dataset {
  /** TODO: write doc comment */
  constructor(name, conn = null) {
    this.tableName = name;
    this.quotedName = db.escapeId(name);
    this.conn = conn;
    this.criteria = null;
    this.sortings = [];
  }

  /** TODO: write doc comment */
  async all(conditions) {
    // TODO: add support for string conditions

    let sql = `SELECT * FROM ${this.quotedName}`;
    sql += this.whereSql(conditions);
    sql += this.orderSql;

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

    const id = await db.insertWithId(sql, this.conn);
    // FIXME: hardcoded `id` column
    // Also, this.get may fail due to inserting the record
    // that does not match the filters
    if (id) return await this.get({ id });

    return record;
  }

  /** TODO: write doc comment */
  async delete(conditions) {
    let sql = `DELETE FROM ${this.quotedName}`;

    sql += this.whereSql(conditions);
    sql += this.orderSql;

    return await db.executeAndAffected(sql, this.conn);
  }

  /** TODO: write doc comment */
  async get(conditions) {
    let sql = `SELECT * FROM ${this.quotedName}`;

    sql += this.whereSql(conditions);
    sql += this.orderSql;
    sql += ' LIMIT 1';

    return await db.selectRow(sql, this.conn);
  }

  /** TODO: write doc comment */
  async count(what = '*') {
    let sql = `SELECT count(${what}) FROM ${this.quotedName}`;

    sql += this.whereSql();

    return await db.selectValue(sql);
  }

  /** TODO: write doc comment */
  async pluck(column, conditions) {
    let sql = `SELECT ${db.escapeId(column)} FROM ${this.quotedName}`;

    sql += this.whereSql(conditions);
    sql += this.orderSql;

    return await db.pluck(sql, this.conn);
  }

  /** TODO: write doc comment */
  async update(assignments, conditions) {
    let sql = `UPDATE ${this.quotedName} SET ${db.assignments(assignments)}`;

    sql += this.whereSql(conditions);
    sql += this.orderSql;

    return await db.executeAndAffected(sql, this.conn);
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
      return { ...additional, ...existing };

    if (!existingIsString) existing = db.conditions(existing);
    if (!additionalIsString) additional = db.conditions(additional);

    return `(${existing}) AND (${additional})`;
  }

  /** @private */
  clone(changes = {}) {
    const copy = new this.constructor(this.tableName, this.conn);

    Object.assign(copy, {
      criteria: this.criteria,
      sortings: this.sortings,
      ...changes
    });

    return copy;
  }

  /** TODO: write doc comment */
  order(...cols) {
    return this.clone({ sortings: [...this.sortings, ...cols] });
  }

  /** TODO: write doc comment */
  where(criteria) {
    return this.clone({ criteria: this.restrictCriteria(criteria) });
  }

  /** @private */
  get orderSql() {
    // FIXME: sanitize input
    // TODO: memoize
    if (this.sortings.length) return ` ORDER BY ${this.sortings.join(', ')}`;

    return '';
  }

  /** @private */
  whereSql(conditions) {
    conditions = this.restrictCriteria(conditions);

    if (isPresent(conditions)) return ` WHERE ${db.conditions(conditions)}`;

    return '';
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
