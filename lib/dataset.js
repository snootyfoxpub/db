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
  }

  /** TODO: write doc comment */
  async all(conditions) {
    // TODO: add support for string conditions

    let sql = `SELECT * FROM ${this.quotedName}`;

    conditions = this.restrictCriteria(conditions);
    if (isPresent(conditions))
      sql += ` WHERE ${db.conditions(conditions)}`;

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

    conditions = this.restrictCriteria(conditions);
    if (isPresent(conditions))
      sql += ` WHERE ${db.conditions(conditions)}`;

    return await db.executeAndAffected(sql, this.conn);
  }

  /** TODO: write doc comment */
  async get(conditions) {
    let sql = `SELECT * FROM ${this.quotedName}`;

    conditions = this.restrictCriteria(conditions);
    if (isPresent(conditions))
      sql += ` WHERE ${db.conditions(conditions)}`;

    sql += ' LIMIT 1';

    return await db.selectRow(sql, this.conn);
  }

  /** TODO: write doc comment */
  async pluck(column, conditions) {
    let sql = `SELECT ${db.escapeId(column)} FROM ${this.quotedName}`;

    conditions = this.restrictCriteria(conditions);
    if (isPresent(conditions))
      sql += ` WHERE ${db.conditions(conditions)}`;

    return await db.pluck(sql, this.conn);
  }

  /** TODO: write doc comment */
  async update(assignments, conditions) {
    let sql = `UPDATE ${this.quotedName} SET ${db.assignments(assignments)}`;

    conditions = this.restrictCriteria(conditions);
    if (isPresent(conditions))
      sql += ` WHERE ${db.conditions(conditions)}`;

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

  /** TODO: write doc comment */
  where(criteria) {
    const copy = new this.constructor(this.tableName, this.conn);
    copy.criteria = this.restrictCriteria(criteria);

    return copy;
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
