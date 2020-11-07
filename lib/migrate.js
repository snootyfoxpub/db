const logger = require('debug');

const debug = logger('db:migrate');

// FIXME: running migrations requires multiple statements flag being set
//
// TODO: destructure used methods from adapter into local consts
const db = {
  ...require('./adapter'),
  dataset: require('./dataset')
}

const fs = require('fs/promises');
const path = require('path');

module.exports = migrator;

const RUNNERS = {
  '.js': applyJSMigration,
  '.sql': applySQLMigration
};

const migrationsLog = db.escapeId('schema_migrations');

/** TODO: add doc comment */
// TODO: add support for multiple migration paths
async function migrator(migrationsPath) {
  await verifySchemaTable();

  const applied = await loadAppliedMigrations();
  const available = await loadAvailableMigrations(migrationsPath);
  const pending = calculatePending(available, applied);

  for (const migration of pending)
    await applyMigration(migration);
}

async function verifySchemaTable() {
  await db.execute(`
    CREATE TABLE IF NOT EXISTS ${migrationsLog} (
      name VARCHAR(255) PRIMARY KEY
    )
  `);
}

async function loadAppliedMigrations() {
  return await db.pluck(`
    SELECT * FROM ${migrationsLog}
  `);
}

async function loadAvailableMigrations(migrationsPath) {
  const files = await fs.readdir(migrationsPath);

  return files.
    map(makeMigrationObject).
    filter(({ runner }) => runner);

  function makeMigrationObject(basename) {
    const extname = path.extname(basename).toLowerCase();
    const fullPath = path.join(migrationsPath, basename);

    return {
      name: basename,
      path: fullPath,
      runner: RUNNERS[extname]
    }
  }
}

async function applyMigration(migration) {
  try {
    debug(`Running ${migration.name}`);
    await migration.runner(migration);
  } catch (err) {
    console.error(migration.name + ':\n\t', err);
    throw err;
  }
  await recordMigrationApplied(migration);
}

async function recordMigrationApplied({ name }) {
  const quotedName = db.escape(name);

  await db.execute(`
    INSERT INTO ${migrationsLog} VALUES (${quotedName})
  `);
}

async function applyJSMigration({ name, path: fullPath }) {
  const migration = require(fullPath);

  await migration(db);
}

async function applySQLMigration({ path: fullPath }) {
  const contents = await fs.readFile(fullPath, { encoding: 'utf-8' });

  // You should enable multipleStatements option in order to support
  // migrations that require more than one query
  // TODO: add support for JS migrations
  await db.execute(contents);
}

/** @private */
function calculatePending(available, applied) {
  const lookup = {};
  available.forEach(entry => (lookup[entry.name] = entry));

  const availableNames = Object.keys(lookup);
  const pendingNames = sortMigrations(
    difference(availableNames, applied)
  );

  return pendingNames.map(name => lookup[name]);
}

function sortMigrations(list) {
  // TODO: sort on timestamps only, padding timestamps as necessary if e.g.
  // one has seconds and the other has not, or one file name contains date
  // only, and the other one consists of date and time.
  return list.sort();
}

function difference(big, small) {
  if (! (small instanceof Set)) small = new Set(small);

  const diff = new Set(big);
  for (const el of small) diff.delete(el);

  return [...diff];
}
