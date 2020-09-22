module.exports = { env };

/** TODO: write doc comment */
function env(key, dflt) {
  if (key in process.env) return process.env[key];

  if (dflt !== undefined) return dflt;

  throw new Error(`Please set ${key} in process environment.`);
}
