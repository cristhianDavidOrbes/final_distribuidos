function createLogger(prefix) {
  return {
    info: (msg, meta = {}) => console.log(`[${prefix}] ${msg}`, meta),
    warn: (msg, meta = {}) => console.warn(`[${prefix}] ${msg}`, meta),
    error: (msg, meta = {}) => console.error(`[${prefix}] ${msg}`, meta),
  };
}

module.exports = { createLogger };
