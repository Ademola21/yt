const Database = require('better-sqlite3');
const { drizzle } = require('drizzle-orm/better-sqlite3');
const { apiKeys } = require('./schema');
const path = require('path');

const dbPath = path.join(__dirname, '..', 'local.db');
const sqlite = new Database(dbPath);
const db = drizzle(sqlite, { schema: { apiKeys } });

module.exports = { db, apiKeys };
