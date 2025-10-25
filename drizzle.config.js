require('dotenv').config();

module.exports = {
  schema: './db/schema.js',
  out: './db/migrations',
  dialect: 'sqlite',
  dbCredentials: {
    url: './local.db',
  },
};
