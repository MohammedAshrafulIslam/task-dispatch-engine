import pg from 'pg';

const { Pool } = pg;

export const pool = new Pool({
 host:'localhost',
 port: 5433,
 user: "ashraf",
 password: "password",
 database: "taskengine",
});

pool.on('connect', () => console.log('PostgreSQL connected'));
pool.on('error', (err: Error) => console.error('PostgreSQL error:', err));