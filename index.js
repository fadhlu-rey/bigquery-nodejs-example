import express from 'express';
import BigQueryClient from './lib/bigquery.js';
import dotenv from 'dotenv';

dotenv.config();

const app = express();
const PORT = 3000;

const bigquery = new BigQueryClient();

app.use(express.json());

app.get('/', (req, res) => {
  return res.json({
    message: 'Hello World',
  });
});

app.get('/segments', async (req, res) => {
  const sql = `SELECT * FROM \`${req.query.table}.segments\`;`;

  const data = await bigquery.query(sql);

  return res.json({
    data,
  });
});

app.post('/segments', async (req, res) => {
  const { user_id, segment, value, is_sync } = req.body;

  const sql = `INSERT INTO \`${req.query.table}.segments\` (user_id, segment, value, is_sync) VALUES (CAST('${user_id}' AS INT64), '${segment}', '${value}', CAST('${is_sync}' AS BOOL));`;

  await bigquery.query(sql);

  return res.json({
    message: 'success',
  });
});

app.put('/segments/:id', async (req, res) => {
  const { segment, value, is_sync } = req.body;

  const sql = `UPDATE \`${req.query.table}.segments\` SET segment = '${segment}', value = '${value}', is_sync = CAST('${is_sync}' AS BOOL) WHERE user_id = CAST('${req.params.id}' AS INT64);`;

  await bigquery.query(sql);

  return res.json({
    message: 'success',
  });
});

app.listen(PORT, () => {
  console.log(`Server is running on PORT ${PORT}`);
});
