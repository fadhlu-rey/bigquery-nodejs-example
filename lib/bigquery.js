import { BigQuery } from '@google-cloud/bigquery';
import dotenv from 'dotenv';

dotenv.config();

class BigQueryClient {
  client = null;
  constructor() {
    this.client = new BigQuery({
      projectId: process.env.GOOGLE_PROJECT_ID,
      keyFilename: './service_account.json',
    });
  }

  async query(sql) {
    const options = {
      query: sql,
    };

    try {
      const [job] = await this.client.createQueryJob(options);
      console.log(`Job ${job.id} started.`);

      const [rows] = await job.getQueryResults();

      return rows;
    } catch (error) {
      throw error;
    }
  }
}

export default BigQueryClient;
