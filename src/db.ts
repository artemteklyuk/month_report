import { Client } from 'pg';
import { settings } from './settings';
import { MongoClient } from 'mongodb';

export async function getDbClients() {
  const apiDbClient = new Client({
    connectionString: settings.API_DB,
  });

  const billingDbClient = new Client({
    connectionString: settings.BILLING_DB,
  });

  const analyticsDbClient = new Client({
    connectionString: settings.ANALYTICS_DB,
  });

  const smtpDbClient = new Client({
    connectionString: settings.SMTP_DB,
  });

  const vacanciesDbClient = new MongoClient(settings.VACANCIES_DB);

  await smtpDbClient.connect();
  await apiDbClient.connect();
  await billingDbClient.connect();
  await analyticsDbClient.connect();
  await vacanciesDbClient.connect();

  return {
    apiDbClient,
    billingDbClient,
    analyticsDbClient,
    smtpDbClient,
    vacanciesDbClient,
  };
}
