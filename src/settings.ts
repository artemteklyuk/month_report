import { config } from 'dotenv';

export const settings = config().parsed as {
  API_DB: string;
  BILLING_DB: string;
  ANALYTICS_DB: string;
  SMTP_DB: string;
  VACANCIES_DB: string;
};
