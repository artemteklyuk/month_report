import { Client } from 'pg';
import format from 'pg-format';
import { getDbClients } from './db';
import { stringify } from 'csv/sync';
import { writeFile } from 'fs/promises';
import { resolve } from 'path';
import { flatten } from 'flat';
import { writeFileSync } from 'fs';
import { MongoClient, ObjectId } from 'mongodb';

async function checkIsUserActive(
  uid: string,
  client: Client
): Promise<boolean> {
  try {
    const query = format(
      `SELECT COUNT(1) FROM purchase AS p LEFT JOIN customer AS c ON p.customer_id = c.id WHERE c.uid = %L AND p.next_billing_at > CURRENT_TIMESTAMP AND (p.is_canceled = FALSE OR p.is_canceled IS NULL)`,
      uid
    );

    const result = await client.query<{ count: number }>(query);
    return result.rows[0]?.count >= 1;
  } catch (error) {
    console.log(error);
    return false;
  }
}

async function getAnyActiveUsers(client: Client) {
  const result = await client.query<{ uid: string }>(
    'SELECT DISTINCT c.uid FROM customer AS c LEFT JOIN purchase AS p ON p.customer_id = c.id WHERE p.id IS NOT NULL;'
  );

  return result.rows.map(({ uid }) => uid);
}

async function countUserLetters(uid: string, client: Client) {
  const query = format(
    `SELECT COUNT(id) AS count
FROM forwarded_messages 
WHERE uid = %L`,
    uid
  );

  const response = await client.query(query);
  return response.rows[0].count;
}

async function countTalentSuccessApplications(uid: string, client: Client) {
  const query = format(
    `SELECT COUNT(r_v.id) AS "count"
FROM
	resume_vacancy AS r_v
  	LEFT JOIN vacancy AS v
    	ON r_v.vacancy_id = v.id
  	LEFT JOIN resume AS r
    	ON r_v.resume_id = r.id
    LEFT JOIN "user" AS u
    	ON r.user_id = u.id
WHERE
	u.uid = %L
  AND r_v.is_responded IS TRUE
  AND v.site_host = 'www.talent.com'`,
    uid
  );

  const response = await client.query(query);
  return response.rows[0].count;
}

async function countUserInvites(uid: string, client: Client) {
  const query = format(
    `SELECT COUNT(m.id) AS mails
FROM
	smtp_users AS s_u
		LEFT JOIN forwarded_messages AS f_m
    	ON s_u.uid = f_m.uid
    LEFT JOIN messages AS m
    	ON f_m.forward_from_message_id = m.id
WHERE
	s_u.uid = %L
	AND m.data::text LIKE '%calendly.com%'`,
    uid
  );

  const response = await client.query(query);
  const { mails: count }: { mails: number } = response.rows[0];

  return count;
}

async function getUserData(
  uid: string,
  apiClient: Client,
  analyticsClient: Client,
  billingClient: Client,
  smtpClient: Client,
  vacanciesDbClient: MongoClient
) {
  try {
    const resumeQuery = format(
      `SELECT r.id, r.serial_number, r.speciality, r.cv_file_url, r.status, r.created_at, r.updated_at, r.generated_cv_id
FROM resume AS r
        LEFT JOIN "user" AS u
        ON r.user_id = u.id
WHERE u.uid = %L
ORDER BY r.serial_number ASC;`,
      uid
    );

    const resume_list: any[] = [];

    const resumes = (
      await apiClient.query<{
        id: number;
        serial_number: string;
        speciality: string;
        cv_file_url: string;
        status: string;
        created_at: Date;
        updated_at: Date;
        generated_cv_id?: number;
      }>(resumeQuery)
    ).rows;

    for (const resume of resumes) {
      resume.cv_file_url = 'https://api.jobhire.ai/' + resume.cv_file_url;

      let generatedCv: any = {
        id: null,
        created_at: null,
        updated_at: null,
        file_url: null,
        source_hash: null,
        status: null,
      };

      if (resume.generated_cv_id) {
        const generatedCvQuery = format(
          `SELECT g_c.*
FROM generated_cv AS g_c
  LEFT JOIN resume AS r
    ON r.generated_cv_id = g_c.id
WHERE r.id = %L;`,
          resume.id
        );

        generatedCv =
          (await apiClient.query<{ file_url?: string }>(generatedCvQuery))
            .rows[0] || null;

        if (generatedCv?.file_url) {
          generatedCv.file_url =
            'http://5.161.185.218:4004/' + generatedCv.file_url;
        }
      }

      const resumeAnswersQuery = format(
        `SELECT r_q.question, r_a.answer
FROM resume AS r
        LEFT JOIN resume_answer AS r_a
        ON r_a.resume_id = r.id
  LEFT JOIN resume_question AS r_q
        ON r_a.resume_question_id = r_q.id
WHERE r.id = %L
ORDER BY r_q.id ASC`,
        resume.id
      );

      const question: any = {};

      (
        await apiClient.query<{
          question: string;
          answer: string[];
        }>(resumeAnswersQuery)
      ).rows.forEach(
        (answer) =>
          (question[answer.question] = answer.answer
            ? answer.answer.join(', ')
            : null)
      );

      const rStats = await vacanciesDbClient
        .db('vacancy_storage')
        .collection('resumeVacancy')
        .aggregate([
          {
            $match: {
              resumeId: resume.id,
              vacancyId: {
                $ne: new ObjectId('000000000000000000000000'),
              },
              respondedAt: {
                $ne: null,
              },
            },
          },
          {
            $group: {
              _id: '$isResponded',
              count: {
                $count: {},
              },
            },
          },
        ])
        .toArray();

      const successApplies = rStats.find(({ _id }) => _id === true)?.count || 0;
      const failedApplies = rStats.find(({ _id }) => _id === false)?.count || 0;

      const firstDayAndWeekApplications = await vacanciesDbClient
        .db('vacancy_storage')
        .collection('resumeVacancy')
        .aggregate([
          {
            $match: {
              resumeId: resume.id,
              vacancyId: {
                $ne: new ObjectId('000000000000000000000000'),
              },
              isResponded: true,
            },
          },
          {
            $sort: {
              respondedAt: -1,
            },
          },
          {
            $limit: 1,
          },
          {
            $project: {
              startDate: {
                $dateToString: {
                  format: '%Y-%m-%d',
                  date: '$respondedAt',
                },
              },
              isoStartDate: {
                $dateTrunc: {
                  date: '$respondedAt',
                  unit: 'day',
                },
              },
            },
          },
          {
            $lookup: {
              from: 'resumeVacancy',
              let: {
                startDate: '$isoStartDate',
              },
              pipeline: [
                {
                  $match: {
                    $expr: {
                      $and: [
                        {
                          $eq: ['$resumeId', resume.id],
                        },
                        {
                          $eq: [
                            {
                              $dateTrunc: {
                                date: '$respondedAt',
                                unit: 'day',
                              },
                            },
                            '$$startDate',
                          ],
                        },
                      ],
                    },
                    isResponded: true,
                    vacancyId: {
                      $ne: new ObjectId('000000000000000000000000'),
                    },
                  },
                },
                {
                  $count: 'firstDayAppliesCount',
                },
              ],
              as: 'firstDayApplies',
            },
          },
          {
            $lookup: {
              from: 'resumeVacancy',
              let: {
                startDate: '$isoStartDate',
              },
              pipeline: [
                {
                  $match: {
                    $expr: {
                      $and: [
                        {
                          $eq: ['$resumeId', resume.id],
                        },
                        {
                          $gte: ['$respondedAt', '$$startDate'],
                        },
                        {
                          $lt: [
                            '$respondedAt',
                            {
                              $add: ['$isoStartDate', 7 * 24 * 60 * 60 * 1000],
                            },
                          ],
                        },
                      ],
                    },
                    isResponded: true,
                    vacancyId: {
                      $ne: new ObjectId('000000000000000000000000'),
                    },
                  },
                },
                {
                  $count: 'firstWeekAppliesCount',
                },
              ],
              as: 'firstWeekApplies',
            },
          },
          {
            $project: {
              startDate: '$isoStartDate',
              firstDayAppliesCount: {
                $arrayElemAt: ['$firstDayApplies.firstDayAppliesCount', 0],
              },
              firstWeekAppliesCount: {
                $arrayElemAt: ['$firstWeekApplies.firstWeekAppliesCount', 0],
              },
            },
          },
        ]);

      const fdaw = await firstDayAndWeekApplications.toArray();

      resume_list.push({
        ...resume,
        first_day_applies_count: fdaw[0]?.firstDayAppliesCount || 0,
        first_week_applies_count: fdaw[0]?.firstWeekAppliesCount || 0,
        applies_start_date: fdaw[0]?.startDate || null,
        generatedCv,
        question,
        successApplies,
        failedApplies,
      });
    }

    const mainUserData = (
      await apiClient.query(
        format(
          `SELECT id, uid, first_name, last_name, email, address, phone_number, birth_date, created_at AS register_date, updated_at, match_rate, is_employed FROM "user" WHERE uid = %L`,
          uid
        )
      )
    ).rows[0];

    if (mainUserData.email.includes('hotger')) {
      console.log('Hotger user');
      return null;
    }

    const questionTitles = (
      await apiClient.query<{ question: string }>(
        'SELECT question FROM user_question ORDER BY id ASC'
      )
    ).rows.map(({ question }) => question);

    const getUserAnswersQuery = format(
      `SELECT u_q.id, u_q.question, u_a.answer
FROM user_answer AS u_a
  LEFT JOIN "user" AS u
    ON u_a.user_id = u.id
  LEFT JOIN user_question AS u_q
    ON u_a.user_question_id = u_q.id
WHERE u.uid = %L
  AND u_a.answer IS NOT NULL
ORDER BY u_q.serial_number ASC;`,
      uid
    );

    const rawUserQuestion = (
      await apiClient.query<{ answer: string[]; question: string }>(
        getUserAnswersQuery
      )
    ).rows;

    const userQuestions: any = {};
    questionTitles.forEach(
      (key) =>
        (userQuestions[key] =
          rawUserQuestion
            .find(({ question }) => question === key)
            ?.answer.join(', ') || null)
    );

    const getUserMetricsQuery = format(
      `SELECT "data"
FROM 
  events
WHERE
  uid = %L
  AND title IN ('register', 'email_retention')`,
      uid
    );

    const userMetrics = joinFields(
      (
        await analyticsClient.query<{ data: object }>(getUserMetricsQuery)
      ).rows.map(({ data }) => data)
    );

    if (!userMetrics['yid']) {
      const yid =
        ((
          await apiClient.query(
            format(
              'SELECT ext_uniq_id AS yid FROM user_metrics WHERE uid = %L',
              uid
            )
          )
        ).rows[0]?.yid as string) || null;

      userMetrics['yid'] = yid ? yid.slice(1, -1) : yid;
    }

    const userSettingsData = await apiClient.query<{
      coverletter_generation: boolean;
    }>(
      format(
        `SELECT s.is_generate_cover_letter AS coverletter_generation
FROM settings AS s
	LEFT JOIN "user" AS u
  	ON s.user_id = u.id
WHERE u.uid = %L`,
        uid
      )
    );

    const coverletter_generation =
      userSettingsData?.rows.length > 0
        ? userSettingsData.rows[0].coverletter_generation
        : null;

    const subscriptionDataRequest = format(
      `SELECT happened_at, data FROM events
WHERE title = 'purchase'
  AND uid = %L;`,
      uid
    );

    const subscriptionData = (
      await analyticsClient.query<{
        happened_at: Date;
        data: {
          value: number;
          is_auto: boolean;
          currency: string;
          invoice_id: string;
          product_id: string;
          product_title: string;
          expiration_date: Date;
          subscription_id: string;
        };
      }>(subscriptionDataRequest)
    ).rows;

    let isActiveSubscriber = await checkIsUserActive(uid, billingClient);
    let subscription: any = {};
    const firstPurchase = subscriptionData.find(({ data }) => !data.is_auto);
    if (firstPurchase) {
      subscription['firstPurchaseDate'] = firstPurchase.happened_at;
      subscription['subscriptionType'] = firstPurchase.data.product_title;
      subscription['subscriptionId'] = firstPurchase.data.subscription_id;
      subscription['subscriptionInvoice'] = firstPurchase.data.invoice_id;
      subscription['productId'] = firstPurchase.data.product_id;
      subscription['productTitle'] = firstPurchase.data.product_title;
      subscription['price'] =
        firstPurchase.data.value + ' ' + firstPurchase.data.currency;

      subscription['autoBillings'] = subscriptionData
        .filter(({ data }) => !!data.is_auto)
        .map(
          ({ happened_at, data }) =>
            `${data.invoice_id}:${data.value} ${
              data.currency
            }:${happened_at.toISOString()}`
        )
        .join('\n');

      const cancelData = (
        await analyticsClient.query(
          format(
            `SELECT happened_at, data FROM events WHERE title = 'subscription_cancel' AND uid = %L`,
            uid
          )
        )
      ).rows[0];

      subscription['cancel'] = cancelData
        ? {
            date: cancelData.happened_at,
            reason: cancelData.data.reason || null,
            comment: cancelData.data.comment || null,
            feedback: cancelData.data.feedback || null,
          }
        : {
            date: null,
            reason: null,
            comment: null,
            feedback: null,
          };
    } else {
      subscription['firstPurchaseDate'] = null;
      subscription['subscriptionType'] = null;
      subscription['subscriptionId'] = null;
      subscription['subscriptionInvoice'] = null;
      subscription['price'] = null;
      subscription['autoBillings'] = null;
      subscription['productId'] = null;
      subscription['productTitle'] = null;
      subscription['cancel'] = {
        date: null,
        reason: null,
        comment: null,
        feedback: null,
      };
    }

    const nps = await analyticsClient.query<{
      title: string;
      data: { question: string; answers: string[] };
    }>(
      format(
        `
SELECT title, data FROM events WHERE uid = %L AND title = ANY(ARRAY[%L])`,
        uid,
        ['nps_1', 'nps_2']
      )
    );

    const nps_1 = nps.rows.find(({ title }) => title === 'nps_1');
    const nps_2 = nps.rows.find(({ title }) => title === 'nps_2');

    let resume_load_on_register: boolean | null = null;
    if (mainUserData.register_date >= new Date('2024-05-15')) {
      const checkResumeLoadQuery = format(
        `SELECT 1 as "yes"
FROM "user" AS u
  LEFT JOIN preloaded_cv AS p_c
    ON LOWER(u.email) = LOWER(p_c.email)
WHERE u.uid = %L AND p_c IS NOT NULL;`,
        uid
      );

      const resumePreload = await apiClient.query<{ yes: 1 }>(
        checkResumeLoadQuery
      );

      resume_load_on_register =
        resumePreload.rows[0] && resumePreload.rows[0].yes === 1;
    }

    let resume_downloaded_before_purchase: boolean | null = null;

    let resume_downloaded_after_purchase: boolean | null = null;

    if (firstPurchase) {
      const afterQuery = format(
        `SELECT 1 AS yes FROM events WHERE uid = %L AND title = 'resume_download' AND happened_at > %L;`,
        uid,
        firstPurchase.happened_at
      );

      const beforeQuery = format(
        `SELECT 1 AS yes FROM events WHERE uid = %L AND title = 'resume_download' AND happened_at < %L;`,
        uid,
        firstPurchase.happened_at
      );

      const resumeDownloadedAfterPurchase = await analyticsClient.query<{
        yes: 1;
      }>(afterQuery);

      const resumeDownloadedBeforePurchase = await analyticsClient.query<{
        yes: 1;
      }>(beforeQuery);

      resume_downloaded_after_purchase =
        resumeDownloadedAfterPurchase.rows[0]?.yes === 1;
      resume_downloaded_before_purchase =
        resumeDownloadedBeforePurchase.rows[0]?.yes === 1;
    }

    const resumeData: any = {};
    let counter = 1;
    resume_list.forEach((resume) => (resumeData[`r_${counter++}`] = resume));

    const invitationsCount = await countUserInvites(uid, smtpClient);
    const lettersCount = await countUserLetters(uid, smtpClient);
    const talentSuccessApplicationsCount = await countTalentSuccessApplications(
      uid,
      apiClient
    );

    return {
      ...mainUserData,
      talentSuccessApplicationsCount,
      isActiveSubscriber,
      // @ts-ignore
      ...flatten({ question: userQuestions }),
      nps_1: nps_1 ? nps_1.data.answers.join(', ') : null,
      nps_2: nps_2 ? nps_2.data.answers.join(', ') : null,
      coverletter_generation,
      resume_load_on_register,
      resume_downloaded_after_purchase,
      resume_downloaded_before_purchase,
      // @ts-ignore
      ...flatten(
        userMetrics || {
          Gclid: null,
          yid: null,
          fbc: null,
          fbclid: null,
          utm_source: null,
          utm_campaign: null,
          utm: null,
        }
      ),
      // @ts-ignore
      ...flatten({ subscription: subscription }),
      // @ts-ignore
      ...flatten(resumeData),
      invitations_count: Number(invitationsCount) || 0,
      lettersCount: Number(lettersCount) || 0,
    };
  } catch (error) {
    console.log(error);
  }
}

function joinFields(objects: object[]) {
  const chosen = [
    'Gclid',
    'yid',
    'fbc',
    'fbclid',
    'utm_source',
    'utm_campaign',
  ];

  const known: any = {};
  const others: string[] = [];
  objects.forEach((obj) => {
    // @ts-ignore
    chosen.forEach((key) => (known[key] = obj[key] || null));

    Object.keys(obj).forEach((key) => {
      if (key.startsWith('utm') && !chosen.includes(key)) {
        // @ts-ignore
        others.push(`${key}=${obj[key]}`);
      }
    });
  });

  return {
    ...known,
    utm: others.join('\n'),
  };
}

async function getDuplicates(client: Client) {
  // prettier-ignore
  const query = 
`SELECT DISTINCT LOWER(u1.email) as email, u1.uid AS uid1, u2.uid AS uid2, u1.email as email1, u2.email as email2
FROM "user" AS u1
        LEFT JOIN "user" AS u2
                ON LOWER(u1.email) = LOWER(u2.email)
WHERE u1.uid <> u2.uid
                        AND u1.uid > u2.uid;`;

  const result = await client.query<{
    email: string;
    uid1: string;
    uid2: string;
    email1: string;
    email2: string;
  }>(query);

  return result.rows;
}

async function run() {
  const {
    apiDbClient,
    analyticsDbClient,
    billingDbClient,
    smtpDbClient,
    vacanciesDbClient,
  } = await getDbClients();

  const paid = await getAnyActiveUsers(billingDbClient);
  const users = [];
  console.log(`Total: ${paid.length}`);

  for (const user of paid) {
    const now = Date.now();
    const data = await getUserData(
      user,
      apiDbClient,
      analyticsDbClient,
      billingDbClient,
      smtpDbClient,
      vacanciesDbClient
    );

    if (!data) {
      continue;
    }

    // console.log(JSON.stringify(data, undefined, 2));

    users.push(data);

    // console.log(data);

    const keys = Object.keys(data);

    console.log(`${user} done in ${Date.now() - now}ms, keys: ${keys.length}`);

    if (keys.length !== 140) {
      console.log(keys.join('\n'));
    }
  }

  await writeFileSync(
    resolve('users-data.json'),
    JSON.stringify(
      users.filter((user) => !!user && Object.keys(user).length === 126),
      undefined,
      2
    )
  );
}

run()
  .catch((err) => console.log(err))
  .finally(process.exit);
