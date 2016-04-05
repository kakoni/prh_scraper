DROP MATERIALIZED VIEW IF EXISTS companies_view;
CREATE MATERIALIZED VIEW companies_view AS
WITH liquidations AS (
  SELECT DISTINCT ON (business_id) business_id,
         liquidations.type,
         liquidations."registrationDate"
  FROM companies
  LEFT JOIN LATERAL jsonb_to_recordset(companies.data -> 'liquidations') liquidations(type text, language text, "endDate" date, "registrationDate" date) ON TRUE
  WHERE (liquidations.language = 'FI' OR liquidations.language IS NULL) AND liquidations."endDate" IS NULL AND liquidations.type IS NOT NULL
  ORDER BY business_id, liquidations."registrationDate" DESC
), terminated AS (
  SELECT business_id,
         registered_entries."registrationDate",
         registered_entries.description AS type
  FROM companies
  LEFT JOIN LATERAL jsonb_to_recordset(companies.data -> 'registeredEntries') registered_entries(status integer, register integer, description text, language text, "endDate" date, "registrationDate" date) ON TRUE
  WHERE registered_entries."endDate" IS NULL AND registered_entries.description = 'Lakannut'
), addresses AS (
  SELECT business_id, address_1[1] as street_1,  address_1[1] as post_code_1,
         address_2[1] as street_2,  address_2[1] as post_code_2
  FROM crosstab(
  $$
    SELECT business_id as section,
           type,
           ARRAY[street, "postCode"]
    FROM companies
    LEFT JOIN LATERAL jsonb_to_recordset(companies.data -> 'addresses') addresses(street text, "postCode" text, city text, "endDate" date, type integer) ON TRUE
    WHERE addresses."endDate" IS NULL
  $$
  ,$$VALUES (1), (2)$$)
  AS ct(business_id text, address_1 text[], address_2 text[])
), business_lines AS (
  SELECT business_id,
         business_lines.code,
         business_lines.name
  FROM companies
  LEFT JOIN LATERAL jsonb_to_recordset(companies.data -> 'businessLines') business_lines(code text, name text, language text, "endDate" date) ON TRUE
  WHERE business_lines.language = 'FI' AND business_lines."endDate" IS NULL
), combined AS (
SELECT COALESCE(terminated.business_id, liquidations.business_id) AS business_id,
       CASE
       WHEN (terminated."registrationDate" > liquidations."registrationDate") THEN terminated.type
       WHEN (liquidations."registrationDate" > terminated."registrationDate") THEN liquidations.type
       WHEN (terminated.type IS NULL) THEN liquidations.type
       WHEN (liquidations.type IS NULL) THEN terminated.type
       ELSE NULL
       END AS type
FROM liquidations
FULL JOIN terminated ON liquidations.business_id = terminated.business_id
)
SELECT COALESCE(companies.business_id) AS business_id,
       CASE
       WHEN combined.type IS NOT NULL THEN combined.type
       ELSE 'NORMAL'
       END AS type,
       addresses.street_1,
       addresses.post_code_1,
       addresses.street_2,
       addresses.post_code_2,
       business_lines.code,
       business_lines.name
FROM companies
LEFT JOIN combined ON companies.business_id = combined.business_id
LEFT JOIN addresses ON companies.business_id = addresses.business_id
LEFT JOIN business_lines ON companies.business_id = business_lines.business_id;
