-- 02_integration.sql
-- Lets Snowflake read the S3 bucket by assuming an IAM role in the AWS
-- account. No key pair, no credential stored in Snowflake. Access is revoked
-- on the AWS side without touching anything here.

USE ROLE ACCOUNTADMIN;
USE DATABASE chrysos_poc;

CREATE STORAGE INTEGRATION IF NOT EXISTS chrysos_poc_integration
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::<ACCOUNT_ID>:role/chrysos-poc-snowflake-role'
  STORAGE_ALLOWED_LOCATIONS = ('s3://<BUCKET>/');

DESC INTEGRATION chrysos_poc_integration;

-- STORAGE_AWS_IAM_USER_ARN and STORAGE_AWS_EXTERNAL_ID from that output go
-- into the role's trust policy. Each integration gets its own external id, so
-- a role shared between two integrations needs both ids listed or the second
-- one fails with sts:AssumeRole denied.
--
-- The role needs s3:GetObject, s3:GetObjectVersion, s3:ListBucket and
-- s3:GetBucketLocation. The integration grants the right to assume the role;
-- it does not grant the role the right to read.
--
-- ALTER, never CREATE OR REPLACE, on an integration already in use.
-- Replacing it issues a new external id and the trust policy stops matching.
