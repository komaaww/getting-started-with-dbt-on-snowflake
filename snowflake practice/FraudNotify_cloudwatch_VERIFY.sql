--事前準備
USE ROLE ACCOUNTADMIN;

--API INTEGRATIONS作成

CREATE OR REPLACE API INTEGRATION fraudNotify_cloudwatch_api
  API_PROVIDER = AWS_API_GATEWAY
  API_AWS_ROLE_ARN = 'arn:aws:iam::340650176156:role/RQC_snowflake_external_role'
  API_ALLOWED_PREFIXES = ('https://1c4rh746lh.execute-api.ap-northeast-1.amazonaws.com/snowflake-fraudNotidy-stage')
  ENABLED = TRUE;

DESCRIBE INTEGRATION fraudNotify_cloudwatch_api;

--SYSADMINにAPI INTEGRATIONSのUsageを付与

GRANT USAGE ON INTEGRATION fraudNotify_cloudwatch_api TO ROLE sysadmin;


USE database IT_DAA;
USE SCHEMA COE_MNG;

--EXTERNAL FUNCTION作成

CREATE OR REPLACE EXTERNAL FUNCTION send_to_cloudwatch(
  payload VARIANT
)
RETURNS VARIANT
API_INTEGRATION = fraudNotify_cloudwatch_api
AS 'https://1c4rh746lh.execute-api.ap-northeast-1.amazonaws.com/snowflake-fraudNotidy-stage';

--SYSADMINにEXTERNAL FUNCTIONのUsageを付与

GRANT USAGE ON FUNCTION send_cloudwatch_metric(STRING, NUMBER, STRING, STRING, VARCHAR) TO ROLE sysadmin;



--検証用テーブル作成

CREATE OR REPLACE TABLE TEST_FOR_CLOUDWATCH(
	ID NUMBER(38,0),
    MESSAGE VARCHAR(10) ,
	TS TIMESTAMP_LTZ(9) DEFAULT CURRENT_TIMESTAMP()	
);

--検証用テーブルのStream作成

CREATE OR REPLACE STREAM STREAM_TEST_FOR_CLOUDWATCH
ON TABLE TEST_FOR_CLOUDWATCH
APPEND_ONLY = TRUE;

--戻り値格納用テーブル作成

CREATE TABLE res_dummy_table (res VARIANT);
select * from res_dummy_table;

--検証用のTaskを作成

CREATE OR REPLACE TASK stream_to_cloudwatch_task
WAREHOUSE = VMH_DEV_BATCH_DAY
SCHEDULE = '1 MINUTE'
WHEN SYSTEM$STREAM_HAS_DATA('STREAM_TEST_FOR_CLOUDWATCH')
AS
INSERT INTO res_dummy_table -- ← 戻り値を捨てる用
SELECT
  send_to_cloudwatch(
    OBJECT_CONSTRUCT(
      'table', 'TEST_FOR_CLOUDWATCH',
      'event_type', 'INSERT',
      'data', OBJECT_CONSTRUCT(*),
      'ingested_at', CURRENT_TIMESTAMP()
    )
  )
FROM STREAM_TEST_FOR_CLOUDWATCH;




--検証用データを挿入
INSERT INTO TEST_FOR_CLOUDWATCH (ID, MESSAGE)
VALUES (1, 'alphaTest');

--Stream確認
SELECT * FROM STREAM_TEST_FOR_CLOUDWATCH;

--Task起動
ALTER TASK stream_to_cloudwatch_task RESUME;

--Task中止
ALTER TASK stream_to_cloudwatch_task SUSPEND;

--Task実行状況確認
SHOW TASKS LIKE 'STREAM_TO_CLOUDWATCH_TASK';
SELECT
  name,
  state,
  scheduled_time,
  completed_time,
  error_code,
  error_message
FROM TABLE(
  INFORMATION_SCHEMA.TASK_HISTORY(
    TASK_NAME => 'STREAM_TO_CLOUDWATCH_TASK',
    RESULT_LIMIT => 10
  )
)
ORDER BY scheduled_time DESC;