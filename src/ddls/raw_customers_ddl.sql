CREATE OR REPLACE TABLE customers (
customer_id INT IDENTITY,
first_name STRING,
last_name STRING,
email STRING,
phone STRING,
date_of_birth DATE,
gender STRING,
street_address STRING,
city STRING,
state STRING,
zip_code STRING,
country STRING,
registration_date TIMESTAMP_TZ,
last_login TIMESTAMP_TZ,
is_active BOOLEAN,
email_verified BOOLEAN,
phone_verified BOOLEAN,
marketing_opt_in BOOLEAN,
preferred_contact_method STRING,
customer_segment STRING,
generated_at TIMESTAMP_TZ,
loaded_at TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(),
batch_id TIMESTAMP_TZ,
source STRING
)


