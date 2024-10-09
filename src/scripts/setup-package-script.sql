-- ################################################################
-- Create SHARED_CONTENT_SCHEMA to share provider data in the application package
-- This script runs whenever we deploy the application
-- whatever data provider needs to share with the application, should have access granted in this script
-- ################################################################
USE {{ package_name }};
create schema if not exists shared_content_schema;

use schema shared_content_schema;
create or replace view EPC_V as select * from ESG_PRD.RAW.EPC;
create or replace view HPI_V as select * from ESG_PRD.RAW.HPI;
create or replace view EMISSIONSINTENSITY_V as select * from ESG_PRD.RAW.EMISSIONSINTENSITY;
create or replace view ELECTRICITYUSEPROPORTION_V as select * from ESG_PRD.RAW.ELECTRICITYUSEPROPORTION;

grant usage on schema shared_content_schema to share in application package {{ package_name }};
grant reference_usage on database ESG_PRD to share in application package {{ package_name }};
grant select on view EPC_V to share in application package {{ package_name }};
grant select on view HPI_V to share in application package {{ package_name }};
grant select on view EMISSIONSINTENSITY_V to share in application package {{ package_name }};
grant select on view ELECTRICITYUSEPROPORTION_V to share in application package {{ package_name }};

