# integration_automation_googlesheets_todataframe

Automated ingestion from a business system (e.g., SharePoint/Sheets) every 5 minutes: validate, batch, dedupe, and upsert to BigQuery; include error handling.

Google Sheet Data Source
|
|
|
V
Upsert Dataframe to u_literature_prod in PostGres
|
|
V
Truncate Load to Google BigQuery (Due to Account limitations, workarounds are implemented) 

Process:
<img width="235" height="1174" alt="image" src="https://github.com/user-attachments/assets/fd719112-d3ce-402b-a956-341c0a9ff0d3" />
<img width="500" height="973" alt="image" src="https://github.com/user-attachments/assets/67b7e87f-6071-4fc3-b275-a1fec7a6173c" />

Libraries Used:
SqlAlchemy, Pandas, GoogleCloud API

DataSource:
https://docs.google.com/spreadsheets/d/15OnfUErAK7ETBD4X-pRyAtgeD--PQeYODL9xPPXWPww/export?gid=131054257
