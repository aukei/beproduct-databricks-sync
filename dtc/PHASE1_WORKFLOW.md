
Base on latest DTC admin guidelines: 
- For phase 1, Customer = KTB (user param), workspace = ${custome}> = KTB (no change), Document = "${customer} WIP" = "KTB WIP" (no change), view = "WIP_ITS_USE" (no change, user param)
- All "Requests" in scope are named "${customer} ${DTC seasoncode} ${brands}" e.g. "KTB FW26 Wrangler". There are many requests of the same Document of other names convention in the workspace, ignore them.
- Tentative flow:
    1. Upload all Requests to Databricks, 1 table for each ${workspace} + ${customer}, so the Databricks DTC table contains additional [Request ref, Seasoncode, Brands] cols.
        1a. Perhaps an extra control table that stores [request ID, view id, customer, seasoncode, brands, last_extracted, msgs] for sync control
        1b. A Request / View can be empty i.e. no rows
    2. Ensure BeProduct Style is synced - at least last last_extracted should be of same day.
    3. Data massage - reference "docs/beproduct_style_interested_fields.txt"
        3a. BeProduct -> DTC: perform upsert on DTC table using BeProduct transformed table on (LF Style#, Seasoncode, Brand), update all indicated non-key fields EXCEPT "Style Image", or write new rows with key fields.
        3b. RowIndex: it is effectively the row_number() over (partition by Seasoncode, Brands) but DTC allow sparse rowIndex. Therefore in case of update, keep original RowIndex. In case of insert, assign coalesce(max(RowIndex) over (partition by Seasoncode, Brands) + 1,1)
        3c. log exceptions to sync log table.
    4. Push back massaged DTC data
        4a. Require delta push. Updated / new rows should have a modified timestamp > last_extracted tracking
        4b. for each update row, use PATCH api to push all non key fields. 
        4c. for each new row, use PATCH api to push all field all fields, with calculated RowIndex value.
        4d. log detail exceptions to sync log table.
    5. No push back to BeProduct in this phase.
