# General info

## Background
- Prepare pipeline that sync data between BeProduct and DTC
- Both BeProduct and DTC are SaaS with RESTful API - sparsely documented
- BeProduct has a python SDK further abstracting part of its API

## SDK & API specs
- BeProduct SDK: https://python.beproduct.com/ (doc) https://github.com/BeProduct/BeProduct.Python.SDK (implementation)
- BeBroduct API: https://developers.beproduct.com/swagger/v1/swagger.json  (Swagger JSON)
- BeProduct environmnts to consider: only 1 environment, data split by Folder
- DTC API & endpoints: ./dtc/DTC-api-2026-05-08.json (postman API project dump), ./dtc/DTC-api-2025-05.pdf (description and example)
- DTC environments to consider: UAT, PRD

## Basic assumptions
- BeProduct, DTC are JSON stores
- BeProduct roughly follows a Parent - Child data model. "STYLE" is the header, link with "Size", "Colorways", "BOM" that store details
- DTC is a Excel-like genric data entry tool. In this project relavent data are store denormalized in a flat wide worksheet.

## General directions
- DTC prefer patch API (=update)to push API (=overwrite)
    - "Document" defines Json schema
    - "Request" instantiate "Documents", hold metadata e.g. RequestName, Owner etc
    - "Sheet" hold actual data of a "Request". As of today, All Requests have 1-to-1 relationship to a Sheet.
    - "View" is subset of fields of a "Document" applicable to any "Request"/"Sheet" of that same type.
    - Document identified by document_id
    - Data identified by (request_id + sheet_id)
    - View identifed by view_id
    
- BeProduct data domains: Styles, Materials, Colors, Images, Blocks, Directory, MasterData
- If Databricks is involved, tables to be put under Unity Catalog lft.beproduct
    - Access ADB with 'databricks' CLI

- BeProduct has full timestamp with timezone (UTC). DTC output timestamp as UTC (on retrieve). DTC expects input timestamp in user profile timezone, treat as +0800 HKT in current setup.

# Phase 1

## Objectives
- Sync selected fields from BeProduct "Style" to DTC "WIP" Requests
- Customer to focus on = KTB
- On DTC, denormalized to STYLE x COLOR x BOM
- The integration workflow to be scheduled and run entirely in Databricks as job
- Data that should be synced
    - Read from BeProduct: [Product Status, Image, LF Style number, Description, Product Category, Product Sub Category, Division, Brand, Color, Garment Finish, Tech Pack Stage, Group, Fabric Placement, Fabric Article]
    - Upsert to corresponding sheet/row in DTC: [Product Status, Style Image, LF Style#, Style Description, Class, Sub Class, Division, Brand, Color / Wash, Garment Finish, Tech Pack Stage,Fabric Group, Placement, Mill Fabric Article #]
    
    
## known data structure and access patterns
- BeProduct: Reference streamlit application implementation in ./app/ui/main.py
    - BeProduct to Databricks: project artifacts in ./databricks/
    - A style is uniquely identified by Customer + Brand + LFStyle# + Season
        - Customer = Folder Name
        - Brand = $."headerData"."fields"[name ="brand"]
        - LFStyle# = $."headerData"."fields"[name = "LF Style Number"]
        - Season = $."headerData"."fields"[name = "SEASON"]
    - 1 Style link with 1 Colorway
        - Data are in $."headerData"."fields"
        - A Colorway can contain more than 1 color name
            - Array $."colorways"
            - Interested in $."colorways"[]."colorName"
        - i.e. 1 style : n color = n DTC rows
    - 1 Style link with 1 BOM
        - Each BOM is a datagrid of Material (i.e. Fabric) + Color
        - There is currently NO direct API to retrieve BOM data
        - At this moment hard code each style to have 2 BOM lines: (Group, Fabric Placement) = [("Main Fabric",  $.headerData[id="core_main_material"].value),('Fabric",value of $.headerData[id="Core_main_material2"].value)].
        

- DTC: Reference ./EXPLORATION_SUMMARY.md, ./databricks/dtc/README.md
    - DTC to Databrricks: project artifacts in ./databricks/dtc/    
    - This phrase primarily focus on Requests of the type "WIP" document using the "Full Version" view (i.e. complete data)
        - Workspace = "<Customer>", i.e. "KTB"
        - Document = "<Customer> WIP", i.e. "KTB WIP" in this instance 
    - DTC Requests are named "<Customer> <SeasonCode> <Brand>"
        - SeasonCode is always 4 character string, e.g. SS26, SS27, FW28
        - Brand is everything after SeasonCode including spaces
        - SeasonCode = SSYY = 2-characters season + 2-digit year.
            - use databricks lft.beproduct.dtc_seacode_mapping (customer, BeProduct Season, DTCcode)            
            - DTC season = SSYY; BeProduct "Year" = 20YY
            - all other season code log errors
    - Use DTC "patch" API for existing (style + color + material)   >> keep "sheetId" / "rowIndex". "rowID" can be used in lieu of "rowIndex" 
        - put in all fields of that row, even if the value is unchanged
        - ref page 18 of pdf: PATCH /v1/sheets/{sheetId}/views/{viewId}

    - Use DTC "patch" API also for new (style + color + material)  >> assign a new rowIndex (max(rowIndex) of current sheet + 1)
        - put in all fields of that row that have value
        
    
    - Retrieve DTC data: get_requests(workspace + document), returns ALL matching (request ID + sheet ID)
    - Patch data: patch(sheet id + view ID [+ rowIndex] + delta data), preferably only affected row (i.e. with rowIndex or rowID)

- Mapping process 
    - match [customer + brand + style# + season + color + material]    
    - new Seasoncode / Brand: need new DTC Sheet
        - Create sheet () return request id + sheet id
        - Then use Patch api to INSERT (dense_rank() over partition by brand/customer/seasonId)
        - For each row do ImageInsert flow below
        - "create sheet" API on p.15 - POST /v1/sheets. This should return both requestId and sheetId on success.
    - new rows on existing sheet:
        - patch api require assign rowIndex (max(DTC sheet)+1)
        - For each of these rows, do ImageInsert flow
    - Update existing row:
        - BeProduct_Style_modifiedAt > DTC_row_Updated_at *beware of timezone*
        - keep request ID, sheet ID, row ID, row Index
        - Non image fields: assign those interested to DTC row
        - Image:  separate from and after row CRUD, for each row updated
            - if DTC.Style_Image is blank, do ImageInsert flow
            - else next row - assume Front image will not change
            "headerData"."frontImage" only. 
             
    - In case BeProduct rows < DTC rows, *"mark (extra DTC rows) Product Status = Drop"*, do not "DELETE rowid"
    
    - **separate add / update image into dedicated notebook** so we can tackle it later.

    - ImageInsert flow
        - If BP "headerData"."frontImage"."origin" is a proper url, use DTC API /v1/sheets/{sheetId}/views/{viewId}/images?rowindex={number}&columnname={text} to push binary into DTC
        - Take "origin" from BeProduct CDN. Treat as binary data.
        - "add sheet image" API on p.16 - POST /v1/sheets/{sheetId}/views/{viewId}/images?rowindex={number}&columnname={text} using multipart/form-data.
        

        
## Tentative flow
- Batch based
1. download BeProduct: Masters + Style + ColorWay
2. download DTC: all WIP Document of <workspace>. Decode Request name & Keep track of "sheetID - <brand> <session>" mapping
3. Join (1) to form full view of current BeProduct Image
4. Compare (2) to (1):
    4.1. For those "1" not matching any "2" > mark for PUSH
        4.1.1. Determine if the DTC Request exists. If not, LOG ERROR
        4.1.2. If exist, push the rows to that Request
    4.2: For those "1" that has matching row(S) in "2", mark for Patch
        4.2.1. For each ROW, call a patch on that Request_id + row_id

