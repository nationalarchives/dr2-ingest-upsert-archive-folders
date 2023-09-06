# DR2 Ingest Upsert Archive Folders

A Lambda that retrieves folders from a DB and creates/updates Entities based on that information 

[Link to the infrastructure code](https://github.com/nationalarchives/dp-terraform-environments)

## Environment Variables

| Name                      | Description                                    |
|---------------------------|------------------------------------------------|
| PRESERVICA_API_URL        | The Preservica API  url                        |
| PRESERVICA_SECRET_NAME    | The secret used to call the Preservica API     |
| ARCHIVE_FOLDER_TABLE_NAME | The name of the table to get folders from      |
