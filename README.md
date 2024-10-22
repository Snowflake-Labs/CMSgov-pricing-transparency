# CMSGov pricing transperancy
**Dated:** Nov-2022

Demonstration of ingesting pricing transperancy data natively into Snowflake. (Without the need of an external tools and processes.)

## TOC
- Health plan price transparency
- Ingesting Pricing Transperancy Files
- Documentation Links
- Concerns and Limitations
- Liability
- Snowflake Features

### Health plan price transparency

In order to help Consumers(patients) understand the cost of various services offered by a health care provider, [CMS](https://www.cms.gov) has put
forth a regulation. As stated by [CMSgov](https://github.com/CMSgov/price-transparency-guide):

```
Health plan price transparency helps consumers know the cost of a covered item or service before receiving care. Beginning July 1, 2022, most group health plans and issuers of group or individual health insurance will begin posting pricing information for covered items and services. This pricing information can be used by third parties, such as researchers and app developers to help consumers better understand the costs associated with their health care. More requirements will go into effect starting on January 1, 2023, and January 1, 2024 which will provide additional access to pricing information and enhance consumers' ability to shop for the health care that best meet their needs.
```

These pricing files have to follow specific schemas and guidelines, which are detailed in this repo [CMSgov/price-transparency-guide](https://github.com/CMSgov/price-transparency-guide).

Some key points to note, when it comes to providing this file are:
- All machine-readable files must be made available via HTTPS. [Ref: Transport mechanism - HTTPS](https://github.com/CMSgov/price-transparency-guide#transport-mechanism---https)
- JSON is one of the supported format
- These machine-readable files must be made available to the public without restrictions [Ref: Public Discoverability](https://github.com/CMSgov/price-transparency-guide#public-discoverability)
- To update the machine-readable files monthly [Ref: Timing Updates For Machine-Readable Files](https://github.com/CMSgov/price-transparency-guide#timing-updates-for-machine-readable-files)
- [Ref: Schema](https://github.com/CMSgov/price-transparency-guide/tree/master/schemas)  

### Ingesting Pricing Transperancy Files
Here are some examples of files, from providers:
  - [Priority Health](https://www.priorityhealth.com/landing/transparency)
  - [Cigna](https://www.cigna.com/legal/compliance/machine-readable-files)
  - [United](https://transparency-in-coverage.uhc.com/)
  - *TO ADD MORE WEBSITES *

These files (ex: https://priorityhealthtransparencymrfs.s3.amazonaws.com/HMO.html) are large (multi GB). They are also distributed as compressed json files. In our example a <span style="color:blue">450MB</span> compressed ZIP file, turned out to be <span style="color:blue">13.5GB</span> uncompressed.

To load these large files; we typically follow the steps:
 1. Downloads the file from the provider
 2. Break the file into individual/smaller components
 3. Ingest each of the components, individually in sequence or parallel

Typically, client use external tools (ex: datastage/informatica/spark) to perform the above process. With the new "Dynamic file access" feature (currently in PrPr), we could parse these large files natively in Snowflake using Snowpark Python stored procedures.

Hence, this repo is a demonstration of how to ingest large files (json) using Snowflake. 

NOTE: downloading of files to a stage, is still out of scope. As this is not possible in Snowflake.

## Documentation Links
- [Doc: Ingestion process flow](./doc/Ingestion_process_flow.md)
- [Doc: Solution walk thru](./doc/Solution.md)
- [Doc: Setup and demo execution](./doc/Setup_demo_execution.md)

## Concerns and Limitations
   
  - The current implementation has been tested only with some samples from CIGNA & Priority Health; Further tests need to be conducxted on larger file sizes
  - Based on the file size, SLAs will vary. For example CIGNA 1TB sized file takes a longer time to ingest vs Priority Health data 10GB sized files takes around 30 min.
  - Based on your needs, further data pipelines would need to be build out and not provided in this demo.
  - Refreshing of external stagest/views will take some time. Hence would be better to have a post processing steps to ingest specific portions of the data based on your needs.

## Liability 
  - All code is shared as-is, It is upto the consumer to productionalize the work and update the code/functionality based on thier situation. We will not be supporting or liable. Effort will be taken to help the consumer to answer any queries related to the functionality 

### Snowflake Features

  - *[Python Dynamic file access](https://docs.snowflake.com/en/LIMITEDACCESS/udf-python-file-handler)*
  - *DAG & Tasks*
