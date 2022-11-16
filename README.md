# CMSGov pricing transperancy

**Dated:** Nov-2022

Demonstration of ingesting pricing transperancy data natively into Snowflake. (Without the need of an external tools and processes.)

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
  - *TO ADD MORE WEBSITES *

These files (ex: https://priorityhealthtransparencymrfs.s3.amazonaws.com/HMO.html) are large (multi GB). They are also distributed as compressed json files. In our example a <span style="color:blue">450MB</span> compressed ZIP file, turned out to be <span style="color:blue">13.5GB</span> uncompressed.

To load these large files; we typically follow the steps:
 1. Downloads the file from the provider
 2. Break the file into individual/smaller components
 3. Ingest each of the components, individually in sequence or parallel

Typically, client use external tools (ex: informatica/spark) to perform the above process. With the new "Dynamic file access" feature (currently in PrPr), we could parse these large files natively in Snowflake using Snowpark Python stored procedures.

Hence, this repo is a demonstration of how to ingest large files (json) using Snowflake. 

NOTE: downloading of files to a stage, is still out of scope. As this is not possible in Snowflake.

## Setup and Demo execution
- [Doc: SQL-worksheet-walkthru](./doc/SQL-worksheet-walkthru.md)

Ingested sample of in_network_rates:negotiated_rates 

![](doc/images/in_network_rates.png)

### Post ingestion sqls
Here are some queries that can be issued against the data.

- [Doc: Sample-post-ingestion-queries](doc/Sample-post-ingestion-queries.md)

---

## Solution
- [Doc: Solution walk thru](./doc/Solution.md)

## Concerns and Limitations

### Snowflake Features

  - *Python Dynamic file access:* is currently in PrPr.
