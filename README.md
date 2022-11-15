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
The solution involves using Python Dynamic file access feature and Snowpark stored procedures to parse
the data from the stage and ingest the same into Snowflake. The 

The Snowpark stored procedure, will not be performing a copy of the file into local sandbox environment, as
sometimes even the compresssed ones can be larger in size, greater than 500 MB. The dynamic file access feature
allow us to read the file directly from the stage.

### Why Snowpark (python) stored procedure ?
  The pricing transperancy files uncompressed can be multi GB order magnitude larger, Parsing these large 
  files using UDTF is not possible as there is a time limit of 30 seconds for the UDTF to finish execution. 
  Otherwise you will get a REQUEST_TIMEOUT exception.

  Stored procedures are however not constrained by such low time limits, they can run for multiple hours, ideally
  in a dedicated warehouse.

  We did look at adopting Java Stored Procedure, however this is a [limitation](https://docs.snowflake.com/en/sql-reference/stored-procedures-java.html#limitations) around reading and writing of files. Hence using we ruled this out.

  ![](./doc/images/limitation_java_stored_proc.png)

  When using Stored procedure, the sandbox environment is limited to 0.5GB and some more, based on the warehouse type
  being used. In any case, these pricing transperancy files when uncompressed could potentially exceed these space limitations
  hence we need to implement a solution that should be able to process the file as a stream, idealy a compressed file stream.

  The [IJSON](https://pypi.org/project/ijson/) library is a widely adopted library in the python space, for processing 
  large json files. This library was default available in the Snowflake Anaconda channel too. The library had the 
  unique functionality to parse the json file, which are streamed and does not require to load the entire file. Thus 
  Snowpark python stored procedure was the choice for the implementation.

### Can we load the entire file into a single record ?
  The short answer NO. You also cannot create a generic solution that will ingest the data into a table.
  There are some close relations between various segments.

  More over there is a vast multiplicity on segments, makes the process complex. Without understanding these
  and blindly loading the data will result in 16MB constraint of Snowflake.

  Hence a good understanding of the data schema is a requirement. 
    - [doc: in-network-rates](https://github.com/CMSgov/price-transparency-guide/tree/master/schemas/in-network-rates)

  For the in-network-rates implementation, we split and store on the following fields
    - negotiated_rates
    - bundled_codes
    - covered_services

## Sample files

  As per CMS all the major health care providers have to share the data. We found the following websites where the
  data is present:

  - [Priority Health](https://www.priorityhealth.com/landing/transparency)
  - *TO ADD MORE WEBSITES *

---
## Concerns and Limitations

### Snowflake Features

  - *Python Dynamic file access:* is currently in PrPr.
  
### Speeding up ingestion

  This demonstration showcases how to parse the large file; the data ingestion is slow on the current implementation. However there are possiblities to parallelize and improve the speed of ingestion. These improvements are left to adopters of this demo.