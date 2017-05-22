# Protempa i2b2 Tools

[Department of Biomedical Informatics](http://bmi.emory.edu), [Emory University](http://www.emory.edu), Atlanta, GA

## What does it do?
This project provides:
* a [Protempa](https://github.com/eurekaclinical/protempa) destination, `edu.emory.cci.aiw.i2b2etl.dest.I2b2Destination`, for loading data into an i2b2 database. Protempa destinations implement the `org.protempa.dest.Destination` interface and process output from the temporal abstraction process.
* a Protempa data source backend, `org.emory.cci.aiw.i2b2etl.dsb.I2b2DataSourceBackend`, for reading data from an i2b2 database. Protempa data source backends implement the `org.protempa.dsb.DataSourceBackend` interface.
* a Protempa knowledge source backend, `org.emory.cci.aiw.i2b2etl.dsb.I2b2KnowledgeSourceBackend`, for fetching clinical concepts from an i2b2 ontology cell. Protempa knowledge source backends implement the `org.protempa.ksb.KnowledgeSourceBackend` interface.

See the Protempa project's README for more details on Protempa's architecture.

## Version 3 development series
Version 3 is primarily focused around performance improvements.

## Version history
### Version 2.4.1

## Build requirements
* [Oracle Java JDK 8](http://www.oracle.com/technetwork/java/javase/overview/index.html)
* [Maven 3.2.5 or greater](https://maven.apache.org)

## Runtime requirements
* [Oracle Java JRE 8](http://www.oracle.com/technetwork/java/javase/overview/index.html)
* [Neo4j Community 2.2.2](http://neo4j.com)

## Building it
The project uses the maven build tool. Typically, you build it by invoking `mvn clean install` at the command line. For simple file changes, not additions or deletions, you can usually use `mvn install`. See https://github.com/eurekaclinical/dev-wiki/wiki/Building-Eureka!-Clinical-projects for more details.

## Maven dependency
```
<dependency>
    <groupId>org.eurekaclinical</groupId>
    <artifactId>aiw-i2b2-etl</artifactId>
    <version>version</version>
</dependency>
```

## Installation
Put the `aiw-i2b2-etl` jarfile and its dependencies in the classpath, and Protempa will automatically register the data source backend and knowledge source backend.

### Additional i2b2 destination installation
The i2b2 destination requires adding tables and stored procedures to the i2b2 data schema. A [Liquibase changelog]( https://github.com/eurekaclinical/aiw-i2b2-etl/blob/master/src/main/resources/dbmigration/i2b2-data-schema-changelog.xml) contains the DDL. The following files contain the SQL for creating the stored procedures:
* Oracle
  * [Package declaration](https://github.com/eurekaclinical/aiw-i2b2-etl/blob/master/src/main/resources/sql/eureka_package_oracle.sql)
  * [Package implementation](https://github.com/eurekaclinical/aiw-i2b2-etl/blob/master/src/main/resources/sql/eureka_package_body_oracle.sql)
* PostgreSQL
  * [Stored procedures](https://github.com/eurekaclinical/aiw-i2b2-etl/blob/master/src/main/resources/sql/eureka_postgresql.sql)

The i2b2 destination requires adding stored procedures to the i2b2 metadata schema. The following files contain the SQL for creating the stored procedures:
* Oracle
  * [Package declaration](https://github.com/eurekaclinical/aiw-i2b2-etl/blob/master/src/main/resources/sql/eureka_meta_package_oracle.sql)
  * [Package implementation](https://github.com/eurekaclinical/aiw-i2b2-etl/blob/master/src/main/resources/sql/eureka_meta_package_body_oracle.sql)
* PostgreSQL
  * [Stored procedures](https://github.com/eurekaclinical/aiw-i2b2-etl/blob/master/src/main/resources/sql/eureka_meta_postgresql.sql)


### Additional i2b2 knowledge source backend installation
See the [eurekaclinical-ontology](https://github.com/eurekaclinical/eurekaclinical-ontology) project's README for how to create an i2b2 metadata schema that the i2b2 knowledge source backend can read. Eureka! Clinical requires some extensions to the schema that i2b2 ships out of the box. The ontology project also contains Liquibase changelogs that install various common terminologies into such an i2b2 metadata schema.

## Using it
Here is an example:
```
import org.protempa.SourceFactory;
import org.protempa.backend.Configurations;
import org.protempa.bconfigs.ini4j.INIConfigurations;
import org.protempa.Protempa;
import org.protempa.dest.Destination;
import org.protempa.dest.map.MapDestination;
import org.protempa.query.DefaultQueryBuilder;
import org.protempa.query.Query;
import edu.emory.cci.aiw.i2b2etl.dest.I2b2Destination;
import edu.emory.cci.aiw.i2b2etl.dest.config.Configuration;

// An implementation of org.protempa.backend.Configurations provides the backends to use.
Configurations backends = new INIConfigurations(new File("src/test/resources"));
SourceFactory sourceFactory = new SourceFactory(backends.load("protempa-config.ini"));

// Use try-with-resources to ensure resources are cleaned up.
try (Protempa protempa = Protempa.newInstance(sourceFactory)) {
    DefaultQueryBuilder q = new DefaultQueryBuilder();
    q.setName("My test query");
    q.setPropositionIds(new String[]{"ICD9:Diagnoses", "ICD9:Procedures", "LAB:LabTest", "Encounter", "MED:medications", "VitalSign",     
        "PatientDetails"}); // an array of the concept ids of the data to retrieve and/or temporal patterns to compute
    Query query = protempa.buildQuery(q);

    // An implementation of org.protempa.dest.Destination processes output from the temporal abstraction process.
    Configuration i2b2Config = //implementation of edu.emory.cci.aiw.i2b2etl.dest.config.Configuration
    Destination dest = new I2b2Destination(i2b2Config); 
    protempa.execute(query, dest);
}
```

The `protempa-config.ini` might contain the following sections for configuring the i2b2 data source backend and knowledge source backend:
```
[edu.emory.cci.aiw.i2b2etl.dsb.I2B2DataSourceBackend]
dataSourceBackendId=HIVE
databaseId = JDBC URL for the data schema
username = username with privileges to read the data schema
password = data schema user's password
schemaName = name of the data schema

[edu.emory.cci.aiw.i2b2etl.ksb.I2b2KnowledgeSourceBackend]
databaseAPI = DATASOURCE # any setters in the implementation that have the @BackendProperty annotation.
databaseId = java:/comp/env/jdbc/I2b2KS
targetTable = EUREKAPHENOTYPEONTOLOGY

[org.protempa.backend.asb.java.JavaAlgorithmBackend]
```

The i2b2 Protempa destination loads data into i2b2 directly into its data schema rather than through its web services APIs for performance.

