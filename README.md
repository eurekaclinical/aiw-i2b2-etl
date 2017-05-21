# Protempa i2b2 Destination

[Department of Biomedical Informatics](http://bmi.emory.edu), [Emory University](http://www.emory.edu), Atlanta, GA

## What does it do?
It is a [Protempa](https://github.com/eurekaclinical/protempa) destination for loading data into an i2b2 database. Protempa destinations implement the `org.protempa.dest.Destination` interface and process output from the temporal abstraction process. See the Protempa project's README for more details on Protempa's architecture.

## Version 3 development series


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

## Using it
Just put the `aiw-i2b2-etl` jarfile and its dependencies in the classpath, and Protempa will automatically register it.

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

This Protempa destination loads data into Neo4j using Neo4j's embedded mode for performance. It will shut Neo4j server down at the beginning of the data load, and start it after the data load has ended.

## License
Unlike other Eureka! Clinical projects, this one is available under [GNU General Public License version 3](http://www.gnu.org/licenses/gpl-3.0-standalone.html) due to the licensing of the Neo4j libraries on which this project depends.
