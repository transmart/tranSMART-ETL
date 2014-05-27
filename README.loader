tranSMART Groovy ETL Pipeline
=============================

Purpose
--------------

This pipeline can be used to load MeSH, Entrez, Gene Ontology, KEGG Pathway,
Ingenuity Pathway, gene expression or SNP array platform's annotation files from
Gene Expression Omnibus (GEO), analyzed Gene Expression data from Omicsoft, SNP
genotyping and copy number data, etc into tranSMART.

Building
--------

You need Maven to build the project. Running `mvn package` will create two _jar_
files in `target/`: one with and another without the project dependencies.

Configuration Files
---------------------

Once you know which dataset needs to be loaded, a corresponding configuration
file needs to be create. The `conf/` subdirectories has samples of these files.
The application expects to find these files under a directory named `conf` under
the working directory. The following is a list of configuration files and their
usage:

  * `log4j.properties` – is global and control log4j behavior.

  * `Common.properties`  - is global and used to configure the loader's JDBC
	connection string.  Modify url and password accordingly.

  * `MeSH.properties` – is used to load MeSH diseases and their synonyms.

  * `Observation.properties` – is used to load observation data into
	BIO\_OBSERVATION and its related tables.

  * `Pathway.properties` – is used to load Gene Ontology, KEGG and Ingenuity
	Pathway data.

  * `SNP.properties` – is used to load Affymetrix GenomeWideSNP 6 SNP Array
	data, which is generated from Affymetrix's apt-probeset-genotype (genotyping
	data) and  apt-copynumber-workflow (copy number data).

  * `Annotation.properties` – is used to load platform GPL files from GEO and
	Affymetrix.

  * `Omicsoft.properties` – is used to analyzed Gene Expression data from
	Omicsoft.


How to Run
---------------

After you modify your configuration file properly, use the entry points in the
following classes:

  * MeSH: `org.transmartproject.pipeline.disease.MeSH`

  * Observation: `org.transmartproject.pipeline.observation.Observation`

  * Pathway:
	* `org.transmartproject.pipeline.pathway.GeneOntology`
	* `org.transmartproject.pipeline.pathway.Ingenuity`
	* `org.transmartproject.pipeline.pathway.KEGG`

  * SNP:
	1. `org.transmartproject.converter.SNPFormatter`
	2. `org.transmartproject.pipeline.plink.PlinkLoader`

  * Annotation: `org.transmartproject.pipeline.annotation.AnnotationLoader`

  * Omicsoft: `org.transmartproject.pipeline.omicsoft.OmicsoftLoader`

  * Dictionaries:
	* `org.transmartproject.pipeline.dictionary.MiRBaseDictionary`
	* `org.transmartproject.pipeline.dictionary.UniProtDictionary`

The command would look like `java -jar target/loader-jar-with-dependencies.jar
<classname>` or `java -cp target/loader-jar-with-dependencies.jar
<classname>`, followed by arguments.

Oracle
---------------
Add to pom.xml new dependency:
`
<dependency>
    <groupId>ojdbc</groupId>
    <artifactId>ojdbc</artifactId>
    <version>14</version>
</dependency>
`
Install to local maven repository
mvn install:install-file -DgroupId=ojdbc -DartifactId=ojdbc -Dversion=14 -Dpackaging=jar -Dfile=lib/ojdbc14.jar -DgeneratePom=true
