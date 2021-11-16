# Synthea Bulk Test Loader

To build:

```
mvn clean install
```

To run (argument 0 is the Synthea directory, argument 1 is the FHIR endpoint base URL):

```
java -jar target/synthea-bulk-loader.jar ../synthea/output/fhir http://localhost:8000
```

