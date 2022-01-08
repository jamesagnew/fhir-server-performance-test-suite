# Synthea Bulk Test Loader

To build:

```
mvn clean install
```

# Test 1: Load Data

```
java -cp target/synthea-bulk-loader.jar bulkload.Test01_LoadDataUsingTransactions ../synthea/output/fhir/ "ENDPOINT1,ENDPOINT2,..." "username:password" 70 true
```

# Test 2: Run Searches

