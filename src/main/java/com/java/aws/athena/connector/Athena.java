//Package
package com.java.aws.athena.connector;

//Time
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.TimeUnit;

//AWS Auth
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;

//Region
import software.amazon.awssdk.regions.Region;

//Client
import software.amazon.awssdk.services.athena.AthenaClient;

//AWS Athena
import software.amazon.awssdk.services.athena.model.GetQueryExecutionRequest;
import software.amazon.awssdk.services.athena.model.GetQueryExecutionResponse;
import software.amazon.awssdk.services.athena.model.GetQueryResultsRequest;
import software.amazon.awssdk.services.athena.model.GetQueryResultsResponse;
import software.amazon.awssdk.services.athena.model.QueryExecutionContext;
import software.amazon.awssdk.services.athena.model.QueryExecutionState;
import software.amazon.awssdk.services.athena.model.ResultConfiguration;
import software.amazon.awssdk.services.athena.model.StartQueryExecutionRequest;
import software.amazon.awssdk.services.athena.model.StartQueryExecutionResponse;

public class Athena {

        // Private constructor
        private Athena() {

        }

        public static AthenaClient authAthena(String accessKey, String secretKey, Region region) {

                // Athena Client Create
                StaticCredentialsProvider credentialsProvider = StaticCredentialsProvider.create(
                                AwsBasicCredentials.create(accessKey, secretKey));

                AthenaClient athenaClient = AthenaClient.builder()
                                .credentialsProvider(credentialsProvider)
                                .region(region)
                                .build();

                return athenaClient;

        }

        public static String buildQuery(AthenaClient client, String database, String bucketOutput, String query) {

                QueryExecutionContext queryExecutionContext = QueryExecutionContext.builder()
                                .database(database)
                                .build();

                ResultConfiguration resultConfiguration = ResultConfiguration.builder()
                                .outputLocation(bucketOutput)
                                .build();

                // Run: Query
                StartQueryExecutionRequest startQueryExecutionRequest = StartQueryExecutionRequest.builder()
                                .queryString(query)
                                .queryExecutionContext(queryExecutionContext)
                                .resultConfiguration(resultConfiguration)
                                .build();

                StartQueryExecutionResponse startQueryExecutionResponse = client
                                .startQueryExecution(startQueryExecutionRequest);

                // Obtaining the Query ID
                String queryExecutionId = startQueryExecutionResponse.queryExecutionId();

                return queryExecutionId;

        }

        public static void queryResults(AthenaClient client, String queryExecutionId) {

                Duration timeout = Duration.ofMinutes(1); // Define timeout
                Instant startTime = Instant.now();

                while (true) {

                        GetQueryExecutionRequest getQueryExecutionRequest = GetQueryExecutionRequest.builder()
                                        .queryExecutionId(queryExecutionId)
                                        .build();

                        GetQueryExecutionResponse getQueryExecutionResponse = client
                                        .getQueryExecution(getQueryExecutionRequest);

                        QueryExecutionState queryExecutionState = getQueryExecutionResponse.queryExecution().status()
                                        .state();

                        if (queryExecutionState == QueryExecutionState.SUCCEEDED) {
                                // Process result
                                GetQueryResultsRequest getQueryResultsRequest = GetQueryResultsRequest.builder()
                                                .queryExecutionId(queryExecutionId)
                                                .build();
                                GetQueryResultsResponse getQueryResultsResponse = client
                                                .getQueryResults(getQueryResultsRequest);
                                System.out.println(getQueryResultsResponse.resultSet().toString());
                                break;

                        } else if (queryExecutionState == QueryExecutionState.FAILED) {
                                // Show the error
                                System.out.println("Query error: " + getQueryExecutionResponse
                                                .queryExecution().status().stateChangeReason());
                                break;

                        } else if (Duration.between(startTime, Instant.now()).compareTo(timeout) > 0) {
                                // Timeout
                                System.out.println("Timeout");
                                break;
                        }
                        try {

                                TimeUnit.SECONDS.sleep(1); // Esperar por 1 segundo
                        }

                        catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                        }
                }

        }

}
