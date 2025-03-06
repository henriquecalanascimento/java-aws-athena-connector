//Package
package com.java.aws.athena.connector;

// Import AWS Region
import software.amazon.awssdk.regions.Region;

//Import AWS - Athena
import software.amazon.awssdk.services.athena.AthenaClient;

public class Main {
        public static void main(String[] args) {

                // AWS Credentials
                String accessKey = "";
                String secretKey = "";

                // Bucket Output
                String bucketOutput = "s3://";

                // Database
                String database = "";

                // Query
                String query = "SELECT * FROM {} limit 10";

                // Region
                Region region = Region.US_EAST_1;

                // Get Athena client
                AthenaClient client = Athena.authAthena(accessKey, secretKey, region);

                // Id Query
                String queryExecutionId = Athena.buildQuery(client, database, bucketOutput, query);

                // Results
                Athena.queryResults(client, queryExecutionId);
        }

}
