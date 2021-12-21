package io.github.soufianeodf.bigquerypoc;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.*;
import lombok.SneakyThrows;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.core.io.ClassPathResource;

import java.io.InputStream;

@SpringBootApplication
public class BigQueryPocApplication implements CommandLineRunner {

    public static void main(String[] args) {
        SpringApplication.run(BigQueryPocApplication.class, args);
    }

    @Override
    public void run(String... args) throws Exception {
        System.out.println("***** app is running *****");

        // TODO(developer): Replace these variables before running the sample.
        String projectId = "big-query-test-335715";
        String datasetName = "dataset_test";
        String tableName = "annual_enterprise_survey";
        String query =
                "SELECT *"
                        + " FROM `"
                        + projectId
                        + "."
                        + datasetName
                        + "."
                        + tableName
                        + "`"
                        + " LIMIT 20";
        query(query);
    }

    @SneakyThrows
    public static void query(String query) {
        try {
            GoogleCredentials credentials;
            try (InputStream credentialsPath = new ClassPathResource("big-query-service-account-credentials.json").getInputStream()) {
                credentials = ServiceAccountCredentials.fromStream(credentialsPath);
            }

            BigQuery bigquery = BigQueryOptions.newBuilder()
                    .setProjectId("big-query-test-335715")
                    .setCredentials(credentials)
                    .build()
                    .getService();

            QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(query).build();

            TableResult results = bigquery.query(queryConfig);

            results.iterateAll()
                    .forEach(row -> row.forEach(val -> System.out.printf("%s\n", val.toString())));

            System.out.println("Query performed successfully.");
        } catch (BigQueryException | InterruptedException e) {
            System.out.println("Query not performed \n" + e.toString());
        }
    }
}
