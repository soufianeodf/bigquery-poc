package io.github.soufianeodf.bigquerypoc;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.*;
import io.github.soufianeodf.bigquerypoc.model.BigQueryDataSet;
import io.github.soufianeodf.bigquerypoc.model.BigQueryTable;
import lombok.SneakyThrows;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.core.io.ClassPathResource;

import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;

@SpringBootApplication
public class BigQueryPocApplication implements CommandLineRunner {

    private static final BigQueryDataSet bigQueryDataSet = new BigQueryDataSet();
    private static final BigQueryTable bigQueryTable = new BigQueryTable();

    public static void main(String[] args) {
        SpringApplication.run(BigQueryPocApplication.class, args);
    }

    @SneakyThrows
    @Override
    public void run(String... args) {
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

        GoogleCredentials credentials;
        try (InputStream credentialsPath = new ClassPathResource("big-query-service-account-credentials.json").getInputStream()) {
            credentials = ServiceAccountCredentials.fromStream(credentialsPath);
        }

        BigQuery bigquery = BigQueryOptions.newBuilder()
                .setProjectId("big-query-test-335715")
                .setCredentials(credentials)
                .build()
                .getService();

//        query(query);

        String temporary_dataset = "temporary_dataset";

        if(bigQueryDataSet.datasetExists(bigquery, temporary_dataset)) {
            bigQueryDataSet.deleteDataset(bigquery, projectId, temporary_dataset);
        }

        bigQueryDataSet.createDataSet(bigquery, temporary_dataset);
//        bigQueryDataSet.deleteDataset(bigquery, projectId, "temporary_dataset");

//        Schema schema = Schema.of(
//                        Field.of("stringField", StandardSQLTypeName.STRING),
//                        Field.of("booleanField", StandardSQLTypeName.BOOL)
//        );

        String temporary_table = "temporary_table";

        if(bigQueryTable.tableExists(bigquery, temporary_dataset, temporary_table)) {
            bigQueryTable.deleteTable(bigquery, temporary_dataset, temporary_table);
        }
//        bigQueryTable.createTable(bigquery, temporary_dataset, temporary_table, schema);

//        Path jsonPath = FileSystems.getDefault().getPath(".", "my-data.csv");
        Path jsonPath = Paths.get(ClassLoader.getSystemResource("electronic-card-transactions.csv").toURI());
        bigQueryTable.loadLocalFile(bigquery, temporary_dataset, temporary_table, jsonPath, FormatOptions.csv(), true);
    }

    @SneakyThrows
    public static void query(BigQuery bigquery, String query) {
        try {

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
