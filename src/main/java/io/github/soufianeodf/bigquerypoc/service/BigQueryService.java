package io.github.soufianeodf.bigquerypoc.service;

import com.google.cloud.bigquery.*;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.nio.file.Path;
import java.nio.file.Paths;

@Slf4j
@Service
public class BigQueryService {

    private final BigQuery bigQuery;
    private final BigQueryDataSetService bigQueryDataSetService;
    private final BigQueryTableService bigQueryTableService;

    @Autowired
    public BigQueryService(@Qualifier("bigQueryBuilder") BigQuery bigQuery, BigQueryDataSetService bigQueryDataSetService, BigQueryTableService bigQueryTableService) {
        this.bigQuery = bigQuery;
        this.bigQueryDataSetService = bigQueryDataSetService;
        this.bigQueryTableService = bigQueryTableService;
    }

    @SneakyThrows
    public void run() {
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

//        query(query);

        String temporary_dataset = "temporary_dataset";

        if(bigQueryDataSetService.datasetExists(bigQuery, temporary_dataset)) {
            bigQueryDataSetService.deleteDataset(bigQuery, projectId, temporary_dataset);
        }

        bigQueryDataSetService.createDataSet(bigQuery, temporary_dataset);
//        bigQueryDataSet.deleteDataset(bigQuery, projectId, "temporary_dataset");

//        Schema schema = Schema.of(
//                        Field.of("stringField", StandardSQLTypeName.STRING),
//                        Field.of("booleanField", StandardSQLTypeName.BOOL)
//        );

        String temporary_table = "temporary_table";

        if(bigQueryTableService.tableExists(bigQuery, temporary_dataset, temporary_table)) {
            bigQueryTableService.deleteTable(bigQuery, temporary_dataset, temporary_table);
        }
//        bigQueryTableService.createTable(bigQuery, temporary_dataset, temporary_table, schema);

//        Path jsonPath = FileSystems.getDefault().getPath(".", "my-data.csv");
        Path jsonPath = Paths.get(ClassLoader.getSystemResource("datasets/electronic-card-transactions.csv").toURI());
        bigQueryTableService.loadLocalFile(bigQuery, temporary_dataset, temporary_table, jsonPath, FormatOptions.csv(), true);
    }

    @SneakyThrows
    public void query(String query) {
        try {

            QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(query).build();

            TableResult results = bigQuery.query(queryConfig);

            results.iterateAll()
                    .forEach(row -> row.forEach(val -> System.out.printf("%s\n", val.toString())));

            log.info("Query performed successfully.");
        } catch (BigQueryException | InterruptedException e) {
            log.info("Query not performed, reason: {}", e.getMessage());
        }
    }
}
