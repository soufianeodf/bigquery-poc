package io.github.soufianeodf.bigquerypoc.model;

import com.google.api.gax.paging.Page;
import com.google.cloud.bigquery.*;
import com.google.cloud.bigquery.BigQuery.TableDataListOption;
import com.google.cloud.bigquery.JobStatistics.LoadStatistics;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;


import static com.google.cloud.bigquery.BigQuery.TableListOption.pageSize;

@Slf4j
public class BigQueryTable {

    public void createTable(BigQuery bigquery, String datasetName, String tableName, Schema schema) {
        try {
            TableId tableId = TableId.of(datasetName, tableName);
            TableDefinition tableDefinition = StandardTableDefinition.of(schema);
            TableInfo tableInfo = TableInfo.newBuilder(tableId, tableDefinition).build();
            bigquery.create(tableInfo);
            log.info("Table {} created successfully", tableName);
        } catch (BigQueryException e) {
            log.info("Table {} was not created, reason: {}", tableName, e.getMessage());
        }
    }

    public void deleteLabelTable(BigQuery bigquery, String datasetName, String tableName, Map<String, String> labels) {
        try {
            Table table = bigquery.getTable(TableId.of(datasetName, tableName));
            table.toBuilder().setLabels(labels).build().update();
            log.info("Table label deleted successfully for table: {}", tableName);
        } catch (BigQueryException e) {
            log.info("Table label was not deleted for table: {}, reason: {}", tableName, e.getMessage());
        }
    }

    public void deleteTable(BigQuery bigquery, String datasetName, String tableName) {
        try {
            boolean success = bigquery.delete(TableId.of(datasetName, tableName));
            if (success) {
                log.info("Table {} deleted successfully", tableName);
            } else {
                log.info("Table {} was not found", tableName);
            }
        } catch (BigQueryException e) {
            log.info("Table {} was not deleted, reason: {}", tableName, e.getMessage());
        }
    }

    public void getTableLabels(BigQuery bigquery, String datasetName, String tableName) {
        try {
            Table table = bigquery.getTable(TableId.of(datasetName, tableName));
            table.getLabels()
                    .forEach((key, value) -> System.out.println("Retrieved labels successfully"));
        } catch (BigQueryException e) {
            log.info("Label was not deleted, reason: {}" + e.getMessage());
        }
    }

    public void getTableInfo(BigQuery bigquery, String projectId, String datasetName, String tableName) {
        try {
            TableId tableId = TableId.of(projectId, datasetName, tableName);
            Table table = bigquery.getTable(tableId);
            log.info("Table info: " + table.getDescription());
        } catch (BigQueryException e) {
            log.info("Table {} not retrieved, reason: {}", tableName, e.getMessage());
        }
    }

    public void listTables(BigQuery bigquery, String projectId, String datasetName, int pageSize) {
        try {
            DatasetId datasetId = DatasetId.of(projectId, datasetName);
            Page<Table> tables = bigquery.listTables(datasetId, pageSize(pageSize));
            tables.iterateAll().forEach(table -> System.out.print(table.getTableId().getTable() + "\n"));

            log.info("Tables listed successfully of dataset: {}", datasetName);
        } catch (BigQueryException e) {
            log.info("Tables were not listed. Error occurred: " + e.getMessage());
        }
    }

    public void browseTable(BigQuery bigquery, String dataset, String table, int pageSize) {
        try {
            // Identify the table itself
            TableId tableId = TableId.of(dataset, table);

            // Page over X records. If you don't need pagination, remove the pageSize parameter.
            TableResult result = bigquery.listTableData(tableId, TableDataListOption.pageSize(pageSize));

            // Print the records
            result.iterateAll()
                    .forEach(row -> row
                            .forEach(fieldValue -> System.out.println(fieldValue.toString() + ", "))
                    );

            log.info("Query ran successfully");
        } catch (BigQueryException e) {
            log.info("Query failed to run, reason: {}", e.getMessage());
        }
    }

    public boolean tableExists(BigQuery bigquery, String datasetName, String tableName) {
        try {
            Table table = bigquery.getTable(TableId.of(datasetName, tableName));
            if (Optional.ofNullable(table).isPresent() && table.exists()) {
                log.info("Table {} already exist", tableName);
                return true;
            } else {
                log.info("Table {} not found", tableName);
                return false;
            }
        } catch (BigQueryException e) {
            log.info("Table {} not found, reason: {}", tableName, e.getMessage());
            return false;
        }
    }

    public void updateTable(BigQuery bigquery, String datasetName, String tableName) {
        try {
            Table table = bigquery.getTable(TableId.of(datasetName, tableName));
            bigquery.update(table.toBuilder().build());
            log.info("Table {} updated successfully", tableName);
        } catch (BigQueryException e) {
            log.info("Table {} was not updated, reason: {}", tableName, e.getMessage());
        }
    }

    public void updateTableDescription(BigQuery bigquery, String datasetName, String tableName, String newDescription) {
        try {
            Table table = bigquery.getTable(datasetName, tableName);
            bigquery.update(table.toBuilder().setDescription(newDescription).build());
            log.info("Table description updated successfully to {} for table: {}", newDescription, tableName);
        } catch (BigQueryException e) {
            log.info("Table description was not updated for table: {}, reason: {}", tableName, e.getMessage());
        }
    }

    public void loadLocalFile(BigQuery bigquery, String datasetName, String tableName, Path csvPath, FormatOptions formatOptions, boolean autodetectSchema) throws IOException, InterruptedException {
        try {
            TableId tableId = TableId.of(datasetName, tableName);

            WriteChannelConfiguration writeChannelConfiguration = WriteChannelConfiguration.newBuilder(tableId)
                                                                    .setFormatOptions(formatOptions)
                                                                    .setAutodetect(autodetectSchema)
                                                                    .build();

            // The location and JobName must be specified; other fields can be auto-detected.
            String jobName = "jobId_" + UUID.randomUUID();
            JobId jobId = JobId.newBuilder().setLocation("us").setJob(jobName).build();

            // Imports a local file into a table.
            try (TableDataWriteChannel writer = bigquery.writer(jobId, writeChannelConfiguration);
                 OutputStream stream = Channels.newOutputStream(writer)) {
                Files.copy(csvPath, stream);
            }

            // Get the Job created by the TableDataWriteChannel and wait for it to complete.
            Job job = bigquery.getJob(jobId);
            Job completedJob = job.waitFor();
            if (completedJob == null) {
                log.info("Job not executed since it no longer exists");
                return;
            } else if (completedJob.getStatus().getError() != null) {
                log.info("BigQuery was unable to load local file to the table due to an error: {}", job.getStatus().getError());
                return;
            }

            // Get output status
            LoadStatistics stats = job.getStatistics();
            log.info("Successfully loaded {} rows", stats.getOutputRows());
        } catch (BigQueryException e) {
            log.info("Local file not loaded, reason: {}" + e.getMessage());
        }
    }
}
