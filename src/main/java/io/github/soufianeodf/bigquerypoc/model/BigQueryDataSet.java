package io.github.soufianeodf.bigquerypoc.model;

import com.google.api.gax.paging.Page;
import com.google.cloud.bigquery.*;
import com.google.cloud.bigquery.BigQuery.DatasetDeleteOption;
import com.google.cloud.bigquery.BigQuery.DatasetListOption;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Map;

import static com.google.cloud.bigquery.BigQuery.TableListOption.*;

@Slf4j
public class BigQueryDataSet {

    public void createDataSet(BigQuery bigquery, String datasetName) {
        try {
            DatasetInfo datasetInfo = DatasetInfo.newBuilder(datasetName).build();

            Dataset newDataset = bigquery.create(datasetInfo);
            String newDatasetName = newDataset.getDatasetId().getDataset();
            log.info("Dataset {} created successfully", newDatasetName);
        } catch (BigQueryException e) {
            log.info("Dataset was not created, reason: {}", e.getMessage());
        }
    }

    public void deleteDataset(BigQuery bigquery, String projectId, String datasetName) {
        try {
            DatasetId datasetId = DatasetId.of(projectId, datasetName);
            boolean success = bigquery.delete(datasetId, DatasetDeleteOption.deleteContents());
            if (success) {
                log.info("Dataset {} deleted successfully", datasetName);
            } else {
                log.info("Dataset {} was not found", datasetName);
            }
        } catch (BigQueryException e) {
            log.info("Dataset {} was not deleted, reason: {}", datasetName, e.getMessage());
        }
    }

    public void deleteDatasetAndContents(BigQuery bigquery, String projectId, String datasetName) {
        try {
            DatasetId datasetId = DatasetId.of(projectId, datasetName);
            // Use the force parameter to delete a dataset and its contents
            boolean success = bigquery.delete(datasetId, BigQuery.DatasetDeleteOption.deleteContents());
            if (success) {
                log.info("Dataset {} deleted with contents successfully", datasetName);
            } else {
                log.info("Dataset {} was not found", datasetName);
            }
        } catch (BigQueryException e) {
            log.info("Dataset {} was not deleted with contents, reason: {}", datasetName, e.getMessage());
        }
    }

    public void deleteLabelDataset(BigQuery bigquery, String datasetName, Map<String, String> labels) {
        try {
            Dataset dataset = bigquery.getDataset(datasetName);
            dataset.toBuilder().setLabels(labels).build().update();
            log.info("Dataset {} label deleted successfully", datasetName);
        } catch (BigQueryException e) {
            log.info("Dataset {} label was not deleted, reason: {}", datasetName, e.getMessage());
        }
    }

    public boolean datasetExists(BigQuery bigquery, String datasetName) {
        try {
            Dataset dataset = bigquery.getDataset(DatasetId.of(datasetName));

            if (dataset != null) {
                log.info("Dataset {} already exists", datasetName);
                return true;
            } else {
                log.info("Dataset {} not found", datasetName);
                return false;
            }
        } catch (BigQueryException e) {
            log.info("Something went wrong, reason: {}", e.getMessage());
            return false;
        }
    }

    public void getDatasetLabels(BigQuery bigquery, String datasetName) {
        try {
            Dataset dataset = bigquery.getDataset(datasetName);

            dataset.getLabels()
                    .forEach((key, value) -> System.out.println("Retrieved labels successfully"));

        } catch (BigQueryException e) {
            log.info("Label was not found, reason: {}" + e.getMessage());
        }
    }

    public void getDatasetInfo(BigQuery bigquery, String projectId, String datasetName, int pageSize) {
        try {
            DatasetId datasetId = DatasetId.of(projectId, datasetName);
            Dataset dataset = bigquery.getDataset(datasetId);

            // View dataset properties
            String description = dataset.getDescription();
            System.out.println(description);

            // View tables in the dataset
            // For more information on listing tables see:
            // https://javadoc.io/static/com.google.cloud/google-cloud-bigquery/0.22.0-beta/com/google/cloud/bigquery/BigQuery.html
            Page<Table> tables = bigquery.listTables(datasetName, pageSize(pageSize));

            tables.iterateAll().forEach(table -> System.out.print(table.getTableId().getTable() + "\n"));

            log.info("Dataset info retrieved successfully for dataset: {}", datasetName);
        } catch (BigQueryException e) {
            log.info("Dataset info not retrieved for dataset: {}", e.getMessage());
        }
    }

    public void listDatasets(BigQuery bigquery, String projectId, int pageSize) {
        try {
            Page<Dataset> datasets = bigquery.listDatasets(projectId, DatasetListOption.pageSize(pageSize));

            if (datasets == null) {
                log.info("Dataset does not contain any models");
                return;
            }

            datasets.iterateAll()
                    .forEach(dataset -> System.out.printf("Success! Dataset ID: %s ", dataset.getDatasetId()));

        } catch (BigQueryException e) {
            log.info("Project does not contain any datasets");
        }
    }

    public void updateDatasetAccess(BigQuery bigquery, String datasetName, Acl newEntry) {
        try {
            Dataset dataset = bigquery.getDataset(datasetName);

            // Get a copy of the ACLs list from the dataset and append the new entry
            ArrayList<Acl> acls = new ArrayList<>(dataset.getAcl());
            acls.add(newEntry);

            bigquery.update(dataset.toBuilder().setAcl(acls).build());
            log.info("Dataset Access Control updated successfully for dataset: {}", datasetName);
        } catch (BigQueryException e) {
            log.info("Dataset Access control was not updated for dataset: {}", datasetName);
        }
    }
}
