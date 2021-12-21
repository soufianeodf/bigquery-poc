package io.github.soufianeodf.bigquerypoc.config;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import lombok.SneakyThrows;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;

import java.io.InputStream;

@Configuration
public class BigQueryConfig {

    @SneakyThrows
    @Bean
    @Qualifier("bigQueryBuilder")
    public BigQuery getBigQuery() {
        String projectId = "big-query-test-335715";

        GoogleCredentials credentials;
        try (InputStream credentialsPath = new ClassPathResource("big-query-service-account-credentials.json").getInputStream()) {
            credentials = ServiceAccountCredentials.fromStream(credentialsPath);
        }

        return BigQueryOptions.newBuilder()
                .setProjectId(projectId)
                .setCredentials(credentials)
                .build()
                .getService();
    }
}
