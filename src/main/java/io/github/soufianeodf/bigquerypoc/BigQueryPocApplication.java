package io.github.soufianeodf.bigquerypoc;

import io.github.soufianeodf.bigquerypoc.service.BigQueryService;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

@SpringBootApplication
public class BigQueryPocApplication {

    public static void main(String[] args) {
        ConfigurableApplicationContext context = SpringApplication.run(BigQueryPocApplication.class, args);
        BigQueryService bigQueryService = context.getBean(BigQueryService.class);
        bigQueryService.run();
    }

}
