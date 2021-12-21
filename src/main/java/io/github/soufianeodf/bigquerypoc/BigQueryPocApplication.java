package io.github.soufianeodf.bigquerypoc;

import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class BigQueryPocApplication implements CommandLineRunner {

    public static void main(String[] args) {
        SpringApplication.run(BigQueryPocApplication.class, args);
    }

    @Override
    public void run(String... args) throws Exception {
        System.out.println("***** app is running *****");

    }
}
