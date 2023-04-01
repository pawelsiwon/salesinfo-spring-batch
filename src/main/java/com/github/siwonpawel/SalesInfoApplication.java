package com.github.siwonpawel;

import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.integration.annotation.IntegrationComponentScan;
import org.springframework.integration.config.EnableIntegration;

@SpringBootApplication
@EnableBatchProcessing
@EnableIntegration
@IntegrationComponentScan
public class SalesInfoApplication {

	public static void main(String[] args) {
		SpringApplication.run(SalesInfoApplication.class, args);
	}

}
