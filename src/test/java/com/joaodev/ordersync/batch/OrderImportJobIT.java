package com.joaodev.ordersync.batch;

import com.joaodev.ordersync.repository.OrderRepository;
import org.junit.jupiter.api.Test;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.parameters.InvalidJobParametersException;
import org.springframework.batch.core.job.parameters.JobParametersBuilder;
import org.springframework.batch.core.launch.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.launch.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.batch.core.launch.JobRestartException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.oracle.OracleContainer;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;


@SpringBootTest
@Testcontainers
public class OrderImportJobIT {

    @Container
    static OracleContainer oracle = new OracleContainer("gvenzl/oracle-free:23-slim-faststart");

    @DynamicPropertySource
    static void registerDatasource(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", oracle::getJdbcUrl);
        registry.add("spring.datasource.username", oracle::getUsername);
        registry.add("spring.datasource.password", oracle::getPassword);
    }

    @Autowired
    private JobOperator jobOperator;

    @Autowired
    private Job orderImportJob;

    @Autowired
    private OrderRepository orderRepository;

    @Test
    void importOrdersFromCsvFile() throws IOException, JobInstanceAlreadyCompleteException, InvalidJobParametersException, JobExecutionAlreadyRunningException, JobRestartException {
        Path csvFile = Files.createTempFile("orders-test", ".csv");
        Files.writeString(csvFile, """
                legacyOrderId,customerName,productCode,quantity,unitPrice,status
                7001,Batch IT Customer,SKU-BATCH-IT,2,25.00,PENDING
                """);

        var jobParameters = new JobParametersBuilder()
                .addString("filePath", csvFile.toAbsolutePath().toString())
                .addLong("timestamp", System.currentTimeMillis())
                .toJobParameters();

        var execution = jobOperator.start(orderImportJob, jobParameters);

        assertThat(execution.getStatus().isUnsuccessful()).isFalse();
        assertThat(orderRepository.findByLegacyOrderId(7001L)).isPresent();

        Files.deleteIfExists(csvFile);
    }
}
