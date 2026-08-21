package com.joaodev.ordersync.batch;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.parameters.JobParametersBuilder;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.beans.factory.annotation.Value;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.file.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static java.nio.file.StandardWatchEventKinds.ENTRY_CREATE;

@Slf4j
@Component
public class FileSystemWatcherService {

    @Value("${app.file-watcher.incoming-dir}")
    private String incomingDir;

    @Value("${app.file-watcher.processed-dir}")
    private String processedDir;

    @Value("${app.file-watcher.error-dir}")
    private String errorDir;

    private final JobOperator jobOperator;
    private final Job orderImportJob;
    private final ExecutorService executor = Executors.newSingleThreadExecutor();

    private WatchService watchService;

    public FileSystemWatcherService(JobOperator jobOperator, Job orderImportJob) {
        this.jobOperator = jobOperator;
        this.orderImportJob = orderImportJob;
    }

    @PostConstruct
    public void start() throws IOException {
        Path incomingPath = Paths.get(incomingDir);
        Files.createDirectories(incomingPath);
        Files.createDirectories(Paths.get(processedDir));
        Files.createDirectories(Paths.get(errorDir));

        watchService = FileSystems.getDefault().newWatchService();
        incomingPath.register(watchService, ENTRY_CREATE);

        executor.submit(this::watchLoop);
        log.info("File system watcher started, monitoring: {}", incomingPath.toAbsolutePath());
    }

    private void watchLoop() {
        while (true) {
            WatchKey key;
            try {
                key = watchService.take();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }

            for (WatchEvent<?> event : key.pollEvents()) {
                if (event.kind() == ENTRY_CREATE) {
                    Path fileName = (Path) event.context();
                    Path fullPath = Paths.get(incomingDir).resolve(fileName);
                    handleNewFile(fullPath);
                }
            }
        }
    }

    private void handleNewFile(Path filePath) {
        if (!filePath.toString().endsWith(".csv")) {
            log.info("Ignoring non-CSV file: {}", filePath);
            return;
        }

        waitUntilFileIsStable(filePath);
        log.info("Detected new file, launching batch job: {}", filePath);

        try {
            var jobParameters = new JobParametersBuilder()
                    .addString("filePath", filePath.toAbsolutePath().toString())
                    .addLong("timestamp", System.currentTimeMillis())
                    .toJobParameters();

            var execution = jobOperator.start(orderImportJob, jobParameters);

            if (execution.getStatus().isUnsuccessful()) {
                moveFile(filePath, Paths.get(errorDir));
            } else {
                moveFile(filePath, Paths.get(processedDir));
            }
        } catch (Exception e) {
            log.error("Failed to process file {}: {}", filePath, e.getMessage());
            moveFile(filePath, Paths.get(errorDir));
        }
    }

    private void waitUntilFileIsStable(Path filePath) {
        try {
            long previousSize = -1;
            long currentSize = Files.size(filePath);
            while (previousSize != currentSize) {
                previousSize = currentSize;
                Thread.sleep(300);
                currentSize = Files.size(filePath);
            }
        } catch (IOException | InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void moveFile(Path source, Path targetDir) {
        try {
            Files.move(source, targetDir.resolve(source.getFileName()), StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            log.error("Failed to move file {} to {}: {}", source, targetDir, e.getMessage());
        }
    }

    @PreDestroy
    public void stop() throws IOException {
        executor.shutdownNow();
        if (watchService != null) {
            watchService.close();
        }
    }
}
