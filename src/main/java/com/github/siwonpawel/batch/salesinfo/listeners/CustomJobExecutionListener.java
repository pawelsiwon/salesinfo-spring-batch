package com.github.siwonpawel.batch.salesinfo.listeners;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.core.JobParameter;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Map;
import java.util.Objects;

@Slf4j
@Component
public class CustomJobExecutionListener implements JobExecutionListener {


    public static final String INPUT_FILE_NAME = "input.file.name";

    @Override
    public void beforeJob(JobExecution jobExecution) {
        log.info("--------------> before job execution");
    }

    @Override
    @SneakyThrows
    public void afterJob(JobExecution jobExecution) {
        Map<String, JobParameter> parameters = jobExecution.getJobParameters().getParameters();

        if (parameters.containsKey(INPUT_FILE_NAME)) {
            compute(jobExecution, parameters);
        }
    }

    private void compute(JobExecution jobExecution, Map<String, JobParameter> parameters) throws IOException {
        Path inputDirectoryAbsolutePath = Path.of(String.valueOf(parameters.get(INPUT_FILE_NAME).getValue()));
        Path inputDirectoryPath = inputDirectoryAbsolutePath.getParent();

        Path processedPath = Paths.get(inputDirectoryPath + File.separator + "processed");
        Path failedPath = Paths.get(inputDirectoryPath + File.separator + "failed");

        String exitStatus = jobExecution.getExitStatus().getExitCode();
        if (exitStatus.equals(ExitStatus.COMPLETED.getExitCode())) {
            createDirectoryIfAbsent(processedPath);
            computeFileMove(inputDirectoryAbsolutePath, processedPath);
        } else if (exitStatus.equals(ExitStatus.FAILED.getExitCode()) || exitStatus.equals(ExitStatus.STOPPED.getExitCode())) {
            createDirectoryIfAbsent(failedPath);
            computeFileMove(inputDirectoryAbsolutePath, failedPath);
        }
    }

    private void computeFileMove(Path inputDirectoryAbsolutePath, Path targetDirectory) throws IOException {
        Path destination = targetDirectory.resolve(inputDirectoryAbsolutePath.getFileName());
        Files.move(inputDirectoryAbsolutePath, destination, StandardCopyOption.ATOMIC_MOVE);
    }

    private void createDirectoryIfAbsent(Path directoryPath) throws IOException {
        Objects.requireNonNull(directoryPath, "the directory path cannot be null");
        if (Files.notExists(directoryPath)) {
            log.warn("---------> creating directory: {}", directoryPath.toAbsolutePath());
            Files.createDirectory(directoryPath);
        }
    }
}
