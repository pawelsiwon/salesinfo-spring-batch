package com.github.siwonpawel.batch.salesinfo.integration;

import lombok.Setter;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.integration.launch.JobLaunchRequest;
import org.springframework.integration.annotation.Transformer;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Component;

import java.io.File;
import java.util.Date;

@Setter
@Component
public class FileMessageToJobRequest {

    private Job job;
    private String filename = "input.file.name";

    @Transformer
    public JobLaunchRequest jobLaunchRequest(Message<File> fileMessage) {
        var jobParameters = new JobParametersBuilder()
                .addString(filename, fileMessage.getPayload().getAbsolutePath())
                .addDate("uniqueness", new Date())
                .toJobParameters();

        return new JobLaunchRequest(job, jobParameters);
    }
}
