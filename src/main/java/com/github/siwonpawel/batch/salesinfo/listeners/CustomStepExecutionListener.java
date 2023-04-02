package com.github.siwonpawel.batch.salesinfo.listeners;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class CustomStepExecutionListener implements StepExecutionListener {

    public static final String DELETE_QUERY = "DELETE FROM sales_info WHERE id > 0";
    private final JdbcTemplate jdbcTemplate;

    @Override
    public void beforeStep(StepExecution stepExecution) {
        int deletedRows = jdbcTemplate.update(DELETE_QUERY);
        log.info("deleted before the step {} rows", deletedRows);
    }

    @Override
    public ExitStatus afterStep(StepExecution stepExecution) {
        if (stepExecution.getExitStatus() == ExitStatus.COMPLETED) {
            log.info("the step finished with status {}", stepExecution.getExitStatus());
        } else {
            log.info("something bad have happened after the step execution");
        }

        return stepExecution.getExitStatus();
    }
}
