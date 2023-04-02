package com.github.siwonpawel.batch.salesinfo.faulttolerance;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.step.skip.SkipLimitExceededException;
import org.springframework.batch.core.step.skip.SkipPolicy;
import org.springframework.batch.item.file.FlatFileParseException;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class CustomSkipPolicy implements SkipPolicy {

    private static final int SKIP_LIMIT = 10;

    @Override
    public boolean shouldSkip(Throwable exception, int skipCount) throws SkipLimitExceededException {
        if (skipCount <= SKIP_LIMIT && exception instanceof FlatFileParseException flatFileParseException) {
            log.info("parse exception at line no. {}: {}", flatFileParseException.getLineNumber(), flatFileParseException.getInput());
            return true;
        }

        if (skipCount <= SKIP_LIMIT && exception instanceof IllegalArgumentException illegalArgumentException) {
            log.info("an error occurred: {}", illegalArgumentException.getMessage());
            return true;
        }

        return false;
    }

}
