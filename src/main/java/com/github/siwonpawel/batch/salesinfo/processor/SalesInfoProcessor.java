package com.github.siwonpawel.batch.salesinfo.processor;

import com.github.siwonpawel.batch.salesinfo.dto.SalesInfoDTO;
import com.github.siwonpawel.batch.salesinfo.mapper.SalesInfoMapper;
import com.github.siwonpawel.domain.SalesInfo;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
@Component
@RequiredArgsConstructor
public class SalesInfoProcessor implements ItemProcessor<SalesInfoDTO, SalesInfo> {

    private final SalesInfoMapper salesInfoMapper;
    private final AtomicInteger count = new AtomicInteger(0);

    @Override
    public SalesInfo process(SalesInfoDTO item) {
        var i = count.incrementAndGet();

        if (i == 2) {
            throw new IllegalArgumentException("throwing exception because I can");
        }

        return salesInfoMapper.mapToEntity(item);
    }
}
