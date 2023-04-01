package com.github.siwonpawel.batch.salesinfo.processor;

import com.github.siwonpawel.batch.salesinfo.dto.SalesInfoDTO;
import com.github.siwonpawel.batch.salesinfo.mapper.SalesInfoMapper;
import com.github.siwonpawel.domain.SalesInfo;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class SalesInfoProcessor implements ItemProcessor<SalesInfoDTO, SalesInfo> {

    private final SalesInfoMapper salesInfoMapper;

    @Override
    public SalesInfo process(SalesInfoDTO item) {
        log.info("processing the item {}", item);
        return salesInfoMapper.mapToEntity(item);
    }
}
