package com.github.siwonpawel.batch.salesinfo.mapper;

import com.github.siwonpawel.batch.salesinfo.dto.SalesInfoDTO;
import com.github.siwonpawel.domain.SalesInfo;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.MappingConstants;

@Mapper(componentModel = MappingConstants.ComponentModel.SPRING)
public interface SalesInfoMapper {

    @Mapping(target = "id", ignore = true)
    SalesInfo mapToEntity(SalesInfoDTO salesInfoDTO);

}
