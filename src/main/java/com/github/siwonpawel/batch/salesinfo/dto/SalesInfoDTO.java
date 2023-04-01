package com.github.siwonpawel.batch.salesinfo.dto;

import lombok.Data;

@Data
public class SalesInfoDTO {
    private String product;
    private String seller;
    private Integer sellerId;
    private Double price;
    private String city;
    private String category;
}
