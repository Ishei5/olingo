package org.apache.olingo.test;

import lombok.*;

@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class ProductDto {

    private String productName;
    private int amount;
    private String storeName;
    private double unitWeight;
    private double totalProductWeight;

}
