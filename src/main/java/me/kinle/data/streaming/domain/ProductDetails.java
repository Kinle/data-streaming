package me.kinle.data.streaming.domain;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonSerialize;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@JsonSerialize
public class ProductDetails {
    private String material;
    private String color;
    private String size;
    private String usage;
    private String originCountry;
}
