package me.kinle.data.streaming.domain;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import me.kinle.data.streaming.domain.convertor.DimensionsConvertor;
import me.kinle.data.streaming.domain.convertor.ManufacturerConvertor;
import me.kinle.data.streaming.domain.convertor.PriceInfoConvertor;
import me.kinle.data.streaming.domain.convertor.ProductDetailsConvertor;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonSerialize;

import java.time.LocalDate;
import java.util.List;
import java.util.Map;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@JsonSerialize
@Entity
public class Product {

    @Id
    private Long id;
    private String name;
    private String description;
    private String category;
    private List<String> tags;
    private LocalDate createdDate;
    private LocalDate updatedDate;
    @Convert(converter = PriceInfoConvertor.class)
    private PriceInfo priceInfo;
    @Convert(converter = ManufacturerConvertor.class)
    private Manufacturer manufacturer;
    @Convert(converter = ProductDetailsConvertor.class)
    private ProductDetails details;
    private boolean available;
    private int stock;
    private double weight;
    @Convert(converter = DimensionsConvertor.class)
    private Dimensions dimensions;
    private List<Double> ratings;
    @Transient
    private Map<String, String> attributes;
    private List<String> images;
    private String barcode;
    private String sku;
    private LocalDate expiryDate;
    private double discount;
}
