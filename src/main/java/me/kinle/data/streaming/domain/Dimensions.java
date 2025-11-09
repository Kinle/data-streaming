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
public class Dimensions {
    private double length;
    private double width;
    private double height;
    private String unit;
}
