package me.kinle.data.streaming.domain;


import net.datafaker.Faker;

import java.time.LocalDate;
import java.util.Arrays;

public class ProductGenerator {
    private static final Faker faker = new Faker();

    public static Product randomProduct(long id) {
        return Product.builder()
                .id(id)
                .name(faker.commerce().productName())
                .description(faker.lorem().sentence())
                .category(faker.commerce().department())
                .tags(Arrays.asList(faker.commerce().material(), faker.color().name(), faker.commerce().promotionCode()))
                .createdDate(LocalDate.now().minusDays(faker.random().nextInt(365)))
                .updatedDate(LocalDate.now())
                .priceInfo(PriceInfo.builder()
                        .price(Double.parseDouble(faker.commerce().price()))
                        .currency(faker.money().currencyCode())
                        .tax(faker.random().nextDouble() * 20)
                        .shippingCost(faker.random().nextDouble() * 10)
                        .build())
                .manufacturer(Manufacturer.builder()
                        .name(faker.company().name())
                        .address(faker.address().fullAddress())
                        .contactNumber(faker.phoneNumber().phoneNumber())
                        .email(faker.internet().emailAddress())
                        .build())
                .details(ProductDetails.builder()
                        .material(faker.commerce().material())
                        .color(faker.color().name())
                        .size(faker.options().option("S", "M", "L", "XL"))
                        .usage(faker.lorem().word())
                        .originCountry(faker.country().name())
                        .build())
                .available(faker.bool().bool())
                .stock(faker.random().nextInt(10000))
                .weight(faker.random().nextDouble() * 100)
                .dimensions(Dimensions.builder()
                        .length(faker.random().nextDouble() * 100)
                        .width(faker.random().nextDouble() * 100)
                        .height(faker.random().nextDouble() * 100)
                        .unit(faker.options().option("cm", "m", "in"))
                        .build())
                .ratings(Arrays.asList(faker.random().nextDouble() * 5, faker.random().nextDouble() * 5))
                .images(Arrays.asList(faker.avatar().image(), faker.avatar().image()))
                .barcode(faker.code().ean13())
                .sku(faker.code().asin())
                .expiryDate(LocalDate.now().plusDays(faker.random().nextInt(365)))
                .discount(faker.random().nextDouble() * 50)
                .build();
    }
}
