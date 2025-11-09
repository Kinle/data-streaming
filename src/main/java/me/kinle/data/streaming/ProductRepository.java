package me.kinle.data.streaming;

import me.kinle.data.streaming.domain.Product;
import org.springframework.data.jpa.repository.JpaRepository;

public interface ProductRepository extends JpaRepository<Product, Long> {
}
