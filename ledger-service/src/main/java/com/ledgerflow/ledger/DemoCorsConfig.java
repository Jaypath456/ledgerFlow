package com.ledgerflow.ledger;

import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.web.servlet.config.annotation.CorsRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

/** Demo profile only: the dashboard (served by payment-service on :8081) may call /demo/**. */
@Configuration(proxyBeanMethods = false)
@Profile("demo")
class DemoCorsConfig implements WebMvcConfigurer {

    @Override
    public void addCorsMappings(CorsRegistry registry) {
        registry.addMapping("/demo/**")
                .allowedOrigins("http://localhost:8081", "http://127.0.0.1:8081")
                .allowedMethods("GET", "POST")
                .allowedHeaders("Content-Type");
    }
}
