package com.silveryark.gateway.broker;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.web.reactive.config.EnableWebFlux;

@SpringBootApplication(scanBasePackages = "com.silveryark")
@EnableWebFlux
public class BrokerApplication {

    public static void main(String[] args) {
        SpringApplication.run(BrokerApplication.class, args);
    }
}
