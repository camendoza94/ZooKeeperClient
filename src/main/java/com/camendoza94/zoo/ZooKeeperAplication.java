package com.camendoza94.zoo;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;

@SpringBootApplication
@EnableDiscoveryClient
public class ZooKeeperAplication {

    public static void main(String[] args) {
        SpringApplication.run(ZooKeeperAplication.class, args);
    }
}
