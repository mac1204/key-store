package com.moniepoint.kvstore;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Main application class for the KV Store.
 * This is a distributed key-value store with Raft consensus protocol.
 */
@SpringBootApplication
@EnableScheduling
public class KvStoreApplication {

    public static void main(String[] args) {
        SpringApplication.run(KvStoreApplication.class, args);
    }
} 