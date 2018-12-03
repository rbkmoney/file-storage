package com.rbkmoney.file.storage.config;

import com.rbkmoney.file.storage.FileStorageSrv;
import com.rbkmoney.file.storage.handler.FileStorageHandler;
import com.rbkmoney.file.storage.service.StorageService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class HandlerConfiguration {

    @Bean
    @Autowired
    public FileStorageSrv.Iface fileStorageHandler(StorageService storageService) {
        return new FileStorageHandler(storageService);
    }
}
