package com.rbkmoney.file.storage.configuration;

import com.rbkmoney.file.storage.FileStorageSrv;
import com.rbkmoney.file.storage.handler.FileStorageHandler;
import com.rbkmoney.file.storage.service.StorageService;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class HandlerConfiguration {

    @Bean
    public FileStorageSrv.Iface fileStorageHandler(StorageService storageService) {
        return new FileStorageHandler(storageService);
    }
}
