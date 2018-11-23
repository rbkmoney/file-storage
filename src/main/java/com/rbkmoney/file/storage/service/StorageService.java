package com.rbkmoney.file.storage.service;

import org.springframework.web.multipart.MultipartFile;

public interface StorageService {

    void store(String fileId, MultipartFile file);
}
