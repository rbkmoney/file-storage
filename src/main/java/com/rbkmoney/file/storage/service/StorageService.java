package com.rbkmoney.file.storage.service;

import com.rbkmoney.damsel.msgpack.Value;
import com.rbkmoney.file.storage.FileData;
import com.rbkmoney.file.storage.NewFileResult;
import com.rbkmoney.file.storage.service.exception.StorageException;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.net.URL;
import java.time.Instant;
import java.util.Map;

public interface StorageService {

    FileData getFileData(String fileId) throws StorageException;

    NewFileResult createNewFile(String fileName, Map<String, Value> metadata, Instant expirationTime) throws StorageException;

    URL generateDownloadUrl(String fileId, Instant expirationTime) throws StorageException;

    void uploadFile(String fileId, MultipartFile multipartFile) throws StorageException, IOException;

}
