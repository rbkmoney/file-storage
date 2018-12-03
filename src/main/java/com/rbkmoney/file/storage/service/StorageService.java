package com.rbkmoney.file.storage.service;

import com.rbkmoney.damsel.msgpack.Value;
import com.rbkmoney.file.storage.FileData;
import com.rbkmoney.file.storage.NewFileResult;
import com.rbkmoney.file.storage.service.exception.StorageException;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.time.Instant;
import java.util.Map;

public interface StorageService {

    FileData getFileData(String fileId) throws StorageException, FileNotFoundException;

    NewFileResult createNewFile(String fileName, Map<String, Value> metadata, Instant expirationTime) throws StorageException;

    URL generateDownloadUrl(String fileId, Instant expirationTime) throws StorageException, FileNotFoundException;

    void uploadFile(String fileId, InputStream inputStream) throws StorageException, IOException;

}
