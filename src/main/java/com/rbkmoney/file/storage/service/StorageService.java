package com.rbkmoney.file.storage.service;

import com.rbkmoney.file.storage.FileData;
import com.rbkmoney.file.storage.NewFileResult;
import com.rbkmoney.file.storage.msgpack.Value;
import com.rbkmoney.file.storage.service.exception.StorageException;

import java.net.URL;
import java.time.Instant;
import java.util.Map;

public interface StorageService {

    NewFileResult createNewFile(Map<String, Value> metadata, Instant expirationTime) throws StorageException;

    URL generateDownloadUrl(String fileDataId, Instant expirationTime) throws StorageException;

    FileData getFileData(String fileDataId) throws StorageException;

}
