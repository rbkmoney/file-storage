package com.rbkmoney.file.storage.service;

import com.rbkmoney.file.storage.FileData;
import com.rbkmoney.file.storage.NewFileResult;
import com.rbkmoney.file.storage.msgpack.Value;

import java.net.URL;
import java.time.Instant;
import java.util.Map;

public interface StorageService {

    NewFileResult createNewFile(Map<String, Value> metadata, Instant expirationTime);

    URL generateDownloadUrl(String fileDataId, Instant expirationTime);

    FileData getFileData(String fileDataId);

}
