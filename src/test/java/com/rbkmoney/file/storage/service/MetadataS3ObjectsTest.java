package com.rbkmoney.file.storage.service;

import com.rbkmoney.file.storage.AbstractIntegrationTest;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.FileNotFoundException;

public class MetadataS3ObjectsTest extends AbstractIntegrationTest {

    @Autowired
    private StorageService storageService;

    @Test
    public void name() throws FileNotFoundException {
//        FileData fileData = storageService.createNewFile("test_file", Collections.emptyMap(), getInstant());
//        URL url = storageService.generateDownloadUrl(fileData.getFileId(), getInstant());

    }
}
