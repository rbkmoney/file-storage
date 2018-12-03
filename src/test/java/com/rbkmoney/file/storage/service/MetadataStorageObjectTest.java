package com.rbkmoney.file.storage.service;

import com.rbkmoney.file.storage.AbstractIntegrationTest;
import com.rbkmoney.file.storage.FileData;
import com.rbkmoney.file.storage.NewFileResult;
import org.apache.thrift.TException;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.FileNotFoundException;
import java.util.Collections;

import static org.junit.Assert.assertEquals;

public class MetadataStorageObjectTest extends AbstractIntegrationTest {

    @Autowired
    private StorageService storageService;

    @Test
    public void extractMetadataTest() throws FileNotFoundException, TException {
        String fileName = "test_file";
        NewFileResult fileResult = client.createNewFile(fileName, Collections.emptyMap(), getDayInstant().toString());

        assertEquals(fileResult.getFileData().getFileName(), fileName);

        FileData fileData = storageService.getFileData(fileResult.getFileData().getFileId());

        assertEquals(fileData, fileResult.getFileData());
    }
}
