package com.rbkmoney.file.storage.service;

import com.rbkmoney.file.storage.AbstractIntegrationTest;
import com.rbkmoney.file.storage.FileData;
import com.rbkmoney.file.storage.FileStorageSrv;
import com.rbkmoney.file.storage.NewFileResult;
import com.rbkmoney.woody.thrift.impl.http.THSpawnClientBuilder;
import org.apache.thrift.TException;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.FileNotFoundException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;

import static org.junit.Assert.assertEquals;

public class MetadataStorageObjectTest extends AbstractIntegrationTest {

    @Autowired
    private StorageService storageService;

    private FileStorageSrv.Iface client;

    @Before
    public void before() throws URISyntaxException {
        client = new THSpawnClientBuilder()
                .withAddress(new URI("http://localhost:" + port + "/file_storage"))
                .withNetworkTimeout(TIMEOUT)
                .build(FileStorageSrv.Iface.class);
    }

    @Test
    public void extractMetadataTest() throws FileNotFoundException, TException {
        String fileName = "test_file";
        NewFileResult fileResult = client.createNewFile(fileName, Collections.emptyMap(), getDayInstant().toString());

        assertEquals(fileResult.getFileData().getFileName(), fileName);

        FileData fileData = storageService.getFileData(fileResult.getFileData().getFileId());

        assertEquals(fileData, fileResult.getFileData());
    }
}
