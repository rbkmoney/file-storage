package com.rbkmoney.file.storage.handler;

import com.rbkmoney.file.storage.AbstractIntegrationTest;
import com.rbkmoney.file.storage.FileStorageSrv;
import com.rbkmoney.file.storage.NewFileResult;
import com.rbkmoney.file.storage.service.StorageService;
import com.rbkmoney.woody.api.flow.error.WRuntimeException;
import com.rbkmoney.woody.thrift.impl.http.THSpawnClientBuilder;
import org.apache.thrift.TException;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Collections;

import static org.junit.Assert.assertEquals;

public class ExpirationTimeStorageObjectTest extends AbstractIntegrationTest {

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

    @Test(expected = WRuntimeException.class)
    public void expiredTimeForFileDataInMetadataTest() throws TException, InterruptedException {
        NewFileResult testFile = client.createNewFile("test_file", Collections.emptyMap(), getSecondInstant().toString());

        Thread.sleep(1000);

        client.getFileData(testFile.getFileData().getFileId());
    }

    @Test(expected = WRuntimeException.class)
    public void expiredTimeForGenerateUrlInMetadataTest() throws TException, InterruptedException {
        NewFileResult testFile = client.createNewFile("test_file", Collections.emptyMap(), getSecondInstant().toString());

        Thread.sleep(1000);

        client.generateDownloadUrl(testFile.getFileData().getFileId(), getSecondInstant().toString());
    }

    @Test
    public void expiredTimeForGenerateUrlConnectionInCephTest() throws TException, IOException, InterruptedException {
        NewFileResult fileResult = client.createNewFile("test_file", Collections.emptyMap(), getDayInstant().toString());

        URL url = storageService.generateDownloadUrl(fileResult.getFileData().getFileId(), getSecondInstant());

        assertEquals(HttpStatus.OK.value(), getHttpURLConnection(url, false, "GET").getResponseCode());

        Thread.sleep(2000);

        assertEquals(HttpStatus.FORBIDDEN.value(), getHttpURLConnection(url, false, "GET").getResponseCode());
    }
}
