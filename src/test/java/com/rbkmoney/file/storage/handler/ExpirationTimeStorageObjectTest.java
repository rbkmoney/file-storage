package com.rbkmoney.file.storage.handler;

import com.rbkmoney.file.storage.AbstractIntegrationTest;
import com.rbkmoney.file.storage.FileNotFound;
import com.rbkmoney.file.storage.NewFileResult;
import com.rbkmoney.file.storage.service.StorageService;
import org.apache.thrift.TException;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;

import java.io.IOException;
import java.net.URL;
import java.util.Collections;

import static org.junit.Assert.assertEquals;

public class ExpirationTimeStorageObjectTest extends AbstractIntegrationTest {

    @Autowired
    private StorageService storageService;

    @Test(expected = FileNotFound.class)
    public void expiredTimeForFileDataInMetadataTest() throws TException, InterruptedException {
        NewFileResult testFile = client.createNewFile("test_file", Collections.emptyMap(), getSecondInstant().toString());

        Thread.sleep(1000);

        client.getFileData(testFile.getFileData().getFileId());
    }

    @Test(expected = FileNotFound.class)
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
