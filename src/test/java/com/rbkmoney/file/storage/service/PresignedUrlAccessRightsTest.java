package com.rbkmoney.file.storage.service;

import com.rbkmoney.file.storage.AbstractIntegrationTest;
import com.rbkmoney.file.storage.FileStorageSrv;
import com.rbkmoney.file.storage.NewFileResult;
import com.rbkmoney.woody.thrift.impl.http.THSpawnClientBuilder;
import org.apache.thrift.TException;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Collections;

import static org.junit.Assert.assertEquals;

public class PresignedUrlAccessRightsTest extends AbstractIntegrationTest {

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
    public void downloadUrlTest() throws TException, IOException {
        Path testFile = Files.createTempFile("", "test_file");
        Files.write(testFile, new byte[0]);

        try {
            Path testActualFile = Files.createTempFile("", "test_actual_file");

            NewFileResult fileResult = client.createNewFile("test_file", Collections.emptyMap(), getDayInstant().toString());

            URL url = storageService.generateDownloadUrl(fileResult.getFileData().getFileId(), getDayInstant());

            assertEquals(HttpStatus.FORBIDDEN.value(), getHttpURLConnection(url, true, "PUT").getResponseCode());

            assertEquals(HttpStatus.OK.value(), getHttpURLConnection(url, false, "GET").getResponseCode());

            HttpURLConnection urlConnection = getHttpURLConnection(url, false, "GET");
            InputStream inputStream = urlConnection.getInputStream();

            Files.copy(inputStream, testActualFile, StandardCopyOption.REPLACE_EXISTING);

            assertEquals(Files.readAllLines(testFile), Files.readAllLines(testActualFile));
        } finally {
            Files.deleteIfExists(testFile);
        }
    }
}
