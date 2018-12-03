package com.rbkmoney.file.storage.service;

import com.rbkmoney.file.storage.AbstractIntegrationTest;
import com.rbkmoney.file.storage.NewFileResult;
import org.apache.thrift.TException;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Collections;

import static org.junit.Assert.assertEquals;

public class PresignedUrlAccessRightsTest extends AbstractIntegrationTest {

    @Autowired
    private StorageService storageService;

    @Test
    public void downloadUrlTest() throws TException, IOException {
        Path testFile = Files.createTempFile("", "test_file");

        Path testActualFile = Files.createTempFile("", "test_actual_file");

        try {
            Files.write(testFile, new byte[0]);

            // создание нового файла
            NewFileResult fileResult = client.createNewFile("test_file", Collections.emptyMap(), getDayInstant().toString());

            // генерация url с доступом только для загрузки
            URL url = storageService.generateDownloadUrl(fileResult.getFileData().getFileId(), getDayInstant());

            // с данной ссылкой нельзя записывать
            assertEquals(HttpStatus.FORBIDDEN.value(), getHttpURLConnection(url, true, "PUT").getResponseCode());

            // можно читать
            assertEquals(HttpStatus.OK.value(), getHttpURLConnection(url, false, "GET").getResponseCode());

            // чтение данных
            HttpURLConnection urlConnection = getHttpURLConnection(url, false, "GET");
            InputStream inputStream = urlConnection.getInputStream();

            // чтение записанного файла из хранилища
            Files.copy(inputStream, testActualFile, StandardCopyOption.REPLACE_EXISTING);

            assertEquals(Files.readAllLines(testFile), Files.readAllLines(testActualFile));
        } finally {
            Files.deleteIfExists(testFile);
            Files.deleteIfExists(testActualFile);
        }
    }
}
