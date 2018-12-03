package com.rbkmoney.file.storage;

import org.apache.thrift.TException;
import org.junit.Test;
import org.springframework.core.io.FileSystemResource;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.RestTemplate;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Collections;

import static org.junit.Assert.assertEquals;

public class FileStorageTest extends AbstractIntegrationTest {

    @Test
    public void uploadAndDownloadFileFromStorageTest() throws IOException, TException {
        Path testFile = Files.createTempFile("", "test_file");

        Path testActualFile = Files.createTempFile("", "test_actual_file");

        try {
            // создание нового файла
            NewFileResult fileResult = client.createNewFile("test_file", Collections.emptyMap(), getDayInstant().toString());
            String uploadUrl = fileResult.getUploadUrl();

            // запись данных в файл
            Files.write(testFile, "Test".getBytes());

            MultiValueMap<String, Object> body = new LinkedMultiValueMap<>();
            // запись файла в тело запроса
            body.add("file", new FileSystemResource(testFile.toFile()));

            HttpHeaders headers = new HttpHeaders();

            HttpEntity<MultiValueMap<String, Object>> requestEntity = new HttpEntity<>(body, headers);

            // запись файла в хранилище через ссылку доступа
            RestTemplate restTemplate = new RestTemplate();
            restTemplate.postForEntity(uploadUrl, requestEntity, Void.class);

            // генерация url с доступом только для загрузки
            String urs = client.generateDownloadUrl(fileResult.getFileData().getFileId(), getDayInstant().toString());

            URL url = new URL(urs);

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
