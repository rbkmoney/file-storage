package com.rbkmoney.file.storage;

import com.rbkmoney.file.storage.service.StorageService;
import org.apache.thrift.TException;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.FileSystemResource;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
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

// все тесты в 1 классе , чтобы сэкономить время на поднятии тест контейнера
public class FileStorageTest extends AbstractIntegrationTest {

    @Autowired
    private StorageService storageService;

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

    @Test
    public void extractMetadataTest() throws TException {
        String fileName = "test_file";
        NewFileResult fileResult = client.createNewFile(fileName, Collections.emptyMap(), getDayInstant().toString());

        assertEquals(fileResult.getFileData().getFileName(), fileName);

        FileData fileData = storageService.getFileData(fileResult.getFileData().getFileId());

        assertEquals(fileData, fileResult.getFileData());
    }
}
