package com.rbkmoney.file.storage.service;

import com.rbkmoney.file.storage.AbstractIntegrationTest;
import com.rbkmoney.file.storage.FileData;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.Path;

public class PresignedUrlAccessRightsTest extends AbstractIntegrationTest {

    @Autowired
    private StorageService storageService;

    @Test
    public void urlForUploadTest() throws IOException {
       /* String stringForWrite = "This text uploaded as an object via presigned URL.";

        Path testFile = Files.createTempFile("", "test_file");

        Path testActualFile = Files.createTempFile("", "test_actual_file");

        try {
            FileData fileData = storageService.createNewFile("test_file", Collections.emptyMap(), getInstant());

            // генерация url с доступом только для выгрузки
            URL url = storageService.generateUploadUrl(fileData.getFileId(), getInstant());

            // ошибка при запросе по url методом get
            assertEquals(HttpStatus.FORBIDDEN.value(), getHttpURLConnection(url, false, "GET").getResponseCode());

            // Length Required при запросе по url методом put
            assertEquals(HttpStatus.LENGTH_REQUIRED.value(), getHttpURLConnection(url, true, "PUT").getResponseCode());

            // запись данных методом put
            HttpURLConnection urlConnection = getHttpURLConnection(url, true, "PUT");

            OutputStreamWriter out = new OutputStreamWriter(urlConnection.getOutputStream());
            out.write(stringForWrite);
            out.close();

            // чтобы завершить загрузку вызываем getResponseCode
            assertEquals(HttpStatus.OK.value(), urlConnection.getResponseCode());

            // файл перезаписывается и затирает метаданные.
            // запись метаданных
//            storageService.rewriteFileData(fileData, getInstant());

            copyFromStorageToFile(fileData, testActualFile);

            // testFile пустой
            assertNotEquals(Files.readAllLines(testFile), Files.readAllLines(testActualFile));

            Files.write(testFile, stringForWrite.getBytes());
            assertEquals(Files.readAllLines(testFile), Files.readAllLines(testActualFile));
        } finally {
            Files.deleteIfExists(testFile);
            Files.deleteIfExists(testActualFile);
        }*/
    }

    @Test
    public void urlForDownloadTest() throws IOException {
        /*Path testFile = Files.createTempFile("", "test_file");
        Files.write(testFile, "4815162342".getBytes());

        Path testActualFile = Files.createTempFile("", "test_actual_file");

        try {
            FileData fileData = storageService.createNewFile("test_file", Collections.emptyMap(), getInstant());

            // запись тестового файла в цеф
            storageService.uploadFile(fileData.getFileId(), testFile);

            // генерация url с доступом только для загрузки
            URL url = storageService.generateDownloadUrl(fileData.getFileId(), getInstant());

            // ошибка при запросе по url методом put
            assertEquals(HttpStatus.FORBIDDEN.value(), getHttpURLConnection(url, true, "PUT").getResponseCode());

            // ок при запросе по url методом get
            assertEquals(HttpStatus.OK.value(), getHttpURLConnection(url, false, "GET").getResponseCode());

            // получение содержимого методом get
            HttpURLConnection urlConnection = getHttpURLConnection(url, false, "GET");
            InputStream inputStream = urlConnection.getInputStream();

            Files.copy(inputStream, testActualFile, StandardCopyOption.REPLACE_EXISTING);

            assertEquals(Files.readAllLines(testFile), Files.readAllLines(testActualFile));
        } finally {
            Files.deleteIfExists(testFile);
            Files.deleteIfExists(testActualFile);
        }*/
    }

    private void copyFromStorageToFile(FileData fileData, Path file) throws IOException {
        /*InputStream inputStream = storageService.getFile(fileData.getFileId());
        Files.copy(inputStream, file, StandardCopyOption.REPLACE_EXISTING);*/
    }

    private HttpURLConnection getHttpURLConnection(URL url, boolean doOutput, String method) throws IOException {
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setDoOutput(doOutput);
        connection.setRequestMethod(method);
        return connection;
    }
}
