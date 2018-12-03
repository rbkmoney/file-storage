package com.rbkmoney.file.storage.service;

import com.rbkmoney.file.storage.AbstractIntegrationTest;
import com.rbkmoney.file.storage.FileData;
import com.rbkmoney.file.storage.FileStorageSrv;
import com.rbkmoney.file.storage.NewFileResult;
import com.rbkmoney.woody.api.flow.error.WRuntimeException;
import com.rbkmoney.woody.thrift.impl.http.THSpawnClientBuilder;
import org.apache.thrift.TException;
import org.junit.Before;
import org.junit.Test;
import org.springframework.core.io.FileSystemResource;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.RestTemplate;

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

public class Aasd extends AbstractIntegrationTest {

    private static final int TIMEOUT = 555000;

    private FileStorageSrv.Iface client;

    @Before
    public void before() throws URISyntaxException {
        client = new THSpawnClientBuilder()
                .withAddress(new URI("http://localhost:" + port + "/file_storage"))
                .withNetworkTimeout(TIMEOUT)
                .build(FileStorageSrv.Iface.class);
    }

    @Test
    public void extractMetadataTest() throws TException {
        NewFileResult testFile = client.createNewFile("test_file", Collections.emptyMap(), getDayInstant().toString());
        FileData fileData = client.getFileData(testFile.getFileData().getFileId());

        assertEquals(fileData, testFile.getFileData());
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
    public void downloadUrlTest() throws TException, IOException {
        Path testFile = Files.createTempFile("", "test_file");
        Files.write(testFile, new byte[0]);

        Path testActualFile = Files.createTempFile("", "test_actual_file");

        NewFileResult fileResult = client.createNewFile("test_file", Collections.emptyMap(), getDayInstant().toString());
        String s = client.generateDownloadUrl(fileResult.getFileData().getFileId(), getDayInstant().toString());

        URL url = new URL(s);

        assertEquals(HttpStatus.FORBIDDEN.value(), getHttpURLConnection(url, true, "PUT").getResponseCode());

        assertEquals(HttpStatus.OK.value(), getHttpURLConnection(url, false, "GET").getResponseCode());

        HttpURLConnection urlConnection = getHttpURLConnection(url, false, "GET");
        InputStream inputStream = urlConnection.getInputStream();

        Files.copy(inputStream, testActualFile, StandardCopyOption.REPLACE_EXISTING);

        assertEquals(Files.readAllLines(testFile), Files.readAllLines(testActualFile));
    }

    @Test
    public void expiredTimeForGenerateUrlConnectionInCephTest() throws TException, IOException, InterruptedException {
        NewFileResult fileResult = client.createNewFile("test_file", Collections.emptyMap(), getDayInstant().toString());
        String s = client.generateDownloadUrl(fileResult.getFileData().getFileId(), getSecondInstant().toString());

        URL url = new URL(s);

        assertEquals(HttpStatus.OK.value(), getHttpURLConnection(url, false, "GET").getResponseCode());

        Thread.sleep(2000);

        assertEquals(HttpStatus.FORBIDDEN.value(), getHttpURLConnection(url, false, "GET").getResponseCode());
    }

    @Test
    public void name() throws IOException, TException {
        NewFileResult fileResult = client.createNewFile("test_file", Collections.emptyMap(), getDayInstant().toString());
        String s = fileResult.getUploadUrl();

        Path testFile = Files.createTempFile("", "test_file");
        Files.write(testFile, "Test".getBytes());

        MultiValueMap<String, Object> body = new LinkedMultiValueMap<>();
        body.add("file", new FileSystemResource(testFile.toFile()));

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.MULTIPART_FORM_DATA);

        HttpEntity<MultiValueMap<String, Object>> requestEntity = new HttpEntity<>(body, headers);

        RestTemplate restTemplate = new RestTemplate();
        restTemplate.postForEntity(s, requestEntity, Void.class);


        Path testActualFile = Files.createTempFile("", "test_actual_file");

        String urs = client.generateDownloadUrl(fileResult.getFileData().getFileId(), getDayInstant().toString());

        URL url = new URL(urs);

        HttpURLConnection urlConnection = getHttpURLConnection(url, false, "GET");
        InputStream inputStream = urlConnection.getInputStream();

        Files.copy(inputStream, testActualFile, StandardCopyOption.REPLACE_EXISTING);

        assertEquals(Files.readAllLines(testFile), Files.readAllLines(testActualFile));
    }

    private void asd() {
/*
        SimpleClientHttpRequestFactory requestFactory = new SimpleClientHttpRequestFactory();
        requestFactory.setBufferRequestBody(false);

        InputStream fis = new FileInputStream(testFile.toFile());

        RequestCallback requestCallback = request -> {
            request.getHeaders().add("Content-type", "application/octet-stream");
            IOUtils.copy(fis, request.getBody());
        };
        HttpMessageConverterExtractor<String> responseExtractor =
                new HttpMessageConverterExtractor<>(String.class, restTemplate.getMessageConverters());
                restTemplate.setRequestFactory(requestFactory);
*/


    }

    private HttpURLConnection getHttpURLConnection(URL url, boolean doOutput, String method) throws IOException {
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setDoOutput(doOutput);
        connection.setRequestMethod(method);
        return connection;
    }
}
