package com.rbkmoney.file.storage;

import com.rbkmoney.TestContainers;
import com.rbkmoney.TestContainersBuilder;
import com.rbkmoney.woody.thrift.impl.http.THSpawnClientBuilder;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

import static org.springframework.boot.test.context.SpringBootTest.WebEnvironment.RANDOM_PORT;

@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = RANDOM_PORT)
@ContextConfiguration(classes = FileStorageApplication.class, initializers = AbstractIntegrationTest.Initializer.class)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
public abstract class AbstractIntegrationTest {

    private static TestContainers testContainers = TestContainersBuilder.builder(false)
            .addCephTestContainer()
            .build();

    @BeforeClass
    public static void beforeClass() {
        testContainers.startTestContainers();
    }

    @AfterClass
    public static void afterClass() {
        testContainers.stopTestContainers();
    }

    @TestConfiguration
    public static class TestContextConfiguration {

        private static final int TIMEOUT = 555000;

        @Value("${local.server.port}")
        protected int port;

        @Bean
        public FileStorageSrv.Iface fileStorageCli() throws URISyntaxException {
            return new THSpawnClientBuilder()
                    .withAddress(new URI("http://localhost:" + port + "/file_storage"))
                    .withNetworkTimeout(TIMEOUT)
                    .build(FileStorageSrv.Iface.class);
        }
    }

    public static class Initializer implements ApplicationContextInitializer<ConfigurableApplicationContext> {

        @Override
        public void initialize(ConfigurableApplicationContext configurableApplicationContext) {
            FileStorageTestPropertyValuesBuilder.build(testContainers).applyTo(configurableApplicationContext);
        }
    }

    protected Instant generateCurrentTimePlusDay() {
        return LocalDateTime.now().plusDays(1).toInstant(getZoneOffset());
    }

    protected Instant generateCurrentTimePlusSecond() {
        return LocalDateTime.now().plusSeconds(1).toInstant(getZoneOffset());
    }

    private ZoneOffset getZoneOffset() {
        return ZoneOffset.systemDefault().getRules().getOffset(LocalDateTime.now());
    }

    protected HttpURLConnection getHttpURLConnection(URL url, String method, boolean doOutput) throws IOException {
        return getHttpURLConnection(url, method, null, doOutput);
    }

    protected HttpURLConnection getHttpURLConnection(URL url, String method, String fileName, boolean doOutput) throws IOException {
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setDoOutput(doOutput);
        connection.setRequestMethod(method);
        if (fileName != null) {
            connection.setRequestProperty("Content-Disposition", "attachment;filename=" + URLEncoder.encode(fileName, StandardCharsets.UTF_8.name()));
        }
        return connection;
    }
}
