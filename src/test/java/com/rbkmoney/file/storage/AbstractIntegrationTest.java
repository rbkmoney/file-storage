package com.rbkmoney.file.storage;

import com.rbkmoney.woody.thrift.impl.http.THSpawnClientBuilder;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.boot.web.server.LocalServerPort;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy;

import java.io.IOException;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

import static org.springframework.boot.test.context.SpringBootTest.WebEnvironment.RANDOM_PORT;

@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = RANDOM_PORT)
@ContextConfiguration(classes = FileStorageApplication.class, initializers = AbstractIntegrationTest.Initializer.class)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
public abstract class AbstractIntegrationTest {

    private static final int TIMEOUT = 555000;
    private static final String SIGNING_REGION = "RU";
    private static final String AWS_ACCESS_KEY = "test";
    private static final String AWS_SECRET_KEY = "test";
    private static final String PROTOCOL = "HTTP";
    private static final String MAX_ERROR_RETRY = "10";
    private static final String BUCKET_NAME = "TEST";

    @LocalServerPort
    protected int port;

    protected FileStorageSrv.Iface client;

    // for up local test version comment this ClassRule
    @ClassRule
    public static GenericContainer cephContainer = new GenericContainer("dr.rbkmoney.com/ceph-demo:latest")
            .withEnv("RGW_NAME", "localhost")
            .withEnv("NETWORK_AUTO_DETECT", "4")
            .withEnv("CEPH_DEMO_UID", "ceph-test")
            .withEnv("CEPH_DEMO_ACCESS_KEY", AWS_ACCESS_KEY)
            .withEnv("CEPH_DEMO_SECRET_KEY", AWS_SECRET_KEY)
            .withEnv("CEPH_DEMO_BUCKET", BUCKET_NAME)
            .withExposedPorts(5000, 80)
            .waitingFor(
                    new HttpWaitStrategy()
                            .forPath("/api/v0.1/health")
                            .forStatusCode(200)
                            .withStartupTimeout(Duration.ofMinutes(10))
            );

    public static class Initializer implements ApplicationContextInitializer<ConfigurableApplicationContext> {
        @Override
        public void initialize(ConfigurableApplicationContext configurableApplicationContext) {
            TestPropertyValues.of(
                    // for up local test version comment this row
                    "storage.endpoint=" + cephContainer.getContainerIpAddress() + ":" + cephContainer.getMappedPort(80),
                    // for up local test version uncomment this row
                    // "storage.endpoint=localhost:32827",
                    "storage.signingRegion=" + SIGNING_REGION,
                    "storage.accessKey=" + AWS_ACCESS_KEY,
                    "storage.secretKey=" + AWS_SECRET_KEY,
                    "storage.clientProtocol=" + PROTOCOL,
                    "storage.clientMaxErrorRetry=" + MAX_ERROR_RETRY,
                    "storage.bucketName=" + BUCKET_NAME
            )
                    .applyTo(configurableApplicationContext);
        }
    }

    @Before
    public void before() throws URISyntaxException {
        client = new THSpawnClientBuilder()
                .withAddress(new URI("http://localhost:" + port + "/file_storage"))
                .withNetworkTimeout(TIMEOUT)
                .build(FileStorageSrv.Iface.class);
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
