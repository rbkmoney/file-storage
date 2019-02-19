package com.rbkmoney;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy;
import org.testcontainers.containers.wait.strategy.WaitStrategy;

import java.time.Duration;

public class TestContainersBuilder {

    public static final String SIGNING_REGION = "RU";
    public static final String AWS_ACCESS_KEY = "test";
    public static final String AWS_SECRET_KEY = "test";
    public static final String PROTOCOL = "HTTP";
    public static final String MAX_ERROR_RETRY = "10";
    public static final String BUCKET_NAME = "TEST";

    private boolean dockerContainersEnable;
    private boolean cephTestContainerEnable;

    private TestContainersBuilder(boolean dockerContainersEnable) {
        this.dockerContainersEnable = dockerContainersEnable;
    }

    public static TestContainersBuilder builder(boolean dockerContainersEnable) {
        return new TestContainersBuilder(dockerContainersEnable);
    }

    public TestContainersBuilder addCephTestContainer() {
        cephTestContainerEnable = true;
        return this;
    }

    public TestContainers build() {
        TestContainers testContainers = new TestContainers();

        if (!dockerContainersEnable) {
            addTestContainers(testContainers);
        } else {
            testContainers.setDockerContainersEnable(true);
        }
        return testContainers;
    }

    private void addTestContainers(TestContainers testContainers) {
        if (cephTestContainerEnable) {
            testContainers.setCephTestContainer(
                    new GenericContainer<>("dr.rbkmoney.com/ceph-demo:latest")
                            .withEnv("RGW_NAME", "localhost")
                            .withEnv("NETWORK_AUTO_DETECT", "4")
                            .withEnv("CEPH_DEMO_UID", "ceph-test")
                            .withEnv("CEPH_DEMO_ACCESS_KEY", AWS_ACCESS_KEY)
                            .withEnv("CEPH_DEMO_SECRET_KEY", AWS_SECRET_KEY)
                            .withEnv("CEPH_DEMO_BUCKET", BUCKET_NAME)
                            .withExposedPorts(5000, 80)
                            .waitingFor(getWaitStrategy("/api/v0.1/health"))
            );
        }
        testContainers.setDockerContainersEnable(false);
    }

    private WaitStrategy getWaitStrategy(String path) {
        return new HttpWaitStrategy()
                .forPath(path)
                .forStatusCode(200)
                .withStartupTimeout(Duration.ofMinutes(10));
    }
}
