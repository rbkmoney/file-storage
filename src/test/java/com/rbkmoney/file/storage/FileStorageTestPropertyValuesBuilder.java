package com.rbkmoney.file.storage;

import com.rbkmoney.TestContainers;
import org.springframework.boot.test.util.TestPropertyValues;

import java.util.ArrayList;
import java.util.List;

import static com.rbkmoney.TestContainersBuilder.*;

public class FileStorageTestPropertyValuesBuilder {

    public static TestPropertyValues build(TestContainers testContainers) {
        List<String> strings = new ArrayList<>();
        if (!testContainers.isDockerContainersEnable()) {
            withUsingTestContainers(testContainers, strings);
        } else {
            withoutUsingTestContainers(strings);
        }

        strings.add("storage.signingRegion=" + SIGNING_REGION);
        strings.add("storage.accessKey=" + AWS_ACCESS_KEY);
        strings.add("storage.secretKey=" + AWS_SECRET_KEY);
        strings.add("storage.clientProtocol=" + PROTOCOL);
        strings.add("storage.clientMaxErrorRetry=" + MAX_ERROR_RETRY);
        strings.add("storage.bucketName=" + BUCKET_NAME);
        return TestPropertyValues.of(strings);
    }

    private static void withUsingTestContainers(TestContainers testContainers, List<String> strings) {
        testContainers.getCephTestContainer().ifPresent(
                c -> strings.add("storage.endpoint=" + c.getContainerIpAddress() + ":" + c.getMappedPort(80))
        );
    }

    private static void withoutUsingTestContainers(List<String> strings) {
        strings.add("storage.endpoint=localhost:32827");
    }
}
