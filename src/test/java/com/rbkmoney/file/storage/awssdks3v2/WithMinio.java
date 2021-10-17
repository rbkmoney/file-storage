package com.rbkmoney.file.storage.awssdks3v2;

import com.rbkmoney.file.storage.FileStorageTest;
import com.rbkmoney.testcontainers.annotations.minio.MinioTestcontainerSingleton;

@MinioTestcontainerSingleton(
        properties = "s3-sdk-v2.enabled=true",
        bucketName = "awssdks3v2")
public class WithMinio extends FileStorageTest {
}
