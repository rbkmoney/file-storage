package com.rbkmoney.file.storage.s3signer;

import com.rbkmoney.file.storage.FileStorageTest;
import com.rbkmoney.testcontainers.annotations.minio.MinioTestcontainerSingleton;

@MinioTestcontainerSingleton(bucketName = "s3signer")
public class WithMinio extends FileStorageTest {
}
