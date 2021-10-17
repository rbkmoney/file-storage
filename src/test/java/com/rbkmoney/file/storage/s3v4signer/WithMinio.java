package com.rbkmoney.file.storage.s3v4signer;

import com.rbkmoney.file.storage.FileStorageTest;
import com.rbkmoney.testcontainers.annotations.minio.MinioTestcontainerSingleton;

@MinioTestcontainerSingleton(
        properties = "s3.signer-override=AWSS3V4SignerType",
        bucketName = "s3v4signer")
public class WithMinio extends FileStorageTest {
}
