package com.rbkmoney.file.storage.s3v4signer;

import com.rbkmoney.file.storage.FileStorageTest;
import com.rbkmoney.testcontainers.annotations.ceph.CephTestcontainerSingleton;

@CephTestcontainerSingleton(
        properties = "s3.signer-override=AWSS3V4SignerType",
        bucketName = "s3v4signer")
public class WithCeph extends FileStorageTest {
}
