package com.rbkmoney.file.storage.s3signer;

import com.rbkmoney.file.storage.FileStorageTest;
import com.rbkmoney.testcontainers.annotations.ceph.CephTestcontainerSingleton;

@CephTestcontainerSingleton(bucketName = "s3signer")
public class WithCeph extends FileStorageTest {
}
