package com.rbkmoney.file.storage.service;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.Upload;
import com.rbkmoney.file.storage.service.exception.StorageException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.digest.DigestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;

@Service
@Slf4j
public class AmazonS3StorageService implements StorageService {

    private final TransferManager transferManager;
    private final AmazonS3 storageClient;
    private final String bucketName;

    @Autowired
    public AmazonS3StorageService(TransferManager transferManager, @Value("${storage.bucketName}") String bucketName) {
        this.transferManager = transferManager;
        this.storageClient = transferManager.getAmazonS3Client();
        this.bucketName = bucketName;
    }

    @PostConstruct
    public void init() {
        if (!storageClient.doesBucketExist(bucketName)) {
            log.info("Create bucket in file storage, bucketId='{}'", bucketName);
            storageClient.createBucket(bucketName);
        }
    }

    @Override
    public void store(String fileId, MultipartFile file) {
        String filename = file.getOriginalFilename();
        log.info("Trying to upload file to storage, filename='{}', bucketId='{}'", filename, bucketName);

        try {
            Path tempFile = createTempFile(file, filename);

            ObjectMetadata objectMetadata = new ObjectMetadata();
            objectMetadata.setContentDisposition("attachment;filename=" + filename);

            PutObjectRequest putObjectRequest = new PutObjectRequest(bucketName, fileId, tempFile.toFile());
            putObjectRequest.setMetadata(objectMetadata);
            Upload upload = transferManager.upload(putObjectRequest);
            try {
                upload.waitForUploadResult();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }

            Files.deleteIfExists(tempFile);

            log.info(
                    "File have been successfully uploaded, fileId='{}', bucketId='{}', filename='{}', md5='{}'",
                    fileId,
                    bucketName,
                    filename,
                    DigestUtils.md5Hex(Files.newInputStream(tempFile))
            );
        } catch (IOException | AmazonClientException ex) {
            throw new StorageException(String.format("Failed to upload file to storage, filename='%s', bucketId='%s'", filename, bucketName), ex);
        }
    }

    private Path createTempFile(MultipartFile file, String filename) throws IOException {
        Path tempFile = Files.createTempFile(filename, "");
        OutputStream outputStream = new FileOutputStream(tempFile.toFile());

        int read;
        byte[] bytes = new byte[1024];

        while ((read = file.getInputStream().read(bytes)) != -1) {
            outputStream.write(bytes, 0, read);
        }
        return tempFile;
    }

    @PreDestroy
    public void terminate() {
        transferManager.shutdownNow(true);
    }
}