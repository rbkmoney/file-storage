package com.rbkmoney.file.storage.service;

import com.amazonaws.AmazonClientException;
import com.amazonaws.HttpMethod;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.Upload;
import com.rbkmoney.file.storage.FileData;
import com.rbkmoney.file.storage.NewFileResult;
import com.rbkmoney.file.storage.configuration.properties.StorageProperties;
import com.rbkmoney.file.storage.service.exception.StorageException;
import com.rbkmoney.file.storage.service.exception.StorageFileNotFoundException;
import com.rbkmoney.file.storage.util.DamselUtil;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.net.URL;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

@Service
@Slf4j
@RequiredArgsConstructor
public class AmazonS3StorageService implements StorageService {

    private static final String FILEDATA_FILE_ID = "x-rbkmoney-filedata-file-id";
    private static final String FILEDATA_FILEDATA_ID = "x-rbkmoney-filedata-filedata-id";
    private static final String FILEDATA_FILE_NAME = "x-rbkmoney-filedata-file-name";
    private static final String FILEDATA_CREATED_AT = "x-rbkmoney-filedata-created-at";
    private static final String FILEDATA_METADATA = "x-rbkmoney-filedata-metadata-";

    private final TransferManager transferManager;
    private final AmazonS3 s3Client;
    private final StorageProperties storageProperties;
    private String bucketName;

    @PostConstruct
    public void init() {
        this.bucketName = storageProperties.getBucketName();
        if (!s3Client.doesBucketExist(bucketName)) {
            log.info("Create bucket in file storage, bucketId='{}'", bucketName);
            s3Client.createBucket(bucketName);
        }
    }

    @Override
    public NewFileResult createNewFile(String fileName, Map<String, com.rbkmoney.damsel.msgpack.Value> metadata, Instant expirationTime) throws StorageException {
        log.info("Trying to create new file to storage, filename='{}', bucketId='{}'", fileName, bucketName);

        try {
            InputStream emptyContent = getEmptyContent();

            String fileDataId = getId();
            String fileId = getId();
            String createdAt = Instant.now().toString();

            FileData fileData = new FileData(
                    fileDataId,
                    fileId,
                    fileName,
                    createdAt,
                    metadata
            );

            // записываем в хранилище пустой файл с метаданными по ключу fileDataId
            uploadRequest(fileDataId, fileData, emptyContent);

            // генерируем ссылку на запись файла в хранилище напрямую в цеф по ключу fileId
            URL uploadUrl = generateUploadUrl(fileId, expirationTime);

            log.info(
                    "File have been successfully created, fileId='{}', bucketId='{}', filename='{}'",
                    fileId,
                    bucketName,
                    fileName
            );

            return new NewFileResult(uploadUrl.toString(), fileData);
        } catch (AmazonClientException ex) {
            throw new StorageException(
                    String.format(
                            "Failed to create new file to storage, filename='%s', bucketId='%s'",
                            fileName,
                            bucketName
                    ),
                    ex
            );
        }
    }

    @Override
    public URL generateDownloadUrl(String fileDataId, Instant expirationTime) throws StorageException {
        String fileId = getValidFileData(fileDataId).getFileId();
        return generatePresignedUrl(fileId, expirationTime, HttpMethod.GET);
    }

    @Override
    public FileData getFileData(String fileDataId) throws StorageException {
        return getValidFileData(fileDataId);
    }

    @PreDestroy
    public void terminate() {
        transferManager.shutdownNow(true);
    }

    private S3Object getS3Object(String fileDataId) throws StorageException {
        try {
            log.info(
                    "Trying to get file from storage, fileDataId='{}', bucketId='{}'",
                    fileDataId,
                    bucketName
            );
            GetObjectRequest getObjectRequest = new GetObjectRequest(bucketName, fileDataId);
            S3Object object = s3Client.getObject(getObjectRequest);
            checkNullable(object, fileDataId, "File");
            log.info(
                    "File have been successfully got from storage, fileDataId='{}', bucketId='{}'",
                    fileDataId,
                    bucketName
            );
            return object;
        } catch (AmazonClientException ex) {
            throw new StorageException(
                    String.format(
                            "Failed to get file from storage, fileDataId='%s', bucketId='%s'",
                            fileDataId,
                            bucketName
                    ),
                    ex
            );
        }
    }

    private void checkFileStatus(S3Object s3Object) throws StorageFileNotFoundException {
        log.info("Check file expiration and uploaded status: ETag='{}'", s3Object.getObjectMetadata().getETag());
        ObjectMetadata objectMetadata = s3Object.getObjectMetadata();

        String fileId = getFileIdFromObjectMetadata(objectMetadata);
        if (s3Client.doesObjectExist(bucketName, fileId)) {
            log.info("File was uploaded: ETag='{}'", s3Object.getObjectMetadata().getETag());
            return;
        }

        // если файл не соотвествует условиям, блокируем доступ к нему
        throw new StorageFileNotFoundException(String.format("File not found: fileId='%s', bucketId='%s', create a new file", s3Object.getKey(), bucketName));
    }

    private FileData extractFileData(ObjectMetadata objectMetadata) {
        log.info("Trying to extract metadata from storage: ETag='{}'", objectMetadata.getETag());
        String fileId = getUserMetadataParameter(objectMetadata, FILEDATA_FILE_ID);
        String fileDataId = getUserMetadataParameter(objectMetadata, FILEDATA_FILEDATA_ID);
        String fileName = getUserMetadataParameter(objectMetadata, FILEDATA_FILE_NAME);
        String createdAt = getUserMetadataParameter(objectMetadata, FILEDATA_CREATED_AT);

        Map<String, com.rbkmoney.damsel.msgpack.Value> metadata = objectMetadata.getUserMetadata().entrySet().stream()
                .filter(entry -> entry.getKey().startsWith(FILEDATA_METADATA) && entry.getValue() != null)
                .collect(
                        Collectors.toMap(
                                o -> o.getKey().substring(FILEDATA_METADATA.length()),
                                o -> DamselUtil.fromJson(o.getValue(), com.rbkmoney.damsel.msgpack.Value.class)
                        )
                );
        log.info(
                "Metadata have been successfully extracted from storage, fileId='{}', bucketId='{}'",
                fileId,
                bucketName
        );
        return new FileData(fileDataId, fileId, fileName, createdAt, metadata);
    }

    private URL generatePresignedUrl(String fileId, Instant expirationTime, HttpMethod httpMethod) throws StorageException {
        try {
            log.info(
                    "Trying to generate presigned url, fileId='{}', bucketId='{}', expirationTime='{}', httpMethod='{}'",
                    fileId,
                    bucketName,
                    expirationTime,
                    httpMethod
            );

            GeneratePresignedUrlRequest request = new GeneratePresignedUrlRequest(bucketName, fileId)
                    .withMethod(httpMethod)
                    .withExpiration(Date.from(expirationTime));
            URL url = s3Client.generatePresignedUrl(request);
            checkNullable(url, fileId, "Presigned url");
            log.info(
                    "Presigned url have been successfully generated, url='{}', fileId='{}', bucketId='{}', expirationTime='{}', httpMethod='{}'",
                    url,
                    fileId,
                    bucketName,
                    expirationTime,
                    httpMethod
            );
            return url;
        } catch (AmazonClientException ex) {
            throw new StorageException(
                    String.format(
                            "Failed to generate presigned url, fileId='%s', bucketId='%s', expirationTime='%s', httpMethod='%s'",
                            fileId,
                            bucketName,
                            expirationTime,
                            httpMethod
                    ),
                    ex
            );
        }
    }

    private ByteArrayInputStream getEmptyContent() {
        return new ByteArrayInputStream(new byte[0]);
    }

    private void uploadRequest(String id, FileData fileData, InputStream inputStream) throws AmazonClientException {
        PutObjectRequest putObjectRequest = createS3Request(id, fileData, inputStream);

        Upload upload = transferManager.upload(putObjectRequest);
        try {
            upload.waitForUploadResult();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private PutObjectRequest createS3Request(String id, FileData fileData, InputStream inputStream) {
        return new PutObjectRequest(
                bucketName,
                id,
                inputStream,
                createObjectMetadata(fileData)
        );
    }

    private ObjectMetadata createObjectMetadata(FileData fileData) {
        ObjectMetadata objectMetadata = new ObjectMetadata();
        objectMetadata.setContentDisposition("attachment;filename=" + fileData.getFileName());
        // filedata parameters
        objectMetadata.addUserMetadata(FILEDATA_FILEDATA_ID, fileData.getFiledataId());
        objectMetadata.addUserMetadata(FILEDATA_FILE_ID, fileData.getFileId());
        objectMetadata.addUserMetadata(FILEDATA_FILE_NAME, fileData.getFileName());
        objectMetadata.addUserMetadata(FILEDATA_CREATED_AT, fileData.getCreatedAt());
        fileData.getMetadata().forEach(
                (key, value) -> objectMetadata.addUserMetadata(FILEDATA_METADATA + key, DamselUtil.toJsonString(value))
        );
        return objectMetadata;
    }

    private FileData getValidFileData(String fileDataId) throws StorageException {
        S3Object s3Object = getS3Object(fileDataId);
        checkFileStatus(s3Object);
        return extractFileData(s3Object.getObjectMetadata());
    }

    private URL generateUploadUrl(String fileId, Instant expirationTime) throws StorageException {
        return generatePresignedUrl(fileId, expirationTime, HttpMethod.PUT);
    }

    private String getFileIdFromObjectMetadata(ObjectMetadata objectMetadata) {
        return getUserMetadataParameter(objectMetadata, FILEDATA_FILE_ID);
    }

    private String getUserMetadataParameter(ObjectMetadata objectMetadata, String key) throws StorageException {
        return Optional.ofNullable(objectMetadata.getUserMetaDataOf(key))
                .orElseThrow(() -> new StorageException("Failed to extract user metadata parameter, " + key + " is null"));
    }

    private String getId() {
        return UUID.randomUUID().toString();
    }

    private void checkNullable(Object object, String fileId, String objectType) throws StorageFileNotFoundException {
        if (Objects.isNull(object)) {
            throw new StorageFileNotFoundException(String.format(objectType + " is null, fileId='%s', bucketId='%s'", fileId, bucketName));
        }
    }
}