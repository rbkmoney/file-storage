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
import com.rbkmoney.file.storage.contorller.UploadFileController;
import com.rbkmoney.file.storage.service.exception.StorageException;
import com.rbkmoney.file.storage.service.exception.StorageFileNotFoundException;
import com.rbkmoney.file.storage.util.DamselUtil;
import com.rbkmoney.geck.common.util.TypeUtil;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.servlet.mvc.method.annotation.MvcUriComponentsBuilder;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

@Service
@Slf4j
@RequiredArgsConstructor
public class AmazonS3StorageService implements StorageService {

    private static final String EXPIRATION_TIME = "x-rbkmoney-file-expiration-time";
    private static final String FILE_UPLOADED = "x-rbkmoney-file-uploaded";
    private static final String FILEDATA_FILE_ID = "x-rbkmoney-filedata-file-id";
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
    public FileData getFileData(String fileId) throws StorageException {
        S3Object s3Object = getS3Object(fileId);
        checkFileStatus(s3Object);
        return extractFileData(s3Object.getObjectMetadata());
    }

    @Override
    public NewFileResult createNewFile(String fileName, Map<String, com.rbkmoney.damsel.msgpack.Value> metadata, Instant expirationTime) throws StorageException {
        log.info("Trying to create new file to storage, filename='{}', bucketId='{}'", fileName, bucketName);

        try {
            // в хранилище сохраняется пустой файл
            InputStream emptyContent = new ByteArrayInputStream(new byte[0]);

            String fileId = getFileId();
            String createdAt = Instant.now().toString();

            FileData fileData = new FileData(
                    fileId,
                    fileName,
                    createdAt,
                    metadata
            );

            writeFileToStorage(fileData, emptyContent, expirationTime);

            URL uploadUrl = createUploadUrl(fileId);

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
    public URL generateDownloadUrl(String fileId, Instant expirationTime) throws StorageException {
        checkFileStatus(getS3Object(fileId));
        return generatePresignedUrl(fileId, expirationTime, HttpMethod.GET);
    }

    @Override
    public void uploadFile(String fileId, MultipartFile multipartFile) throws StorageException, IOException {
        log.info("Trying to upload file to storage, filename='{}', bucketId='{}'", fileId, bucketName);

        try {
            S3Object object = getS3Object(fileId);

            checkFileStatus(object);

            ObjectMetadata objectMetadata = object.getObjectMetadata();
            objectMetadata.addUserMetadata(FILE_UPLOADED, "true");
            objectMetadata.setContentLength(multipartFile.getSize());

            PutObjectRequest putObjectRequest = new PutObjectRequest(
                    bucketName,
                    fileId,
                    multipartFile.getInputStream(),
                    objectMetadata
            );
            putObjectRequest.setMetadata(object.getObjectMetadata());

            Upload upload = transferManager.upload(putObjectRequest);
            try {
                upload.waitForUploadResult();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }

            log.info(
                    "File have been successfully uploaded, fileId='{}', bucketId='{}'",
                    fileId,
                    bucketName
            );
        } catch (AmazonClientException ex) {
            throw new StorageException(
                    String.format(
                            "Failed to to upload file to storage, filename='%s', bucketId='%s'",
                            fileId,
                            bucketName
                    ),
                    ex
            );
        }
    }

    @PreDestroy
    public void terminate() {
        transferManager.shutdownNow(true);
    }

    private S3Object getS3Object(String fileId) throws StorageException {
        try {
            log.info(
                    "Trying to get file from storage, fileId='{}', bucketId='{}'",
                    fileId,
                    bucketName
            );
            GetObjectRequest getObjectRequest = new GetObjectRequest(bucketName, fileId);
            S3Object object = s3Client.getObject(getObjectRequest);
            checkNullable(object, fileId, "File");
            log.info(
                    "File have been successfully got from storage, fileId='{}', bucketId='{}'",
                    fileId,
                    bucketName
            );
            return object;
        } catch (AmazonClientException ex) {
            throw new StorageException(
                    String.format(
                            "Failed to get file from storage, fileId='%s', bucketId='%s'",
                            fileId,
                            bucketName
                    ),
                    ex
            );
        }
    }

    private void checkFileStatus(S3Object s3Object) throws StorageFileNotFoundException {
        log.info("Check file expiration and uploaded status: ETag='{}'", s3Object.getObjectMetadata().getETag());
        ObjectMetadata objectMetadata = s3Object.getObjectMetadata();

        Boolean isUploaded = getBooleanFromObjectMetadata(objectMetadata);
        if (isUploaded) {
            log.info("File was uploaded: ETag='{}'", s3Object.getObjectMetadata().getETag());
            return;
        }

        Date expirationTime = getDateFromObjectMetadata(objectMetadata);
        Date time = new Date();
        if (time.getTime() < expirationTime.getTime()) {
            log.info("File was not uploaded, but expiration time is valid: ETag='{}'", s3Object.getObjectMetadata().getETag());
            return;
        }

        // если файл не соотвествует условиям, блокируем доступ к нему
        throw new StorageFileNotFoundException(String.format("File access error: fileId='%s', bucketId='%s', create a new file", s3Object.getKey(), bucketName));
    }

    private FileData extractFileData(ObjectMetadata objectMetadata) {
        log.info("Trying to extract metadata from storage: ETag='{}'", objectMetadata.getETag());
        String fileId = getUserMetadataParameter(objectMetadata, FILEDATA_FILE_ID);
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
        return new FileData(fileId, fileName, createdAt, metadata);
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

    private void writeFileToStorage(FileData fileData, InputStream inputStream, Instant expirationTime) throws AmazonClientException {
        PutObjectRequest request = createS3Request(fileData, inputStream, expirationTime);
        s3Client.putObject(request);
    }

    private PutObjectRequest createS3Request(FileData fileData, InputStream inputStream, Instant expirationTime) {
        return new PutObjectRequest(
                bucketName,
                fileData.getFileId(),
                inputStream,
                createObjectMetadata(fileData, expirationTime)
        );
    }

    private ObjectMetadata createObjectMetadata(FileData fileData, Instant expirationTime) {
        ObjectMetadata objectMetadata = new ObjectMetadata();
        objectMetadata.setContentDisposition("attachment;filename=" + fileData.getFileName());
        // file parameters
        objectMetadata.addUserMetadata(EXPIRATION_TIME, expirationTime.toString());
        objectMetadata.addUserMetadata(FILE_UPLOADED, "false");
        // filedata parameters
        objectMetadata.addUserMetadata(FILEDATA_FILE_ID, fileData.getFileId());
        objectMetadata.addUserMetadata(FILEDATA_FILE_NAME, fileData.getFileName());
        objectMetadata.addUserMetadata(FILEDATA_CREATED_AT, fileData.getCreatedAt());
        fileData.getMetadata().forEach(
                (key, value) -> objectMetadata.addUserMetadata(FILEDATA_METADATA + key, DamselUtil.toJsonString(value))
        );
        return objectMetadata;
    }

    private URL createUploadUrl(String fileId) {
        try {
            return MvcUriComponentsBuilder.fromMethodName(
                    UploadFileController.class,
                    "handleFileUpload",
                    fileId,
                    null
            )
                    .buildAndExpand()
                    .encode()
                    .toUri()
                    .toURL();
        } catch (MalformedURLException e) {
            throw new StorageException(
                    String.format(
                            "Exception createUploadUrl: fileId='%s', bucketId='%s', create a new file",
                            fileId,
                            bucketName
                    ),
                    e);
        }
    }

    private Boolean getBooleanFromObjectMetadata(ObjectMetadata objectMetadata) {
        String isUploadedString = getUserMetadataParameter(objectMetadata, FILE_UPLOADED);
        return Boolean.valueOf(isUploadedString);
    }

    private Date getDateFromObjectMetadata(ObjectMetadata objectMetadata) throws StorageException {
        String expirationTime = getUserMetadataParameter(objectMetadata, EXPIRATION_TIME);
        return Date.from(TypeUtil.stringToInstant(expirationTime));
    }

    private String getUserMetadataParameter(ObjectMetadata objectMetadata, String key) throws StorageException {
        return Optional.ofNullable(objectMetadata.getUserMetaDataOf(key))
                .orElseThrow(() -> new StorageException("Failed to extract user metadata parameter, " + key + " is null"));
    }

    private String getFileId() {
        return UUID.randomUUID().toString();
    }

    private void checkNullable(Object object, String fileId, String objectType) throws StorageFileNotFoundException {
        if (Objects.isNull(object)) {
            throw new StorageFileNotFoundException(String.format(objectType + " is null, fileId='%s', bucketId='%s'", fileId, bucketName));
        }
    }
}