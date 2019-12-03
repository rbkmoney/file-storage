package com.rbkmoney.file.storage.service.exception;

public class StorageWaitingUploadException extends StorageException {

    public StorageWaitingUploadException(String message) {
        super(message);
    }

    public StorageWaitingUploadException(String message, Throwable cause) {
        super(message, cause);
    }
}