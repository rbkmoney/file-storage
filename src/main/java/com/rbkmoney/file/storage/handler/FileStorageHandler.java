package com.rbkmoney.file.storage.handler;

import com.rbkmoney.damsel.msgpack.Value;
import com.rbkmoney.file.storage.FileData;
import com.rbkmoney.file.storage.FileNotFound;
import com.rbkmoney.file.storage.FileStorageSrv;
import com.rbkmoney.file.storage.NewFileResult;
import com.rbkmoney.file.storage.service.StorageService;
import com.rbkmoney.file.storage.service.exception.StorageException;
import com.rbkmoney.file.storage.service.exception.StorageFileNotFoundException;
import com.rbkmoney.geck.common.util.TypeUtil;
import com.rbkmoney.woody.api.flow.error.WUnavailableResultException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.thrift.TException;

import java.net.URL;
import java.time.Instant;
import java.util.Map;

import static com.rbkmoney.file.storage.util.CheckerUtil.checkString;

@RequiredArgsConstructor
@Slf4j
public class FileStorageHandler implements FileStorageSrv.Iface {

    private final StorageService storageService;

    @Override
    public NewFileResult createNewFile(Map<String, Value> metadata, String expiresAt) throws TException {
        try {
            log.info("Request createNewFile metadata: {}, expiresAt: {}", metadata, expiresAt);
            // stringToInstant уже содержит проверки аргемента
            Instant instant = TypeUtil.stringToInstant(expiresAt);
            NewFileResult newFile = storageService.createNewFile(metadata, instant);
            log.info("Response: newFileResult: {}", newFile);
            return newFile;
        } catch (StorageFileNotFoundException e) {
            log.warn("Warn when createNewFile: file not found");
            throw new FileNotFound();
        } catch (StorageException e) {
            log.warn("Warn when createNewFile: storage problem");
            throw new WUnavailableResultException(e);
        } catch (Exception e) {
            log.error("Error when createNewFile");
            throw new TException(e);
        }
    }

    @Override
    public String generateDownloadUrl(String id, String expiresAt) throws TException {
        try {
            log.info("Request generateDownloadUrl id: {}, expiresAt: {}", id, expiresAt);
            checkString(id, "Bad request parameter, id required and not empty arg");
            checkString(expiresAt, "Bad request parameter, expiresAt required and not empty arg");
            // stringToInstant уже содержит проверки аргемента
            Instant instant = TypeUtil.stringToInstant(expiresAt);
            URL url = storageService.generateDownloadUrl(id, instant);
            log.info("Response: url: {}", url);
            return url.toString();
        } catch (StorageFileNotFoundException e) {
            log.warn("Warn when createNewFile: file not found");
            throw new FileNotFound();
        } catch (StorageException e) {
            log.warn("Warn when createNewFile: storage problem");
            throw new WUnavailableResultException(e);
        } catch (Exception e) {
            log.error("Error when createNewFile");
            throw new TException(e);
        }
    }

    @Override
    public FileData getFileData(String id) throws TException {
        try {
            log.info("Request getFileData id: {}", id);
            checkString(id, "Bad request parameter, id required and not empty arg");
            FileData fileData = storageService.getFileData(id);
            log.info("Response: fileData: {}", fileData);
            return fileData;
        } catch (StorageFileNotFoundException e) {
            log.warn("Warn when createNewFile: file not found");
            throw new FileNotFound();
        } catch (StorageException e) {
            log.warn("Warn when createNewFile: storage problem");
            throw new WUnavailableResultException(e);
        } catch (Exception e) {
            log.error("Error when createNewFile");
            throw new TException(e);
        }
    }
}
