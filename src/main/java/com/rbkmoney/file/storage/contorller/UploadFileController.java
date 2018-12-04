package com.rbkmoney.file.storage.contorller;

import com.rbkmoney.file.storage.service.StorageService;
import com.rbkmoney.file.storage.service.exception.StorageFileNotFoundException;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import static com.rbkmoney.file.storage.util.CheckerUtil.checkFile;
import static com.rbkmoney.file.storage.util.CheckerUtil.checkString;

@RestController
@RequestMapping("/api/v1")
@Api(description = "File upload API")
@RequiredArgsConstructor
@Slf4j
public class UploadFileController {

    private final StorageService storageService;

    @ApiOperation(value = "Request upload file")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "File was uploaded"),
            @ApiResponse(code = 401, message = "File id not found"),
            @ApiResponse(code = 500, message = "Internal service error")
    })
    @PostMapping("/upload")
    public ResponseEntity handleFileUpload(@RequestParam(value = "file_id") String fileId,
                                           @RequestParam(value = "file") MultipartFile file) {
        try {
            log.info("Request handleFileUpload fileId: {}", fileId);
            checkFile(file, "Bad request parameter, file required and not empty arg");
            checkString(fileId, "Bad request parameter, fileId required and not empty arg");
            storageService.uploadFile(fileId, file);
            ResponseEntity<Object> responseEntity = ResponseEntity.ok().build();
            log.info("Response: ResponseEntity: {}", responseEntity);
            return responseEntity;
        } catch (StorageFileNotFoundException e) {
            log.error("Error when handleFileUpload e: ", e);
            return ResponseEntity.notFound().build();
        } catch (Exception e) {
            log.error("Error when handleFileUpload e: ", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("Failed to request upload file");
        }
    }
}
