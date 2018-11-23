package com.rbkmoney.file.storage.contorller;

import com.rbkmoney.file.storage.service.StorageService;
import io.swagger.annotations.*;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

@RestController
@Api(description = "Api для операций с файлами")
@RequiredArgsConstructor
public class FileController {

    private final StorageService storageService;

    @PostMapping("/upload")
    @ApiOperation(value = "Выгрузить файл на сервер")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Файл выгружен на сервер")
    })
    public ResponseEntity handleFileUpload(@ApiParam(value = "выгружаемый файл", required = true)
                                           @RequestParam(value = "file")
                                                   MultipartFile file,
                                           @ApiParam(value = "id файла", required = true)
                                           @RequestParam(value = "file_id")
                                                   String fileId) {
        storageService.store(fileId, file);
        return new ResponseEntity(HttpStatus.OK);
    }
}
