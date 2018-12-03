package com.rbkmoney.file.storage.util;

import com.google.common.base.Strings;
import org.apache.thrift.TException;
import org.springframework.web.multipart.MultipartFile;

import java.util.regex.Pattern;

public class CheckerUtil {

    private static final Pattern PATTERN = Pattern.compile("^[a-zA-Z0-9 ,-_.]*$");

    public static void checkString(String string, String exMessage) throws TException {
        if (Strings.isNullOrEmpty(string)) {
            throw new TException(exMessage);
        }
    }

    public static void checkRegexString(String string, String exMessage) throws TException {
        if (!PATTERN.matcher(string).matches()) {
            throw new TException(exMessage);
        }
    }


    public static void checkFile(MultipartFile file, String exMessage) throws TException {
        if (file.isEmpty() || file.getSize() == 0) {
            throw new TException(exMessage);
        }
    }
}
