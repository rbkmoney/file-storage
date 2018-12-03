package com.rbkmoney.file.storage.util;

import com.google.common.base.Strings;
import org.apache.thrift.TException;

public class CheckerUtil {

    public static void checkString(String string, String exMessage) throws TException {
        if (Strings.isNullOrEmpty(string)) {
            throw new TException(exMessage);
        }
    }
}
