package com.rbkmoney.file.storage.util;

import java.util.stream.Stream;

import static io.github.benas.randombeans.EnhancedRandomBuilder.aNewEnhancedRandom;

public class RandomBeans {

    public static <T> T random(Class<T> type, String... excludedFields) {
        return aNewEnhancedRandom().nextObject(type, excludedFields);
    }

    public static <T> Stream<T> randomStreamOf(int amount, Class<T> type, String... excludedFields) {
        return aNewEnhancedRandom().objects(type, amount, excludedFields);
    }
}
