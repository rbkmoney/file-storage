package com.rbkmoney.file.storage.util;

import com.rbkmoney.geck.common.util.TypeUtil;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.testcontainers.shaded.org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

import static com.rbkmoney.file.storage.util.RandomBeans.random;
import static java.time.LocalDateTime.now;
import static java.time.ZoneId.systemDefault;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public abstract class PrimitiveGeneratorUtil {

    private final LocalDateTime fromTime = LocalDateTime.now().minusHours(3);
    private final LocalDateTime toTime = LocalDateTime.now().minusHours(1);
    private final LocalDateTime inFromToPeriodTime = LocalDateTime.now().minusHours(2);

    public static String generateDate() {
        return TypeUtil.temporalToString(LocalDateTime.now());
    }

    public static Long generateLong() {
        return random(Long.class);
    }

    public static Integer generateInt() {
        return random(Integer.class);
    }

    public static String generateString() {
        return random(String.class);
    }

    public static LocalDateTime generateLocalDateTime() {
        return random(LocalDateTime.class);
    }

    public static Instant generateCurrentTimePlusDay() {
        return now().plusDays(1).toInstant(getZoneOffset());
    }

    public static ZoneOffset getZoneOffset() {
        return systemDefault().getRules().getOffset(now());
    }

    public static String getContent(InputStream content) throws IOException {
        return IOUtils.toString(content, StandardCharsets.UTF_8);
    }

    public LocalDateTime getFromTime() {
        return fromTime;
    }

    public LocalDateTime getToTime() {
        return toTime;
    }

    public LocalDateTime getInFromToPeriodTime() {
        return inFromToPeriodTime;
    }

    public static Instant generateCurrentTimePlusSecond() {
        return LocalDateTime.now().plusSeconds(1).toInstant(getZoneOffset());
    }
}
