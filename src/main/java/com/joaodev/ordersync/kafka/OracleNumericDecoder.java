package com.joaodev.ordersync.kafka;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Base64;

public class OracleNumericDecoder {

    private OracleNumericDecoder() {
    }

    public static Long decodeVariableScaleDecimalAsLong(String base64Value) {
        byte[] bytes = Base64.getDecoder().decode(base64Value);
        return new BigInteger(bytes).longValue();
    }

    public static Integer decodeVariableScaleDecimalAsInt(String base64Value) {
        byte[] bytes = Base64.getDecoder().decode(base64Value);
        return new BigInteger(bytes).intValue();
    }

    public static BigDecimal decodeDecimal(String base64Value, int scale) {
        byte[] bytes = Base64.getDecoder().decode(base64Value);
        BigInteger unscaled = new BigInteger(bytes);
        return new BigDecimal(unscaled, scale);
    }
}
