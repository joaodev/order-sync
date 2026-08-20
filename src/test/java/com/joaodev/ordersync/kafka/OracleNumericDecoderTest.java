package com.joaodev.ordersync.kafka;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class OracleNumericDecoderTest {

    @Test
    void decodesVariableScaleDecimalAsLong() {
        // "AQ==" is Base64 for the single byte 0x01, i.e. BigInteger value 1
        assertThat(OracleNumericDecoder.decodeVariableScaleDecimalAsLong("AQ==")).isEqualTo(1L);
    }

    @Test
    void decodesVariableScaleDecimalAsInt() {
        // "Cg==" is Base64 for the single byte 0x0A, i.e. BigInteger value 10
        assertThat(OracleNumericDecoder.decodeVariableScaleDecimalAsInt("Cg==")).isEqualTo(10);
    }

    @Test
    void decodesDecimalWithScale() {
        // "E34=" decodes to the thow bytes representing unscaled value 4990, scale 2 > 49.90
        assertThat(OracleNumericDecoder.decodeDecimal("E34=", 2))
                .isEqualByComparingTo("49.90");
    }
}
