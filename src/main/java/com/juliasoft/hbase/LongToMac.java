package com.juliasoft.hbase;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.function.ScalarFunction;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.*;

import java.util.List;

import static org.apache.phoenix.parse.FunctionParseNode.BuiltInFunction;
import static org.apache.phoenix.parse.FunctionParseNode.Argument;

/**
 * Convert UNSIGNED_LONG to 48-bit MAC Address String
 *
 * Преобразует UNSIGNED_LONG в MAC Address String
 */
@BuiltInFunction(name = LongToMac.NAME, args = {
        @Argument(allowedTypes = {
                PUnsignedLong.class, PLong.class
        })
})
public class LongToMac extends ScalarFunction {
    public static final String NAME = "LONGTOMAC";

    public LongToMac() {
    }

    public LongToMac(List<Expression> children) {
        super(children);
    }

    @Override
    public String getName() {
        return NAME;
    }

    public static String longToMac(Long address) {
        return new StringBuilder() // ("00:00:")
                .append(long2Hex((address >> 40) & 0xff)).append(":")
                .append(long2Hex((address >> 32) & 0xff)).append(":")
                .append(long2Hex((address >> 24) & 0xff)).append(":")
                .append(long2Hex((address >> 16) & 0xff)).append(":")
                .append(long2Hex((address >> 8) & 0xff)).append(":")
                .append(long2Hex((address >> 0) & 0xff))
                .toString();
    }

    private static String long2Hex(long v) {
        String s = Long.toHexString(v);
        return (v > 0x0f) ? s : "0" + s;
    }

    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        Expression arg = getChildren().get(0);
        if (!arg.evaluate(tuple, ptr)) {
            return false;
        }

        Long macLong = (Long) PUnsignedLong.INSTANCE.toObject(ptr, arg.getDataType());
        if (macLong == null) {
            return true;
        }

        ptr.set(PVarchar.INSTANCE.toBytes(longToMac(macLong)));
        return true;
    }

    @Override
    public PDataType getDataType() {
        return PVarchar.INSTANCE;
    }
}
