package com.juliasoft.hbase;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.function.ScalarFunction;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PUnsignedLong;
import org.apache.phoenix.schema.types.PVarchar;

import java.util.List;

import static org.apache.phoenix.parse.FunctionParseNode.BuiltInFunction;
import static org.apache.phoenix.parse.FunctionParseNode.Argument;

/**
 * Convert 48-bit MAC Address String to UNSIGNED_LONG
 *
 * Преобразует MAC Address String в UNSIGNED_LONG
 */
@BuiltInFunction(name = MacToLong.NAME, args = {@Argument(allowedTypes = {PVarchar.class})})
public class MacToLong extends ScalarFunction {
    public static final String NAME = "MACTOLONG";

    public MacToLong() {
    }

    public MacToLong(List<Expression> children) {
        super(children);
    }

    @Override
    public String getName() {
        return NAME;
    }

    public Long macToLong(String mac) {
        String[] parts = mac.split(":");
        long result = 0;
        for (String part : parts) {
            int p = Integer.parseInt(part, 16);
            result <<= 8;
            result += p;
        }
        return result;
    }

    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        Expression arg = getChildren().get(0);
        if (!arg.evaluate(tuple, ptr)) {
            return false;
        }

        String mac = (String) PVarchar.INSTANCE.toObject(ptr, arg.getDataType());
        if (mac == null) {
            return true;
        }

        ptr.set(PUnsignedLong.INSTANCE.toBytes(macToLong(mac)));
        return true;
    }

    @Override
    public PDataType getDataType() {
        return PUnsignedLong.INSTANCE;
    }
}
