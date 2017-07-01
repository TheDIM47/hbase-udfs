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
 * Flip sign bit in byte (negative number conversion)
 *
 * Инвертим (flip) старший бит в байте
 * для корректного преобразования отрицательных чисел (байт)
 */
@BuiltInFunction(name = FlipByte.NAME, args = {
        @Argument(allowedTypes = {PTinyint.class})
})
public class FlipByte extends ScalarFunction {
    public static final String NAME = "FLIPBYTE";

    public FlipByte() {
    }

    public FlipByte(List<Expression> children) {
        super(children);
    }

    @Override
    public String getName() {
        return NAME;
    }

    private static byte flipByte(byte b) {
        return (byte) (b ^ (byte) 0x80);
    }

    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        Expression arg = getChildren().get(0);
        if (!arg.evaluate(tuple, ptr)) {
            return false;
        }

        Byte b = (Byte) PTinyint.INSTANCE.toObject(ptr, arg.getDataType());
        if (b == null) {
            return true;
        }

        ptr.set(PTinyint.INSTANCE.toBytes(flipByte(b)));
        return true;
    }

    @Override
    public PDataType getDataType() {
        return PTinyint.INSTANCE;
    }
}
