package com.juliasoft.hbase;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.function.ScalarFunction;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.*;

import java.sql.Timestamp;
import java.util.List;

import static org.apache.phoenix.parse.FunctionParseNode.BuiltInFunction;
import static org.apache.phoenix.parse.FunctionParseNode.Argument;

/**
 * Convert java.util.Date to UNSIGNED_LONG (inverted-date format)
 *
 * Преобразуем дату в UNSIGNED_LONG (inverted-date format)
 */
@BuiltInFunction(name = DateToInv.NAME, args = {
        @Argument(allowedTypes = {PTimestamp.class, PDate.class})
})
public class DateToInv extends ScalarFunction {
    public static final String NAME = "DATETOINV";

    public DateToInv() {
    }

    public DateToInv(List<Expression> children) {
        super(children);
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        Expression arg = getChildren().get(0);
        if (!arg.evaluate(tuple, ptr)) {
            return false;
        }
        Timestamp date = (Timestamp) PTimestamp.INSTANCE.toObject(ptr, arg.getDataType());
        if (date == null) {
            return true;
        }
        ptr.set(PUnsignedLong.INSTANCE.toBytes(Long.MAX_VALUE - date.getTime()));
        return true;
    }

    @Override
    public PDataType getDataType() {
        return PUnsignedLong.INSTANCE;
    }
}
