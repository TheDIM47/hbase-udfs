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
 * Convert UNSIGNED_LONG (inverted-date format) to java.sql.Timestamp
 *
 * Преобразуем UNSIGNED_LONG (inverted-date format) в Таймштамп
 */
@BuiltInFunction(name = InvToDate.NAME, args = {
        @Argument(allowedTypes = {PLong.class, PUnsignedLong.class})
})
public class InvToDate extends ScalarFunction {
    public static final String NAME = "INVTODATE";

    public InvToDate() {
    }

    public InvToDate(List<Expression> children) {
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

        Long longDate = (Long) PUnsignedLong.INSTANCE.toObject(ptr, arg.getDataType());
        if (longDate == null) {
            return true;
        }

        Long invertedDate = Long.MAX_VALUE - longDate;

        ptr.set(PTimestamp.INSTANCE.toBytes(new Timestamp(invertedDate)));
        return true;
    }

    @Override
    public PDataType getDataType() {
        return PTimestamp.INSTANCE;
    }

    @Override
    public OrderPreserving preservesOrder() {
        return OrderPreserving.YES;
    }
}
