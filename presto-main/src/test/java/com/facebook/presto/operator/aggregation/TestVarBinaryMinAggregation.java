package com.facebook.presto.operator.aggregation;

import com.facebook.presto.block.Block;
import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.slice.Slice;
import com.facebook.presto.slice.Slices;
import com.facebook.presto.tuple.Tuple;
import com.google.common.base.Charsets;
import com.google.common.collect.Ordering;
import com.google.common.primitives.Ints;

import static com.facebook.presto.operator.aggregation.VarBinaryVariableWidthMinAggregation.VAR_BINARY_MIN;
import static com.facebook.presto.tuple.TupleInfo.SINGLE_VARBINARY;

public class TestVarBinaryMinAggregation
        extends AbstractTestAggregationFunction
{
    @Override
    public Block getSequenceBlock(int start, int length)
    {
        BlockBuilder blockBuilder = new BlockBuilder(SINGLE_VARBINARY);
        for (int i = 0; i < length; i++) {
            blockBuilder.append(Slices.wrappedBuffer(Ints.toByteArray(i)));
        }
        return blockBuilder.build();
    }

    @Override
    public NewAggregationFunction getFunction()
    {
        return VAR_BINARY_MIN;
    }

    @Override
    public Object getExpectedValue(int start, int length)
    {
        if (length == 0) {
            return null;
        }
        Slice min = null;
        for (int i = 0; i < length; i++) {
            Slice slice = Slices.wrappedBuffer(Ints.toByteArray(i));
            min = (min == null) ? slice : Ordering.natural().min(min, slice);
        }
        return min.toString(Charsets.UTF_8);
    }

    @Override
    public Object getActualValue(AggregationFunctionStep function)
    {
        Tuple value = function.evaluate();
        if (value.isNull(0)) {
            return null;
        }
        return value.getSlice(0).toString(Charsets.UTF_8);
    }
}
