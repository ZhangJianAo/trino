/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.operator.aggregation.state;

import io.airlift.stats.cardinality.HyperLogLog;
import io.trino.array.ObjectBigArray;
import io.trino.spi.function.AccumulatorStateFactory;

import static io.airlift.slice.SizeOf.instanceSize;
import static java.util.Objects.requireNonNull;

public class HyperLogLogStateFactory
        implements AccumulatorStateFactory<HyperLogLogState>
{
    @Override
    public HyperLogLogState createSingleState()
    {
        return new SingleHyperLogLogState();
    }

    @Override
    public HyperLogLogState createGroupedState()
    {
        return new GroupedHyperLogLogState();
    }

    public static class GroupedHyperLogLogState
            extends AbstractGroupedAccumulatorState
            implements HyperLogLogState
    {
        private static final int INSTANCE_SIZE = instanceSize(GroupedHyperLogLogState.class);
        private final ObjectBigArray<HyperLogLog> hlls = new ObjectBigArray<>();
        private long size;

        @Override
        public void ensureCapacity(int size)
        {
            hlls.ensureCapacity(size);
        }

        @Override
        public HyperLogLog getHyperLogLog()
        {
            return hlls.get(getGroupId());
        }

        @Override
        public void setHyperLogLog(HyperLogLog value)
        {
            requireNonNull(value, "value is null");
            hlls.set(getGroupId(), value);
        }

        @Override
        public void addMemoryUsage(int value)
        {
            size += value;
        }

        @Override
        public long getEstimatedSize()
        {
            return INSTANCE_SIZE + size + hlls.sizeOf();
        }
    }

    public static class SingleHyperLogLogState
            implements HyperLogLogState
    {
        private static final int INSTANCE_SIZE = instanceSize(SingleHyperLogLogState.class);
        private HyperLogLog hll;

        @Override
        public HyperLogLog getHyperLogLog()
        {
            return hll;
        }

        @Override
        public void setHyperLogLog(HyperLogLog value)
        {
            hll = value;
        }

        @Override
        public void addMemoryUsage(int value)
        {
            // noop
        }

        @Override
        public long getEstimatedSize()
        {
            long estimatedSize = INSTANCE_SIZE;
            if (hll != null) {
                estimatedSize += hll.estimatedInMemorySize();
            }
            return estimatedSize;
        }
    }
}
