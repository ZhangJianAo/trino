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
package io.trino.plugin.memory;

import io.airlift.configuration.Config;
import io.airlift.units.DataSize;
import jakarta.validation.constraints.NotNull;

public class MemoryConfig
{
    private int splitsPerNode = Runtime.getRuntime().availableProcessors();
    private DataSize maxDataPerNode = DataSize.of(128, DataSize.Unit.MEGABYTE);
    private boolean enableLazyDynamicFiltering = true;

    public int getSplitsPerNode()
    {
        return splitsPerNode;
    }

    @Config("memory.splits-per-node")
    public MemoryConfig setSplitsPerNode(int splitsPerNode)
    {
        this.splitsPerNode = splitsPerNode;
        return this;
    }

    @NotNull
    public DataSize getMaxDataPerNode()
    {
        return maxDataPerNode;
    }

    @Config("memory.max-data-per-node")
    public MemoryConfig setMaxDataPerNode(DataSize maxDataPerNode)
    {
        this.maxDataPerNode = maxDataPerNode;
        return this;
    }

    public boolean isEnableLazyDynamicFiltering()
    {
        return enableLazyDynamicFiltering;
    }

    @Config("memory.enable-lazy-dynamic-filtering")
    public MemoryConfig setEnableLazyDynamicFiltering(boolean enableLazyDynamicFiltering)
    {
        this.enableLazyDynamicFiltering = enableLazyDynamicFiltering;
        return this;
    }
}
