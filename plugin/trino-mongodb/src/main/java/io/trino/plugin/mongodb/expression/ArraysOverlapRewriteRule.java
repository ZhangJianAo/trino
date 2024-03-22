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
package io.trino.plugin.mongodb.expression;

import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.matching.Property;
import io.trino.plugin.base.expression.ConnectorExpressionRule;
import io.trino.plugin.mongodb.MongoColumnHandle;
import io.trino.spi.block.IntArrayBlock;
import io.trino.spi.block.LongArrayBlock;
import io.trino.spi.block.VariableWidthBlock;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Constant;
import io.trino.spi.expression.FunctionName;
import io.trino.spi.expression.Variable;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.Type;
import org.bson.Document;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static io.trino.plugin.mongodb.TypeUtils.isPushdownSupportedArrayElementType;
import static io.trino.plugin.mongodb.TypeUtils.translateValue;

public class ArraysOverlapRewriteRule
        implements ConnectorExpressionRule<Call, Document>
{
    private final Pattern<Call> expressionPattern;

    public ArraysOverlapRewriteRule()
    {
        Pattern<Call> p = Pattern.typeOf(Call.class);
        Property<Call, ?, FunctionName> functionName = Property.property("functionName", Call::getFunctionName);
        this.expressionPattern = p.with(functionName.equalTo(new FunctionName("arrays_overlap")));
    }

    @Override
    public Pattern<Call> getPattern()
    {
        return this.expressionPattern;
    }

    @Override
    public Optional<Document> rewrite(Call expression, Captures captures, RewriteContext<Document> context)
    {
        List<ConnectorExpression> arguments = expression.getArguments();

        if (arguments.size() != 2) {
            return Optional.empty();
        }

        Variable arrVariable = null;
        Constant constValue = null;

        if (arguments.get(0) instanceof Variable v) {
            arrVariable = v;
        }
        else if (arguments.get(1) instanceof Variable v) {
            arrVariable = v;
        }

        if (arguments.get(0) instanceof Constant c) {
            constValue = c;
        }
        else if (arguments.get(1) instanceof Constant c) {
            constValue = c;
        }

        if (arrVariable == null || constValue == null) {
            return Optional.empty();
        }

        String columnName = arrVariable.getName();

        if (!context.getAssignments().containsKey(columnName)) {
            return Optional.empty();
        }

        MongoColumnHandle columnHandle = (MongoColumnHandle) context.getAssignment(columnName);
        Type elementType = ((ArrayType) columnHandle.getType()).getElementType();
        if (!isPushdownSupportedArrayElementType(elementType)) {
            return Optional.empty();
        }

        List<?> mongoValueList = translateArray(constValue.getValue(), elementType);

        if (!mongoValueList.isEmpty()) {
            return Optional.of(
                    new Document(columnHandle.getQualifiedName(),
                            new Document("$elemMatch",
                                    new Document("$in", mongoValueList))));
        }
        else {
            return Optional.empty();
        }
    }

    private List<?> translateArray(Object trinoArray, Type targetType)
    {
        ArrayList result = new ArrayList();
        if (trinoArray instanceof LongArrayBlock lab) {
            for (int i = 0; i < lab.getPositionCount(); i++) {
                Optional<Object> mongoValue = translateValue(lab.getLong(i), targetType);
                if (mongoValue.isPresent()) {
                    result.add(mongoValue.get());
                }
                else {
                    return List.of();
                }
            }
        }
        else if (trinoArray instanceof IntArrayBlock iab) {
            List<?> intArray = Arrays.stream(iab.getRawValues())
                    .mapToObj(i -> translateValue((long) i, targetType))
                    .flatMap(Optional::stream).toList();
            if (intArray.size() == iab.getPositionCount()) {
                return intArray;
            }
            else {
                return List.of();
            }
        }
        else if (trinoArray instanceof VariableWidthBlock vwb) {
            for (int i = 0; i < vwb.getPositionCount(); i++) {
                Optional<Object> mongoValue = translateValue(vwb.getSlice(i), targetType);
                if (mongoValue.isPresent()) {
                    result.add(mongoValue.get());
                }
                else {
                    return List.of();
                }
            }
        }
        return result;
    }
}
