/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.runtime.functions.scalar;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.SpecializedFunction.SpecializedContext;
import org.apache.flink.table.runtime.functions.SqlJsonUtils;
import org.apache.flink.util.FlinkRuntimeException;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import javax.annotation.Nullable;

import java.io.IOException;

/** Implementation of {@link BuiltInFunctionDefinitions#JSON_UNQUOTE}. */
@Internal
public class JsonUnquoteFunction extends BuiltInScalarFunction {

    public JsonUnquoteFunction(SpecializedContext context) {
        super(BuiltInFunctionDefinitions.JSON_UNQUOTE, context);
    }

    private static boolean isValidJsonVal(String jsonInString) {
       return SqlJsonUtils.isJsonValue(jsonInString);
    }

    private String unescape(String inputStr) {

        StringBuilder result = new StringBuilder();
        int i = 1;
        while (i < inputStr.length() - 1) {
            if (inputStr.charAt(i) == '\\' && i + 1 < inputStr.length()) {
                i++; // move to the next char
                char nextChar = inputStr.charAt(i++);

                switch (nextChar) {
                    case '"':
                        result.append(nextChar);
                        break;
                    case '\\':
                        result.append(nextChar);
                        break;
                    case '/':
                        result.append(nextChar);
                        break;
                    case 'b':
                        result.append('\b');
                        break;
                    case 'f':
                        result.append('\f');
                        break;
                    case 'n':
                        result.append('\n');
                        break;
                    case 'r':
                        result.append('\r');
                        break;
                    case 't':
                        result.append('\t');
                        break;
                    case 'u':
                        result.append(fromUnicodeEscapedRepr(inputStr, i));
                        i = i + 4;
                        break;
                    default:
                        throw new IllegalArgumentException("Unterminated character");
                }

            } else {
                result.append(inputStr.charAt(i));
                i++;
            }
        }
        return result.toString();
    }

    private static String fromUnicodeEscapedRepr(String input, int curPos) {

        StringBuilder number = new StringBuilder();
        if (curPos + 4 > input.length()) {
            throw new RuntimeException("Not enough unicode digits! ");
        }
        for (char x : input.substring(curPos, curPos + 4).toCharArray()) {
            if (!Character.isLetterOrDigit(x)) {
                throw new RuntimeException("Bad character in unicode escape.");
            }
            number.append(Character.toLowerCase(x));
        }
        int code = Integer.parseInt(number.toString(), 16);
        return String.valueOf((char) code);
    }

    public @Nullable Object eval(Object input) {

        try {
            if (input == null) {
                return null;
            }
            BinaryStringData bs = (BinaryStringData) input;
            String inputStr = bs.toString();
            if (isValidJsonVal(inputStr)) {

                return new BinaryStringData(unescape(inputStr));
            } else {
                return new BinaryStringData(inputStr);
            }
        } catch (Throwable t) {
            throw new FlinkRuntimeException(t);
        }
    }
}
