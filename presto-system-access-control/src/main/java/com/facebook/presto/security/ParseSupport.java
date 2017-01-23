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
package com.facebook.presto.security;

import com.facebook.presto.spi.PrestoException;

import java.nio.Buffer;
import java.nio.CharBuffer;

import static com.facebook.presto.spi.StandardErrorCode.SYNTAX_ERROR;
import static java.lang.String.format;

class ParseSupport
{
    private ParseSupport()
    {
    }

    static char peek(CharBuffer buffer)
    {
        return buffer.charAt(0);
    }

    static Buffer skip(Buffer buffer, int skip)
    {
        return buffer.position(buffer.position() + skip);
    }

    private static int parseInt(CharBuffer buffer, int numberLength)
    {
        try {
            int result = Integer.parseInt(buffer.subSequence(0, numberLength).toString());
            skip(buffer, numberLength);
            return result;
        }
        catch (NumberFormatException e) {
            throw new PrestoException(
                    SYNTAX_ERROR,
                    format(
                            "Invalid integer at '>%s' in string '%s'",
                            buffer.toString(),
                            buffer.rewind().toString()),
                    e);
        }
    }

    static int strtoi10(CharBuffer buffer)
    {
        // Consume leading whitespace
        for (char c; Character.isWhitespace(peek(buffer)); ) {
            c = buffer.get();
        }

        int numberLen = 0;

        if (peek(buffer) == '+' || peek(buffer) == '-') {
            numberLen += 1;
        }

        for (int i = numberLen; i < buffer.remaining() && Character.isDigit(buffer.charAt(i)); ++i) {
            ++numberLen;
        }

        return parseInt(buffer, numberLen);
    }

    static int strcspn(CharBuffer buffer, String characters)
    {
        int i;
        for (i = 0; i < buffer.remaining() && characters.indexOf(buffer.charAt(i)) == -1; ) {
            ++i;
        }
        return i;
    }

    static int strchr(CharBuffer buffer, char c)
    {
        for (int i = 0; i < buffer.remaining();  ++i) {
            if (buffer.charAt(i) == c) {
                return i;
            }
        }
        return -1;
    }
}
