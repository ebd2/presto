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
import org.testng.annotations.Test;

import java.nio.CharBuffer;

import static com.facebook.presto.security.ParseSupport.peek;
import static com.facebook.presto.security.ParseSupport.skip;
import static com.facebook.presto.security.ParseSupport.strchr;
import static com.facebook.presto.security.ParseSupport.strcspn;
import static com.facebook.presto.security.ParseSupport.strtoi10;
import static com.facebook.presto.security.ParseSupport.wrap;
import static org.testng.Assert.assertEquals;

public class TestParseSupport
{
    @Test
    public void testStrtol10Valid()
    {
        assertEquals(strtoi10(wrap("0")), 0);
        assertEquals(strtoi10(wrap("2179")), 2179);
        assertEquals(strtoi10(wrap("  2179")), 2179);
        assertEquals(strtoi10(wrap("\t2179")), 2179);
        assertEquals(strtoi10(wrap("+2179")), 2179);
        assertEquals(strtoi10(wrap("-2179   ")), -2179);
        assertEquals(strtoi10(wrap("2179asdf")), 2179);
        assertEquals(strtoi10(wrap("\t-2179$1")), -2179);
        assertEquals(strtoi10(wrap(Integer.toString(Integer.MAX_VALUE))), Integer.MAX_VALUE);
        assertEquals(strtoi10(wrap(Integer.toString(Integer.MIN_VALUE))), Integer.MIN_VALUE);

        CharBuffer buffer = wrap("2179asdf");
        assertEquals(strtoi10(buffer), 2179);
        assertEquals(buffer.position(), 4);
    }

    @Test(expectedExceptions = PrestoException.class)
    public void testStrtol10NoNumber()
    {
        strtoi10(wrap("asdf"));
    }

    @Test(expectedExceptions = PrestoException.class)
    public void testStrtol10NoNumberLeadingWhitespace()
    {
        strtoi10(wrap("   -asdf"));
    }

    @Test(expectedExceptions = PrestoException.class)
    public void testTooPositive()
    {
        long t = (long) Integer.MAX_VALUE + 1L;
        strtoi10(wrap(Long.toString(t)));
    }

    @Test(expectedExceptions = PrestoException.class)
    public void testTooNegative()
    {
        long t = (long) Integer.MIN_VALUE - 1L;
        strtoi10(wrap(Long.toString(t)));
    }

    @Test
    public void testStrcspn()
    {
        String chars = "$]";
        CharBuffer buffer = wrap("[2$1asdf$2]");
        assertEquals(strcspn(buffer, chars), 2);
        skip(buffer, 2);
        assertEquals(strcspn(buffer, chars), 0);
        skip(buffer, 2);
        assertEquals(peek(buffer), 'a');
        assertEquals(strcspn(buffer, chars), 4);
        skip(buffer, 4);
        assertEquals(peek(buffer), '$');
        assertEquals(strcspn(buffer, chars), 0);
        skip(buffer, 2);
        assertEquals(peek(buffer), ']');
        assertEquals(strcspn(buffer, chars), 0);
    }

    @Test
    public void testStrchr()
    {
        CharBuffer buffer = wrap("asdsf");
        int startPosition = buffer.position();

        // character at position
        assertEquals(strchr(buffer, 'a'), 0);
        assertEquals(buffer.position(), startPosition);

        // make sure we find the first instance when there are multiple instances
        assertEquals(strchr(buffer, 's'), 1);
        assertEquals(buffer.position(), startPosition);

        // character at limit
        assertEquals(strchr(buffer, 'f'), 4);
        assertEquals(buffer.position(), startPosition);

        // character not in buffer
        assertEquals(strchr(buffer, 'g'), -1);
        assertEquals(buffer.position(), startPosition);

        // position not at actual beginning of buffer
        buffer.position(strchr(buffer, 's'));

        assertEquals(strchr(buffer, 's'), 0);
        skip(buffer, 1);
        assertEquals(strchr(buffer, 's'), 1);
    }
}
