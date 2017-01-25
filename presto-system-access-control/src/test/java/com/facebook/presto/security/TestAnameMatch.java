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

import org.testng.annotations.Test;

import java.nio.CharBuffer;

import static com.facebook.presto.security.ParseSupport.wrap;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestAnameMatch
{
    @Test
    public void testValid()
    {
        CharBuffer buffer = wrap("(asdf)");
        AnameMatch match = AnameMatch.parse(buffer);
        assertEquals(buffer.position(), buffer.limit());

        assertTrue(match.anameDoMatch("asdf"));
        assertFalse(match.anameDoMatch("1asdf"));
        assertFalse(match.anameDoMatch("asdfg"));
    }

    @Test
    public void testNoMatchExpression()
    {
        CharBuffer buffer = wrap("/asdf/qwer/");
        int startPosition = buffer.position();
        AnameMatch match = AnameMatch.parse(buffer);
        assertEquals(buffer.position(), startPosition);

        assertTrue(match.anameDoMatch("zxcv"));
    }

    @Test
    public void testInvalidRegexp()
    {
        CharBuffer buffer = wrap("(***)");
        AnameMatch match = AnameMatch.parse(buffer);
        assertEquals(buffer.position(), buffer.limit());

        assertFalse(match.anameDoMatch("asdf"));
    }
}
