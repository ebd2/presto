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

import javax.security.auth.kerberos.KerberosPrincipal;

import static com.facebook.presto.security.TestParseSupport.wrap;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

public class TestAnameSelstring
{
    @Test
    public void testValid()
    {
        AnameSelstring selstring = AnameSelstring.parse(wrap("[2:$2;$1]"));

        KerberosPrincipal principal = new KerberosPrincipal("robertbyrd/dalton@REALM.COM");
        assertEquals(selstring.anameGetSelstring(principal).get(), "dalton;robertbyrd");

        principal = new KerberosPrincipal("robertbyrd@REALM.COM");
        assertFalse(selstring.anameGetSelstring(principal).isPresent());
    }

    @Test(expectedExceptions = PrestoException.class, expectedExceptionsMessageRegExp = ".*no selection string.*")
    public void testNoSelstring()
    {
        AnameSelstring.parse(wrap("2:$2;$1]"));
    }

    @Test(expectedExceptions = PrestoException.class, expectedExceptionsMessageRegExp = ".*Invalid integer.*")
    public void testCountNotInteger()
    {
        AnameSelstring.parse(wrap("[two:$2;$1]"));
    }

    @Test(expectedExceptions = PrestoException.class, expectedExceptionsMessageRegExp = ".*negative component.*")
    public void testNegativeCount()
    {
        AnameSelstring.parse(wrap("[-2:$2;$1]"));
    }

    @Test(expectedExceptions = PrestoException.class, expectedExceptionsMessageRegExp = ".*followed by a colon.*")
    public void testCountNoColon()
    {
        AnameSelstring.parse(wrap("[2$2;$1]"));
    }

    @Test(expectedExceptions = PrestoException.class, expectedExceptionsMessageRegExp = ".*Invalid integer.*")
    public void testReferenceNotInteger()
    {
        AnameSelstring.parse(wrap("[2:$2;$one]"));
    }

    @Test(expectedExceptions = PrestoException.class, expectedExceptionsMessageRegExp = ".*Maximum allowed.*")
    public void testReferenceTooBig()
    {
        AnameSelstring.parse(wrap("[2:$3;$1]"));
    }

    @Test(expectedExceptions = PrestoException.class, expectedExceptionsMessageRegExp = "Unterminated selection.*")
    public void testUnterminatedSelstring()
    {
        AnameSelstring.parse(wrap("[2:$2;$1"));
    }
}
