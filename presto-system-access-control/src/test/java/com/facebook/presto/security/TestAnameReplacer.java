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

import static com.facebook.presto.security.TestParseSupport.wrap;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestAnameReplacer
{
    private void testReplacer(String ruleString, String selstring, String expected)
    {
        CharBuffer rule = wrap(ruleString);
        AnameReplacer replacer = AnameReplacer.parse(rule);
        assertTrue(replacer.canTranslate());
        assertEquals(replacer.anameReplacer(selstring), expected);
        assertEquals(rule.position(), rule.limit());
    }

    @Test
    public void testValid()
    {
        testReplacer(
                "s/Baton/Eau/",
                "Busted flat in Baton Rouge",
                "Busted flat in Eau Rouge");
    }

    @Test
    public void testNonGlobalFirstOnly()
    {
        testReplacer(
                "s/a/A/",
                "Waiting for a train",
                "WAiting for a train");
    }

    @Test
    public void testGlobalAll()
    {
        testReplacer(
                "   s/as/asn't/g",
                "I was feeling near as faded as my jeans",
                "I wasn't feeling near asn't faded asn't my jeans");
    }

    @Test
    public void testMatchBeginning()
    {
        testReplacer(
                "   s/Bobby/Leroy/",
                "Bobby thumbed a diesel down",
                "Leroy thumbed a diesel down");
    }

    @Test
    public void testMatchEnd()
    {
        testReplacer(
                "s/it rained/the asteroid impact/",
                "Just before it rained",
                "Just before the asteroid impact");
    }

    @Test
    public void testMatchAnchored()
    {
        testReplacer(
                "s/Orleans$/Jersey/",
                "And it rode us all the way to New Orleans",
                "And it rode us all the way to New Jersey");

        testReplacer(
                "s/^I/Cthulu/",
                "I pulled my harpoon out of my dirty red bandanna",
                "Cthulu pulled my harpoon out of my dirty red bandanna");
    }

    @Test
    public void testMultipleReplacementRules()
    {
        testReplacer(
                "s/soft/solemnly/ s/Bobby/Peter Frampton/  s/the blues/Bohemian Rhapsody/",
                "I was playin' soft while Bobby sang the blues, yeah",
                "I was playin' solemnly while Peter Frampton sang Bohemian Rhapsody, yeah");
    }

    @Test(expectedExceptions = PrestoException.class, expectedExceptionsMessageRegExp = ".*must start.*")
    public void testStartNoS()
    {
        AnameReplacer.parse(wrap("/Orleans$/Jersey/"));
    }

    @Test(expectedExceptions = PrestoException.class, expectedExceptionsMessageRegExp = ".*must start.*")
    public void testStartNoSlash()
    {
        AnameReplacer.parse(wrap("sOrleans$/Jersey/"));
    }

    @Test(expectedExceptions = PrestoException.class, expectedExceptionsMessageRegExp = ".*must start.*")
    public void testBadStart()
    {
        AnameReplacer.parse(wrap("Orleans$/Jersey/"));
    }

    @Test(expectedExceptions = PrestoException.class, expectedExceptionsMessageRegExp = "Unterminated pattern.*")
    public void testUnterminatedPattern()
    {
        AnameReplacer.parse(wrap("s/Orleans$"));
    }

    @Test(expectedExceptions = PrestoException.class, expectedExceptionsMessageRegExp = "Unterminated pattern.*")
    public void testNoPattern()
    {
        AnameReplacer.parse(wrap("s/"));
    }

    @Test(expectedExceptions = PrestoException.class, expectedExceptionsMessageRegExp = "Unterminated replacement.*")
    public void testUnterminatedReplacement()
    {
        AnameReplacer.parse(wrap("s/Orleans$/Jersey"));
    }

    @Test(expectedExceptions = PrestoException.class, expectedExceptionsMessageRegExp = "Unterminated replacement.*")
    public void testNoReplacement()
    {
        AnameReplacer.parse(wrap("s/Orleans$/"));
    }
}
