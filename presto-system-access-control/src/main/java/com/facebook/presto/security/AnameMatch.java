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

import java.nio.CharBuffer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import static com.facebook.presto.security.ParseSupport.peek;
import static com.facebook.presto.security.ParseSupport.skip;
import static com.facebook.presto.security.ParseSupport.strchr;
import static com.facebook.presto.spi.StandardErrorCode.SYNTAX_ERROR;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class AnameMatch
{
    private Pattern pattern;
    private boolean hasPattern;
    private boolean validPattern;

    public AnameMatch(Pattern pattern)
    {
        this.pattern = requireNonNull(pattern, "pattern is null");
        this.hasPattern = true;
        this.validPattern = true;
    }

    public AnameMatch(boolean hasPattern, boolean validPattern)
    {
        this.hasPattern = hasPattern;
        this.validPattern = validPattern;
    }

    public static AnameMatch parse(CharBuffer ruleText)
    {
        if (peek(ruleText) != '(') {
            return new AnameMatch(false, false);
        }

        // Consume leading parenthesis.
        skip(ruleText, 1);

        int patternLength = strchr(ruleText, ')');
        if (patternLength == -1) {
            throw new PrestoException(
                    SYNTAX_ERROR,
                    format("Unterminated (pattern) in %s", ruleText.rewind().toString()));
        }

        String patternString = ruleText.subSequence(0, patternLength).toString();

        // Consume pattern and trailing parenthesis
        skip(ruleText, patternString.length() + 1);

        try {
            Pattern pattern = Pattern.compile(patternString);
            return new AnameMatch(pattern);
        }
        catch (PatternSyntaxException e) {
            /*
             * I swear this actually follows the Kerberos 5 version 1.15
             * implementation of aname_do_match(). See src/lib/krb5/os/localauth_rule.c.
             * If the pattern fails to compile, the result is KRB5_LNAME_NOTRANS.
             */
            return new AnameMatch(true, false);
        }
    }

    public boolean anameDoMatch(String selstring)
    {
        // A rule without a matching pattern matches everything.
        if (!hasPattern) {
            return true;
        }

        // A rule with an invalid pattern matches nothing.
        if (!validPattern) {
            return false;
        }

        Matcher matcher = pattern.matcher(selstring);
        return matcher.find() && matcher.start() == 0 && matcher.end() == selstring.length();
    }
}
