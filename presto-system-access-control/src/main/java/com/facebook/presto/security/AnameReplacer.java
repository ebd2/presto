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
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;

import java.nio.CharBuffer;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import static com.facebook.presto.security.ParseSupport.peek;
import static com.facebook.presto.security.ParseSupport.skip;
import static com.facebook.presto.security.ParseSupport.strchr;
import static com.facebook.presto.spi.StandardErrorCode.SYNTAX_ERROR;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class AnameReplacer
{
    private List<ReplacementRule> replacementRules;

    public AnameReplacer(List<ReplacementRule> replacementRules)
    {
        this.replacementRules = ImmutableList.copyOf(requireNonNull(replacementRules, "replacementRules is null"));
    }

    public static AnameReplacer parse(CharBuffer ruleText)
    {
        return new AnameReplacer(ReplacementRule.parse(ruleText));
    }

    public boolean canTranslate()
    {
        boolean result = true;

        for (ReplacementRule rule : replacementRules) {
            result = result && rule.canTranslate();
        }

        return result;
    }

    public String anameReplacer(String selString)
    {
        String current = selString;
        for (ReplacementRule rule : replacementRules) {
            current = rule.doReplacement(current);
        }
        return current;
    }

    @VisibleForTesting
    static class ReplacementRule
    {
        private boolean validPattern;
        private Pattern rule;
        private String replacement;
        private boolean doGlobal;

        public ReplacementRule(Pattern rule, String replacement, boolean doGlobal)
        {
            this.rule = requireNonNull(rule, "rule is null");
            this.replacement = requireNonNull(replacement, "replacement is null");
            this.doGlobal = requireNonNull(doGlobal, "global is null");
            this.validPattern = true;
        }

        public ReplacementRule()
        {
            this.validPattern = false;
        }

        public static List<ReplacementRule> parse(CharBuffer ruleText)
        {
            ImmutableList.Builder<ReplacementRule> result = ImmutableList.builder();
            while (ruleText.hasRemaining()) {
                String ruleString;
                Pattern rule;
                String replacementString;
                boolean doGlobal = false;

                while (Character.isWhitespace(peek(ruleText))) {
                    skip(ruleText, 1);
                }

                if (!(ruleText.charAt(0) == 's' && ruleText.charAt(1) == '/')) {
                    throw new PrestoException(SYNTAX_ERROR,
                            format(
                                    "ReplacementRule must start with 's/' in rule %s",
                                    ruleText.rewind().toString()));
                }

                // consume "s/"
                skip(ruleText, 2);

                int ruleLength = strchr(ruleText, '/');
                if (ruleLength == -1) {
                    throw new PrestoException(SYNTAX_ERROR,
                            format(
                                    "Unterminated pattern in rule '%s'",
                                    ruleText.rewind().toString()));
                }

                ruleString = ruleText.subSequence(0, ruleLength).toString();
                // consume rule and terminating /
                skip(ruleText, ruleLength + 1);

                int replacementLength = strchr(ruleText, '/');
                if (replacementLength == -1) {
                    throw new PrestoException(SYNTAX_ERROR,
                            format(
                                    "Unterminated replacement in rule '%s'",
                                    ruleText.rewind().toString()));
                }

                // consume replacement and terminating /
                replacementString = ruleText.subSequence(0, replacementLength).toString();
                skip(ruleText, replacementLength + 1);

                if (ruleText.hasRemaining() && peek(ruleText) == 'g') {
                    doGlobal = true;
                    skip(ruleText, 1);
                }

                try {
                    rule = Pattern.compile(ruleString);
                    result.add(new ReplacementRule(rule, replacementString, doGlobal));
                }
                catch (PatternSyntaxException e) {
                    /*
                     * Weird, but consistent with Kerberos 5 1.15.
                     * See do_replacement in src/lib/krb5/os/localauth_rule.c.
                     */
                    rule = Pattern.compile(ruleString);
                }
            }

            return result.build();
        }

        public boolean canTranslate()
        {
            // See the catch where we compile the pattern.
            return validPattern;
        }

        public String doReplacement(String current)
        {
            checkState(validPattern, "Can't call doReplacement with an invalid pattern");

            StringBuilder result = new StringBuilder();
            int searchAt = 0;
            Matcher matcher = rule.matcher(current);

            while (matcher.find()) {
                result.append(current.substring(searchAt, matcher.start()));
                result.append(replacement);
                searchAt = matcher.end();
                if (!doGlobal) {
                    break;
                }
            }

            result.append(current.substring(searchAt));

            return result.toString();
        }
    }
}
