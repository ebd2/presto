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
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;

import javax.security.auth.kerberos.KerberosPrincipal;

import java.nio.CharBuffer;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.facebook.presto.security.KerberosExactMatchAccessControl.getUserName;
import static com.facebook.presto.security.ParseSupport.peek;
import static com.facebook.presto.security.ParseSupport.skip;
import static com.facebook.presto.security.ParseSupport.strcspn;
import static com.facebook.presto.security.ParseSupport.strtoi10;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_USER_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.StandardErrorCode.SYNTAX_ERROR;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class AnameSelstring
{
    private int componentCount;
    private List<SelstringPart> parts;

    public AnameSelstring(int componentCount, List<SelstringPart> parts)
    {
        this.componentCount = componentCount;
        this.parts = requireNonNull(parts, "parts is null");
    }

    /*
     * Ported from aname_get_selstring
     */
    public static AnameSelstring parse(CharBuffer ruleText)
    {
        ImmutableList.Builder<SelstringPart> parts = ImmutableList.builder();

        if (ruleText.get() != '[') {
            throw new PrestoException(
                    NOT_SUPPORTED,
                    format(
                            "auth_to_local rules with no selection string are not supported in rule %s",
                            ruleText.rewind().toString()));
        }

        int componentCount = strtoi10(ruleText);

        if (componentCount < 0) {
            throw new PrestoException(
                    SYNTAX_ERROR,
                    format(
                            "A negative component count is not allowed in rule %s",
                            ruleText.rewind().toString()));
        }

        if (peek(ruleText) != ':') {
            throw new PrestoException(
                    SYNTAX_ERROR,
                    format(
                            "Component count must be followed by a colon ':' in rule %s",
                            ruleText.rewind().toString()));
        }

        // consume :
        skip(ruleText, 1);

        while (true) {
            int literalLength = strcspn(ruleText, "$]");
            if (literalLength > 0) {
                /*
                 * Can't refer to ruleText in the lamba. It's final, but mutable,
                 * so the position will very likely be something different when
                 * subSequence is called.
                 */
                String literal = ruleText.subSequence(0, literalLength).toString();
                parts.add((components, realm) -> literal);
            }
            skip(ruleText, literalLength);

            if (!ruleText.hasRemaining() || peek(ruleText) != '$') {
                break;
            }

            // consume $
            skip(ruleText, 1);
            int componentIndex = strtoi10(ruleText);

            if (componentIndex > componentCount) {
                throw new PrestoException(
                        GENERIC_USER_ERROR,
                        format(
                                "Invalid component index %d. Maximum allowed by rule is %s. Rule is '%s'",
                                componentIndex,
                                componentCount,
                                ruleText.rewind().toString()));
            }

            if (componentIndex > 0) {
                parts.add((components, realm) -> components.get(componentIndex - 1));
            }
            else {
                parts.add((components, realm) -> realm);
            }
        }

        if (!ruleText.hasRemaining() || peek(ruleText) != ']') {
            throw new PrestoException(
                    SYNTAX_ERROR,
                    format(
                            "Unterminated selection string in rule %s",
                            ruleText.rewind().toString()));
        }

        return new AnameSelstring(componentCount, parts.build());
    }

    private static List<String> splitComponents(KerberosPrincipal principal)
    {
        return ImmutableList.copyOf(Splitter.on("/").split(getUserName(principal)));
    }

    public Optional<String> anameGetSelstring(KerberosPrincipal principal)
    {
        String realmName = principal.getRealm();
        List<String> userNameComponents = splitComponents(principal);

        if (userNameComponents.size() != componentCount) {
            return Optional.empty();
        }

        return Optional.of(
                parts.stream()
                .map(part -> part.get(userNameComponents, realmName))
                .collect(Collectors.joining()));
    }

    private interface SelstringPart
    {
        String get(List<String> components, String realm);
    }
}
