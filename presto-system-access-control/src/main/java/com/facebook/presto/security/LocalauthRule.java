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

import javax.security.auth.kerberos.KerberosPrincipal;

import java.nio.CharBuffer;
import java.util.Optional;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

public class LocalauthRule
        implements Function<KerberosPrincipal, Optional<String>>
{
    public static final String TYPE = "RULE";

    private AnameSelstring anameSelstring;
    private AnameMatch anameMatch;
    private AnameReplacer anameReplacer;

    public LocalauthRule(AnameSelstring selstring, AnameMatch match, AnameReplacer replacer)
    {
        this.anameSelstring = requireNonNull(selstring, "anameSelstring is null");
        this.anameMatch = requireNonNull(match, "anameMatch is null");
        this.anameReplacer = requireNonNull(replacer, "anameReplacer is null");
    }

    public static LocalauthRule parse(CharBuffer residual)
    {
        AnameSelstring anameSelstring = AnameSelstring.parse(residual);
        AnameMatch anameMatch = AnameMatch.parse(residual);
        AnameReplacer anameReplacer = AnameReplacer.parse(residual);

        return new LocalauthRule(anameSelstring, anameMatch, anameReplacer);
    }

    public Optional<String> apply(KerberosPrincipal principal)
    {
        Optional<String> selstring = anameSelstring.anameGetSelstring(principal);

        if (!selstring.isPresent()) {
            return Optional.empty();
        }

        if (!anameMatch.anameDoMatch(selstring.get())) {
            return Optional.empty();
        }

        if (!anameReplacer.canTranslate()) {
            return Optional.empty();
        }

        return Optional.of(anameReplacer.anameReplacer(selstring.get()));
    }
}
