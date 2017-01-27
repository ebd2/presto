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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import javax.security.auth.kerberos.KerberosPrincipal;

import java.nio.CharBuffer;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import static com.facebook.presto.security.ParseSupport.skip;
import static com.facebook.presto.security.ParseSupport.strchr;
import static com.facebook.presto.security.ParseSupport.wrap;
import static com.facebook.presto.spi.StandardErrorCode.SYNTAX_ERROR;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class AuthToLocal
{
    private static final Map<
            String,
            Function<CharBuffer, Function<KerberosPrincipal, Optional<String>>>> parsers =
                    ImmutableMap.of(
                            LocalauthRule.TYPE, LocalauthRule::parse);

    private final List<Function<KerberosPrincipal, Optional<String>>> mappingFunctions;
    private final String realm;

    public AuthToLocal(String realm, List<Function<KerberosPrincipal, Optional<String>>> mappingFunctions)
    {
        this.mappingFunctions = ImmutableList.copyOf(requireNonNull(mappingFunctions, "mappingFunctions is null"));
        this.realm = requireNonNull(realm);
    }

    public static AuthToLocal parse(String realm, List<String> authToLocalMappings)
    {
        ImmutableList.Builder<Function<KerberosPrincipal, Optional<String>>> mappingFunctions =
                ImmutableList.builder();

        for (String mapping : authToLocalMappings) {
            CharBuffer mappingBuffer = wrap(mapping);
            String type = getType(mappingBuffer);

            Function<CharBuffer, Function<KerberosPrincipal, Optional<String>>> parser =
                    parsers.get(type);

            if (parser == null) {
                throw new PrestoException(
                        SYNTAX_ERROR,
                        format("Unrecognized auth_to_local type %s in line %s", type, mapping));
            }

            // consume type and :
            skip(mappingBuffer, type.length() + 1);
            mappingFunctions.add(parser.apply(mappingBuffer));
        }

        return new AuthToLocal(realm, mappingFunctions.build());
    }

    public static String getType(CharBuffer mapping)
    {
        int typeLength = strchr(mapping, ':');
        return mapping.subSequence(0, typeLength).toString();
    }

    public Optional<String> krb5AnameToLocalname(KerberosPrincipal principal)
    {
        if (!principal.getRealm().equals(realm)) {
            return Optional.empty();
        }

        for (Function<KerberosPrincipal, Optional<String>> mappingFunction : mappingFunctions) {
            Optional<String> localname = mappingFunction.apply(principal);
            if (localname.isPresent()) {
                return localname;
            }
        }

        return Optional.empty();
    }

    public boolean an2lnUserok(KerberosPrincipal principal, String user)
    {
        Optional<String> mappedName = krb5AnameToLocalname(principal);
        return mappedName.isPresent() && mappedName.get().equals(user);
    }
}
