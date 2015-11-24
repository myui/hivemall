/*
 * Hivemall: Hive scalable Machine Learning Library
 *
 * Copyright (C) 2015 Makoto YUI
 * Copyright (C) 2013-2015 National Institute of Advanced Industrial Science and Technology (AIST)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package hivemall.mix.utils;

public final class StringUtils {

    // Join a list of strings using the given separator
    public static String join(String sep, Iterable<String> elements) {
        StringBuilder sb = new StringBuilder();
        for (String e : elements) {
            if (e != null) {
                if (sb.length() > 0) {
                    sb.append(sep);
                }
                sb.append(e);
            }
        }
        return sb.toString();
    }
}
