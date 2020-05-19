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
package io.prestosql.client;

import okhttp3.OkHttpClient;
import org.apache.arrow.flight.FlightClient;

public final class StatementClientFactory
{
    private StatementClientFactory() {}

    public static StatementClient newStatementClient(OkHttpClient httpClient, ClientSession session, String query)
    {
        return newStatementClient(httpClient, null, session, query);
    }

    public static StatementClient newStatementClient(OkHttpClient httpClient, FlightClient flightClient, ClientSession session, String query)
    {
        if (flightClient == null) {
            return new StatementClientV1(httpClient, session, query);
        }
        else {
            return new StatementClientV2(httpClient, flightClient, session, query);
        }
    }
}
