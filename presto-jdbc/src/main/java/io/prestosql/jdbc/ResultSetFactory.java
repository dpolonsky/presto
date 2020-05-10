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
package io.prestosql.jdbc;

import io.prestosql.client.StatementClient;

import java.sql.SQLException;
import java.util.function.Consumer;

import static io.prestosql.jdbc.QueryResultFormat.JSON;

public class ResultSetFactory
{
    private ResultSetFactory()
    {
    }

    static PrestoResultSet getResultSet(StatementClient client, long maxRows, Consumer<QueryStats> progressCallback, WarningsManager warningsManager)
            throws SQLException
    {
        switch (QueryResultFormat.lookupByName(client.getSetSessionProperties().get("JDBC_QUERY_RESULT_FORMAT")).orElse(JSON)) {
            case ARROW:
                return new ArrowResultSet(client, maxRows, progressCallback, warningsManager);
            case JSON:
                return new PrestoResultSet(client, maxRows, progressCallback, warningsManager);
            default:
                throw new RuntimeException("Unsupported query result format");
        }
    }
}
