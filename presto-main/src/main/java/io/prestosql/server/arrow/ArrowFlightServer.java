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
package io.prestosql.server.arrow;

import com.google.inject.Inject;
import io.prestosql.dispatcher.QueuedStatementResource;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.util.AutoCloseables;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Locale;

public class ArrowFlightServer
        implements AutoCloseable
{
    private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ArrowFlightServer.class);
    public static int defaultArrowFlightPort = 47470;
    private final FlightServer flightServer;
    private final ArrowServerConfig arrowServerConfig;
    private final InMemoryStore mem;
    private final RootAllocator allocator;
    private final Location location;
    private QueuedStatementResource queuedStatementResource;

    /**
     * Constructs a new instance using Allocator for allocating buffer storage that binds
     * to the given location.
     */
    @Inject
    public ArrowFlightServer(ArrowServerConfig arrowServerConfig, QueuedStatementResource queuedStatementResource)
    {
        allocator = new RootAllocator(Long.MAX_VALUE);
        this.arrowServerConfig = arrowServerConfig;
        this.queuedStatementResource = queuedStatementResource;
        location = Location.forGrpcInsecure(getLocalCanonicalHostName(), arrowServerConfig.getArrowServerPort());
        this.mem = new InMemoryStore(allocator, location);
        this.flightServer = FlightServer.builder(allocator, location, mem).build();
        try {
            flightServer.start();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Main method starts the server listening to localhost:.
     */
    public static void main(String[] args)
            throws Exception
    {
        ArrowServerConfig arrowServerConfig = new ArrowServerConfig();
        arrowServerConfig.setArrowServerPort(defaultArrowFlightPort);
        final ArrowFlightServer efs = new ArrowFlightServer(arrowServerConfig, null);
        efs.start();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                System.out.println("\nExiting...");
                AutoCloseables.close(efs);
            }
            catch (Exception e) {
                e.printStackTrace();
            }
        }));
        efs.awaitTermination();
    }

    private static String getLocalCanonicalHostName()
    {
        try {
            return InetAddress.getLocalHost().getCanonicalHostName().toLowerCase(Locale.US);
        }
        catch (UnknownHostException e) {
            throw new UncheckedIOException(e);
        }
    }

    public Location getLocation()
    {
        return location;
    }

    public void start()
            throws IOException
    {
        flightServer.start();
    }

    public void awaitTermination()
            throws InterruptedException
    {
        flightServer.awaitTermination();
    }

    public InMemoryStore getStore()
    {
        return mem;
    }

    @Override
    public void close()
            throws Exception
    {
        AutoCloseables.close(mem, flightServer, allocator);
    }
}
