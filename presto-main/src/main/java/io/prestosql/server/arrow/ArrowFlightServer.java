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
    public static int DEFAULT_ARROW_FLIGHT_PORT = 47470;

    private QueuedStatementResource queuedStatementResource;
    private final FlightServer flightServer;
    private final ArrowServerConfig arrowServerConfig;
    private final InMemoryStore mem;
    private final RootAllocator allocator;
    private final Location location;

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
        arrowServerConfig.setArrowServerPort(DEFAULT_ARROW_FLIGHT_PORT);
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

    private static String getLocalCanonicalHostName()
    {
        try {
            return InetAddress.getLocalHost().getCanonicalHostName().toLowerCase(Locale.US);
        }
        catch (UnknownHostException e) {
            throw new UncheckedIOException(e);
        }
    }
}
