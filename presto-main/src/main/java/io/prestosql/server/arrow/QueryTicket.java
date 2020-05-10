package io.prestosql.server.arrow;

import java.io.IOException;
import java.util.List;

import org.apache.arrow.flight.Ticket;
import org.apache.arrow.util.Preconditions;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.base.Throwables;

@JsonSerialize
public class QueryTicket
{

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final List<String> path;
    private final int ordinal;

    // uuid to ensure that a stream from one node is not recreated on another node and mixed up.
    private final String uuid;

    /**
     * Constructs a new instance.
     *
     * @param path Path to data
     * @param ordinal A counter for the stream.
     * @param uuid  A unique identifier for this particular stream.
     */
    @JsonCreator
    public QueryTicket(@JsonProperty("path") List<String> path, @JsonProperty("ordinal") int ordinal,
            @JsonProperty("uuid") String uuid) {
        super();
        Preconditions.checkArgument(ordinal >= 0);
        this.path = path;
        this.ordinal = ordinal;
        this.uuid = uuid;
    }

    public List<String> getPath() {
        return path;
    }

    public int getOrdinal() {
        return ordinal;
    }

    public String getUuid() {
        return uuid;
    }

    /**
     * Deserializes a new instance from the protocol buffer ticket.
     */
    public static QueryTicket from(Ticket ticket) {
        try {
            return MAPPER.readValue(ticket.getBytes(), QueryTicket.class);
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    /**
     *  Creates a new protocol buffer Ticket by serializing to JSON.
     */
    public Ticket toTicket() {
        try {
            return new Ticket(MAPPER.writeValueAsBytes(this));
        } catch (JsonProcessingException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ordinal;
        result = prime * result + ((path == null) ? 0 : path.hashCode());
        result = prime * result + ((uuid == null) ? 0 : uuid.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        QueryTicket other = (QueryTicket) obj;
        if (ordinal != other.ordinal) {
            return false;
        }
        if (path == null) {
            if (other.path != null) {
                return false;
            }
        } else if (!path.equals(other.path)) {
            return false;
        }
        if (uuid == null) {
            if (other.uuid != null) {
                return false;
            }
        } else if (!uuid.equals(other.uuid)) {
            return false;
        }
        return true;
    }

}
