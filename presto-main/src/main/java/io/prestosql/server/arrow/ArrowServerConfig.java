package io.prestosql.server.arrow;

import io.airlift.configuration.Config;

public class ArrowServerConfig
{
    int arrowServerPort;

    public int getArrowServerPort()
    {
        return arrowServerPort;
    }

    @Config("arrow.server.port")
    public void setArrowServerPort(int arrowServerPort)
    {
        this.arrowServerPort = arrowServerPort;
    }
}
