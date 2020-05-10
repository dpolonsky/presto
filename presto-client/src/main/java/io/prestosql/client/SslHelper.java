package io.prestosql.client;

import org.bouncycastle.openssl.jcajce.JcaPEMWriter;
import org.bouncycastle.util.io.pem.PemObject;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.security.PrivateKey;
import java.security.cert.Certificate;

// com.dremio.flight.SslHelper
public class SslHelper
{
    private static InputStream keyToStream(PrivateKey key)
            throws IOException
    {
        final StringWriter writer = new StringWriter();
        final JcaPEMWriter pemWriter = new JcaPEMWriter(writer);
        pemWriter.writeObject(new PemObject("PRIVATE KEY", key.getEncoded()));
        pemWriter.flush();
        pemWriter.close();
        String pemString = writer.toString();
        return new ByteArrayInputStream(pemString.getBytes());
    }

    private static InputStream certsToStream(Certificate[] certs)
            throws IOException
    {

        final StringWriter writer = new StringWriter();
        final JcaPEMWriter pemWriter = new JcaPEMWriter(writer);
        for (Certificate cert : certs) {
            pemWriter.writeObject(cert);
        }
        pemWriter.flush();
        pemWriter.close();
        String pemString = writer.toString();
        return new ByteArrayInputStream(pemString.getBytes());
    }
}
