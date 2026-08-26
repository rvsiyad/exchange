package dev.rvsiyad.exchange.ledger;

import com.github.dockerjava.api.model.Volume;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

/**
 * A disposable single-replica TigerBeetle for integration tests, mirroring
 * the docker-compose service: format once, then start. The datafile lives on
 * an anonymous volume because TigerBeetle opens it with O_DIRECT, which the
 * container's overlay filesystem does not support.
 *
 * Shared with the gateway and settlement test suites via this module's
 * test-jar.
 */
public final class TigerBeetleContainers {

    public static final String IMAGE = "ghcr.io/tigerbeetle/tigerbeetle:0.16.27";

    private TigerBeetleContainers() {
    }

    public static GenericContainer<?> create() {
        // The whole script must reach `sh -c` as ONE argument, so it is set
        // through the modifier — withCommand(String) would word-split it.
        var script = "/tigerbeetle format --cluster=0 --replica=0 --replica-count=1 /data/test.tigerbeetle"
                + " && exec /tigerbeetle start --addresses=0.0.0.0:3000 /data/test.tigerbeetle";
        return new GenericContainer<>(IMAGE)
                .withExposedPorts(3000)
                .withCreateContainerCmdModifier(cmd -> {
                    cmd.withEntrypoint("/bin/sh", "-c");
                    cmd.withCmd(script);
                    cmd.withVolumes(new Volume("/data"));
                    cmd.getHostConfig().withSecurityOpts(java.util.List.of("seccomp=unconfined"));
                })
                .waitingFor(Wait.forListeningPort());
    }

    /** The client's address parser takes ip:port, not hostnames. */
    public static String address(GenericContainer<?> container) {
        var host = container.getHost();
        return ("localhost".equals(host) ? "127.0.0.1" : host) + ":" + container.getMappedPort(3000);
    }
}
