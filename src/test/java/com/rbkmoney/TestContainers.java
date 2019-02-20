package com.rbkmoney;

import lombok.NoArgsConstructor;
import lombok.Setter;
import org.testcontainers.containers.GenericContainer;

import java.util.Optional;

@NoArgsConstructor
@Setter
public class TestContainers {

    private Boolean dockerContainersEnable;
    private GenericContainer cephTestContainer;

    public Optional<GenericContainer> getCephTestContainer() {
        return Optional.ofNullable(cephTestContainer);
    }

    public Boolean isDockerContainersEnable() {
        return dockerContainersEnable;
    }

    public void startTestContainers() {
        if (!isDockerContainersEnable()) {
            getCephTestContainer().ifPresent(GenericContainer::start);
        }
    }

    public void stopTestContainers() {
        if (!isDockerContainersEnable()) {
            getCephTestContainer().ifPresent(GenericContainer::stop);
        }
    }
}
