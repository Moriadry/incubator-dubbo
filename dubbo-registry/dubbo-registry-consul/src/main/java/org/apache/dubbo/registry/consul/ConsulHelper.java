package org.apache.dubbo.registry.consul;

import com.ecwid.consul.v1.ConsulClient;
import com.ecwid.consul.v1.QueryParams;
import com.ecwid.consul.v1.Response;
import com.ecwid.consul.v1.health.HealthServicesRequest;
import com.ecwid.consul.v1.health.model.HealthService;
import org.apache.dubbo.common.URL;
import org.apache.dubbo.registry.client.ServiceInstance;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import static org.apache.dubbo.registry.consul.AbstractConsulRegistry.SERVICE_TAG;
import static org.apache.dubbo.registry.consul.AbstractConsulRegistry.WATCH_TIMEOUT;
import static org.apache.dubbo.registry.consul.AbstractConsulRegistry.DEFAULT_WATCH_TIMEOUT;


final class ConsulHelper {

    public static Response<List<HealthService>> getHealthServices(String service, long index, int watchTimeout, String tag, ConsulClient client) {
        HealthServicesRequest request = HealthServicesRequest.newBuilder()
                .setTag(tag)
                .setQueryParams(new QueryParams(watchTimeout, index))
                .setPassing(true)
                .build();
        return client.getHealthServices(service, request);
    }

    public static List<HealthService> getHealthServices(Map<String, List<String>> services, ConsulClient client) {
        return services.entrySet().stream()
                .filter(s -> s.getValue().contains(SERVICE_TAG))
                .map(s -> ConsulHelper.getHealthServices(s.getKey(), -1, -1, SERVICE_TAG, client).getValue())
                .flatMap(Collection::stream)
                .collect(Collectors.toList());
    }

    public static int buildWatchTimeout(URL url) {
        return url.getParameter(WATCH_TIMEOUT, DEFAULT_WATCH_TIMEOUT) / 1000;
    }

    public static String buildIdForRegistry(URL url) {
        // let's simply use url's hashcode to generate unique service id for now
        return Integer.toHexString(url.hashCode());
    }

    public static String buildIdForServiceDiscovery(ServiceInstance serviceInstance) {
        return Integer.toHexString(serviceInstance.hashCode());
    }

}
