package net.floodlightcontroller.loadbalancer.RouteByToS;

import net.floodlightcontroller.core.module.IFloodlightService;
import net.floodlightcontroller.routing.IRoutingService;
import net.floodlightcontroller.routing.Link;

import java.util.Map;
import java.util.Set;

/**
 * Created by Victor on 2017/2/26.
 */
public interface IRouteByToS extends IFloodlightService, IRoutingService {
    public Map<Link, Double> getLinkCost();

    public Map<Long, Set<Link>> getWholeTopology() ;
}
