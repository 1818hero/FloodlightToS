package net.floodlightcontroller.loadbalancer.RouteByToS;

import net.floodlightcontroller.core.module.IFloodlightService;
import net.floodlightcontroller.routing.IRoutingService;
import net.floodlightcontroller.routing.Link;
import net.floodlightcontroller.routing.Route;

import java.util.Map;
import java.util.Set;

/**
 * Created by Victor on 2017/2/26.
 */
public interface IRouteByToS extends IFloodlightService, IRoutingService {
    public Map<Link, Double> getLinkCost();

    /**
     * 根据ToS级别获取相应路由
     * @param src 源交换机dpid
     * @param dst 目的交换机dpid
     * @param cookie cookie(暂时未使用)
     * @param ToSLevel 该匹配包对应的ToS级别
     * @param tunnelEnabled
     * @return
     */
    public Route getRoute(long src, long dst, long cookie, Byte ToS, boolean tunnelEnabled);
    public Route getRoute(long srcId, short srcPort, long dstId, short dstPort, long cookie, Byte ToS, boolean tunnelEnabled);
    public Map<Long, Set<Link>> getWholeTopology() ;
}
