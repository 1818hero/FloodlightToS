package net.floodlightcontroller.linkCostService;

import net.floodlightcontroller.core.module.IFloodlightService;
import net.floodlightcontroller.linkdiscovery.LinkInfo;
import net.floodlightcontroller.routing.Link;
import java.util.Map;
import java.util.Set;

public interface ILinkCostService extends IFloodlightService {
	public Map<Link,Double> getLinkCost();
	public Map<Link,LinkBandwidthType> getLinkTypeMap();
	public double getMaxLinkCompacity();
	public double getLinkCompacity(Link link);
	public Map<Long, Set<Link>> getSwitchLinks();
	public Map<Link, LinkInfo> getLinks();
}
