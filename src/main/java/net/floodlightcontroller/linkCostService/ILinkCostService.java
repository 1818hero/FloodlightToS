package net.floodlightcontroller.linkCostService;

import net.floodlightcontroller.core.module.IFloodlightService;
import net.floodlightcontroller.routing.Link;
import java.util.Map;

public interface ILinkCostService extends IFloodlightService {
	public Map<Link,Double> getLinkCost();
	public Map<Link,LinkBandwidthType> getLinkTypeMap();
	public double getMaxLinkCompacity();
}
