package net.floodlightcontroller.loadbalancer.energySaving;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.openflow.protocol.OFFlowMod;
import org.openflow.protocol.OFMatch;
import org.openflow.protocol.OFPortMod;
import org.openflow.protocol.OFType;
import org.openflow.protocol.Wildcards;
import org.openflow.protocol.Wildcards.Flag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.floodlightcontroller.core.IFloodlightProviderService;
import net.floodlightcontroller.core.IOFSwitch;
import net.floodlightcontroller.core.ImmutablePort;
import net.floodlightcontroller.core.module.FloodlightModuleContext;
import net.floodlightcontroller.core.module.FloodlightModuleException;
import net.floodlightcontroller.core.module.IFloodlightModule;
import net.floodlightcontroller.core.module.IFloodlightService;
import net.floodlightcontroller.core.util.SingletonTask;
import net.floodlightcontroller.linkCostService.ILinkCostService;
import net.floodlightcontroller.linkdiscovery.ILinkDiscoveryService;
import net.floodlightcontroller.mst.Mst;
import net.floodlightcontroller.packet.IPv4;
import net.floodlightcontroller.routing.IRoutingService;
import net.floodlightcontroller.routing.Link;
import net.floodlightcontroller.threadpool.IThreadPoolService;

public class EnergySavingBaseOnMst implements IFloodlightModule,
		IFloodlightService {

	private IFloodlightProviderService floodlightProvider;
	private IThreadPoolService threadPool;
	private ILinkDiscoveryService linkDiscoveryManager;
	private IRoutingService routingService;
	private Mst mst;
	private ILinkCostService linkCostService;
	private SingletonTask newInstanceTask;
	protected static Logger log = LoggerFactory
			.getLogger(EnergySavingBaseOnMst.class);
	// 网络的完整拓扑（网络节能前）
	private Map<Long, Set<Link>> wholeTopology;
	private Map<Long, Set<Link>> currentTopology;
	private int linkNumber;
	// 链路权重
	private Map<Link, Integer> linkCost;
	private Link maxWeightLink = null;
	private Integer maxWeight = 0;
	private Integer threshold = 8;

	/**
	 * 周期性的检测网络链路权重，返回链路权重大于设定阈值的链路
	 * 
	 * @param linkCost
	 * @return
	 */
	public Link detectLinkWeight(Map<Link, Integer> linkCost) {

		// 周期检测链路权重，周期开始时总是将下列值初始化为null和0
		maxWeightLink = null;
		maxWeight = 0;

		Set<Link> links = linkCost.keySet();
		Iterator<Link> iteratorLinks = links.iterator();
		while (iteratorLinks.hasNext()) {
			Link link = iteratorLinks.next();
			Integer weight = linkCost.get(link);
			if (weight >= maxWeight) {
				maxWeight = weight;
				maxWeightLink = link;
			}
		}
		log.info("maxWeight {},link {}", new Object[] { maxWeight,
				maxWeightLink });
		if (maxWeight > threshold) {
			return maxWeightLink;
		}
		return null;
	}

	/**
	 * 返回拥塞链路的环装替代链路，该方法假设拥塞链路是无向链路
	 * 
	 * @param wholeTopology
	 * @param currentTopology
	 * @param overloadLink
	 * @return
	 */
	public Link getLoopLinkNonBaseDirected(Link overloadLink,
			Map<Long, Set<Link>> wholeTopology,
			Map<Long, Set<Link>> currentTopology) {
		Link selectedLink = getLoopLinkBaseDirected(overloadLink,
				wholeTopology, currentTopology);
		if (selectedLink == null) {
			Link link = findSelectedLink(wholeTopology, overloadLink.getDst(),
					overloadLink.getSrc());
			return getLoopLinkBaseDirected(link, wholeTopology, currentTopology);
		} else {
			return selectedLink;
		}
	}

	/**
	 * 返回拥塞链路的环装替代链路，该方法假设拥塞链路是有向链路
	 * 
	 * @param overloadLink
	 * @param wholeTopology
	 * @param currentTopology
	 * @return
	 */
	public Link getLoopLinkBaseDirected(Link overloadLink,
			Map<Long, Set<Link>> wholeTopology,
			Map<Long, Set<Link>> currentTopology) {

		int count = currentTopology.size();
		if (overloadLink == null) {
			return null;
		}
		long src = overloadLink.getSrc();
		long dst = overloadLink.getDst();
		boolean[] visited = new boolean[count + 1];
		for (int i = 0; i <= count; i++) {
			visited[i] = false;
		}
		PriorityBlockingQueue<Long> queue = new PriorityBlockingQueue<Long>();
		queue.add(dst);
		Link tempLink = null;
		Link selectedLink = null;
		while (!queue.isEmpty()) {
			long dpid = queue.poll();
			Set<Link> links = currentTopology.get(dpid);
			Iterator<Link> iter = links.iterator();
			while (iter.hasNext()) {
				tempLink = iter.next();
				long tempDst = tempLink.getDst();
				if (generalLinkEquals(tempLink, overloadLink)) {
					continue;
				}
				selectedLink = findSelectedLink(wholeTopology, tempDst, src);
				if (selectedLink != null) {
					if (!this.generalLinkEquals(overloadLink, selectedLink)) {
						return selectedLink;
					}
				}
				if (!visited[(int) tempDst]) {
					visited[(int) tempDst] = true;
					queue.add(tempDst);
				}
			}
		}
		return null;
	}

	/**
	 * 从给定拓扑中找到一条和指定源节点、目的节点一致的链路
	 * 
	 * @param wholeTopology
	 * @param src
	 * @param dst
	 * @return
	 */
	public Link findSelectedLink(Map<Long, Set<Link>> wholeTopology, long src,
			long dst) {
		Set<Link> links = wholeTopology.get(src);
		Iterator<Link> iterator = links.iterator();
		while (iterator.hasNext()) {
			Link link = iterator.next();
			if (link.getDst() == dst) {
				return link;
			}
		}
		return null;
	}

	/**
	 * 判断两条有向链路是否隶属于同一条无向链路，供getLoopLinkBaseDirected使用
	 * 
	 * @param link1
	 * @param link2
	 * @return
	 */
	public boolean generalLinkEquals(Link link1, Link link2) {
		long src = link1.getSrc();
		short srcPort = link1.getSrcPort();
		long dst = link1.getDst();
		short dstPort = link1.getDstPort();
		Link reverseLink = new Link(dst, dstPort, src, srcPort);
		if (link1.equals(link2) || reverseLink.equals(link2)) {
			return true;
		}
		return false;
	}

	/**
	 * 完成对网络拓扑信息的复制, 将网络的初始拓扑保存下来
	 */
	public void copySwitchLinks() {
		linkNumber = 0; // 重新对linkNumber进行赋值，故对其做初始化操作；
		Map<Long, Set<Link>> switchLinks = linkDiscoveryManager
				.getSwitchLinks();
		Set<Long> keys = switchLinks.keySet();
		Iterator<Long> iter1 = keys.iterator();
		while (iter1.hasNext()) {
			Long key = iter1.next();
			Set<Link> links = switchLinks.get(key);
			Set<Link> srcLink = new HashSet<Link>();
			Iterator<Link> iter2 = links.iterator();
			while (iter2.hasNext()) {
				Link link = new Link();
				link = iter2.next();
				if (key == link.getSrc()) {
					srcLink.add(link);
					linkNumber++;
				}
			}
			currentTopology.put(key, srcLink);
		}
		log.info("EnergySavingBaseOnMst.copySwitchLinks linkNumber {}",linkNumber);
	}

	public boolean setLinkUp(Link link) {
		short portNumber = link.getSrcPort();
		long dpid = link.getSrc();
		IOFSwitch ofs = floodlightProvider.getSwitch(dpid);
		if (setPortUp(ofs, portNumber)) {
			log.info("EnergySavingBaseOnMst.setLinkUp {} up", link);
			return true;
		}
		return false;
	}

	public boolean setPortUp(IOFSwitch ofs, short portNumber) {
		// 获得OpenFlow交换机的某个端口的物理地址
		ImmutablePort ofpPort = ofs.getPort(portNumber);
		if (ofpPort == null)
			return false;
		if (portNumber == -2)
			return false;
		byte[] macAddress = ofpPort.getHardwareAddress();
		// 定义OFPortMod命令
		OFPortMod mymod = (OFPortMod) floodlightProvider.getOFMessageFactory()
				.getMessage(OFType.PORT_MOD);
		// 设置OFPortMod命令的相关参数
		mymod.setPortNumber(portNumber);
		// mymod.setConfig(0); 开启某个端口
		mymod.setConfig(0);
		mymod.setHardwareAddress(macAddress);
		mymod.setMask(0xffffffff);
		// 将OFPortMod命令发送到指定的交换机中，进行执行！
		try {
			ofs.write(mymod, null);
			ofs.flush();
		} catch (Exception e) {
			log.error("link down fail");
		}
		return true;
	}

	public void deleteFlowEntry(long dpid, short portNumber) {
		IOFSwitch sw=floodlightProvider.getSwitch(dpid);
		OFMatch match = new OFMatch();
		match.setWildcards(Wildcards.FULL.matchOn(Flag.TP_DST));
		match.setNetworkProtocol(IPv4.PROTOCOL_UDP);
		match.setTransportDestination((short) 5001);
		OFFlowMod ofFlowMod = (OFFlowMod) floodlightProvider
				.getOFMessageFactory().getMessage(OFType.FLOW_MOD);
		ofFlowMod.setMatch(match);
		ofFlowMod.setCommand(OFFlowMod.OFPFC_DELETE);
		ofFlowMod.setOutPort(portNumber);

		try {
			sw.write(ofFlowMod, null);
			sw.flush();
			log.info("EnergySavingBaseOnMst.deleteFlowEntry Dpid{} portNumber{}",new Object[]{sw.getId(),portNumber});
		} catch (Exception e) {
			log.info("EnergySavingBaseOnMst.deleteFlowEntry error Dpid{} portNumber{}",new Object[]{sw.getId(),portNumber});
		}
	}

	@Override
	public Collection<Class<? extends IFloodlightService>> getModuleServices() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<Class<? extends IFloodlightService>, IFloodlightService> getServiceImpls() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Collection<Class<? extends IFloodlightService>> getModuleDependencies() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void init(FloodlightModuleContext context)
			throws FloodlightModuleException {
		floodlightProvider = context
				.getServiceImpl(IFloodlightProviderService.class);
		threadPool = context.getServiceImpl(IThreadPoolService.class);
		mst = context.getServiceImpl(Mst.class);
		linkCostService = context.getServiceImpl(ILinkCostService.class);
		linkDiscoveryManager = context
				.getServiceImpl(ILinkDiscoveryService.class);
		routingService=context.getServiceImpl(IRoutingService.class);
	}

	@Override
	public void startUp(FloodlightModuleContext context)
			throws FloodlightModuleException {
		currentTopology = new HashMap<Long, Set<Link>>();
		ScheduledExecutorService ses = threadPool.getScheduledExecutor();
		newInstanceTask = new SingletonTask(ses, new Runnable() {
			public void run() {
				try {
					wholeTopology = mst.getWholeTopology();
					copySwitchLinks(); // 保存当前网络的拓扑到currentTopology；
					linkCost = linkCostService.getLinkCost();
					Link overloadLink = detectLinkWeight(linkCost);
					if (overloadLink != null) {
						Link loopLink = getLoopLinkNonBaseDirected(
								overloadLink, wholeTopology, currentTopology);
						if (loopLink != null) {
							deleteFlowEntry(overloadLink.getSrc(),overloadLink.getSrcPort());
							log.info("LoopLink {}",loopLink);
							setLinkUp(loopLink);
						}
					}
					//log.info("route {}",routingService.getRoute(1, 4, 0));
				} catch (Exception e) {
					e.printStackTrace();
				} finally {
					newInstanceTask.reschedule(10, TimeUnit.SECONDS);
				}
			}
		});
		newInstanceTask.reschedule(30, TimeUnit.SECONDS);
	}

}
