package net.floodlightcontroller.loadbalancer.energySaving;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.openflow.protocol.OFPortMod;
import org.openflow.protocol.OFType;
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
import net.floodlightcontroller.routing.Link;
import net.floodlightcontroller.threadpool.IThreadPoolService;

public class EnergySavingBaseOnMst implements IFloodlightModule,
		IFloodlightService {

	private IFloodlightProviderService floodlightProvider;
	private IThreadPoolService threadPool;
	private ILinkDiscoveryService linkDiscoveryManager;
	private Mst mst;
	private ILinkCostService linkCostService;
	private SingletonTask newInstanceTask;
	protected static Logger log = LoggerFactory
			.getLogger(EnergySavingBaseOnMst.class);
	// 网络的完整拓扑（网络节能前）
	private Map<Long, Set<Link>> wholeTopology;
	private Map<Long, Set<Link>> currentTopology;
	// 链路权重
	private Map<Link, Integer> linkCost;
	private Link maxWeightLink = null;
	private Integer maxWeight = 0;
	private Integer threshold = 95;

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
				weight = maxWeight;
				maxWeightLink = link;
			}
		}
		if (maxWeight > threshold) {
			return maxWeightLink;
		}
		return null;
	}

	/**
	 * 返回过载链路的替代链路
	 * 
	 * @param wholeTopology
	 * @param currentTopology
	 * @param overloadLink
	 * @return
	 */
	public Link findLoopLinkInTopology(Map<Long, Set<Link>> wholeTopology,
			Map<Long, Set<Link>> currentTopology, Link overloadLink) {
		// 由于链路都是双向的链路，所以需要判断两次，正向链路和反向链路，而方向时相对的；

		Long dst = overloadLink.getDst(); // 得到超过链路利用率的链路的目的节点；
		Set<Link> links = currentTopology.get(dst);
		Iterator<Link> iteratorLinks = links.iterator();
		while (iteratorLinks.hasNext()) {
			Link link = iteratorLinks.next();
			Long dst2 = link.getDst();
			Link link2 = selectLink(wholeTopology, overloadLink.getSrc(), dst2);
			if (link2 != null) {
				return link2;
			}
		}

		Long src = overloadLink.getSrc(); // 得到超过链路利用率的链路的目的节点；
		Set<Link> links2 = currentTopology.get(src);
		Iterator<Link> iteratorLinks2 = links2.iterator();
		while (iteratorLinks2.hasNext()) {
			Link link = iteratorLinks2.next();
			Long dst3 = link.getDst();
			Link link3 = selectLink(wholeTopology, overloadLink.getDst(), dst3);
			if (link3 != null) {
				return link3;
			}
		}
		return null;
	}

	/**
	 * 根据指定的源节点和目的节点，寻找处对应的链路
	 * 
	 * @param switchLinks
	 * @param src
	 * @param dst
	 * @return
	 */
	public Link selectLink(Map<Long, Set<Link>> wholeTopology, long src,
			long dst) {
		Long srcId = new Long(src);
		Set<Link> links = wholeTopology.get(srcId);
		Iterator<Link> iterator = links.iterator();
		while (iterator.hasNext()) {
			Link link = iterator.next();
			if (link.getSrc() == src && link.getDst() == dst) {
				return link;
			}
		}
		return null;
	}

	/**
	 * 完成对网络拓扑信息的复制, 将网络的初始拓扑保存下来
	 */
	public void copySwitchLinks() {

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
				}
			}
			currentTopology.put(key, srcLink);
		}
	}

	public boolean setLinkUp(Link link) {
		short portNumber = link.getSrcPort();
		long dpid = link.getSrc();
		IOFSwitch ofs = floodlightProvider.getSwitch(dpid);
		if (setPortUp(ofs, portNumber)) {
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
			log.info("PortUp Failed");
		}
		return true;
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
	}

	@Override
	public void startUp(FloodlightModuleContext context)
			throws FloodlightModuleException {

		ScheduledExecutorService ses = threadPool.getScheduledExecutor();
		newInstanceTask = new SingletonTask(ses, new Runnable() {
			public void run() {
				try {
					wholeTopology = mst.getWholeTopology();
					copySwitchLinks(); // 保存当前网络的拓扑到currentTopology；
					linkCost = linkCostService.getLinkCost();
					Link overloadLink = detectLinkWeight(linkCost);
					if (overloadLink != null) {
						Link loopLink = findLoopLinkInTopology(wholeTopology,
								currentTopology, overloadLink);
						if (loopLink != null) {
							setLinkUp(loopLink);
						}
					}
				} catch (Exception e) {
					e.printStackTrace();
				} finally {
				}
			}
		});
		newInstanceTask.reschedule(30, TimeUnit.SECONDS);
	}

}
