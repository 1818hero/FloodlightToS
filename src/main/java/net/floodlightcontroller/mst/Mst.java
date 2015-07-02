package net.floodlightcontroller.mst;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.openflow.protocol.OFPhysicalPort;
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
import net.floodlightcontroller.routing.Link;
import net.floodlightcontroller.threadpool.IThreadPoolService;

public class Mst implements IFloodlightModule, IFloodlightService {

	// initial topology of the network
	private Map<Long, Set<Link>> wholeTopology = null;
	// minimum spanning tree topology of the network
	private Map<Long, Set<Link>> mstTopology = null;

	private Map<Long, Set<Link>> downLinksTopology = null;

	protected SingletonTask newInstanceTask = null;
	protected Map<Link, Integer> linkCost = null;

	// get the linkCost from linkCostManager
	protected IFloodlightProviderService floodlightProvider = null;
	protected ILinkCostService linkCostManager = null;
	protected ILinkDiscoveryService linkDiscoveryManager = null;
	protected static Logger log = LoggerFactory.getLogger(Mst.class);
	private IThreadPoolService threadPool = null;

	/**
	 * 获得最小生成树的拓扑
	 * 
	 * @return
	 */
	public Map<Long, Set<Link>> getMstTopology() {
		return this.mstTopology;
	}

	/**
	 * 获得整个网络的初始拓扑
	 * 
	 * @return
	 */
	public Map<Long, Set<Link>> getWholeTopology() {
		return this.wholeTopology;
	}

	/**
	 * 
	 * 测试用，打印链路输出
	 * 
	 * @param switchLinks
	 */
	public void printSwitchLinks(Map<Long, Set<Link>> switchLinks) {

		int count = 1;
		Set<Long> keySet = switchLinks.keySet();
		Iterator<Long> iterator1 = keySet.iterator();
		while (iterator1.hasNext()) {
			Long id = iterator1.next();
			Set<Link> links = switchLinks.get(id);
			Iterator<Link> iterator2 = links.iterator();
			while (iterator2.hasNext()) {
				Link link = iterator2.next();
				System.out.println((count++) + " " + link);
			}
		}

	}

	/**
	 * 将多条链路关闭
	 * 
	 * @param downLinks
	 */
	public void doDownLinkOperation(Map<Long, Set<Link>> downLinks) {
		try {
			Set<Long> keys = downLinks.keySet();
			Iterator<Long> iteratorKeys = keys.iterator();
			while (iteratorKeys.hasNext()) {
				Long dpid = iteratorKeys.next();
				IOFSwitch ofs = floodlightProvider.getAllSwitchMap().get(dpid);
				Set<Link> links = downLinks.get(dpid);
				Iterator<Link> iteratorLinks = links.iterator();
				while (iteratorLinks.hasNext()) {
					Link link = iteratorLinks.next();
					setLinkDown(ofs, link);
				}
			}
		} catch (Exception e) {
			log.info(" 没有要关闭的链路 ");
		}
	}

	/**
	 * 关闭指定的链路
	 * 
	 * @param ofs
	 * @param link
	 * @return
	 */
	public boolean setLinkDown(IOFSwitch ofs, Link link) {
		short portNumber = link.getSrcPort();
		if (setPortDown(ofs, portNumber)) {
			return true;
		} else {
			return false;
		}
	}

	/***
	 * 将指定的交换机端口关闭
	 */
	public boolean setPortDown(IOFSwitch ofs, short portNumber) {

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
		mymod.setConfig(OFPhysicalPort.OFPortConfig.OFPPC_PORT_DOWN.getValue());
		mymod.setHardwareAddress(macAddress);
		mymod.setMask(0xffffffff);
		// 将OFPortMod命令发送到指定的交换机中，进行执行！
		try {
			ofs.write(mymod, null);
			ofs.flush();
		} catch (Exception e) {
			log.info("PortDown Failed");
		}
		return true;
	}

	/**
	 * 寻找要关闭的网络链路；
	 * 
	 * @param wholeTopology
	 * @param mstTopology
	 * @return
	 */
	public Map<Long, Set<Link>> findDownLink(
			Map<Long, Set<Link>> wholeTopology, Map<Long, Set<Link>> mstTopology) {

		Map<Long, Set<Link>> downLinksAll = new HashMap<Long, Set<Link>>();
		Set<Long> allKeys = wholeTopology.keySet();
		Iterator<Long> iterator1 = allKeys.iterator();
		while (iterator1.hasNext()) {
			Long dpid = iterator1.next();
			Set<Link> links = wholeTopology.get(dpid);
			Iterator<Link> iterator2 = links.iterator();
			while (iterator2.hasNext()) {
				Link link = iterator2.next();
				if (!mstTopology.get(dpid).contains(link)) {
					Set<Link> downLinksByDpid1 = downLinksAll.get(dpid);
					if (downLinksByDpid1 == null) { // 如果已存在，则直接将该链路加入其中，否则需要new出一个hashSet；
						HashSet<Link> downLinksByDpid2 = new HashSet<Link>();
						downLinksByDpid2.add(link);
						downLinksAll.put(dpid, downLinksByDpid2);
					} else {
						downLinksByDpid1.add(link);
					}
				}

			}
		}
		return downLinksAll;

	}

	/**
	 * 产生最小生成树的拓扑
	 * 
	 * @param wholeTopology
	 * @param linkCost
	 * @return
	 */
	public Map<Long, Set<Link>> generateMstTopology(
			Map<Long, Set<Link>> wholeTopology, Map<Link, Integer> linkCost) {

		// 对最小生成树生成的拓扑进行初始化，最初始时整个只存在节点，而不再存在链路；
		Set<Long> keySet = wholeTopology.keySet();
		Iterator<Long> iterator = keySet.iterator();

		if (keySet.isEmpty()) { // 若没有连接mininet，则做返回处理；
			return null;
		}

		while (iterator.hasNext()) {
			Set<Link> links = new HashSet<Link>();
			mstTopology.put(iterator.next(), links);
		}

		// 由于交换机的dpid是从1开始的，所以对数组要做特殊处理，整个数组长度+1，并且索引值为0的不使用
		// 整个拓扑中交换机的个数
		int length = keySet.size();
		// prim算法所需要的一些变量
		long[] value = new long[length + 1];
		boolean[] visited = new boolean[length + 1];
		long[] parent = new long[length + 1];

		for (int i = 0; i < length + 1; i++) {
			value[i] = Long.MAX_VALUE;
			visited[i] = false;
		}

		// 下标为0的索引，为无效的, 将其标为true；
		visited[0] = true;
		parent[0] = -1;

		// 选定dpid为0的交换机为根节点
		// 根据网络的流量信息，可以选择根节点为流量最大的节点，需要了解MST中根节点的作用；选择依据；

		value[1] = 0;
		parent[1] = -1;

		for (int i = 0; i < length; i++) {

			int switchId = selectSwitch(value, visited);
			visited[switchId] = true;
			Long id = new Long(switchId);
			Set<Link> links = wholeTopology.get(id);
			Iterator<Link> iterator2 = links.iterator();

			while (iterator2.hasNext()) {
				Link link = iterator2.next();

				if (!linkCost.containsKey(link)) {
					linkCost.put(link, 1); // 当linkCost中不包含link时，就设置为默认值1；
				}
				if (!visited[(int) link.getDst()]
						&& linkCost.get(link) < value[(int) link.getDst()]) {
					value[(int) link.getDst()] = linkCost.get(link);
					parent[(int) link.getDst()] = switchId;
				}
			}
		}

		/*
		 * for(Long node:parent){ System.out.print(node); System.out.print(" ");
		 * } System.out.println();
		 */

		for (int i = 2; i < parent.length; i++) {
			Link link = selectLink(wholeTopology, parent[i], i);
			mstTopology.get(link.getSrc()).add(link);
			Link link2 = selectLink(wholeTopology, i, parent[i]);
			mstTopology.get(link2.getSrc()).add(link2);
		}
		return mstTopology;
	}

	/**
	 * 选择链路权重最小的节点，并且该节点未被选中过
	 * 
	 * @param value
	 * @param visited
	 * @return
	 */
	public int selectSwitch(long[] value, boolean[] visited) {
		int length = value.length;
		int min_index = 1;
		long minValue = Long.MAX_VALUE;
		for (int i = 0; i < length; i++) {
			if (value[i] < minValue && !visited[i]) {
				min_index = i;
				minValue = value[i];
			}
		}
		return min_index;
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
			wholeTopology.put(key, srcLink);

		}

		/*
		 * Set<Long> dpids=wholeTopology.keySet(); Iterator<Long>
		 * iter3=keys.iterator(); while(iter3.hasNext()){ long
		 * dpid=iter3.next(); Set<Link> links=wholeTopology.get(dpid);
		 * Iterator<Link> iter4=links.iterator(); while(iter4.hasNext()){ Link
		 * link=iter4.next(); System.out.println(link); } }
		 */
	}

	@Override
	public Collection<Class<? extends IFloodlightService>> getModuleServices() {
		// TODO Auto-generated method stub
		Collection<Class<? extends IFloodlightService>> l = new ArrayList<Class<? extends IFloodlightService>>();
		l.add(Mst.class);
		return l;
	}

	@Override
	public Map<Class<? extends IFloodlightService>, IFloodlightService> getServiceImpls() {
		// TODO Auto-generated method stub
		Map<Class<? extends IFloodlightService>, IFloodlightService> m = new HashMap<Class<? extends IFloodlightService>, IFloodlightService>();
		// We are the class that implements the service
		m.put(Mst.class, this);
		return m;
	}

	@Override
	public Collection<Class<? extends IFloodlightService>> getModuleDependencies() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void init(FloodlightModuleContext context)
			throws FloodlightModuleException {
		// TODO Auto-generated method stub
		floodlightProvider = context
				.getServiceImpl(IFloodlightProviderService.class);
		linkCostManager = context.getServiceImpl(ILinkCostService.class);
		linkDiscoveryManager = context
				.getServiceImpl(ILinkDiscoveryService.class);
		threadPool = context.getServiceImpl(IThreadPoolService.class);
	}

	@Override
	public void startUp(FloodlightModuleContext context)
			throws FloodlightModuleException {
		// TODO Auto-generated method stub
		wholeTopology = new HashMap<Long, Set<Link>>();
		mstTopology = new HashMap<Long, Set<Link>>();
		ScheduledExecutorService ses = threadPool.getScheduledExecutor();
		newInstanceTask = new SingletonTask(ses, new Runnable() {
			public void run() {
				try {
					linkCost = linkCostManager.getLinkCost();
					copySwitchLinks();
					mstTopology = generateMstTopology(wholeTopology, linkCost);
					downLinksTopology = findDownLink(wholeTopology, mstTopology);
					doDownLinkOperation(downLinksTopology);
					log.info("最小生成树拓扑生成");
				} finally {
					 //newInstanceTask.reschedule(10, TimeUnit.SECONDS);
				}
			}
		});

		newInstanceTask.reschedule(15, TimeUnit.SECONDS);

	}

}
