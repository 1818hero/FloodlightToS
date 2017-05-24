package net.floodlightcontroller.linkCostService;

import net.floodlightcontroller.core.IFloodlightProviderService;
import net.floodlightcontroller.core.IOFSwitch;
import net.floodlightcontroller.core.IOFSwitch.PortChangeType;
import net.floodlightcontroller.core.IOFSwitchListener;
import net.floodlightcontroller.core.ImmutablePort;
import net.floodlightcontroller.core.module.FloodlightModuleContext;
import net.floodlightcontroller.core.module.FloodlightModuleException;
import net.floodlightcontroller.core.module.IFloodlightModule;
import net.floodlightcontroller.core.module.IFloodlightService;
import net.floodlightcontroller.core.util.SingletonTask;
import net.floodlightcontroller.linkdiscovery.ILinkDiscoveryService;
import net.floodlightcontroller.linkdiscovery.LinkInfo;
import net.floodlightcontroller.routing.Link;
import net.floodlightcontroller.threadpool.IThreadPoolService;
import org.openflow.protocol.OFPort;
import org.openflow.protocol.OFStatisticsRequest;
import org.openflow.protocol.statistics.OFPortStatisticsReply;
import org.openflow.protocol.statistics.OFPortStatisticsRequest;
import org.openflow.protocol.statistics.OFStatistics;
import org.openflow.protocol.statistics.OFStatisticsType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class LinkCostManager implements ILinkCostService, IFloodlightModule,
		IOFSwitchListener {

	private Map<Link, Double> linkCost = new HashMap<Link, Double>(); // dijkstra算法使用的链路权重
	//private Map<Link, Double> linkCostEnergySaving = new HashMap<Link, Double>(); // 网络节能使用的链路
	private IFloodlightProviderService floodlightProvider = null;
	private IThreadPoolService threadPool = null;
	private SingletonTask  newInstanceTask = null;
	private ILinkDiscoveryService linkDiscoveryManager = null;
	private Map<Long, Map<Short, Long[]>> lastTimePortTraffic = new HashMap<Long, Map<Short, Long[]>>();
	protected static Logger log = LoggerFactory
			.getLogger(LinkCostManager.class);
	private Map<Long, Map<Short, List<Double>>> switchPortRateMap = new HashMap<Long, Map<Short, List<Double>>>();
	private boolean initialFlag = true;

	//光网络节点集合
	private static Set<Long> FiberNodeSet = new HashSet<>();
    //每个link的类型
	private Map<Link, LinkBandwidthType> linkTypeMap = new HashMap<>();

	//当前网络中链路最大剩余带宽
	private double MaxLinkCompacity;

	//当前的拓扑信息
    private Map<Long, IOFSwitch> switchMap = new HashMap<>();
    private Map<Long, Set<Link>> switchLinks = new HashMap<>();
	private  Map<Link, LinkInfo> allLinks = new HashMap<>();

	//配置类：预先设定光节点set
	private void setFiberNodeSet(){
		FiberNodeSet.add(new Long(1));
		FiberNodeSet.add(new Long(2));
		FiberNodeSet.add(new Long(3));
		FiberNodeSet.add(new Long(4));
		FiberNodeSet.add(new Long(8));

	}

	/**
	 * linkCost的getter方法
	 * 
	 * @return
	 */
	// getter of linkCost
	@Override
	public synchronized Map<Link,Double> getLinkCost() {
		return linkCost;
	}

	@Override
	public synchronized Map<Link,LinkBandwidthType> getLinkTypeMap(){
	    return linkTypeMap;
    }

    @Override
    public synchronized double getMaxLinkCompacity() {
        return MaxLinkCompacity;
    }

	@Override
	public synchronized double getLinkCompacity(Link link) {
		try {
			return linkTypeMap.get(link).getBandwidth();
		}catch (Exception e){
			log.error("No such Link in current network");
		}
		return -1;
	}

    @Override
    public Map<Long, Set<Link>> getSwitchLinks() {
        return this.switchLinks;
    }

	@Override
	public Map<Link, LinkInfo> getLinks() {
		return this.allLinks;
	}

	//	/**
//	 * linkCostEnergySaving的getter方法
//	 * @return
//	 */
//	public Map<Link,Double> getLinkCostEnergySaving() {
//		return this.linkCostEnergySaving;
//	}

    /**
     * 工具类：判断链路类型
     */
    public LinkBandwidthType judgeLinkType(Link link){
        long src = link.getSrc();
        long dst = link.getDst();
        if(FiberNodeSet.contains(src)&&FiberNodeSet.contains(dst))  return LinkBandwidthType.FiberLink;
        else return LinkBandwidthType.CableLink;
    }


	/**
	 * 更新linkCost的值以及linkType
	 */
	public synchronized void updateLinkCost() {
		if (!initialFlag) {
			synchronized (switchPortRateMap) {
				Set<Long> switchIds = switchLinks.keySet(); // 虽然给出的文档中key是switchId，但是并不能完全对应与link中dpid，为正确还是使用link中的dpid
				Iterator<Long> iteratorSwitchId = switchIds.iterator();
                linkCost.clear();	//新增
                MaxLinkCompacity = -1;   //重置最大链路容量
				while (iteratorSwitchId.hasNext()) {
					long dpid = iteratorSwitchId.next();
					Set<Link> links = switchLinks.get(dpid);
					Iterator<Link> iteratorLink = links.iterator();
					while (iteratorLink.hasNext()) {
						Link link = iteratorLink.next();
						short portNumber = link.getSrcPort();
						long dpid1 = link.getSrc();
						Double cost = switchPortRateMap.get(dpid1).get(
								portNumber).get(0)+switchPortRateMap.get(dpid1).get(portNumber).get(1);   //选取链路源端口的发送速率和接收速率之和作为这个链路的链路权重
                        linkCost.put(link, cost);
                        //更新链路类型，默认为CableLink
                        linkTypeMap.put(link, judgeLinkType(link));
                        if (MaxLinkCompacity < linkTypeMap.get(link).getBandwidth()-cost){
                            MaxLinkCompacity = linkTypeMap.get(link).getBandwidth()-cost;
                        }
					}
				}
			}
		}
        else initialFlag=false;  //新增
	}
	
//	public void updateLinkCostEnergySaving(){
//		if(initialFlag){
//			synchronized(switchPortRateMap){
//				Map<Long, Set<Link>> topologyLink = linkDiscoveryManager
//						.getSwitchLinks();
//				Set<Long> switchIds = topologyLink.keySet(); // 虽然给出的文档中key是switchId，但是并不能完全对应与link中dpid，为正确还是使用link中的dpid
//				Iterator<Long> iteratorSwitchId = switchIds.iterator();
//				while (iteratorSwitchId.hasNext()) {
//					long dpid = iteratorSwitchId.next();
//					Set<Link> links = topologyLink.get(dpid);
//					Iterator<Link> iteratorLink = links.iterator();
//					while (iteratorLink.hasNext()) {
//						Link link = iteratorLink.next();
//						short portNumber = link.getSrcPort();
//						long dpid1 = link.getSrc();
//						Double costT = switchPortRateMap.get(dpid1).get(
//								portNumber).get(0);   //始终选取一个源端口的发送速率作为这个链路的链路权重
//						Double costR = switchPortRateMap.get(dpid1).get(portNumber).get(1);
//						Double cost = costT > costR? costT : costR;   //始终选取发送速率和接收速率中的较大值作为节能策略时的链路权重
//						linkCostEnergySaving.put(link, cost);
//					}
//				}
//			}
//		}
//
//	}

	/**
	 * 记录5s内端口的发送速率和接收速率
	 */

	public void mapTrafficToLinkCost() {
//		if (!initialFlag) { // 当要进行更新linkCost时，就删除linkCost；
//			linkCost.clear();
//		}
		Map<Long, List<OFStatistics>> netTraffic = this.collectTraffic();
		Set<Long> dpids = netTraffic.keySet(); // 网络中的所有的交换机的dpid；

		Iterator<Long> dpidIterator = dpids.iterator();

		while (dpidIterator.hasNext()) {
			Long dpid = dpidIterator.next();
			Iterator<OFStatistics> iteratorOFStatistics = netTraffic.get(dpid)
					.iterator();
			HashMap<Short, Long[]> portTraffic = new HashMap<Short, Long[]>();
			
			HashMap<Short, List<Double>> portRateMap = new HashMap<Short, List<Double>>();

			while (iteratorOFStatistics.hasNext()) {

				OFPortStatisticsReply portReply = (OFPortStatisticsReply) iteratorOFStatistics
						.next();
				short portNumber = portReply.getPortNumber();

				// 由于交换机中的链路都是全双工的，当在计算端口的流量统计信息时，就需要得到某个端口的发送流量和接收流量的最大值
				Long transmitBytes = portReply.getTransmitBytes();
				Long receiveBytes = portReply.getReceiveBytes();
				// 将当前的发送和接收的流量信息进行保存
				Long[] currentPortTraffic = new Long[2];
				// 将接受和发送数据的最大值作为判断是否关闭链路的依据
				currentPortTraffic[0] = transmitBytes;
				currentPortTraffic[1] = receiveBytes;

				if (!initialFlag) {

					long transmitBytesIn5s = currentPortTraffic[0]
							- lastTimePortTraffic.get(dpid).get(portNumber)[0];
					long receiveBytesIn5s = currentPortTraffic[1]
							- lastTimePortTraffic.get(dpid).get(portNumber)[1];
					List<Double> portByteRate=new ArrayList<Double>(2);  //这个数组存放着该端口的发送和接收速率
					
					Double transmitRate = 8* transmitBytesIn5s/(1024.0 * 1024.0 * 5);  //发送速率
					Double receiveRate = 8* receiveBytesIn5s/(1024.0 * 1024.0 * 5);   //接受速率
					
					portByteRate.add(transmitRate);  //下标为0存放发送速率
					portByteRate.add(receiveRate);  //下标为1存放接受速率
					// portByteRate表示在间隔5s内发送的数据总数，以字节的单位进行计算
					portRateMap.put(portNumber,
							portByteRate);

				}
				portTraffic.put(portNumber, currentPortTraffic);	//表示总收发信息量

			}

			if (!initialFlag) {
				switchPortRateMap.put(dpid, portRateMap);
			}
			lastTimePortTraffic.put(dpid, portTraffic);
		}

		//initialFlag = initialFlag ? false : true;

	}

	/***
	 * 获取交换机各个端口接收的数据包统计信息
	 * 
	 */
	public Map<Long, List<OFStatistics>> collectTraffic() {

		// 获取控制器所连接的所有的交换机的集合
		Set<Long> switchSet = switchMap.keySet();
		Iterator<Long> iter = switchSet.iterator();
		// 网络中的流量信息
		Map<Long, List<OFStatistics>> networkTrafficTemp = new HashMap<Long, List<OFStatistics>>();

		// 从网络中获取到所有交换机的端口接收到的数据包的统计，保存到networkTrafficTemp中
		while (iter.hasNext()) {
			Long dpid = iter.next();
			IOFSwitch sw = floodlightProvider.getSwitch(dpid);
			// 如果不存在交换机，就返回
			if (sw == null)
				return null;
			Future<List<OFStatistics>> future = null;
			List<OFStatistics> values = null;

			OFStatisticsRequest req = new OFStatisticsRequest();
			req.setStatisticType(OFStatisticsType.PORT);
			int requestLength = req.getLengthU();

			OFPortStatisticsRequest specificReq = new OFPortStatisticsRequest();
			specificReq.setPortNumber((short) OFPort.OFPP_NONE.getValue());
			req.setStatistics(Collections
					.singletonList((OFStatistics) specificReq));
			requestLength += specificReq.getLength();
			req.setLengthU(requestLength);

			try {
				future = sw.queryStatistics(req);
				values = future.get(10, TimeUnit.SECONDS);
			} catch (Exception e) {
				log.error("Failure retrieving statistics from switch " + sw, e);
			}

			networkTrafficTemp.put(dpid, values);
		}

		return networkTrafficTemp;
	}

	@Override
	public Collection<Class<? extends IFloodlightService>> getModuleServices() {
		// TODO Auto-generated method stub
		Collection<Class<? extends IFloodlightService>> l = new ArrayList<Class<? extends IFloodlightService>>();
		l.add(ILinkCostService.class);
		return l;
	}

	@Override
	public Map<Class<? extends IFloodlightService>, IFloodlightService> getServiceImpls() {
		// TODO Auto-generated method stub
		Map<Class<? extends IFloodlightService>, IFloodlightService> m = new HashMap<Class<? extends IFloodlightService>, IFloodlightService>();
		// We are the class that implements the service
		m.put(ILinkCostService.class, this);
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
		threadPool = context.getServiceImpl(IThreadPoolService.class);
		linkDiscoveryManager = context
				.getServiceImpl(ILinkDiscoveryService.class);
		setFiberNodeSet();
	}

	@Override
	public void startUp(FloodlightModuleContext context)
			throws FloodlightModuleException {
		// TODO Auto-generated method stub
		ScheduledExecutorService ses = threadPool.getScheduledExecutor();
		floodlightProvider.addOFSwitchListener(this);
		// 以T=5为周期进行链路权值的更新操作，这个动作时一直都在进行的；
		newInstanceTask = new SingletonTask(ses, new Runnable() {
			public void run() {
			    switchMap.clear();
			    switchLinks.clear();
			    allLinks.clear();
                switchMap.putAll(floodlightProvider.getAllSwitchMap());
                synchronized (linkDiscoveryManager) {
					switchLinks.putAll(linkDiscoveryManager.getSwitchLinks());
					allLinks.putAll(linkDiscoveryManager.getLinks());
				}
				try {
					mapTrafficToLinkCost();
					updateLinkCost();
					//updateLinkCostEnergySaving();
				} catch (Exception e) {
					e.printStackTrace();
				} finally {
					newInstanceTask.reschedule(5, TimeUnit.SECONDS);
				}
			}
		});
		newInstanceTask.reschedule(10, TimeUnit.SECONDS);
	}

	@Override
	public void switchAdded(long switchId) {
		this.initialFlag = true;

	}

	@Override
	public void switchRemoved(long switchId) {
		// TODO Auto-generated method stub

	}

	@Override
	public void switchActivated(long switchId) {
		// TODO Auto-generated method stub

	}

	@Override
	public void switchPortChanged(long switchId, ImmutablePort port,
			PortChangeType type) {
		// TODO Auto-generated method stub

	}

	@Override
	public void switchChanged(long switchId) {
		// TODO Auto-generated method stub

	}

}
