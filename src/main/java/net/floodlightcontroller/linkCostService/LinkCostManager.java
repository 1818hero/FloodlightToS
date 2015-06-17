package net.floodlightcontroller.linkCostService;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.openflow.protocol.OFPort;
import org.openflow.protocol.OFStatisticsRequest;
import org.openflow.protocol.statistics.OFPortStatisticsReply;
import org.openflow.protocol.statistics.OFPortStatisticsRequest;
import org.openflow.protocol.statistics.OFStatistics;
import org.openflow.protocol.statistics.OFStatisticsType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import net.floodlightcontroller.routing.Link;
import net.floodlightcontroller.threadpool.IThreadPoolService;

public class LinkCostManager implements ILinkCostService, IFloodlightModule,IOFSwitchListener{
	
	private Map<Link, Integer> linkCost = new HashMap<Link, Integer>();   //dijkstra算法使用的链路权重
	private IFloodlightProviderService floodlightProvider=null;
	private IThreadPoolService threadPool=null;
	private SingletonTask newInstanceTask=null;
	private ILinkDiscoveryService linkDiscoveryManager = null;
	private Map<Long,Map<Short,Long[]>> lastTimePortTraffic=new HashMap<Long,Map<Short,Long[]>>();
	protected static Logger log = LoggerFactory.getLogger(LinkCostManager.class);
	private Map<Long,Map<Short,Double>> portUtilization_x10=new HashMap<Long,Map<Short,Double>>();
	private boolean initialFlag = true;
	
	/**
	 * linkCost的getter方法
	 * @return
	 */
	//getter of linkCost
	@Override
	public Map<Link,Integer> getLinkCost(){
		return linkCost;
	}
	
	/**
	 * 更新linkCost的值
	 */
	public void updateLinkCost(){
		
		if(initialFlag){
			synchronized(portUtilization_x10){
				Map<Long,Set<Link>> topologyLink=linkDiscoveryManager.getSwitchLinks();
				Set<Long> switchIds= topologyLink.keySet();  //虽然给出的文档中key是switchId，但是并不能完全对应与link中dpid，为正确还是使用link中的dpid
				Iterator<Long> iteratorSwitchId=switchIds.iterator();
				while(iteratorSwitchId.hasNext()){
					long dpid=iteratorSwitchId.next();
					Set<Link> links=topologyLink.get(dpid);
					Iterator<Link> iteratorLink=links.iterator();
					while(iteratorLink.hasNext()){
						Link link=iteratorLink.next();
						short portNumber=link.getSrcPort();
						long dpid1=link.getSrc();
						Double cost=portUtilization_x10.get(dpid1).get(portNumber);
						linkCost.put(link,(int)Math.ceil(cost*10));
					}
				}
			}
		}
	}
	
	/**
	 * 设置linkCost的值，linkCost的值时通过链路利用率映射为一个linkCost的一个值
	 */
	
	public void mapTrafficToLinkCost(){
		if(!initialFlag){     //当要进行更新linkCost时，就删除linkCost；
			linkCost.clear();  
		}
		
		Map<Long,List<OFStatistics>> netTraffic=new HashMap<Long,List<OFStatistics>>();
		netTraffic=this.collectTraffic();
		Set<Long> dpids=netTraffic.keySet();    //网络中的所有的交换机的dpid；
		
		Iterator<Long> dpidIterator=dpids.iterator();
		
		while(dpidIterator.hasNext()){
			
			Long dpid=dpidIterator.next();
			Iterator<OFStatistics> iteratorOFStatistics=netTraffic.get(dpid).iterator();
			HashMap<Short,Long[]> portTraffic=new HashMap<Short,Long[]>();
			HashMap<Short,Double> portUtilizationx10Map=new HashMap<Short,Double>();
			
			while(iteratorOFStatistics.hasNext()){
	        	
	        	OFPortStatisticsReply portReply=(OFPortStatisticsReply)iteratorOFStatistics.next();
	        	short portNumber=portReply.getPortNumber();
	        	
	        	//由于交换机中的链路都是全双工的，当在计算端口的流量统计信息时，就需要得到某个端口的发送流量和接收流量的最大值
	        	Long transmitBytes=portReply.getTransmitBytes();
	        	Long receiveBytes=portReply.getReceiveBytes();
	        	//将当前的发送和接收的流量信息进行保存
	        	Long[] currentPortTraffic=new Long[2];
	        	//将接受和发送数据的最大值作为判断是否关闭链路的依据
	        	currentPortTraffic[0]=transmitBytes;
	        	currentPortTraffic[1]=receiveBytes;
	        	
	        	if( !initialFlag ){
	        		
	        		long transmitByteRate =currentPortTraffic[0]-lastTimePortTraffic.get(dpid).get(portNumber)[0];
	        		long receiveByteRate = currentPortTraffic[1]-lastTimePortTraffic.get(dpid).get(portNumber)[1];
	        		long portByteRate_5s=0;
	        		
	        		if( transmitByteRate > receiveByteRate ){
	        			portByteRate_5s = transmitByteRate;
	        		}else{
	        			portByteRate_5s = receiveByteRate;
	        		}
	        		
	        		//portByteRate表示在间隔5s内发送的数据总数，以字节的单位进行计算
	        	    double portRate_Mbps=8*portByteRate_5s/(1024.0*1024.0*5);
	        		
	        	    if(portRate_Mbps<0.0001){
	        			portRate_Mbps=0;
	        		}
	        	    
	        		//默认设置的链路带宽时100Mbps；
	        		//double portUtilization_eachPort_x10=(portRate_Mbps/100.0)*10;
	        	    double portUtilization_eachPort_x10=portRate_Mbps;
	        		portUtilizationx10Map.put(portNumber, portUtilization_eachPort_x10);
	        		
	        	}
	        	portTraffic.put( portNumber, currentPortTraffic );
	        	
	        }
			
			if(!initialFlag){
				portUtilization_x10.put(dpid,portUtilizationx10Map);
			}
			lastTimePortTraffic.put(dpid,portTraffic);
		}
		initialFlag=initialFlag?false:true;
		
	}
	/*public void print(){
		
		Set<Long> dpids=portUtilization_x10.keySet();
		
		Iterator<Long> iteratorDpid= dpids.iterator();
		while(iteratorDpid.hasNext()){
			long dpid=iteratorDpid.next();
			Map<Short,Double> portTraffic=portUtilization_x10.get(dpid);
			Set<Short> portId=portTraffic.keySet();
			Iterator<Short> iteratorPort = portId.iterator();
			while(iteratorPort.hasNext()){
				short port=iteratorPort.next();
				double traffic=portTraffic.get(port);
				
				System.out.println(dpid+" "+ port+ " "+traffic);
			}
		}
	}*/
	/***
	 * 获取交换机各个端口接收的数据包统计信息
	 * 
	*/
	public Map<Long,List<OFStatistics>> collectTraffic(){
		
		//获取控制器所连接的所有的交换机的集合
		Set<Long> switchSet=floodlightProvider.getAllSwitchMap().keySet();
		Iterator<Long> iter=switchSet.iterator();
		//网络中的流量信息
		Map<Long,List<OFStatistics>> networkTrafficTemp = new HashMap<Long,List<OFStatistics>>();
		
		//从网络中获取到所有交换机的端口接收到的数据包的统计，保存到networkTrafficTemp中
		while(iter.hasNext()){
			Long dpid=iter.next();
			IOFSwitch sw = floodlightProvider.getSwitch(dpid);
			//如果不存在交换机，就返回
			if(sw==null)
				return null;
			Future<List<OFStatistics>> future=null;
			List<OFStatistics> values = null;

	    	OFStatisticsRequest req = new OFStatisticsRequest();
	    	req.setStatisticType(OFStatisticsType.PORT);
	        int requestLength = req.getLengthU();
	        
	        OFPortStatisticsRequest specificReq = new OFPortStatisticsRequest();
	        specificReq.setPortNumber((short)OFPort.OFPP_NONE.getValue());
	        req.setStatistics(Collections.singletonList((OFStatistics)specificReq));
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
		log.info("-----the operation of collecting traffic from network is done!---");
		
		return networkTrafficTemp;
    }
	@Override
	public Collection<Class<? extends IFloodlightService>> getModuleServices() {
		// TODO Auto-generated method stub
		Collection<Class<? extends IFloodlightService>> l =
                new ArrayList<Class<? extends IFloodlightService>>();
        l.add(ILinkCostService.class);
		return l;
	}

	@Override
	public Map<Class<? extends IFloodlightService>, IFloodlightService> getServiceImpls() {
		// TODO Auto-generated method stub
		Map<Class<? extends IFloodlightService>,
        IFloodlightService> m =
            new HashMap<Class<? extends IFloodlightService>,
                IFloodlightService>();
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
		floodlightProvider = context.getServiceImpl(IFloodlightProviderService.class);
		threadPool = context.getServiceImpl(IThreadPoolService.class);
		linkDiscoveryManager = context.getServiceImpl(ILinkDiscoveryService.class);
	}
	@Override
	public void startUp(FloodlightModuleContext context)
			throws FloodlightModuleException {
		// TODO Auto-generated method stub
		ScheduledExecutorService ses = threadPool.getScheduledExecutor();
		floodlightProvider.addOFSwitchListener(this);
		//以T=5为周期进行链路权值的更新操作，这个动作时一直都在进行的；
		newInstanceTask = new SingletonTask(ses, new Runnable(){
			public void run(){
				try{
					log.info("开始计算链路权重");
					mapTrafficToLinkCost();
					updateLinkCost();
					log.info("计算结束");
				}catch(Exception e){
					e.printStackTrace();
				}finally{
					newInstanceTask.reschedule(5,TimeUnit.SECONDS);	
				}					
			}
		});
		newInstanceTask.reschedule(10, TimeUnit.SECONDS);
	}

	@Override
	public void switchAdded(long switchId) {
		this.initialFlag=true;
		System.out.println("new switch is added!");
		
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
