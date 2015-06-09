package net.floodlightcontroller.energySaving;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;
import java.util.HashMap;


import net.floodlightcontroller.core.IFloodlightProviderService;
import net.floodlightcontroller.core.IOFSwitch;
import net.floodlightcontroller.core.ImmutablePort;
import net.floodlightcontroller.core.module.FloodlightModuleContext;

import java.util.concurrent.Future;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.openflow.protocol.OFPhysicalPort;
import org.openflow.protocol.OFPort;
import org.openflow.protocol.OFPortMod;
import org.openflow.protocol.OFStatisticsRequest;
import org.openflow.protocol.OFType;
import org.openflow.protocol.statistics.OFPortStatisticsReply;
import org.openflow.protocol.statistics.OFPortStatisticsRequest;
import org.openflow.protocol.statistics.OFStatistics;
import org.openflow.protocol.statistics.OFStatisticsType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.floodlightcontroller.core.module.FloodlightModuleException;
import net.floodlightcontroller.core.module.IFloodlightModule;
import net.floodlightcontroller.core.module.IFloodlightService;
import net.floodlightcontroller.core.util.SingletonTask;
import net.floodlightcontroller.linkdiscovery.ILinkDiscoveryService;
import net.floodlightcontroller.routing.IRoutingService;
import net.floodlightcontroller.routing.Link;
import net.floodlightcontroller.threadpool.IThreadPoolService;

public class Copy_2_of_EnergySaving implements IFloodlightModule {
	
	protected static Logger log = LoggerFactory.getLogger(Copy_2_of_EnergySaving.class);
	protected IThreadPoolService threadPool;    //线程池
	protected SingletonTask newInstanceTask=null;
	private IFloodlightProviderService floodlightProvider;
	private ILinkDiscoveryService linkDiscoveryManager;
	private IRoutingService routeEngine;
	private Map<Long, Set<Link>> switchLinks;
	private Map<Long,HashMap<Short,Long>> lastTimePortTraffic=new HashMap<Long,HashMap<Short,Long>>(); //上次收集到的交换机各个端口的流量信息（以数据包的个数来衡量）
	private Map<Long,List<OFStatistics>> networkTraffic=null; //网络中各个交换机的流量统计信息
	protected Map<Long,Set<Link>> copySwitchLinks=new HashMap<Long,Set<Link>>();
	protected static int activeLinkNumber=0;
	protected int t=0;
	protected Map<Long,Set<Short>> nodePortPairDown=null;
	
	private static boolean initialFlag = true;  	//这个flag为true表示不进行判断节能的策略,主要是为了是的获得流量增量中的初始化流量统计信息
	private static boolean reEnergySavingFlag = false;
	
	private static boolean finalFlag=true; 			//指示是否要执行final语句
    
	private int amount; //表示copySwitchLinks中交换机的数量
	
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
	/**
	 * 将该模块依赖注册到控制器
	 */
	@Override
	public Collection<Class<? extends IFloodlightService>> getModuleDependencies() {
		// TODO Auto-generated method stub
		Collection<Class<? extends IFloodlightService>> l = 
                new ArrayList<Class<? extends IFloodlightService>>();
		l.add(IFloodlightProviderService.class);
		l.add(IThreadPoolService.class);
	    l.add(ILinkDiscoveryService.class);
	    l.add(IRoutingService.class);
		return l;
	}
	/*********************************************************************************************************/
	/**
	 * 完成对网络拓扑信息的复制
	 */
	public void copySwitchLinks(){
		
		activeLinkNumber=0;
		
		switchLinks=linkDiscoveryManager.getSwitchLinks();  //Map(Long,set<Link>) switchLinks
		
		Set<Long> keys=switchLinks.keySet();
		
		Iterator<Long> iter1=keys.iterator();
		
		while(iter1.hasNext()){
			
			Long key=iter1.next();
			Set<Link> links=switchLinks.get(key);
			Set<Link> srcLink=new HashSet<Link>();
			Iterator<Link> iter2=links.iterator();
			
			while(iter2.hasNext()){
				Link link=new Link();
				link=iter2.next();
				if(key==link.getSrc()){
					srcLink.add(link);
					activeLinkNumber++;     //记录网络中活跃的链路数
				}
			}
			
			copySwitchLinks.put(key, srcLink);
		}
		log.info("The active link number is {}", activeLinkNumber);
	}
	
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
	
	/**
	 * 依据统计的流量信息来进行节能的策略
	 */
	public void energySaving(){
		
		copySwitchLinks();  //对拓扑信息进行复制保存
		
		this.networkTraffic = this.collectTraffic();  //将统计到的流量信息先存储下来
		
		if( networkTraffic==null ){
			return;
		}
		
		Set<Long> dpids=this.networkTraffic.keySet(); //网络中的所有的交换机
		
		Iterator<Long> iter=dpids.iterator();
					
		while(iter.hasNext()){
			
			Long dpid=iter.next();
			
			IOFSwitch sw=floodlightProvider.getSwitch(dpid);
			
			Iterator<OFStatistics> iterator=this.networkTraffic.get(dpid).iterator();
			
			HashMap<Short,Long> hashMap=new HashMap<Short,Long>();
			
			while(iterator.hasNext()){
	        	
	        	OFPortStatisticsReply portReply=(OFPortStatisticsReply)iterator.next();
	        	short portNumber=portReply.getPortNumber();
	        	//如果这个端口是交换机与控制器的通信端口(short类型的65535转换为int后为-2)或者是主机和交换机链接的端口
	        	//(默认端口号是1)则不做任何的处理
	        	if( portNumber == -2 || portNumber == 1 ){  
	        		continue;
	        	}
	        	
	        	if(nodePortPairDown.containsKey(dpid)){
	        		break;
	        		/*if(nodePortPairDown.get(dpid).contains(portNumber)){
	        			continue;
	        		}*/
	        	}
	        	
	        	//由于交换机中的链路都是全双工的，当在计算端口的流量统计信息时，就需要得到某个端口的发送流量和接收流量的最大值
	        	Long transmitBytes=portReply.getTransmitBytes();
	        	Long receiveBytes=portReply.getReceiveBytes();
	        	Long currentPortTraffic;
	        	
	        	//将接受和发送数据的最大值作为判断是否关闭链路的依据
	        	if(transmitBytes > receiveBytes ){
	        		currentPortTraffic=transmitBytes;
	        	}else{
	        		currentPortTraffic=receiveBytes;
	        	}
	        		        	
	        	String switch_Port=dpid.toString()+"："+Integer.toString(portNumber);
	        	
	        	log.info("[Dpid:port]:{},switch Port Traffic:{}",switch_Port,
	            	currentPortTraffic );
	        	
	        	if( !initialFlag ){        //flag初始化为true，节能策略不执行,用来初始化lastTimePortTraffic
	        		
	        		long portByteRate=currentPortTraffic-lastTimePortTraffic.get(dpid).get(portNumber);
	        		
	        		//portByteRate_MB表示在间隔5s内发送的数据总数，以字节的单位进行计算
	        	    double portRate_Mbps=8*portByteRate/(1024.0*1024.0*5);
	        		
	        	    log.info("the port speed in Mbps is {}",portRate_Mbps);
	        	    
	        	    //判断流量是否大于所设定的阈值
	        	    //链路的带宽是100Mbps,设定阈值为0.15*100
	        	    if( portRate_Mbps < 20 ){
	        			
		        		if(canBeTurnDown(sw.getId(),portReply.getPortNumber())){
		        			setPortDown(sw,portReply.getPortNumber());
		        			reEnergySavingFlag = true;
		        		}
		        	} 		
	        	}
	        	hashMap.put( portNumber, currentPortTraffic );
	        }
			
			if( reEnergySavingFlag ){
				
				lastTimePortTraffic.clear(); 	//由于要重新进行能量节省操作，需要清空初始化流量信息，重新进行初始化流量信息			
				initialFlag=true;  				//初始化流量信息
				reEnergySavingFlag=false;
				finalFlag=false;
				newInstanceTask.reschedule(10,TimeUnit.SECONDS);  // 等待10s（以保证网络的状态稳定），重新进行节能操作。
				return; 							//执行完这个语句后，当前的while结束，重新进行节能模式的初始化收集流量操作。
			}
			
			lastTimePortTraffic.put(dpid,hashMap);
		}
		finalFlag=true;
		
	}
	/**
	 * 判断指定的端口是否能够关闭？
	 * 即判断要关闭的两个端口所属的交换机是否存在其他的环路
	 */
	public boolean canBeTurnDown(long srcId,short srcPortNumber){
		
		Set<Link> links=copySwitchLinks.get(srcId);  
		//如果该交换机在拓扑中不存在链路
		if(links==null){
			return false;
		}
		Iterator<Link> iter=links.iterator();
		
		Link backwardLink=null;  //关闭端口有关的前向链路和后向链路
		Link forwardLink=null;
		
		while(iter.hasNext()){             //寻找出要关闭的前向链路，break语句前的forwardLink是要寻找的链路
			forwardLink=iter.next();
			if(forwardLink.getSrcPort()==srcPortNumber){
				break;
			}
			forwardLink=null;
		}
		//如果找不到任何的前向链路，就返回；这种情况一般发生在与主机连接的端口（1端口）和与控制器相连接的端口（-2端口）
		
		if(forwardLink==null){
			return false;
		}
		
		long dstId=forwardLink.getDst();    //前向链路的目的交换机
		short dstPortNumber=forwardLink.getDstPort();  //前向交换机的目的端口
		backwardLink=new Link(dstId,dstPortNumber,srcId,srcPortNumber);  //实例化后向向链路
		
		if(copySwitchLinks.get(srcId).size()==1){
			copySwitchLinks.remove(srcId);
		}else{
			copySwitchLinks.get(srcId).remove(forwardLink); //删除前向链路	
		}
		
		if(copySwitchLinks.get(dstId).size()==1){
			copySwitchLinks.remove(dstId);
		}else{
			copySwitchLinks.get(dstId).remove(backwardLink); //删除后向链路
		}
		
		//如果不能删除这条链路，就撤销删除链路
		if(!isReachable(srcId,dstId)){ 
			if(copySwitchLinks.get(srcId)==null){
				Set<Link> newForwardLink=new HashSet<Link>();
				newForwardLink.add(forwardLink);
				copySwitchLinks.put(srcId, newForwardLink);
			}else{
				if(copySwitchLinks.get(dstId)==null){
					Set<Link> newBackwardLink= new HashSet<Link>();
					newBackwardLink.add(backwardLink);
					copySwitchLinks.put(dstId,newBackwardLink);
				}else{
					copySwitchLinks.get(srcId).add(forwardLink);
					copySwitchLinks.get(dstId).add(backwardLink);
				}
			}
			return false;
		}
		//当可以关闭链路，此时将连接链路的两个端口都进行关闭
		//下面的语句是关闭目的端口，而源端口在energySaving中完成,由于floodlightcontroller中当某个端口关闭，核心模块会把
		//该端口所连接的链路关闭，所以下面这条命令不用执行
		//setPortDown(floodlightProvider.getSwitch(dstId),dstPortNumber);
		
		//当这个端口可以被关闭（administrately），就将该端口对（src and dst）置于 nodePortPairDown中
		
		//将即将关闭的源端口置入nodePortPairDown中
		if(nodePortPairDown.containsKey(srcId)){
			nodePortPairDown.get(srcId).add(srcPortNumber);
		}else{
			Set<Short> portNumber=new HashSet<Short>();
			portNumber.add(srcPortNumber);
			nodePortPairDown.put(srcId, portNumber);
		}
		
		//将即将关闭的目的端口置入nodePortPairDown中
		if(nodePortPairDown.containsKey(dstId)){
			nodePortPairDown.get(dstId).add(dstPortNumber);
		}else{
			Set<Short> portNumber=new HashSet<Short>();
			portNumber.add(dstPortNumber);
			nodePortPairDown.put(dstId, portNumber);
		}
		
		return true;
		
	}
	
	/**
	 *利用BFS算法对两个节点之间的连通性进行测试，如果这两个节点之间连通，则返回true
	 */
	public boolean isReachable(long src,long dst){
		
		amount=copySwitchLinks.size();
		//如果源节点和目的节点是相同，那么返回true；
		if(src==dst){
			return true;
		}
		//如果链路表示拓扑中不包含该节点，那就表示没有任何一条链路与其相连，自然就是不可达的。
		if(copySwitchLinks.get(src)==null||copySwitchLinks.get(dst)==null){
			return false;
		}
		
		
		boolean[] visited=new boolean[amount+1];
		
		for(int i=0;i<=amount;i++){
			visited[i]=false;
		}
		//为BFS算法建立一个队列
		PriorityBlockingQueue<Long> queue=new PriorityBlockingQueue<Long>();
		queue.add(src);
		
		long destination;
		Link link;
		while(!queue.isEmpty()){
			
			src=queue.poll();
			
			Set<Link> links=copySwitchLinks.get(src);
			Iterator<Link> iter=links.iterator();
			while(iter.hasNext()){
				
				link=iter.next();
				destination=link.getDst();
				if(destination==dst){
					return true;
				}
				if(!visited[(int)destination]){
					visited[(int)destination]=true;
					queue.add(destination);
				}
			}
		}
		
		return false;
	}
	
	/***
	 *将指定的交换机端口关闭 
	 */
	public void setPortDown(IOFSwitch ofs,short portNumber){
		           
		//获得OpenFlow交换机的某个端口的物理地址
		ImmutablePort ofpPort = ofs.getPort(portNumber);
		if(ofpPort==null)
			return;
		if(portNumber==-2)
			return;
		byte[] macAddress=ofpPort.getHardwareAddress();
		//定义OFPortMod命令
		OFPortMod mymod=(OFPortMod)floodlightProvider.getOFMessageFactory().getMessage(OFType.PORT_MOD);
		//设置OFPortMod命令的相关参数
		mymod.setPortNumber(portNumber); 
		//mymod.setConfig(0); 开启某个端口
		mymod.setConfig(OFPhysicalPort.OFPortConfig.OFPPC_PORT_DOWN.getValue());
		mymod.setHardwareAddress(macAddress);
		mymod.setMask(0xffffffff);
		
		//将OFPortMod命令发送到指定的交换机中，进行执行！
		
		try{
			
			ofs.write(mymod,null);
			ofs.flush();
			/*String s=ofs+" "+Integer.toString(portNumber);
			log.info("{} is turn down!",s);*/
			}catch(Exception e){
					
				log.info("PortDown Failed");
			
			}	
		return;
	}
	/**
	 * 输出网络中所有的链路信息
	 */
	public void showSwitchLinks(){ 
		Set<Long> keys=copySwitchLinks.keySet();
		Iterator<Long> iter1=keys.iterator();
		while(iter1.hasNext()){
			Long key=iter1.next();
			Set<Link> links=copySwitchLinks.get(key);
			Iterator<Link> iter2=links.iterator();
			while(iter2.hasNext()){
				Link link=iter2.next();
				System.out.println(key+":"+link);
			}
		}
	
	}
	/**
	 *打印指定交换机之间的路由
	 */
	public void getSpecificRoute(long src,long dst){
		System.out.println(routeEngine.getRoute(src, dst, 0));
	}
	/*********************************************************************************************************/
	
	@Override
	public void init(FloodlightModuleContext context)
			throws FloodlightModuleException {
		// TODO Auto-generated method stub
		floodlightProvider = context.getServiceImpl(IFloodlightProviderService.class);
		threadPool = context.getServiceImpl(IThreadPoolService.class);
		linkDiscoveryManager=context.getServiceImpl(ILinkDiscoveryService.class);
		routeEngine=context.getServiceImpl(IRoutingService.class);
	}

	@Override
	public void startUp(FloodlightModuleContext context) {
		// TODO Auto-generated method stub
		
		nodePortPairDown=new HashMap<Long,Set<Short>>();  //实例化关闭的交换机端口对
		ScheduledExecutorService ses = threadPool.getScheduledExecutor();
		newInstanceTask = new SingletonTask(ses, new Runnable(){
			public void run(){
				try{	
					energySaving();
					copySwitchLinks();
				}finally{
						if(finalFlag){
							initialFlag=false;
							newInstanceTask.reschedule(5,TimeUnit.SECONDS);
						}
						
				}					
			}
		});
		newInstanceTask.reschedule(30, TimeUnit.SECONDS);
	}

}
