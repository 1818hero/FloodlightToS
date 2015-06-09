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

import org.openflow.protocol.OFMatch;
import org.openflow.protocol.OFPhysicalPort;
import org.openflow.protocol.OFPort;
import org.openflow.protocol.OFPortMod;
import org.openflow.protocol.OFFlowMod;
import org.openflow.protocol.OFStatisticsRequest;
import org.openflow.protocol.OFType;
import org.openflow.protocol.Wildcards;
import org.openflow.protocol.Wildcards.Flag;
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
import net.floodlightcontroller.packet.IPv4;
import net.floodlightcontroller.routing.IRoutingService;
import net.floodlightcontroller.routing.Link;
import net.floodlightcontroller.threadpool.IThreadPoolService;

public class EnergySaving implements IFloodlightModule {
	
	protected static Logger log = LoggerFactory.getLogger(EnergySaving.class);
	protected IThreadPoolService threadPool;    //线程池
	protected SingletonTask newInstanceTask=null;
	private IFloodlightProviderService floodlightProvider;
	private ILinkDiscoveryService linkDiscoveryManager;
	private IRoutingService routeEngine;
	private Map<Long, Set<Link>> switchLinks;
	private Map<Long,HashMap<Short,Long[]>> lastTimePortTraffic=new HashMap<Long,HashMap<Short,Long[]>>(); //上次收集到的交换机各个端口的流量信息（以数据包的个数来衡量）
	private Map<Long,List<OFStatistics>> networkTraffic=null; //网络中各个交换机的流量统计信息
	protected Map<Long,Set<Link>> copySwitchLinks=new HashMap<Long,Set<Link>>();
	protected static int activeLinkNumber=0;
	protected int t=0;
	protected Map<Long,Set<Short>> nodePortPairDown=null;
	private Map<Long,Set<Link>> initialTopology=null;    //
	
	private static boolean downPortFlag = false;  	//这个flag为true表示不进行判断节能的策略,主要是为了是的获得流量增量中的初始化流量统计信息
	private static boolean reDoEnergySaving = false;
	private static boolean getRateFlag=true;
	private boolean initialFlag=true;     //这个标识位的设置是为了在控制器启动时获取网络的初始拓扑。
	
	private int amount; //表示copySwitchLinks中交换机的数量
	private long initialTime = 0;  //节能策略开始运行的时间
	private long currentTime = 0;
	
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
	
	public void setInitialTopology(){
		switchLinks=linkDiscoveryManager.getSwitchLinks();
		Set<Long> switchIds=switchLinks.keySet();
		Iterator<Long> iter=switchIds.iterator();
		while(iter.hasNext()){
			
			Long key=iter.next();
			Set<Link> links=switchLinks.get(key);
			Set<Link> srcLink=new HashSet<Link>();
			Iterator<Link> iter1=links.iterator();
			
			while(iter1.hasNext()){
				Link link=new Link();
				link=iter1.next();
				if(key==link.getSrc()){
					srcLink.add(link);
				}
			}
			initialTopology.put(key, srcLink);
		}
		
	}
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
	 * 依据统计的流量信息来进行节能的策略，这个整个节能模块的入口程序！！！！
	 */
	public void energySaving(){
		System.out.println("******************************************************");
		this.networkTraffic = this.collectTraffic();     //收集网络的流量信息
		
		if( networkTraffic==null ){
			return;
		}
		
		Set<Long> dpids=this.networkTraffic.keySet();    //网络中的所有的交换机
		
		Iterator<Long> iter=dpids.iterator();
		
		while(iter.hasNext()){
			
			Long dpid=iter.next();
			
			IOFSwitch sw=floodlightProvider.getSwitch(dpid);
			
			Iterator<OFStatistics> iterator=this.networkTraffic.get(dpid).iterator();
			
			HashMap<Short,Long[]> hashMap=new HashMap<Short,Long[]>();
			
			while(iterator.hasNext()){
	        	
	        	OFPortStatisticsReply portReply=(OFPortStatisticsReply)iterator.next();
	        	short portNumber=portReply.getPortNumber();
	        	//如果这个端口是交换机与控制器的通信端口(short类型的65535转换为int后为-2)或者是主机和交换机链接的端口
	        	//(默认端口号是1)则不做任何的处理
	        	if( portNumber == -2 || portNumber == 1 ){  
	        		continue;
	        	}
	        	
	        	
	        	//当该端口被关闭后就不再进行判断操作。
	        	if(nodePortPairDown.containsKey(dpid)){
	        		if(nodePortPairDown.get(dpid).contains(portNumber)){
	        			continue;
	        		}
	        	}
	        	
	        	//由于交换机中的链路都是全双工的，当在计算端口的流量统计信息时，就需要得到某个端口的发送流量和接收流量的最大值
	        	Long transmitBytes=portReply.getTransmitBytes();
	        	Long receiveBytes=portReply.getReceiveBytes();
	        	Long[] currentPortTraffic=new Long[2];
	        	
	        	//将接受和发送数据的最大值作为判断是否关闭链路的依据
	        	currentPortTraffic[0]=transmitBytes;
	        	currentPortTraffic[1]=receiveBytes;
	        		        	
	        	String switch_Port=dpid.toString()+"："+Integer.toString(portNumber);
	        	
	        	
	        	
	        	if( downPortFlag ){        //downFlagFlag初始化为false，节能策略不执行,用来初始化lastTimePortTraffic
	        		
	        		long transmitByteRate =currentPortTraffic[0]-lastTimePortTraffic.get(dpid).get(portNumber)[0];
	        		long receiveByteRate = currentPortTraffic[1]-lastTimePortTraffic.get(dpid).get(portNumber)[1];
	        		long portByteRate=0;
	        		
	        		if(transmitByteRate>receiveByteRate){
	        			portByteRate = transmitByteRate;
	        		}else{
	        			portByteRate = receiveByteRate;
	        		}
	        		//portByteRate_MB表示在间隔5s内发送的数据总数，以字节的单位进行计算
	        	    double portRate_Mbps=8*portByteRate/(1024.0*1024.0*5);
	        		if(portRate_Mbps<0.0001){
	        			portRate_Mbps=0;
	        		}
	        		
	        	    log.info("Dpid:port({}) speed in Mbps is {}",switch_Port,portRate_Mbps);
	        	    	        	            	    
	        	    //判断流量是否大于所设定的阈值
	        	    //链路的带宽是100Mbps,设定阈值为0.2*100
	        	    if( portRate_Mbps < 20 ){
	        	    	        			
		        		if(canBeTurnDown(sw.getId(),portNumber) && setPortDown(sw,portNumber)){
		        			reDoEnergySaving = true;
		        			break;
		        		}
		        	} 		
	        	}
	        	hashMap.put( portNumber, currentPortTraffic );
	        }
			
			if( reDoEnergySaving ){      //reDoEnergySaving flag初始化为false,这是因为初始化不需要重新进行节能操作
				
				lastTimePortTraffic.clear(); 	//由于要重新进行能量节省操作，需要清空初始化流量信息，重新进行初始化流量信息			
				
				//下面的flag都要回归到模块运行的初始化阶段
				downPortFlag = false;  				//初始化流量信息
				reDoEnergySaving= false;            //下一步，继续收集流量！而不能重新去做节能
                //把getRateFlag设置为false，是为了让网络稳定后，重新进行节能操作！
			    getRateFlag=false;
				return; 							//执行完这个语句后，当前的while结束，重新进行节能模式的初始化收集流量操作。
			}
			
			lastTimePortTraffic.put(dpid,hashMap);
		}
		
		if(!downPortFlag){
			downPortFlag=true;
			getRateFlag=true;
		}
		
		
	}
	/**
	 * 判断指定的端口是否能够关闭？
	 * 即判断要关闭的两个端口所属的交换机是否存在其他的环路
	 */
	public boolean canBeTurnDown(long srcId,short srcPortNumber){
		
		copySwitchLinks();
		
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
		
		if(copySwitchLinks.get(srcId).size()==1){      //如果当前端口的交换机只有当前的一条链路，则直接删除该set即可
			copySwitchLinks.remove(srcId);
		}else{
			copySwitchLinks.get(srcId).remove(forwardLink); //删除前向链路	
		}
		
		if(copySwitchLinks.get(dstId).size()==1){      //如果当前端口的交换机只有当前的一条链路，则直接删除该set即可
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
		//删除源交换机和目的交换机的流表项，删除的传输层中目的端口是5001的流表项
		IOFSwitch dstSwitch=floodlightProvider.getSwitch(dstId);
		IOFSwitch srcSwitch=floodlightProvider.getSwitch(srcId);
		deleteFlowEntry(srcSwitch,srcPortNumber);
		deleteFlowEntry(dstSwitch,dstPortNumber);
		
		return true;
		
	}
	
	/**
	 * 利用BFS算法对两个节点之间的连通性进行测试，如果这两个节点之间连通，则返回true
	 * 该方法只在canBeTurnDown()中使用，其他方法没有调用
	 */
	public boolean isReachable(long src,long dst){
		
		copySwitchLinks();  //为防止出现ArrayOutOfBoundary错误，需要先更新一下链路相连map的复制copySwitchLinks
		amount=copySwitchLinks.size();
		//如果源节点和目的节点是相同，那么返回true；
		if(src==dst){
			return true;
		}
		
		//如果链路表示拓扑中不包含该节点，那就表示没有任何一条链路与其相连，自然就是不可达的。
		if(!copySwitchLinks.containsKey(src)||!copySwitchLinks.containsKey(dst)){
			return false;
		}
		
		boolean[] visited=new boolean[amount+1];  //建立一个长度时amount+1的数组，完全是为了利用下标与switchId的对应
		
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
			Iterator<Link> iter=links.iterator();   //空指针指向异常
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
	 *  将指定的交换机端口关闭 
	 */
	public boolean setPortDown(IOFSwitch ofs,short portNumber){
		           
		//获得OpenFlow交换机的某个端口的物理地址
		ImmutablePort ofpPort = ofs.getPort(portNumber);
		if(ofpPort==null)
			return false;
		if(portNumber==-2)
			return false;
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
		}catch(Exception e){		
			log.info("PortDown Failed");
		}	
		return true;
	}
	
	/**
	 * 删除交换机中的某一个流表项
	 */
	public void deleteFlowEntry(IOFSwitch sw,short portNumber){

		OFMatch match=new OFMatch();
		match.setWildcards(Wildcards.FULL.matchOn(Flag.TP_DST));
		match.setNetworkProtocol(IPv4.PROTOCOL_UDP);
		match.setTransportDestination((short)5001);
		OFFlowMod ofFlowMod=(OFFlowMod)floodlightProvider.getOFMessageFactory().getMessage(OFType.FLOW_MOD);
		ofFlowMod.setMatch(match);
		ofFlowMod.setCommand(OFFlowMod.OFPFC_DELETE); 
		ofFlowMod.setOutPort(portNumber);
		
		try{
			sw.write(ofFlowMod,null);
			sw.flush();
			
		}catch(Exception e){
			
			log.info("erase the flow entry of the specified portNumber failed!");
			
		}
		
		
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
		
		nodePortPairDown = new HashMap<Long,Set<Short>>();        //实例化关闭的交换机端口对
		ScheduledExecutorService ses = threadPool.getScheduledExecutor();
		initialTime = System.currentTimeMillis();
		newInstanceTask = new SingletonTask(ses, new Runnable(){
			public void run(){
				try{
					if(initialFlag){
						setInitialTopology();
						initialFlag=false;
					}
					currentTime= System.currentTimeMillis();
					if ((currentTime-initialTime)<=300000 ){      //节能策略只运行6分钟，关闭链路的操作最坏的情况下在5分钟后，保持稳定
						energySaving();
					}
				}finally{
					if(getRateFlag){
						newInstanceTask.reschedule(5,TimeUnit.SECONDS);    //为计算网络链路速率而设定的时间间隔
					}else{
						newInstanceTask.reschedule(10,TimeUnit.SECONDS);  // 等待10s（以保证网络的状态稳定），重新进行节能操作。
					}
				    
						
				}					
			}
		});
		//整个节能模块，在系统完成初始化后的20s后，开始进行节能操作
		newInstanceTask.reschedule(20, TimeUnit.SECONDS);
	}

}