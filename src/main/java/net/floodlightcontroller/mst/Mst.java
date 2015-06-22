package net.floodlightcontroller.mst;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.floodlightcontroller.core.module.FloodlightModuleContext;
import net.floodlightcontroller.core.module.FloodlightModuleException;
import net.floodlightcontroller.core.module.IFloodlightModule;
import net.floodlightcontroller.core.module.IFloodlightService;
import net.floodlightcontroller.core.util.SingletonTask;
import net.floodlightcontroller.linkCostService.ILinkCostService;
import net.floodlightcontroller.linkdiscovery.ILinkDiscoveryService;
import net.floodlightcontroller.routing.Link;
import net.floodlightcontroller.threadpool.IThreadPoolService;

public class Mst implements IFloodlightModule {
	
	// initial topology of the network
	private Map<Long,Set<Link>> wholeTopology=null;
	// minimum spanning tree topology of the network
	private Map<Long,Set<Link>> mstTopology=null;
	protected SingletonTask newInstanceTask=null;
	protected Map<Link,Integer> linkCost=null;
	
	// get the linkCost from linkCostManager
	protected ILinkCostService linkCostManager=null;
	protected ILinkDiscoveryService linkDiscoveryManager=null;
	protected static Logger log=LoggerFactory.getLogger(Mst.class);
	private IThreadPoolService threadPool=null;
	
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
	
	/**
	 * 产生最小生成树的拓扑
	 * @param wholeTopology
	 * @param linkCost
	 * @return
	 */
	public Map<Long,Set<Link>> generateMstTopology(Map<Long,Set<Link>> wholeTopology,Map<Link,Integer> linkCost){
		
		
		//对最小生成树生成的拓扑进行初始化，最初始时整个只存在节点，而不再存在链路；
		Set<Long> keySet=wholeTopology.keySet();
		Iterator<Long> iterator=keySet.iterator();
		
		if(keySet.isEmpty()){   //若没有连接mininet，则做返回处理；
			return null;
		}
		
		while(iterator.hasNext()){
			Set<Link> links=new HashSet<Link>();
			mstTopology.put(iterator.next(), links);
		}
		
		//由于交换机的dpid是从1开始的，所以对数组要做特殊处理，整个数组长度+1，并且索引值为0的不使用
		//整个拓扑中交换机的个数
		int length=keySet.size();
		//prim算法所需要的一些变量
		long[] value=new long[length+1];
		boolean[] visited=new boolean[length+1];
		long[] parent=new long[length+1];
				
		for(int i=0;i<length+1;i++){
			value[i]=Long.MAX_VALUE;
			visited[i]=false;
		}
		
		//下标为0的索引，为无效的, 将其标为true；
		visited[0]=true;
		parent[0]=-1;
		
		//选定dpid为0的交换机为根节点
		//根据网络的流量信息，可以选择根节点为流量最大的节点，需要了解MST中根节点的作用；选择依据；
		
		value[1]=0;           
		parent[1]=-1;
				
		for(int i=0;i<length;i++){
					
			int switchId=selectSwitch(value,visited);
			System.out.println(switchId);
			visited[switchId]=true;
			Long id=new Long(switchId);
			Set<Link> links=wholeTopology.get(id);
			Iterator<Link> iterator2=links.iterator();
					
			while(iterator2.hasNext()){
				Link link=iterator2.next();
				
				if(!linkCost.containsKey(link)){
					linkCost.put(link, 1);     //当linkCost中不包含link时，就设置为默认值1；
				}
				if(!visited[(int)link.getDst()] && linkCost.get(link)<value[(int)link.getDst()]){
					value[(int)link.getDst()]=linkCost.get(link);
					parent[(int)link.getDst()]=switchId;
				}
			}
		}
		
		/*for(Long node:parent){
			System.out.print(node);
			System.out.print(" ");
		}
		System.out.println();*/
		
		//
		for(int i=2;i < parent.length;i++){
			Link link=selectLink(wholeTopology,parent[i],i);
			mstTopology.get(link.getSrc()).add(link);
			Link link2=selectLink(wholeTopology,i,parent[i]);
			mstTopology.get(link2.getSrc()).add(link2);
		}
		
		//打印输出，非算法必须
		Set<Long> keySet2=mstTopology.keySet();
		Iterator<Long> iterator3 = keySet2.iterator();
		int i=1;
		while(iterator3.hasNext()){
			Long id=iterator3.next();
			Set<Link> links=mstTopology.get(id);
			Iterator<Link> iterator4=links.iterator();
			while(iterator4.hasNext()){
				System.out.println((i++)+" "+id+" "+iterator4.next());
			}
		}
				
		return mstTopology;
	}
	/**
	 * 选择链路权重最小的节点，并且该节点未被选中过
	 * @param value
	 * @param visited
	 * @return
	 */
	public int selectSwitch(long[] value,boolean[] visited){
		int length=value.length;
		int min_index=1;
		long minValue=Long.MAX_VALUE;
		for(int i=0;i<length;i++){
			if(value[i]< minValue && !visited[i]){
				min_index=i;
				minValue=value[i];
			}
		}
		return min_index;
	}
	/**
	 * 根据指定的源节点和目的节点，寻找处对应的链路
	 * @param switchLinks
	 * @param src
	 * @param dst
	 * @return 
	 */
	public Link selectLink(Map<Long,Set<Link>> wholeTopology,long src,long dst){
		Long srcId=new Long(src);
		Set<Link> links=wholeTopology.get(srcId);
		Iterator<Link> iterator=links.iterator();
		while(iterator.hasNext()){
			Link link=iterator.next();
			if(link.getSrc()==src && link.getDst()==dst){
				return link;
			}
		}
		return null;
	}
	
	/**
	 * 完成对网络拓扑信息的复制, 将网络的初始拓扑保存下来
	 */
	public void copySwitchLinks(){
		
		Map<Long,Set<Link>> switchLinks=linkDiscoveryManager.getSwitchLinks();  
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
				}
			}
			wholeTopology.put(key, srcLink);
			
		}
		
		/*Set<Long> dpids=wholeTopology.keySet();
		Iterator<Long> iter3=keys.iterator();
		while(iter3.hasNext()){
			long dpid=iter3.next();
			Set<Link> links=wholeTopology.get(dpid);
			Iterator<Link> iter4=links.iterator();
			while(iter4.hasNext()){
				Link link=iter4.next();
				System.out.println(link);
			}
		}*/
	}

	@Override
	public void init(FloodlightModuleContext context)
			throws FloodlightModuleException {
		// TODO Auto-generated method stub
		linkCostManager=context.getServiceImpl(ILinkCostService.class);
		linkDiscoveryManager=context.getServiceImpl(ILinkDiscoveryService.class);
		threadPool = context.getServiceImpl(IThreadPoolService.class);
	}

	@Override
	public void startUp(FloodlightModuleContext context)
			throws FloodlightModuleException {
		// TODO Auto-generated method stub
		wholeTopology=new HashMap<Long,Set<Link>>();
		mstTopology=new HashMap<Long,Set<Link>>();
		
		ScheduledExecutorService ses = threadPool.getScheduledExecutor();
		newInstanceTask = new SingletonTask(ses, new Runnable(){
			public void run(){
				try{
					linkCost=linkCostManager.getLinkCost();
					copySwitchLinks();
					generateMstTopology(wholeTopology,linkCost);
				}finally{
					newInstanceTask.reschedule(10, TimeUnit.SECONDS);
				}					
			}
		});
		
		newInstanceTask.reschedule(10,TimeUnit.SECONDS);
		
	}

}
