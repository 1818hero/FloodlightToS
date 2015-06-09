package net.floodlightcontroller.test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.openflow.protocol.OFFlowMod;
import org.openflow.protocol.OFMatch;
import org.openflow.protocol.OFType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.floodlightcontroller.core.IFloodlightProviderService;
import net.floodlightcontroller.core.IOFSwitch;
import net.floodlightcontroller.core.module.FloodlightModuleContext;
import net.floodlightcontroller.core.module.FloodlightModuleException;
import net.floodlightcontroller.core.module.IFloodlightModule;
import net.floodlightcontroller.core.module.IFloodlightService;
import net.floodlightcontroller.core.util.SingletonTask;
import net.floodlightcontroller.energySaving.EnergySaving;
import net.floodlightcontroller.packet.IPv4;
import net.floodlightcontroller.routing.IRoutingService;
import net.floodlightcontroller.threadpool.IThreadPoolService;
import net.floodlightcontroller.linkCostService.ILinkCostService;

public class Test implements IFloodlightModule {
	
	private IFloodlightProviderService floodlightProvider=null;
	private IThreadPoolService threadPool=null;
	private IRoutingService routeEngine=null;
	protected SingletonTask newInstanceTask=null;
	private ILinkCostService linkCost=null;
	protected static Logger log = LoggerFactory.getLogger(EnergySaving.class);
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
		// TODO Auto-generated method stub
		floodlightProvider = context.getServiceImpl(IFloodlightProviderService.class);
		threadPool = context.getServiceImpl(IThreadPoolService.class);
		routeEngine=context.getServiceImpl(IRoutingService.class);
		linkCost = context.getServiceImpl(ILinkCostService.class);

	}
	
	/**
	 *打印指定两个交换机之间的路由
	 */
	public void getSpecificRoute(long src,long dst){
		System.out.println(routeEngine.getRoute(src, dst, 0));
	}
	
	/**
	 * 删除交换机中的某一个流表项
	 */
	public void deleteFlowEntry(long switchNumber,short portNumber){
		IOFSwitch sw=floodlightProvider.getSwitch(switchNumber);
		OFMatch match=new OFMatch();
		match.setNetworkProtocol(IPv4.PROTOCOL_ICMP);
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

	@Override
	public void startUp(FloodlightModuleContext context)
			throws FloodlightModuleException {
		// TODO Auto-generated method stub
		ScheduledExecutorService ses = threadPool.getScheduledExecutor();
		newInstanceTask = new SingletonTask(ses, new Runnable(){
			public void run(){
				try{
					System.out.println(linkCost);
					//method wanted to be invoked;  
				}catch(Exception e){
					e.printStackTrace();
				}finally{
					newInstanceTask.reschedule(10,TimeUnit.SECONDS);	
				}					
			}
		});
		newInstanceTask.reschedule(10, TimeUnit.SECONDS);
	}

}
