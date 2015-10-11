package net.floodlightcontroller.loadbalancer.energySaving;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
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

public class AsynchronousJob implements IFloodlightModule {
	
	
	private static final Logger log = LoggerFactory.getLogger(AsynchronousJob.class);
	private IFloodlightProviderService floodlightProvider = null;
	private IThreadPoolService threadPool = null;
	private ILinkDiscoveryService linkDiscoveryManager = null;
	private SingletonTask newInstanceTask= null;
	private EnergySavingBaseOnMst energySavingBaseOnMst =null;

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
	
	public void copySwitchLinks(List<Link> currentLinks) {
		
		Map<Long, Set<Link>> switchLinks = linkDiscoveryManager
				.getSwitchLinks();
		Set<Long> keys = switchLinks.keySet();
		Iterator<Long> iter1 = keys.iterator();
		while (iter1.hasNext()) {
			Long key = iter1.next();
			Set<Link> links = switchLinks.get(key);
			Iterator<Link> iter2 = links.iterator();
			while (iter2.hasNext()) {
				Link link = new Link();
				link = iter2.next();
				if (key == link.getSrc()) {
					currentLinks.add(link);
				}
			}
		}
		
	}
	public boolean setLinkUp(Link link) {
		short portNumber = link.getDstPort();
		long dpid = link.getDst();
		IOFSwitch ofs = floodlightProvider.getSwitch(dpid);
		if (setPortUp(ofs, portNumber)
				& setPortUp(floodlightProvider.getSwitch(link.getSrc()),
						link.getSrcPort())) {
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
			log.error("link up fail");
			return false;
		}
		return true;
	}

	@Override
	public void init(FloodlightModuleContext context)
			throws FloodlightModuleException {
		floodlightProvider = context
				.getServiceImpl(IFloodlightProviderService.class);
		threadPool = context.getServiceImpl(IThreadPoolService.class);
		linkDiscoveryManager = context
				.getServiceImpl(ILinkDiscoveryService.class);
		energySavingBaseOnMst = context.getServiceImpl(EnergySavingBaseOnMst.class);

	}

	@Override
	public void startUp(FloodlightModuleContext context)
			throws FloodlightModuleException {
		ScheduledExecutorService executorService = threadPool.getScheduledExecutor();      
		newInstanceTask = new SingletonTask(executorService,new Runnable(){
			public void run(){
				try{
					List<Link> currentLinks = new ArrayList<Link>();
					copySwitchLinks(currentLinks);
					List<Link> haveSetUpLinks = energySavingBaseOnMst.getHaveSetUpLinks();
					if(haveSetUpLinks != null){
						for(Link link:haveSetUpLinks){
							if(!currentLinks.contains(link)){
								setLinkUp(link);
								log.info("AsynchronousJob set Link up: {}", link);
							}
						}
					}
				}catch(Exception e){
					
				}finally{
					newInstanceTask.reschedule(10, TimeUnit.SECONDS);
				}
				
			}
		});
		log.info("【测试】");
		newInstanceTask.reschedule(20, TimeUnit.SECONDS);

	}

}
