package net.floodlightcontroller.loadbalancer.RouteByToS;

import net.floodlightcontroller.core.*;
import net.floodlightcontroller.core.module.FloodlightModuleContext;
import net.floodlightcontroller.core.module.FloodlightModuleException;
import net.floodlightcontroller.core.module.IFloodlightModule;
import net.floodlightcontroller.core.module.IFloodlightService;
import net.floodlightcontroller.core.util.SingletonTask;
import net.floodlightcontroller.devicemanager.IDevice;
import net.floodlightcontroller.devicemanager.IDeviceService;
import net.floodlightcontroller.linkCostService.ILinkCostService;
import net.floodlightcontroller.linkCostService.LinkCostManager;
import net.floodlightcontroller.linkdiscovery.ILinkDiscoveryService;
import net.floodlightcontroller.routing.Link;
import net.floodlightcontroller.routing.Route;
import net.floodlightcontroller.threadpool.IThreadPoolService;
import org.openflow.protocol.OFMessage;
import org.openflow.protocol.OFType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Created by Victor on 2017/2/8.
 */
public class RouteByToS implements IFloodlightModule, IRouteByToS{
    private IFloodlightProviderService floodlightProvider;
    private IThreadPoolService threadPool;
    private ILinkDiscoveryService linkDiscoveryManager;
    private ILinkCostService linkCostService;
    private IDeviceService deviceManager;
    private SingletonTask newInstanceTask;
    //链路权重<链路，速率>
    private Map<Link, Double> linkCost;
    //拓扑图<dpId, 链路>
    private Map<Long, Set<Link>> wholeTopology;
    protected static Logger log = LoggerFactory
            .getLogger(RouteByToS.class);


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
                Link link = iter2.next();
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

    public Map<Link, Double> getLinkCost() {
        return linkCost;
    }

    public Map<Long, Set<Link>> getWholeTopology() {
        return wholeTopology;
    }

    @Override
    public Collection<Class<? extends IFloodlightService>> getModuleServices() {
        Collection<Class<? extends IFloodlightService>> l = new ArrayList<Class<? extends IFloodlightService>>();
        l.add(IRouteByToS.class);
        return l;
    }

    @Override
    public Map<Class<? extends IFloodlightService>, IFloodlightService> getServiceImpls() {
        Map<Class<? extends IFloodlightService>, IFloodlightService> m = new HashMap<Class<? extends IFloodlightService>, IFloodlightService>();
        // We are the class that implements the service
        m.put(IRouteByToS.class, this);
        return m;
    }

    @Override
    public Collection<Class<? extends IFloodlightService>> getModuleDependencies() {
        return null;
    }

    @Override
    public void init(FloodlightModuleContext context) throws FloodlightModuleException {
        floodlightProvider = context
                .getServiceImpl(IFloodlightProviderService.class);
        threadPool = context.getServiceImpl(IThreadPoolService.class);
        linkCostService = context.getServiceImpl(ILinkCostService.class);
        linkDiscoveryManager = context
                .getServiceImpl(ILinkDiscoveryService.class);
        deviceManager = context.getServiceImpl(IDeviceService.class);
    }

    @Override
    public void startUp(FloodlightModuleContext context) throws FloodlightModuleException {
        wholeTopology = new HashMap<Long, Set<Link>>();

        ScheduledExecutorService ses = threadPool.getScheduledExecutor();
        newInstanceTask = new SingletonTask(ses, new Runnable(){
           public void run(){
               try {
                    linkCost = linkCostService.getLinkCost();   //获取链路速率
                    copySwitchLinks();  //获取拓扑
                    Collection<? extends IDevice> allDevices = deviceManager
                           .getAllDevices();
                    log.info("run RouteByToS");

               }catch (Exception e){
                   log.error("exception",e);
               }finally{
                   newInstanceTask.reschedule(15, TimeUnit.SECONDS);
               }
           }
        });
        newInstanceTask.reschedule(20, TimeUnit.SECONDS);
    }

    @Override
    public Route getRoute(long src, long dst, long cookie) {
        return null;
    }

    @Override
    public Route getRoute(long src, long dst, long cookie, boolean tunnelEnabled) {
        return null;
    }

    @Override
    public Route getRoute(long srcId, short srcPort, long dstId, short dstPort, long cookie) {
        return null;
    }

    @Override
    public Route getRoute(long srcId, short srcPort, long dstId, short dstPort, long cookie, boolean tunnelEnabled) {
        return null;
    }

    @Override
    public ArrayList<Route> getRoutes(long longSrcDpid, long longDstDpid, boolean tunnelEnabled) {
        return null;
    }

    @Override
    public boolean routeExists(long src, long dst) {
        return false;
    }

    @Override
    public boolean routeExists(long src, long dst, boolean tunnelEnabled) {
        return false;
    }
}
