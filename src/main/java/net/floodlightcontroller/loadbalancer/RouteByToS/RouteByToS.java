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
import net.floodlightcontroller.linkdiscovery.ILinkDiscoveryService;
import net.floodlightcontroller.linkdiscovery.LinkInfo;
import net.floodlightcontroller.routing.Link;
import net.floodlightcontroller.routing.Route;
import net.floodlightcontroller.threadpool.IThreadPoolService;
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
    //ToS分级数目
    private static int ToSLevelNum = 6;
    //链路最大参考速率
    private static double MaxSpeed = 100;
    //链路最小门限因子
    private static double factor = 0.5;


    //链路权重<链路，速率>
    private Map<Link, Double> linkCost;
    //预测链路权重<链路，速率>
    private Map<Link, Double> predictLinkCost;
    //拓扑图<dpId, 链路>
    private Map<Long, Set<Link>> wholeTopology;

    //dpId和拓扑邻接矩阵下标之间的映射关系
    private Map<Long, Integer>  dpIdMap;

    //拓扑图(邻接矩阵形式)
    private List<List<Integer>> TopoMatrix;
    //拓扑中的设备
    private Collection<? extends IDevice> allDevices;
    //拓扑中的交换机数目
    private int switchNum = 0;
    // 路由表,即ToS分级下的path
    private List<List<List<Integer>>> routeTable;
    // 顶点集合
    private char[] mVexs;
    // ToS分级下的拓扑
    //private List<List<List<Integer>>> mMatrix;
    // 最大值
    private static final int INF = Integer.MAX_VALUE;

    protected static Logger log = LoggerFactory
            .getLogger(RouteByToS.class);

    /**
     * 完成对网络拓扑信息的复制, 将网络的初始拓扑保存下来
     * getSwitchLinks获得的是双工链路，这里将其视为一条
     */
    public void copySwitchLinks() {
        wholeTopology = new HashMap<>();
        TopoMatrix = new ArrayList<>();

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
        /**
         * 将拓扑结构复制为邻接矩阵的形式
         * @Author Victor
         */

        Set<Long> allSwitches = wholeTopology.keySet();
        switchNum = allSwitches.size();
        for(int i=0;i<switchNum;i++){   //初始化TopoMatrix
            TopoMatrix.add(new ArrayList<>());
            for(int j=0;j<switchNum;j++)    TopoMatrix.get(i).add(INF);
        }
        dpIdMap = new HashMap<>();
        int index = 0;
        //建立dpid和index之间的映射关系
        for(long dpid : allSwitches)    dpIdMap.put(dpid, index++);

        Map<Link,LinkInfo> allLinks = linkDiscoveryManager.getLinks();
        //建立邻接矩阵形式的拓扑图
        for(Link link : allLinks.keySet()){
            int srcIndex = dpIdMap.get(link.getSrc());
            int dstIndex = dpIdMap.get(link.getDst());
            TopoMatrix.get(srcIndex).set(dstIndex, 1);
            TopoMatrix.get(dstIndex).set(srcIndex, 1);
        }
        log.info("copy topo finished");
		/*
		 * Set<Long> dpids=wholeTopology.keySet(); Iterator<Long>
		 * iter3=keys.iterator(); while(iter3.hasNext()){ long
		 * dpid=iter3.next(); Set<Link> links=wholeTopology.get(dpid);
		 * Iterator<Link> iter4=links.iterator(); while(iter4.hasNext()){ Link
		 * link=iter4.next(); System.out.println(link); } }
		 */
    }

    /**
     * floyd最短路径。
     * 即，统计图中各个顶点间的最短路径。
     *
     * 参数说明：
     *     path -- 路径。path[i][j]=k表示，"顶点i"到"顶点j"的最短路径会经过顶点k，即routeTable[i]
     *     dist -- 长度数组。即，dist[i][j]=sum表示，"顶点i"到"顶点j"的最短路径的长度是sum。
     *     mMatrix -- 某个ToS阈值下的拓扑邻接矩阵
     */
    public void floyd(int[][] path, int[][] dist, int[][] mMatrix) {

        // 初始化
        for (int i = 0; i < switchNum; i++) {
            for (int j = 0; j < switchNum; j++) {
                dist[i][j] = mMatrix[i][j];    // "顶点i"到"顶点j"的路径长度为"i到j的权值"。
                path[i][j] = j;                // "顶点i"到"顶点j"的最短路径是经过顶点j。
            }
        }

        // 计算最短路径
        for (int k = 0; k < switchNum; k++) {
            for (int i = 0; i < switchNum; i++) {
                for (int j = 0; j < switchNum; j++) {

                    // 如果经过下标为k顶点路径比原两点间路径更短，则更新dist[i][j]和path[i][j]
                    int tmp = (dist[i][k]==INF || dist[k][j]==INF) ? INF : (dist[i][k] + dist[k][j]);
                    if (dist[i][j] > tmp) {
                        // "i到j最短路径"对应的值设，为更小的一个(即经过k)
                        dist[i][j] = tmp;
                        // "i到j最短路径"对应的路径，经过k
                        path[i][j] = path[i][k];
                    }
                }
            }
        }
    }


    public void floyd(List<List<Integer>> path, List<List<Integer>> dist, List<List<Integer>> mMatrix) {

        // 初始化
        for (int i = 0; i < switchNum; i++) {
            for (int j = 0; j < switchNum; j++) {
                dist.get(i).set(j,mMatrix.get(i).get(j));    // "顶点i"到"顶点j"的路径长度为"i到j的权值"。
                path.get(i).set(j,j);                // "顶点i"到"顶点j"的最短路径是经过顶点j。
            }
        }

        // 计算最短路径
        for (int k = 0; k < switchNum; k++) {
            for (int i = 0; i < switchNum; i++) {
                for (int j = 0; j < switchNum; j++) {
                    // 如果经过下标为k顶点路径比原两点间路径更短，则更新dist[i][j]和path[i][j]
                    int tmp = (dist.get(i).get(k)==INF || dist.get(k).get(j)==INF) ? INF : (dist.get(i).get(k) + dist.get(k).get(j));
                    if (dist.get(i).get(j) > tmp) {
                        // "i到j最短路径"对应的值设，为更小的一个(即经过k)
                        dist.get(i).set(j,tmp);
                        // "i到j最短路径"对应的路径，经过k
                        path.get(i).set(j,path.get(i).get(k));
                    }
                }
            }
        }
    }

    /**
     * 根据ToS级别生成对应的拓扑图，然后得到对应的路由表
     * @return
     */
    public void routeCompute(){
        double threshold = factor*MaxSpeed;
//        if(TopoChanged||CostLevelChanged) {
            double level = MaxSpeed*(1-factor)/ToSLevelNum;
            List<List<Integer>> curTopoMatrix = new ArrayList<>();  //初始化不同ToS分级下的邻接矩阵
            for(int i=0;i<switchNum;i++){
                curTopoMatrix.add(new ArrayList<>(switchNum));
            }
            for (int i = 0; i < ToSLevelNum; i++) {

                threshold+=level;
            }
 //       }
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
        routeTable = new ArrayList<>();
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
                    predictLinkCost = linkCost;     //暂时先这么写

                    allDevices = deviceManager.getAllDevices();
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
