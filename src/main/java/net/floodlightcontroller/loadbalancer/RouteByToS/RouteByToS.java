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
import net.floodlightcontroller.routing.RouteId;
import net.floodlightcontroller.threadpool.IThreadPoolService;
import net.floodlightcontroller.topology.NodePortTuple;
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
    private Map<Integer, Long>  IndexMap;

    //拓扑图(邻接矩阵形式)
    private List<List<Link>> TopoMatrix;
    //拓扑中的设备
    private Collection<? extends IDevice> allDevices;
    //拓扑中的交换机数目
    private int switchNum = 0;
    // 路由表，即ToS分级下的path
    private List<List<List<Integer>>> routeTable;
    // 距离表，即ToS分级下的dist
    private List<List<List<Integer>>> dist;
    // 路由结果
    private List<Map<RouteId,Route>> routeCache;
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
        dpIdMap = new HashMap<>();
        IndexMap = new HashMap<>();

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

        int index = 0;
        //建立dpid和index之间的映射关系
        for(long dpid : allSwitches){
            dpIdMap.put(dpid, index);
            IndexMap.put(index, dpid);
            index++;
        }

        for(int i=0;i<switchNum;i++){   //初始化TopoMatrix
            TopoMatrix.add(new ArrayList<>(switchNum));
            for(int j=0;j<switchNum;j++)   {
                if(i!=j)    TopoMatrix.get(i).add(null);
                else    TopoMatrix.get(i).add(new Link());  // 什么都没有的link表示自己连接自己
            }
        }


        Map<Link,LinkInfo> allLinks = linkDiscoveryManager.getLinks();
        //建立邻接矩阵形式的拓扑图
        for(Link link : allLinks.keySet()){
            int srcIndex = dpIdMap.get(link.getSrc());
            int dstIndex = dpIdMap.get(link.getDst());
            TopoMatrix.get(srcIndex).set(dstIndex, link);
            TopoMatrix.get(dstIndex).set(srcIndex, link);
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
        try {
            // 初始化
            for (int i = 0; i < switchNum; i++) {
                for (int j = 0; j < switchNum; j++) {
                    dist.get(i).set(j, mMatrix.get(i).get(j));    // "顶点i"到"顶点j"的路径长度为"i到j的权值"。
                    path.get(i).set(j, j);                // "顶点i"到"顶点j"的最短路径是经过顶点j。
                }
            }

            // 计算最短路径
            for (int k = 0; k < switchNum; k++) {
                for (int i = 0; i < switchNum; i++) {
                    for (int j = 0; j < switchNum; j++) {
                        // 如果经过下标为k顶点路径比原两点间路径更短，则更新dist[i][j]和path[i][j]
                        int tmp = (dist.get(i).get(k) == INF || dist.get(k).get(j) == INF) ? INF : (dist.get(i).get(k) + dist.get(k).get(j));
                        if (dist.get(i).get(j) > tmp) {
                            // "i到j最短路径"对应的值设，为更小的一个(即经过k)
                            dist.get(i).set(j, tmp);
                            // "i到j最短路径"对应的路径，经过k (path是从第一个坐标为起点，到达第二个坐标时的下一跳)
                            path.get(i).set(j, path.get(i).get(k));
                        }
                    }
                }
            }
        }catch (Exception e){
            log.error("Floyd failed to execute!");
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
            //不同ToS分级下的邻接矩阵(1表示邻接)
            List<List<Integer>> curTopoMatrix = new ArrayList<>();
            //初始化距离矩阵
            dist = new ArrayList<>();
            //初始化总的routeTable
            routeTable = new ArrayList<>();
            //初始化总的routeCache
            routeCache = new ArrayList<>();
            //初始化邻接矩阵
            for(int i=0;i<switchNum;i++){
                curTopoMatrix.add(new ArrayList<>(switchNum));
                for(int j=0;j < switchNum;j++){
                    if(i!=j)    curTopoMatrix.get(i).add(INF);
                    else curTopoMatrix.get(i).add(0);
                }
            }
            for (int i = 0; i < ToSLevelNum; i++) {
                //初始化每一张routeTable、dist以及routeCache
                routeTable.add(new ArrayList<>());
                dist.add(new ArrayList<>());
                routeCache.add(new HashMap<RouteId,Route>());
                List<List<Integer>> everyRouteTable = routeTable.get(i);
                List<List<Integer>> everyDist = dist.get(i);

                for(int j=0;j<switchNum;j++){
                    everyRouteTable.add(new ArrayList<>(switchNum));
                    everyDist.add(new ArrayList<>(switchNum));
                    //在floyd算法中还会被初始化
                    for(int k=0;k<switchNum;k++){
                        everyRouteTable.get(j).add(INF);
                        everyDist.get(j).add(INF);
                    }
                }
                Set<Link> linkSet = predictLinkCost.keySet();
                //构造当前ToS下的拓扑邻接矩阵
                for(Link link : linkSet){
                    double curLoad = predictLinkCost.get(link);
                    int srcIndex = dpIdMap.get(link.getSrc());
                    int dstIndex = dpIdMap.get(link.getDst());
                    if(curLoad < threshold) {   //当未达到拥塞门限时则视为链路开放
                        curTopoMatrix.get(srcIndex).set(dstIndex, 1);
                        curTopoMatrix.get(dstIndex).set(srcIndex, 1);
                    }
                }
                //Floyd算法计算该ToS下的最短路
                floyd(everyRouteTable,everyDist,curTopoMatrix);
                //将路由计算结果写入routeCache
                Map<RouteId,Route> curCache = routeCache.get(i);
                //循环+递归加入路径cache
                for(int src=0;src<switchNum;src++){
                    for(int dst=0;dst<switchNum;dst++){
                        addCache(src,dst,curCache,everyDist,everyRouteTable);
                    }
                }
                threshold+=level;
            }
 //       }
    }

    /**
     * 递归增添路径到cache
     * @param src 起点对应邻接矩阵的下标
     * @param dst 终点对应的邻接矩阵的下标
     * @param curCache 当前ToS级别下的路径cache
     * @param everyDist 当前ToS级别下的距离矩阵
     * @param everyRouteTable 当前ToS级别下的路由表
     */
    public void addCache(int src,int dst,
                         Map<RouteId,Route> curCache,
                         List<List<Integer>> everyDist,
                         List<List<Integer>> everyRouteTable
                         )
    {
        //如果路径存在并且在cache中不存在该路径则添加路径
        if(everyDist.get(src).get(dst)!=INF) {
            RouteId curId = new RouteId(IndexMap.get(src),IndexMap.get(dst));
            if(!curCache.containsKey(curId)){
                List<NodePortTuple> path = new ArrayList<>();
                Route r = new Route(curId,null);
                int nextHopIndex = everyRouteTable.get(src).get(dst);
                if(nextHopIndex!=dst){
                    RouteId nextHopId = new RouteId((IndexMap.get(nextHopIndex)),IndexMap.get(dst));
                    //如果cache中不存在下一跳到终点的记录，则递归寻找
                    if(!curCache.containsKey(nextHopId)){
                        addCache(nextHopIndex,dst,curCache,everyDist,everyRouteTable);
                    }
                    path.addAll((List<NodePortTuple>)curCache.get(nextHopId));
                }
                //根据拓扑矩阵确定src到nextHop之间的端口对应关系
                NodePortTuple srcNPT ;
                NodePortTuple nextHopNPT;
                Link link = TopoMatrix.get(src).get(nextHopIndex);
                //必然只有这两种情况，否则我去吃屎
                if(link.getSrc()==IndexMap.get(src)){
                    srcNPT = new NodePortTuple(link.getSrc(),link.getSrcPort());
                    nextHopNPT = new NodePortTuple(link.getDst(),link.getDstPort());
                }
                else{
                    srcNPT = new NodePortTuple(link.getDst(),link.getDstPort());
                    nextHopNPT = new NodePortTuple(link.getSrc(),link.getSrcPort());
                }
                path.add(0,nextHopNPT);
                path.add(0,srcNPT);
                curCache.put(curId,new Route(curId,path));
            }
        }
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
                    predictLinkCost = linkCost;     //暂时先这么写
                    routeCompute();
                    allDevices = deviceManager.getAllDevices();
                    log.info("run RouteByToS");

               }catch (Exception e){
                   log.error("exception",e);
               }finally{
                   newInstanceTask.reschedule(120, TimeUnit.SECONDS);
               }
           }
        });
        newInstanceTask.reschedule(120, TimeUnit.SECONDS);
    }
    @Override
    public Route getRoute(long srcId, short srcPort, long dstId, short dstPort, long cookie, int TosLevel, boolean tunnelEnabled){
        // Return null the route source and desitnation are the
        // same switchports.
        if (srcId == dstId && srcPort == dstPort)
            return null;

        List<NodePortTuple> nptList;
        NodePortTuple npt;
        Route r = getRoute(srcId, dstId,0 , TosLevel, true);
        if (r == null && srcId != dstId) return null;

        if (r != null) {
            nptList= new ArrayList<NodePortTuple>(r.getPath());
        } else {
            nptList = new ArrayList<NodePortTuple>();
        }
        npt = new NodePortTuple(srcId, srcPort);
        nptList.add(0, npt); // add src port to the front
        npt = new NodePortTuple(dstId, dstPort);
        nptList.add(npt); // add dst port to the end

        RouteId id = new RouteId(srcId, dstId);
        r = new Route(id, nptList);
        return r;
    }
    @Override
    public Route getRoute(long src, long dst, long cookie, int ToSLevel, boolean tunnelEnabled){
        if(src==dst) return null;
        RouteId id = new RouteId(src, dst);
        Route result = null;
        int i=ToSLevel;
        if(i>ToSLevelNum) i=ToSLevelNum;    //如果包对应的级别超过ToSLevel总数，则按最高级别选择路由
        while(result==null) {
            try {
                Map<RouteId, Route> curCache = routeCache.get(i);
                if (curCache.containsKey(id)) {
                    result = curCache.get(id);
                } else if (i < ToSLevelNum) {   //如果当前级别不存在路径就提高ToS级别
                    i++;
                } else {
                    log.error("Route not found");
                    return null;
                }
            }catch (Exception e){
                log.error("Route not found");
                return null;
            }
        }
        if (log.isTraceEnabled()) {
            log.trace("getRoute: {} -> {}", id, result);
        }
        return result;
    }

    @Override
    public Route getRoute(long src, long dst, long cookie) {
        return getRoute(src, dst, cookie, true);
    }

    @Override
    public Route getRoute(long src, long dst, long cookie, boolean tunnelEnabled) {
        //tunnelEnabled和cookie未使用
        return getRoute(src, dst, cookie, ToSLevelNum, true);    //默认最高级别
    }

    @Override
    public Route getRoute(long srcId, short srcPort, long dstId, short dstPort, long cookie) {
        return getRoute(srcId, srcPort, dstId, dstPort, cookie, ToSLevelNum, true);//默认最高级别
    }

    @Override
    public Route getRoute(long srcId, short srcPort, long dstId, short dstPort, long cookie, boolean tunnelEnabled) {
        return getRoute(srcId, srcPort, dstId, dstPort, cookie, ToSLevelNum, true);//默认最高级别
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
