package net.floodlightcontroller.forwarding;

import net.floodlightcontroller.core.FloodlightContext;
import net.floodlightcontroller.core.IFloodlightProviderService;
import net.floodlightcontroller.core.IListener;
import net.floodlightcontroller.core.IOFSwitch;
import net.floodlightcontroller.core.annotations.LogMessageCategory;
import net.floodlightcontroller.core.annotations.LogMessageDoc;
import net.floodlightcontroller.core.module.FloodlightModuleContext;
import net.floodlightcontroller.core.module.FloodlightModuleException;
import net.floodlightcontroller.core.module.IFloodlightModule;
import net.floodlightcontroller.core.module.IFloodlightService;
import net.floodlightcontroller.core.util.AppCookie;
import net.floodlightcontroller.counter.ICounterStoreService;
import net.floodlightcontroller.devicemanager.IDevice;
import net.floodlightcontroller.devicemanager.IDeviceService;
import net.floodlightcontroller.devicemanager.SwitchPort;
import net.floodlightcontroller.loadbalancer.RouteByToS.IRouteByToS;
import net.floodlightcontroller.packet.Ethernet;
import net.floodlightcontroller.routing.*;
import net.floodlightcontroller.topology.ITopologyService;
import org.openflow.protocol.*;
import org.openflow.protocol.action.OFAction;
import org.openflow.protocol.action.OFActionOutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

import static java.lang.Thread.sleep;

/**
 * Created by Victor on 2017/2/26.
 */
@LogMessageCategory("Flow Programming")
public class ForwardByToS extends ForwardingBase implements IFloodlightModule {
    protected static Logger log = LoggerFactory.getLogger(ForwardByToS.class);
    private IRouteByToS router = null;
    private static int INF = Integer.MAX_VALUE;

    @Override
    public Command processPacketInMessage(IOFSwitch sw, OFPacketIn pi, IRoutingDecision decision, FloodlightContext cntx) {
        Ethernet eth = IFloodlightProviderService.bcStore.get(cntx,
                IFloodlightProviderService.CONTEXT_PI_PAYLOAD);
        Map<Long,Set<Link>> topo = router.getWholeTopology();
        // If a decision has been made we obey it
        // otherwise we just forward
        if (decision != null) {
            if (log.isTraceEnabled()) {
                log.trace("Forwaring decision={} was made for PacketIn={}",
                        decision.getRoutingAction().toString(),
                        pi);
            }

            switch(decision.getRoutingAction()) {
                case NONE:
                    // don't do anything
                    return Command.CONTINUE;
                case FORWARD_OR_FLOOD:
                case FORWARD:
                    doForwardFlow(sw, pi, cntx, false);
                    return Command.CONTINUE;
                case MULTICAST:
                    // treat as broadcast
                    doFlood(sw, pi, cntx);
                    return Command.CONTINUE;
                case DROP:
                    doDropFlow(sw, pi, decision, cntx);
                    return Command.CONTINUE;
                default:
                    log.error("Unexpected decision made for this packet-in={}",
                            pi, decision.getRoutingAction());
                    return Command.CONTINUE;
            }
        } else {
            if (log.isTraceEnabled()) {
                log.trace("No decision was made for PacketIn={}, forwarding",
                        pi);
            }

            if (eth.isBroadcast() || eth.isMulticast()) {
                // For now we treat multicast as broadcast
                doFlood(sw, pi, cntx);
            } else {
                doForwardFlow(sw, pi, cntx, false);
            }
        }

        return Command.CONTINUE;
    }


    @LogMessageDoc(level="ERROR",
            message="Failure writing drop flow mod",
            explanation="An I/O error occured while trying to write a " +
                    "drop flow mod to a switch",
            recommendation=LogMessageDoc.CHECK_SWITCH)
    protected void doDropFlow(IOFSwitch sw, OFPacketIn pi, IRoutingDecision decision, FloodlightContext cntx) {
        // initialize match structure and populate it using the packet
        OFMatch match = new OFMatch();
        match.loadFromPacket(pi.getPacketData(), pi.getInPort());
        if (decision.getWildcards() != null) {
            match.setWildcards(decision.getWildcards());
        }

        // Create flow-mod based on packet-in and src-switch
        OFFlowMod fm =
                (OFFlowMod) floodlightProvider.getOFMessageFactory()
                        .getMessage(OFType.FLOW_MOD);
        List<OFAction> actions = new ArrayList<OFAction>(); // Set no action to
        // drop
        long cookie = AppCookie.makeCookie(FORWARDING_APP_ID, 0);

        fm.setCookie(cookie)
                .setHardTimeout((short) 0)
                .setIdleTimeout((short) 5)
                .setBufferId(OFPacketOut.BUFFER_ID_NONE)
                .setMatch(match)
                .setActions(actions)
                .setLengthU(OFFlowMod.MINIMUM_LENGTH); // +OFActionOutput.MINIMUM_LENGTH);

        try {
            if (log.isDebugEnabled()) {
                log.debug("write drop flow-mod sw={} match={} flow-mod={}",
                        new Object[] { sw, match, fm });
            }
            messageDamper.write(sw, fm, cntx);
        } catch (IOException e) {
            log.error("Failure writing drop flow mod", e);
        }
    }

    /**
     * 根据ToS查询对应路由表
     * @param sw
     * @param pi
     * @param cntx
     * @param requestFlowRemovedNotifn
     */
    protected void doForwardFlow(IOFSwitch sw, OFPacketIn pi,
                                 FloodlightContext cntx,
                                 boolean requestFlowRemovedNotifn) {
        OFMatch match = new OFMatch();
        match.loadFromPacket(pi.getPacketData(), pi.getInPort());


        IDevice dstDevice =
                IDeviceService.fcStore.
                        get(cntx, IDeviceService.CONTEXT_DST_DEVICE);
        if (dstDevice != null) {
            IDevice srcDevice =
                    IDeviceService.fcStore.
                            get(cntx, IDeviceService.CONTEXT_SRC_DEVICE);
            if (srcDevice == null) {
                log.debug("No device entry found for source device");
                return;
            }
            // Install all the routes where both src and dst have attachment
            // points.  Since the lists are stored in sorted order we can
            // traverse the attachment points in O(m+n) time
            SwitchPort[] srcDaps = srcDevice.getAttachmentPoints();

            SwitchPort[] dstDaps = dstDevice.getAttachmentPoints();

            byte DSCP = match.getNetworkTypeOfService();
            //ToS计算规则
            int ToSLevel = (DSCP&0x03)+((DSCP>>2)&0x03)+((DSCP>>4)&0x03);
            int PathCost = INF;
            SwitchPort pickedSrc = null;
            SwitchPort pickedDst = null;
            //从所有接入点中选择路径最短的一条
            for(SwitchPort srcDap : srcDaps){
                for(SwitchPort dstDap : dstDaps){
                    Route r = router.getRoute(srcDap.getSwitchDPID(), dstDap.getSwitchDPID(), 0, ToSLevel, true);
                    if(r!=null){
                        if(r.getRouteCount()<PathCost){
                            PathCost = r.getRouteCount();
                            pickedSrc = srcDap;
                            pickedDst = dstDap;
                        }
                    }
                }
            }
            Route route = router.getRoute(pickedSrc.getSwitchDPID(),
                    (short)pickedSrc.getPort(),
                    pickedDst.getSwitchDPID(),
                    (short)pickedDst.getPort(),
                    0,ToSLevel,true);

            if (route != null) {
                if (log.isTraceEnabled()) {
                    log.trace("pushRoute match={} route={} " +
                                    "destination={}:{}",
                            new Object[] {match, route,
                                    pickedDst.getSwitchDPID(),
                                    pickedSrc.getPort()});
                }
                long cookie =
                        AppCookie.makeCookie(FORWARDING_APP_ID, 0);

                // if there is prior routing decision use wildcard
                Integer wildcard_hints = null;
                IRoutingDecision decision = null;
                if (cntx != null) {
                    decision = IRoutingDecision.rtStore
                            .get(cntx,
                                    IRoutingDecision.CONTEXT_DECISION);
                }
                if (decision != null) {
                    wildcard_hints = decision.getWildcards();
                } else {
                    // L2 only wildcard if there is no prior route decision
                    wildcard_hints = ((Integer) sw
                            .getAttribute(IOFSwitch.PROP_FASTWILDCARDS))
                            .intValue()
                            & ~OFMatch.OFPFW_IN_PORT
                            & ~OFMatch.OFPFW_DL_VLAN
                            & ~OFMatch.OFPFW_DL_SRC
                            & ~OFMatch.OFPFW_DL_DST
                            & ~OFMatch.OFPFW_NW_SRC_MASK
                            & ~OFMatch.OFPFW_NW_DST_MASK;
                }

                pushRoute(route, match, wildcard_hints, pi, sw.getId(), cookie,
                        cntx, requestFlowRemovedNotifn, false,
                        OFFlowMod.OFPFC_ADD);
            }
        }else {
            // Flood since we don't know the dst device
            doFlood(sw, pi, cntx);
        }
    }

    /**
     * Creates a OFPacketOut with the OFPacketIn data that is flooded on all ports unless
     * the port is blocked, in which case the packet will be dropped.
     * @param sw The switch that receives the OFPacketIn
     * @param pi The OFPacketIn that came to the switch
     * @param cntx The FloodlightContext associated with this OFPacketIn
     */
    @LogMessageDoc(level="ERROR",
            message="Failure writing PacketOut " +
                    "switch={switch} packet-in={packet-in} " +
                    "packet-out={packet-out}",
            explanation="An I/O error occured while writing a packet " +
                    "out message to the switch",
            recommendation=LogMessageDoc.CHECK_SWITCH)
    protected void doFlood(IOFSwitch sw, OFPacketIn pi, FloodlightContext cntx) {
        if (topology.isIncomingBroadcastAllowed(sw.getId(),
                pi.getInPort()) == false) {
            if (log.isTraceEnabled()) {
                log.trace("doFlood, drop broadcast packet, pi={}, " +
                                "from a blocked port, srcSwitch=[{},{}], linkInfo={}",
                        new Object[] {pi, sw.getId(),pi.getInPort()});
            }
            return;
        }

        // Set Action to flood
        OFPacketOut po =
                (OFPacketOut) floodlightProvider.getOFMessageFactory().getMessage(OFType.PACKET_OUT);
        List<OFAction> actions = new ArrayList<OFAction>();
        if (sw.hasAttribute(IOFSwitch.PROP_SUPPORTS_OFPP_FLOOD)) {
            actions.add(new OFActionOutput(OFPort.OFPP_FLOOD.getValue(),
                    (short)0xFFFF));
        } else {
            actions.add(new OFActionOutput(OFPort.OFPP_ALL.getValue(),
                    (short)0xFFFF));
        }
        po.setActions(actions);
        po.setActionsLength((short) OFActionOutput.MINIMUM_LENGTH);

        // set buffer-id, in-port and packet-data based on packet-in
        short poLength = (short)(po.getActionsLength() + OFPacketOut.MINIMUM_LENGTH);
        po.setBufferId(OFPacketOut.BUFFER_ID_NONE);
        po.setInPort(pi.getInPort());
        byte[] packetData = pi.getPacketData();
        poLength += packetData.length;
        po.setPacketData(packetData);
        po.setLength(poLength);

        try {
            if (log.isTraceEnabled()) {
                log.trace("Writing flood PacketOut switch={} packet-in={} packet-out={}",
                        new Object[] {sw, pi, po});
            }
            messageDamper.write(sw, po, cntx);
        } catch (IOException e) {
            log.error("Failure writing PacketOut switch={} packet-in={} packet-out={}",
                    new Object[] {sw, pi, po}, e);
        }

        return;
    }


    @Override
    public Collection<Class<? extends IFloodlightService>> getModuleServices() {
        return null;
    }

    @Override
    public Map<Class<? extends IFloodlightService>, IFloodlightService> getServiceImpls() {
        return null;
    }

    @Override
    public Collection<Class<? extends IFloodlightService>> getModuleDependencies() {
        return null;
    }

    @Override
    public void init(FloodlightModuleContext context) throws FloodlightModuleException {
        super.init();
        this.floodlightProvider = context.getServiceImpl(IFloodlightProviderService.class);
        this.deviceManager = context.getServiceImpl(IDeviceService.class);
        this.routingEngine = context.getServiceImpl(IRoutingService.class);
        this.topology = context.getServiceImpl(ITopologyService.class);
        this.counterStore = context.getServiceImpl(ICounterStoreService.class);
        router = context.getServiceImpl(IRouteByToS.class);
    }

    @Override
    public void startUp(FloodlightModuleContext context) throws FloodlightModuleException {
        super.startUp();
        log.info("forwarding");
    }


}
