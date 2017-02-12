package net.floodlightcontroller.forwarding;

import net.floodlightcontroller.core.FloodlightContext;
import net.floodlightcontroller.core.IListener;
import net.floodlightcontroller.core.IOFSwitch;
import net.floodlightcontroller.core.annotations.LogMessageCategory;
import net.floodlightcontroller.core.module.FloodlightModuleContext;
import net.floodlightcontroller.core.module.FloodlightModuleException;
import net.floodlightcontroller.core.module.IFloodlightModule;
import net.floodlightcontroller.core.module.IFloodlightService;
import net.floodlightcontroller.loadbalancer.RouteByToS.IRouteByToS;
import net.floodlightcontroller.routing.ForwardingBase;
import net.floodlightcontroller.routing.IRoutingDecision;
import org.openflow.protocol.OFPacketIn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;

import static java.lang.Thread.sleep;

/**
 * Created by Victor on 2017/2/26.
 */
@LogMessageCategory("Flow Programming")
public class ForwardByToS extends ForwardingBase implements IFloodlightModule {
    protected static Logger log = LoggerFactory.getLogger(ForwardByToS.class);
    private IRouteByToS router = null;

    @Override
    public Command processPacketInMessage(IOFSwitch sw, OFPacketIn pi, IRoutingDecision decision, FloodlightContext cntx) {
        return null;
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
        router = context.getServiceImpl(IRouteByToS.class);
    }

    @Override
    public void startUp(FloodlightModuleContext context) throws FloodlightModuleException {
        try {
            for(int i=0;i<100;i++) {
                sleep(10000);
                log.info(router.getWholeTopology().toString());
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        //log.info(router);
    }


}
