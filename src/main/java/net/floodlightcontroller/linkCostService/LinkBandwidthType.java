package net.floodlightcontroller.linkCostService;

/**
 * Created by victor on 2017/4/12.
 */
public enum LinkBandwidthType {
    FiberLink(20.0),CableLink(10.0);
    private double bandwidth;
    LinkBandwidthType(double bandwidth){
        this.bandwidth = bandwidth;
    }
    public double getBandwidth(){
        return bandwidth;
    }
}
