package net.floodlightcontroller.linkCostService;

/**
 * Created by victor on 2017/4/12.
 */
public enum LinkType {
    FiberLink(100.0),CableLink(10.0);
    private double bandwidth;
    LinkType(double bandwidth){
        this.bandwidth = bandwidth;
    }
    public double getBandwidth(){
        return bandwidth;
    }
}
