package net.floodlightcontroller.loadbalancer;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import java.util.ArrayList;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.util.ArrayList;
import net.floodlightcontroller.loadbalancer.LBPoolSerializer;
import net.floodlightcontroller.loadbalancer.LoadBalancer.IPClient;

@JsonSerialize(
        using = LBPoolSerializer.class
)
public class LBPool {
    protected String id = String.valueOf((int)(Math.random() * 10000.0D));
    protected String name = null;
    protected String tenantId = null;
    protected String netId = null;
    protected short lbMethod = 0;
    protected byte protocol = 0;
    protected ArrayList<String> members = new ArrayList();
    protected ArrayList<String> monitors = new ArrayList();
    protected short adminState = 0;
    protected short status = 0;
    protected String vipId;
    protected int previousMemberIndex = -1;

    public LBPool() {
    }

    public String pickMember(LoadBalancer.IPClient client) {
        if(this.members.size() > 0) {
            this.previousMemberIndex = (this.previousMemberIndex + 1) % this.members.size();
            return (String)this.members.get(this.previousMemberIndex);
        } else {
            return null;
        }
    }
}

