"""Custom topology example
Adding the 'topos' dict with a key/value pair to generate our newly defined
topology enables one to pass in '--topo=mytopo' from the command line.
"""
 
from mininet.node import RemoteController
from mininet.node import CPULimitedHost
from mininet.link import TCLink
from mininet.net import Mininet
from mininet.util import dumpNodeConnections
from mininet.log import setLogLevel
from mininet.topo import Topo
from mininet.cli import CLI
from time import sleep
from random import uniform
import logging

logging.basicConfig(level=logging.INFO)

class MyTopo( Topo ):
    "Simple topology example."
 
    def __init__( self ):
        "Create custom topo."
 
        # Initialize topology
        Topo.__init__( self )
        L1 = 4
        L2 = L1 * 2 
        L3 = L2
        c = []
        a = []
        e = []
          
        # add core ovs  
        for i in range( L1 ):
            sw = self.addSwitch( 'c{}'.format( i + 1 ) )
            c.append( sw )
    
        # add aggregation ovs
        for i in range( L2 ):
            sw = self.addSwitch( 'a{}'.format( L1 + i + 1 ) )
            a.append( sw )
    
        # add edge ovs
        for i in range( L3 ):
            sw = self.addSwitch( 'e{}'.format( L1 + L2 + i + 1 ) )
            e.append( sw )
 
        # add links between core and aggregation ovs
        for i in range( L1 ):
            sw1 = c[i]
            for sw2 in a[i/2::L1/2]:
                self.addLink(sw2, sw1, bw=10, delay='5ms', max_queue_size=1000, use_htb=True)
                #self.addLink( sw2, sw1 )
 
        # add links between aggregation and edge ovs
        for i in range( 0, L2, 2 ):
            for sw1 in a[i:i+2]:
                for sw2 in e[i:i+2]:
                    self.addLink(sw2, sw1, bw=10, delay='5ms', max_queue_size=1000, use_htb=True)
                    #self.addLink( sw2, sw1 )
 
        #add hosts and its links with edge ovs
        count = 1
        for sw1 in e:
            for i in range(2):
                host = self.addHost( 'h{}'.format( count ),cpu=.7/(L1*2*2)  )
                self.addLink( sw1, host,bw= 10 )
                count += 1
topos = { 'mytopo': ( lambda: MyTopo() ) }
def simpleTest():
    "Create and test a simple network"
    BW = 9                                  # set the rate of the communication
    topo=MyTopo()
    net=Mininet( topo,controller=lambda a: RemoteController( a, ip='127.0.0.1', port=6633),host=CPULimitedHost,link=TCLink)
    net.start()
    dumpNodeConnections(net.hosts)
    CLI(net)
    for i in xrange(1):
        iperfCommunication(net,BW,i)
    sleep(400)
    net.stop()
def iperfCommunication(net,BW,count):
    countHost = len( net.hosts )   #the count of the hosts in the topology
    serverList=[]
    for clientId in range(1,countHost+1):
        probablitities = [0.3,0.2,0.5]
        hostList = getHostList(clientId,countHost)  # get the candidate of communication
        serverId = randomPick(hostList,probablitities)
        logging.info("h%s connect to h%s" %(clientId,serverId))
        if serverId  not in  serverList:
            serverList.append(serverId)
            server = net.get("h{}".format(serverId))
            server.cmd("iperf -s -u >> /home/weking/log/server.log &")
        client = net.get("h{}".format(clientId))
        client.cmd("iperf -c 10.0.0.%s -t 120 -i 1 -b %sM  >> /home/weking/log/client_h%s_%s.log &" %(serverId,BW,clientId,count))
        sleep(3)
def getHostList(key,count,i=2,j=4,m=8):
    hostList=[]

    if (key+i)%count == 0:
        hostList.append(count)
    else:
        hostList.append((key+i)%count)
    
    if (key+j)%count == 0:
        hostList.append(count)
    else:
        hostList.append((key+j)%count)
    
    if (key+m)%count == 0:
        hostList.append(count)
    else:
        hostList.append((key+m)%count)
    return hostList

def randomPick(list, probabilities):  
    x = uniform(0,1)  
    element = None
    cumulative_probability = 0.0  
    for item, item_probability in zip(list, probabilities):  
        cumulative_probability += item_probability
        element = item
        if x < cumulative_probability:
            break 
    return element


if __name__ == '__main__':
        # Tellmininet to print useful information
    setLogLevel( 'info' )
    simpleTest()