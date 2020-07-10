import logging
from circinus_tools import time_tools as tt
from sprint_tools.Constellation_STN import Constellation_STN
from .sim_agents import AgentType
import pickle as pkl

## Simulation component for physical transmission.
#
#  Intended as pass-through between TX and RX components on two different 
#  agents.  It will effectively provide routing to the correct agent,
#  verify the physics of the transmission, provide for attenuation,
#  and ultimately pass through the message.  This is part of the overall
#  environmental simulator. Functions provided by this module in this 
#  direct-call mode can be replaced by TCP/IP to facilitate agents as separate
#  entities (processes, on HITL, etc).
class Transmission_Simulator:

    ## Init T_S with appropriate params  (should be given by simulation config)
    #  @param self      The object pointer.
    #  @param en_attn   Enable attenuation calculations         (default disabled)
    #  @param en_stoch  Enable non-deterministic (stochastic)   (default disabled)
    #  @param en_bb     Enable backbone. TODO: replace w/ Top Graph
    def __init__(self, en_attn=False, en_stoch=False, simulation=None):
        # Transmission_Simulator configuration
        self.atten_en   = en_attn
        self.stoch_en   = en_stoch
        self.bb_en      = False     # Turn on independently

        # References to external resources
        self.pointer_to_simulation   = simulation     # NOT OWNED HERE - reference to 

        # Assess the configuration
        if (not self.bb_en) and (not self.pointer_to_simulation.access_truth_stn):
            raise RuntimeError("Either Backbone must be enabled, or STN must \
                be provided.")
        elif self.bb_en and self.pointer_to_simulation.access_truth_stn:  # Existence of STN and enabling of backbone conflict
            logging.warn("Backbone enabled but constellation stn provided. \
                Constellation stn will be ignored.")
        
    ## Set the backbone to on, or not
    #
    #  @param en_bb     Enable backbone - used for hotstart & control trial    
    def setBackbone(self, en_bb):
        self.bb_en = en_bb

    ## Freespace Transmit is called by one agent messaging another over RF/optical.
    #
    #  The T_S should be able to simulate the entire transmission without 
    #  access to the actual model of either agent, so the actual transmitted
    #  signal must be fully described, and the start/end points as well.
    #  TODO - Steps 1,2,3 as shown not yet implemented
    #  @param self      The object pointer.
    #  @param sndr      Reference to the sending agent
    #  @param dest      Reference to the destination agent
    #  @param payload   The encoded (serialized) message to be transmitted. 
    #  @param tx_sig    Descriptive of transmitted signal (power, gain, vector, freq, as needed)
    #  @param time       The time at which transmission begins.
    def free_space_transmit(self, sndr_ID, dest_ID, payload, tx_sig):
        
        
        # If it's a satID, get the corresponding reference; if not, get the GS; if neither, must be GSNet (which should fail on assertion below)
        sndr = self.pointer_to_simulation.sats_by_id.get(sndr_ID, self.pointer_to_simulation.gs_by_id.get(sndr_ID, self.pointer_to_simulation.gs_network))
        dest = self.pointer_to_simulation.sats_by_id.get(dest_ID, self.pointer_to_simulation.gs_by_id.get(dest_ID, self.pointer_to_simulation.gs_network))
        
        #mjdt = tt.datetime2mjd(self.pointer_to_simulation.CurGlobalTime) # BROKEN: sometimes when unpickling, CurGlobalTime does not update
        mjdt = tt.datetime2mjd(sndr._curr_time_dt)

        assert not (AgentType.GSNET in [sndr.type, dest.type])  # New paradigm: GSNet shouldn't use this directly anymore (model policy, so assertion)

        # 1 - Check if access is valid (geometry)  check_access_available(
        if not self.bb_en:
            if not self.pointer_to_simulation.access_truth_stn.check_access_available(sndr_ID, dest_ID, mjdt):
                print("Access unavailable.")
                # add to failed_dictionary to track failure stats by type (if we are executing an activity at all)
                if sndr.exec._curr_exec_acts:
                    sndr.state_recorder.anamoly_dict['Invalid geometry at transmission time'].add(sndr.exec._curr_exec_acts[0].wind)
                return None, False
                

        # 2 - Calculate attenuation to determine signal seen by receiver (if enabled)
        if self.atten_en:
            raise NotImplementedError

        # 3 - Apply stochastic effect (if enabled)
        if self.stoch_en:
            raise NotImplementedError

        # 4 - Hand message to destination agent - Should ultimately hand to a Radio
        return dest.fst.receive(sndr_ID, payload, None)  #TODO call receive on MATCHING receiver (by freq, etc)
        # 5 - Dest. Agent will ACK or not; pass this back to Sender 


    ## Hardnet Transmit is called by two ground-based agents using direct connections.
    #
    #  The modeling of a ground-based network is out of scope for the current
    #  SPRINT work.  It _could_ be modeled with stochastic delays and failures, 
    #  buffering and synchronization behavior.  However, SPRINT considers the
    #  ground network monolithic due to the ease and timescale at which transmissions
    #  occur over internet-type networks.  A concerned user could expand this
    #  functionality with timed queues, etc to implement non-monolithic sim.
    #  TODO - consider whether modeling downtime of GSs is concerned with this.
    #  @param self      The object pointer.
    #  @param sndr      Reference to the sending agent
    #  @param dest      Reference to the destination agent
    #  @param payload   The encoded (serialized) message to be transmitted.    
    def hardnet_transmit(self, sndr_ID, dest_ID, payload):

        sndr = self.pointer_to_simulation.sats_by_id.get(sndr_ID, self.pointer_to_simulation.gs_by_id.get(sndr_ID, self.pointer_to_simulation.gs_network))
        dest = self.pointer_to_simulation.sats_by_id.get(dest_ID, self.pointer_to_simulation.gs_by_id.get(dest_ID, self.pointer_to_simulation.gs_network))

        if( AgentType.SAT in [sndr.type, dest.type]):  # A value error b/c it violates a real constraint, not policy 
            raise ValueError("{}:{} or {}:{} is of type Agent.SAT. Use free_space_transmit, not hardnet_transmit.".format(sndr.ID, sndr.type, dest.ID, dest.type))
        # Hand message to destination agent - Should ultimately hand it to a NIC

        return dest.receive_planning_info(sndr_ID, pkl.loads(payload), None)  # TODO - put pass-through NIC (similarly to FST) instead of decoding here
