import logging
from circinus_tools import time_tools as tt
from sprint_tools.Constellation_STN import Constellation_STN
from .sim_agents import AgentType
import pickle as pkl

"""
Simulation component for physical transmission.

 Intended as pass-through between TX and RX components on two different 
 agents.  It will effectively provide routing to the correct agent,
 verify the physics of the transmission, provide for attenuation,
 and ultimately pass through the message.  This is part of the overall
 environmental simulator. Functions provided by this module in this 
 direct-call mode can be replaced by TCP/IP to facilitate agents as separate
 entities (processes, on HITL, etc).
"""

class Transmission_Simulator:


    def __init__(self, en_attn=False, en_stoch=False, simulation=None, removed_sat = False):
        """
        Init T_S with appropriate params  (should be given by simulation config)
        :param en_attn:     Enable attenuation calculations         (default disabled)
        :param en_stoch:    Enable non-deterministic (stochastic)   (default disabled)
        :param simulation:  Reference to host simulation. If removed_sat and host is sat, then simulation is RemovedSatellite type
        :param removed_sat: True if sim involves satellite running on separate device
        """
        # Transmission_Simulator configuration
        self.atten_en    = en_attn
        self.stoch_en    = en_stoch
        self.bb_en       = False     # Turn on independently
        self.removed_sat = removed_sat
        # References to external resources
        self.pointer_to_simulation   = simulation     # NOT OWNED HERE - reference to 

        # Assess the configuration
        if (not self.bb_en) and (not self.pointer_to_simulation.access_truth_stn):
            raise RuntimeError("Either Backbone must be enabled, or STN must \
                be provided.")
        elif self.bb_en and self.pointer_to_simulation.access_truth_stn: # Existence of STN and enabling of backbone conflict
            logging.warn("Backbone enabled but constellation stn provided. \
                Constellation stn will be ignored.")
        

    def setBackbone(self, en_bb):
        """
        Set the backbone to on or not
        :param en_bb: Enable backbone - used for hotstart & control trial

        """
        self.bb_en = en_bb


    def free_space_transmit(self, sndr_ID:str, dest_ID:str, payload:bytes, tx_sig, removed:bool = False):
        """
         Freespace Transmit is called by one agent messaging another over RF/optical.

         The T_S should be able to simulate the entire transmission without
         access to the actual model of either agent, so the actual transmitted
         signal must be fully described, and the start/end points as well.

        :param sndr_ID:  Sending agent ID
        :param dest_ID:  Destination agent ID
        :param payload:  The encoded (serialized) message to be transmitted.
        :param tx_sig:   Descriptive of transmitted signal (power, gain, vector, freq, as needed)
        :param removed:  True if sim involves a removed satellite
        """
        # If it's a satID, get the corresponding reference; if not, get the GS; if neither, must be GSNet
        # (which should fail on assertion below)
        sndr = self.pointer_to_simulation.getReferenceByID(sndr_ID)
        assert AgentType.GSNET != sndr.type
        assert self.pointer_to_simulation.getAgentType(dest_ID) != AgentType.GSNET
        
        mjdt = tt.datetime2mjd(sndr._curr_time_dt)
        # 1 - Check if access is valid (geometry)
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
        
        if self.removed_sat:
            # Send message to dest_ID server
            return self.pointer_to_simulation.send_message(pkl.loads(payload),target_id = dest_ID)
            # return None,True
        else:
            # 4 - Hand message to destination agent - Should ultimately hand to a Radio
            dest = self.pointer_to_simulation.getReferenceByID(dest_ID)
            return dest.fst.receive(sndr_ID, payload, None)
            # 5 - Dest. Agent will ACK or not; pass this back to Sender

    def hardnet_transmit(self, sndr_ID:str, dest_ID:str, payload:bytes):
        """
         TODO - consider whether modeling downtime of GSs is concerned with this.
         Hardnet Transmit is called by two ground-based agents using direct connections.

         The modeling of a ground-based network is out of scope for the current
         SPRINT work.  It _could_ be modeled with stochastic delays and failures,
         buffering and synchronization behavior.  However, SPRINT considers the
         ground network monolithic due to the ease and timescale at which transmissions
         occur over internet-type networks.  A concerned user could expand this
         functionality with timed queues, etc to implement non-monolithic sim.

        :param sndr_ID: Sending agent ID
        :param dest_ID: Destination agent ID
        :param payload: The encoded (serialized) message to be transmitted.
        """

        sndrType = self.pointer_to_simulation.getAgentType(sndr_ID)
        destType = self.pointer_to_simulation.getAgentType(dest_ID)
        if AgentType.SAT in {sndrType,destType}:
            raise ValueError(
                "{}:{} or {}:{} is of type Agent.SAT. Use free_space_transmit, not hardnet_transmit.".format(sndr_ID,
                                                                                                             sndrType,
                                                                                                             dest_ID,
                                                                                                             destType))

        dest = self.pointer_to_simulation.getReferenceByID(dest_ID)

        # Hand message to destination agent - Should ultimately hand it to a NIC
        with dest.lock:
            return dest.receive_planning_info(sndr_ID, pkl.loads(payload)['payload'], None)  # TODO - put pass-through NIC (similarly to FST) instead of decoding here
