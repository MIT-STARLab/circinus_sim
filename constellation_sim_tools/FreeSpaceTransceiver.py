import pickle as pkl

"""
Transceiver model for both optics AND radios (FST)

 Only difference is the parameters right now. If that becomes
 different, maybe abstract the TX/RX functions to a Radio and LaserComm subbclasses
 The pipeline: Agent_1 > FST1_TX > T_S > FST2_RX > Agent_2
"""

class FreeSpaceTransceiver:

    def __init__(self,transceiver_parameters,simulation,host_ref):
        """
        Constructor for FST
        :param transceiver_parameters: Parameters of the model, including 'gain', 'power', 'freq_TX',
                                       'freq_RX', 'plex' ('DU', 'sim', or 'tx_', or 'rx_')
        :param simulation:             Pointer the the Top level sim in which everything lives
        :param host_ref:               Pointer to the Agent to which this belongs
        """
        self.gain    = transceiver_parameters['gain']
        self.power   = transceiver_parameters['power']
        self.freq_TX = transceiver_parameters['freq_TX']
        self.freq_RX = transceiver_parameters['freq_RX']
        self.plex    = transceiver_parameters['plex']      

        if self.plex!='DU':
            raise NotImplementedError

        # Not so much related to the function, but we need to have it to transmit
        self.pointer_to_simulation = simulation
        self.host_ref = host_ref

    def transmit(self, dest_ID:str, payload:dict, options=None):
        """
        generic transmit function

        :param dest_ID: destination host ID, be it sat or GS
        :param payload: The message, and any details, including type of message & size
        :param options: Optional, can dictate no response required, or reduced power,
                        persistant attempts
        :return:
        """
        return self.pointer_to_simulation.Transmission_Simulator.free_space_transmit(
                self.host_ref.ID,  # sndr_ID
                dest_ID, 
                pkl.dumps(payload),
                None # Add TX power sim to this
            )

    def receive(self, sndr_ID:str, message:dict, signal):
        """
        Generic receive function

        :param sndr_ID: ID of the host, be it sat or GS
        :param message: The message, and any details, including type of message & size
        :param signal:  All the details of the received signal (power, freq, noise?)
        :return:
        """
        # TODO - Verify our schedule SAYS we're in receive mode
        # TODO - parse signal for compatability (freq_RX) and strength

        # Parse payload for type & handle it appropriately

        try: # Deserialize message if in bytes
            message = pkl.loads(message)
        except: pass # message already deserialized
            
        msgType = message['req_type']
        payload = message['payload']

        if msgType == 'PLAN':
            return None, self.host_ref.receive_planning_info(sndr_ID, payload, None)

        elif msgType == 'BDT':      # Bulk Data transfer segment
            return self.host_ref.get_exec().receive_poll_unroll(payload)

        elif msgType == 'STATES':   # Agent State Propagation message
            return None,self.host_ref.get_exec().state_x_rec(payload)
        else:
            raise NotImplementedError
