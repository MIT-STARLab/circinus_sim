import pickle as pkl

## Transceiver model for both optics AND radios (FST)
#
#  Only difference is the parameters right now. If that becomes
#  different, maybe abstract the TX/RX functions to a Radio and LaserComm subbclasses
#  The pipeline: Agent_1 > FST1_TX > T_S > FST2_RX > Agent_2
class FreeSpaceTransceiver:

    ## Constructor for FST
    #
    #  @param transceiver_parameters    Parameters of the model, including 'gain', 'power', 'freq_TX', 'freq_RX', 'plex' ('DU', 'sim', or 'tx_', or 'rx_')
    #  @param simulation    Pointer the the Top level sim in which everything lives
    #  @param host_ref      Pointer to the Agent to which this belongs
    def __init__(self,
            transceiver_parameters,
            simulation,
            host_ref,
        ):

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


    ## generic transmit function
    #
    #  @param self      The FST
    #  @param dest_ID   destination host ID, be it sat or GS
    #  @param payload   The message, and any details, including type of message & size
    #  @param options   Optional, can dictate no response required, or reduced power, or persistant attempts
    def transmit(self, dest_ID, payload, options=None):
        # TODO - call free_space_transmit
        # TODO - break free_space_transmit to not require self of sndr
        # TODO - break free_space_transmit to not require time
        return self.pointer_to_simulation.Transmission_Simulator.free_space_transmit(
                self.host_ref.ID,  # sndr_ID
                dest_ID, 
                pkl.dumps(payload),
                None # Add TX power sim to this
            )


    ## Generic receive function
    #  
    #  @param sndr_ID   ID of the host, be it sat or GS
    #  @param payload   The message, and any details, including type of message & size
    #  @param signal    All the details of the received signal (power, freq, noise?)
    def receive(self, sndr_ID, payload, signal):
        # TODO - Verify our schedule SAYS we're in receive mode
        # TODO - parse signal for compatability (freq_RX) and strength
        # Parse payload for type & handle it appropriately
        
        msg = pkl.loads(payload)  # deserialize the payload so we can check the type field - akin to demodularization

        if msg['type'] == 'PLAN':
            res = self.host_ref.receive_planning_info(sndr_ID, msg, None)
            return None, res 
        elif msg['type'] == 'BDT':      # Bulk Data transfer segment
            return self.host_ref.get_exec().receive_poll_unroll(msg)
        elif msg['type'] == 'STATES':   # Agent State Propagation message
            return self.host_ref.get_exec().state_x_rec(msg)
        else:
            raise NotImplementedError
