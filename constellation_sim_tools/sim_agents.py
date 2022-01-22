#  Contains objects  that are used in simulation loop to represent high-level agents within simulation, including satellites, ground stations, and the ground station network
#
# @author Kit Kennedy

from datetime import datetime, timedelta
import pickle as pkl

import circinus_tools.metrics.metrics_utils as met_util
import circinus_tools.time_tools as tt
from .sim_agent_components import DataStore, ExecutiveAgentStateRecorder
from .sim_sat_components import SatScheduleArbiter,SatExecutive,SatStateSimulator,SatStateRecorder
from .sim_gsnetwork_components import GroundNetworkPS,GroundNetworkStateRecorder
from .sim_gs_components import GSStateSimulator,GSSchedulePassThru,GSExecutive
from .FreeSpaceTransceiver import FreeSpaceTransceiver
from sprint_tools.Sprint_Types import AgentType
from threading import RLock
import queue
SAT_STATE_JSON_VER = '0.1'

class SimAgent:
    """Super class for simulation agents within constellation"""

    ## Construct Generalized portion of a component sim
    def __init__(self,ID,agType,sim_start_dt,sim_end_dt,simulation,schedule_disruptions = None,removed = False):

        self.ID = ID
        assert(agType in AgentType)
        self.type = agType # AgentType.GS/SAT/GSNET

        self.lock = RLock()
        # TODO - extract params from config
        # TODO - PRI - move to at least 2 units (one lasercomm, one low rate radio)
        # TODO - abstract to arbitrary number of radios
        # TODO - make radios correspond w/ asym freqs
        transceiver_parameters = {  # Should come from config
                'gain'      : None,
                'power'     : None,
                'freq_TX'   : '402e6',
                'freq_RX'   : '402e6',
                'plex'      : 'DU'
            }

        self.fst = FreeSpaceTransceiver(transceiver_parameters, simulation, self)
        self.simulation = simulation
        # current time for the agent. note that individual components within the agent store their own times
        self._curr_time_dt = sim_start_dt
        self.sim_start_dt = sim_start_dt
        self.sim_end_dt = sim_end_dt
        self.GlobalSim = simulation  # This points to the globally maintained simulator
        if schedule_disruptions:
            self.schedule_disruptions = schedule_disruptions if ID in schedule_disruptions.keys() else None
        else:
            self.schedule_disruptions = schedule_disruptions
        
        self.removed = removed
        self.no_more_bdt = False

    def is_removed(self):
        return self.removed
        
    def get_plan_db(self):
        """ get planning info database used by this agent """
        #  intended to be a virtual method
        raise NotImplementedError
    
    def receive_message(self,msg:dict):
        """ Receives incoming message """

        with self.lock:
            sender_ID = msg['sender']
            return self.fst.receive(sender_ID, msg, None)

    def make_planning_message(self,dest_ID:str,info_option='all',exchangeRequired:bool=False,
                              new_time=None,handle_lp:bool=False,plan_prop:bool=False,waitForReply = False):
        """
        Make PLAN message
        :param dest_ID: ID of destination
        :param info_option:
        :param exchangeRequired: True if requires PLAN response from destination
        :param new_time:
        :param handle_lp:
        :param plan_prop: True if message is part of larger PLAN propagation
        :param waitForReply: True if sender should wait until reply is received
        :return: The PLAN message
        """
        sndr_plan_db = self.get_plan_db()

        # Collect the information per agent for transmission
        last_state_hists_by_ID = {}
        for key, value in sndr_plan_db.sat_state_hist_by_id.items():
            last_state_hists_by_ID[key] = value[-1]

        sndr_last_ttc_hist_by_agent_id = {}
        for key, value in sndr_plan_db.ttc_update_hist_by_agent_id.items():
            sndr_last_ttc_hist_by_agent_id[key] = value.last_update_time[-1]

        # Build a message here which can be plainly serialized and deserialized for transmission
        plan_msg = {
            'req_type':'PLAN',  # tell the receiver to route this to the plan ingestion function
            'payload': {
                        # Required Plan info:
                        'sndr_SRC_vals' :                           list(sndr_plan_db.sim_rt_conts_by_id.values()),
                        'last_state_hists_by_ID' :                  last_state_hists_by_ID,
                        'sndr_last_ttc_hist_by_agent_id':           sndr_last_ttc_hist_by_agent_id,
                        '_curr_time_dt':                            self._curr_time_dt,
                        'info_option':                              info_option,
                        'new_time':                                 new_time if new_time else self._curr_time_dt,

                        },
            'sender':self.ID,
            'dest':dest_ID,
            'exchange':exchangeRequired,
            'waitForReply': waitForReply,
            'handle_lp': handle_lp,
            'plan_prop': plan_prop
        }

        if handle_lp:
            plan_msg['payload']['schedule_disruption_replan_communicated'] = \
                self.arbiter.schedule_disruption_replan_communicated
            plan_msg['payload']['sat_windows_dict'] = self.arbiter.plan_db.sat_windows_dict

        return plan_msg

    def send_planning_info(self,dest_ID:str,info_option:str='all',exchangeRequired:bool=False,new_time = None,
                           handle_lp:bool=False,plan_prop:bool= False, waitForReply : bool = False):
        """
         TODO - store plan in msg instead of having the plan_db call each other
         TODO - this function should be in a transmitter sim module

        Send planning message over transmission medium from cur agent to dest agent
        :param dest_ID:          ID of the destination
        :param info_option:
        :param exchangeRequired: True if requires PLAN as response, False otherwise
        :param new_time:
        :param handle_lp:        True if being sent as part of downlink
        :param plan_prop:        True if being sent as part of PLAN propagation
        """
        if not info_option in ['all','ttc_only','routes_only']:
            raise RuntimeWarning('unknown info sharing option: %s'%(info_option))

        self.lock.acquire()

        plan_msg = self.make_planning_message(dest_ID,info_option=info_option,
                                              exchangeRequired = exchangeRequired,new_time = new_time,
                                              handle_lp = handle_lp,plan_prop = plan_prop, waitForReply = waitForReply)

        if self.type in {AgentType.SAT}:
            plan_msg['payload']['sat_windows_dict'] = self.arbiter.plan_db.sat_windows_dict
            plan_msg['payload']['schedule_disruption_replan_communicated'] = \
                self.arbiter.schedule_disruption_replan_communicated


        if AgentType.SAT in [self.type, self.simulation.getAgentType(dest_ID)]:
            self.lock.release()
            result = self.fst.transmit(dest_ID,plan_msg)

            if not self.is_removed():
                if exchangeRequired: success = result
                else: _, success = result
                return success

        else: # must be GSNet<->GS

            success = self.GlobalSim.Transmission_Simulator.hardnet_transmit(self.ID,dest_ID,pkl.dumps(plan_msg))
            self.lock.release()
            return success

    def receive_planning_info(self, sndr_ID:str, payload:dict, rx_sig):
        """
        Accept a signal and message from Transmission medium

         Reversing paradigm above to focus on "receiver"
         The same objects are used, it's just reversed as to how
         it actually takes effect.  Some params embedded in message.

        :param sndr_ID: The ID of the sender
        :param payload: Arbitrary info/message holder.  Passes params, ultimately the actual plan as well.
        :param rx_sig:  Descriptive of received signal (power per area at rec)
        :return:
        """

        assert (sndr_ID != self.ID), f"{sndr_ID} is not self's ID {self.ID}"  # At this moment, no self-sending
    
        # Parse message components into constituent pieces
        self.get_plan_db().ingest_planning_info(
                payload['sndr_SRC_vals'],
                payload['last_state_hists_by_ID'],
                payload['sndr_last_ttc_hist_by_agent_id'],
                payload['_curr_time_dt'],
                payload['info_option']
            )

        self.post_planning_info_rx(payload["info_option"],payload['_curr_time_dt'],sndr_ID)

        return True


    def post_planning_info_rx(self,info_option,time,sndr_ID):
        """ perform any actions required after receiving new planning information"""
        # intended to be implemented in subclass
        raise NotImplementedError


class SimExecutiveAgent(SimAgent):
    """ superclass for agents that are capable of executing plans in the simulation (e.g. satellites, ground stations)"""

    def __init__(self,ID,agType,sim_start_dt,sim_end_dt,simulation,schedule_disruptions=None, removed = False):
        super().__init__(ID,agType,sim_start_dt,sim_end_dt,simulation,schedule_disruptions, removed)

        # to be created in subclass
        self.state_sim = None

    def execution_step(self,new_time_dt):
        self.exec.execute_acts(new_time_dt)

    def get_exec(self):
        return self.exec

    def get_act_hist(self):
        return self.state_recorder.get_act_hist()

    def get_DS_hist(self):
        return self.state_recorder.get_DS_hist()

    def get_curr_data_conts(self):

        return self.state_sim.get_curr_data_conts()


class SimSatellite(SimExecutiveAgent):
    """class for simulation satellites"""
    
    def __init__(self,ID,sat_indx,sim_start_dt,sim_end_dt,sat_scenario_params,sim_satellite_params,act_timing_helper,
                 simulation, sat_stn,schedule_disruptions:bool=None,removed:bool = False):
        """initializes based on parameters

        :param simulation: A ConstellationSim reference if not removed, else a RemovedSatellite reference if removed
        :param removed:    True if simulating satellite running on a separate device, False otherwise
        """

        super().__init__(ID,AgentType.SAT,sim_start_dt,sim_end_dt,simulation,schedule_disruptions, removed)

        #  the satellite index. this is used for indexing in internal data structures
        self.sat_indx = sat_indx

        #  list of all simulation satellites (needed for inter-satellite transactions).  to be set after initialization.
        self.all_sim_sats = None
        
        #  list of all simulation ground stations (needed for  down links).  to be set after initialization.
        self.all_sim_gs = None

        self.local_stn_knowledge = sat_stn  # TODO - change this from a link to global to actual local (partial, imperfect)

        #  internal satellite simulation objects
        self.state_sim = SatStateSimulator(self,
            sim_start_dt,
            sim_satellite_params['state_simulator'],
            sat_scenario_params['power_params'],
            sat_scenario_params['data_storage_params'],
            sat_scenario_params['initial_state'],
        )
        self.arbiter = SatScheduleArbiter(self,sim_start_dt,sim_end_dt,sim_satellite_params['sat_schedule_arbiter_params'],act_timing_helper)
        self.exec = SatExecutive(self,sim_start_dt,dv_epsilon=sim_satellite_params['dv_epsilon_Mb'])
        self.state_recorder = SatStateRecorder(sim_start_dt)

        self.prop_reg       = True
        self.prop_cadence   = 600   # s
        self.last_broadcast = datetime(1, 1, 1, 0, 0)     # datetime
        self.last_broadcast_lock = RLock()

        # adds references between sat sim objects
        self.arbiter.state_sim = self.state_sim
        self.state_sim.sat_exec = self.exec
        self.state_sim.state_recorder = self.state_recorder
        self.exec.state_sim = self.state_sim
        self.exec.scheduler = self.arbiter
        self.exec.state_recorder = self.state_recorder

        self.time_epsilon_td = timedelta(seconds = sim_satellite_params['time_epsilon_s'])

        self.stats = {
            'plan_props_succeded': 0,
            'plan_props_attempted': 0
        }

        # BDT params
        self.window_id = 0
        self.bdt_ids_acked = queue.Queue()
        self.bdt_ids_to_dc = {}
        self.bdt_active_dcs = set()
        self.bdt_ids_to_msg_id = {}
        self.bdt_ids_lock = RLock()

    @property
    def sat_id(self):
        return self.ID

    @property
    def index(self):
        return self.sat_indx

    @property
    def lp_agent_id(self):
        """Get the name to use when creating new routes in the LP"""
        return '%s'%(self.ID)

    @property
    def dc_agent_id(self):
        """Get the name to use when creating new data containers in the executive"""
        return '%s_dc'%(self.ID)

    def state_update_step(self,new_time_dt,lp_wrapper):
        """ update the state of the satellite using the new time"""
        if new_time_dt < self._curr_time_dt:
            raise RuntimeWarning('Saw earlier time')

        # note that the order of these update steps is not arbitrary. See their definition file for more information.
        self.state_sim.update(new_time_dt)
        self.arbiter.update(new_time_dt,planner_wrapper=lp_wrapper)
        self.exec.update(new_time_dt)

        self._curr_time_dt = new_time_dt

    def get_sat_from_indx(self,sat_indx:int):
        """
        Gets the satellite reference based on index.
        If this is removed, cannot get sat reference
        """
        if self.removed: raise RuntimeWarning("Satellite is removed. Cannot get other satellite reference.")
        return self.all_sim_sats[sat_indx]

    def get_gs_from_indx(self,gs_indx:int):
        """
        Gets the gs reference based on index.
        If this is removed, cannot get gs reference
        """
        if self.removed: raise RuntimeWarning("Satellite is removed. Cannot get gs reference.")
        return self.all_sim_gs[gs_indx]

    def get_plan_db(self):
        return self.arbiter.get_plan_db()

    def post_planning_info_rx(self,info_option:str,new_time,sender_id:str):
        """
        perform any actions required after receiving new planning information (satellite-specific)
        
        @param info_option      A string, either 'all','routes_only', or 'ttc_only'
        @param new_time         The new time to send planning information
        @param sender_id        The ID of the sender to reply back to
        """

        # only flag a planning info update (and thus cause replanning) if we received updated routes
        if info_option in {'all','routes_only'}:
            self.arbiter.flag_planning_info_rx_external()

    def get_ecl_winds(self):
        return self.get_plan_db().get_ecl_winds(self.sat_id)

    def get_ES_hist(self):
        return self.state_recorder.get_ES_hist()

    def inject_obs(self,obs_list):
        self.exec.inject_obs(obs_list)

    def get_merged_cmd_update_hist(self,gs_agents,gs_id_ignore_list):
        """ gets the merged command update history for this satellite
        
        Gets the update history for every ground station as seen by this satellite,
        and then merges these into a single update history. The update history for
        ground station gs_indx for this satellite  is a recording of when this
        satellite last heard from that ground station. By merging across all ground
        stations, we get a recording of when the satellite last heard from any
        ground station, which we assume is a good proxy for when ground commanding
        was last updated. ( note the underlying assumption that all ground stations
        have equal relevance for commanding the satellite)

        :param gs_agents: The ordered list of Ground Stations if not removed sim, else the ordered
                          list of gs_ids
        :type gs_agents: list
        :param gs_id_ignore_list: The list of gs_ids to ignore
        :type gs_id_ignore_list:  list
        :returns: The merged and updated histograms
        :rtype: {[type]}
        """

        if self.is_removed():
            agent_ids = [gs_id for gs_id in gs_agents if not gs_id in gs_id_ignore_list]
        else:
            agent_ids = [gs_agent.ID for gs_agent in gs_agents if not gs_agent.ID in gs_id_ignore_list]

        update_hists = self.get_plan_db().get_ttc_update_hist_for_agent_ids(agent_ids)

        return met_util.merge_update_histories( update_hists,self.sim_end_dt)


    def plan_prop(self,const_sim,glob_time,en_bb=False):
        """
        Satellite should try to update sats in view if it has new plans to share
        (from others, not necessarily locally unless consensus reached)

        Get a list of satellites we can see but haven't shared the latest known plan with yet;
        list is cleared externally when we receive new info; so if we get info we already know
        from another sat, it doesn't inf-loop.

        :param const_sim: The sim environmemt which called this. If removed satellite, type is
                          RemovedSatellite, else ConstellationSim
        :param glob_time: Time to use for checking STN.
        :param en_bb:     Can restore the original default backbone behavior, but that nixes this whole function
        """

        if not en_bb: # No else; as the GS/GSNetwork will fully propagate if bb assumed

            pot_ss = self.local_stn_knowledge.get_sats_with_cur_access_to(self.ID,
                                                                          glob_time,const_sim.getAllSatIDs())


            sats_to_share_with = [s for s in pot_ss if (s not in self.arbiter.cur_sats_propped_to)]

            if self.is_removed:
                # If removed simulation, send planning information to each
                # but do NOT updated the number of successful plan props yet
                for sat_ID in sats_to_share_with:

                    self.send_planning_info(sat_ID,info_option = 'routes_only',plan_prop=True)
                    self.stats['plan_props_attempted'] += 1
                    self.arbiter.cur_sats_propped_to.append(sat_ID)  # If good response, mark updated

            else:
                for sat_ID in sats_to_share_with:
                    success = self.send_planning_info(sat_ID,info_option='routes_only',plan_prop=True)
                    if success: # share your best known plan with them

                        self.arbiter.sats_propped_to[sat_ID].append(glob_time)
                        self.arbiter.cur_sats_propped_to.append(sat_ID) # If good response, mark updated
                        self.stats['plan_props_succeded'] += 1

                    self.stats['plan_props_attempted'] += 1


    def push_down_L_plan(self,const_sim,time):
        """
        Sends PLAN message to any ground station in sight, if any

        :param const_sim:
        :param time: The current time
        :return: True if successful, false otherwise
        """
        pot_gss = self.local_stn_knowledge.check_groundlink_available(self.ID,time)

        if(len(pot_gss) > 0):
            if self.send_planning_info(pot_gss[0],info_option='routes_only',handle_lp=True):
                with self.lock:
                    # currently, this updates the GroundStationNetwork's planningDB with any new routes
                    self.arbiter.set_external_share_plans_updated(False)
                return True # pushing down L_plan succeeded (now gs_network.plan_db has the new info)
        return False # pushing down L_plan failed

    def lp_has_run(self):
        """
        Returns True if LP has run, else false
        :rtype: bool
        """
        if self.is_removed(): self.lock.acquire()
        updated = self.arbiter.check_external_share_plans_updated()

        if self.is_removed(): self.lock.release()
        return updated

    def add_xlink_failure_info(self,payload:dict):
        """
        Mutates `payload` by adding additional information for xlink executive act failures
        Stores the additional payload information for future use, particularly when performing
        `_cleanup_act_execution_context`

        :param payload: The input payload of a XLINK_FAILURE message
        """

        if self.is_removed(): self.lock.acquire()
        window = payload['window']

        if window.rx_sat == self.index:
            inRXPlan = window in self.exec._scheduled_exec_acts
            storageFull = self.state_sim.DS_state >= self.state_sim.DS_max - self.state_sim.dv_epsilon

            payload['IN_RX_PLAN'] = inRXPlan
            payload['DATA_STORAGE_FULL'] = storageFull

        else:
            inTXPlan = window in self.exec._scheduled_exec_acts
            geometryValid = window not in self.state_recorder.anamoly_dict[
                'Invalid geometry at transmission time']
            hasDataContainer = window not in self.state_recorder.anamoly_dict[
                'No tx data containers associated with route']

            payload['IN_TX_PLAN'] = inTXPlan
            payload['GEOMETRY_VALID'] = geometryValid
            payload['HAS_DATA_CONTAINER'] = hasDataContainer

        self.get_exec().xlink_failure_info.put(pkl.dumps(window),payload)

        if self.is_removed(): self.lock.release()

class SimGroundStation(SimExecutiveAgent):
    """class for simulation ground stations"""
    
    def __init__(self,ID,gs_indx,name,gs_network,sim_start_dt,sim_end_dt,sim_gs_params,act_timing_helper,
                 simulation,schedule_disruptions=None, removed = False):
        """initializes based on parameters
        
        initializes based on parameters
        :type params: dict
        """
        super().__init__(ID,AgentType.GS,sim_start_dt,sim_end_dt,simulation,schedule_disruptions, removed)

        self.gs_indx = gs_indx
        self.name = name
        self.gs_network = gs_network

        #  internal ground station simulation objects
        self.state_sim = GSStateSimulator(self,sim_start_dt)
        self.scheduler_pass_thru = GSSchedulePassThru(self,sim_start_dt,sim_end_dt,act_timing_helper)
        self.exec = GSExecutive(self,sim_start_dt)
        self.state_recorder = ExecutiveAgentStateRecorder(sim_start_dt)

        self.state_sim.state_recorder = self.state_recorder
        self.exec.state_sim = self.state_sim
        self.exec.scheduler = self.scheduler_pass_thru
        self.exec.state_recorder = self.state_recorder

        self.time_epsilon_td = timedelta(seconds = sim_gs_params['time_epsilon_s'])

        # For analysis only
        self.stats = {
            'plan_uplinks_attempted':0,
            'plan_uplinks_succeded':0
        }

    @property
    def gs_id(self):
        return self.ID

    @property
    def index(self):
        return self.gs_indx

    @property
    def dc_agent_id(self):
        """Get the name to use when creating new data containers in the executive"""
        return '%s_dc'%(self.ID)

    def state_update_step(self,new_time_dt):
        """ update the state of the satellite using the new time"""

        if self.is_removed(): self.lock.acquire()

        if new_time_dt < self._curr_time_dt:
            raise RuntimeWarning('Saw earlier time')

        # note that the order of these update steps is not arbitrary. See their definition file for more information.

        self.state_sim.update(new_time_dt)
        self.scheduler_pass_thru.update(new_time_dt,planner_wrapper=None)
        self.exec.update(new_time_dt)

        self._curr_time_dt = new_time_dt

        if self.is_removed():self.lock.release()

    def get_plan_db(self):
        return self.scheduler_pass_thru.get_plan_db()

    def post_planning_info_rx(self,info_option:str,new_time_dt,sndr_ID:str):
        """ perform any actions required after receiving new planning information (satellite-specific)"""
        if info_option in ['all','routes_only']:
            self.scheduler_pass_thru.flag_planning_info_rx_external()

    def get_sat_tlm_update_hist(self,sat):
        """gets the telemetry update history for a single satellite
        
        Gets the update history for a single satellite as seen by this ground station. The update history for satellite sat_indx for this ground station is a recording of when this ground station last heard from that satellite. By merging the update history returned here across satellites, you can get the merged telemetry update history for a single satellite across the full ground station network
        :param sat: The id of the satellite or satellite object
        :type sat:  String or Satellite
        :returns:   The update history for the satellite in this ground station
        :rtype: {[dict]}
        """
        sat_id = sat if type(sat) == str else sat.ID

        return self.get_plan_db().get_ttc_update_hist_for_agent_ids([sat_id])[0]

    def receive_planning_info(self, sndr_ID:str, msg:dict, rx_sig):
        """
        Pass through to GSN (global) if received with keyword from a sat
        :param sndr_ID:     ID of sender
        :param msg:         The unserialized message
        :param rx_sig:
        :return:
        """
        senderType = self.GlobalSim.getAgentType(sndr_ID)
        if senderType == AgentType.SAT:   # if came from a pass, forward onto intro sender
                # ingest TT&C first to update dict
                self.get_plan_db().ingest_agent_ttc(msg['_curr_time_dt'],msg['sndr_last_ttc_hist_by_agent_id'])
                return self.gs_network.receive_planning_info(sndr_ID, msg, rx_sig)
        else:
            return super().receive_planning_info(sndr_ID, msg, rx_sig)


    def plan_prop(self,const_sim,glob_time,en_bb=False):
        """
        Groundstation should try to update sats in view if GS_Net thinks their plans are stale
        GS_Network is directly accessable, as we've assumed the GS network is monolithic.
        Run through a message if that model is unacceptable.
        If the GS->Sat succeeds, exec a Sat->GS for free.

        :param const_sim: The sim environment which called this
                          (so we can get direct access for the send_planning_info shennanigan...)
        :param glob_time: Time to use for checking STN and to mark if a plan is shared.
        :param en_bb:     Can restore the original default backbone behavior, sets accessible sats to all
        :return:
        """
        with self.gs_network.lock:
            # check if there's overlap b/w the non-updated GS's and things we have access to
            staleSats = self.gs_network.get_stale_sats() # stale sats, kept track in the global resource "GS_Network'
            if not en_bb:
                accessable_stale_sats = self.gs_network.global_stn.get_sats_with_cur_access_to(self.ID,glob_time,staleSats)
            else:
                accessable_stale_sats = staleSats

        if self.is_removed():
            #
            with self.lock and self.gs_network.lock:
                for ss_ID in accessable_stale_sats:
                    self.send_planning_info(ss_ID,info_option = 'routes_only',exchangeRequired = True,plan_prop=True)
                    self.stats['plan_uplinks_attempted'] += 1
        else:

            for ss_ID in accessable_stale_sats: # For the list of accessable stale sats

                if self.send_planning_info(ss_ID,info_option='routes_only',exchangeRequired=True):
                    # If successful transmission, update stats and share best known plan
                    self.gs_network.mark_sat_updated(ss_ID, glob_time)   # If good response, mark updated
                    const_sim.sats_by_id[ss_ID].send_planning_info(self.ID,info_option='routes_only')
                    self.stats['plan_uplinks_succeded'] += 1

                self.stats['plan_uplinks_attempted'] += 1


class SimGroundNetwork(SimAgent):
    """class for simulation ground network"""
    
    def __init__(self,ID,name,sim_start_dt,sim_end_dt,num_sats:int,num_gs:int,sim_gs_network_params,
                 act_timing_helper,simulation,sats_by_id,global_stn,schedule_disruptions:bool=None,removed:bool=False):
        """initializes based on parameters
        
        initializes based on parameters
        :type params: dict
        """
        super().__init__(ID,AgentType.GSNET,sim_start_dt,sim_end_dt,simulation,schedule_disruptions,removed)
        

        self.name = name
        self.gs_list = []

        self.scheduler = GroundNetworkPS(self,sim_start_dt,sim_end_dt,sim_gs_network_params['gsn_ps_params'],act_timing_helper,simulation)
        self.state_recorder = GroundNetworkStateRecorder(sim_start_dt,num_sats,num_gs)

        self.scheduler.state_recorder = self.state_recorder

        self.time_epsilon_td = timedelta(seconds = sim_gs_network_params['time_epsilon_s'])

        self.last_updated_by_satID = dict.fromkeys(sats_by_id.keys(),0)

        self.global_stn = global_stn



    def state_update_step(self,new_time_dt,gp_wrapper):
        with self.lock:
            if new_time_dt < self._curr_time_dt:
                raise RuntimeWarning('Saw earlier time')
            self.scheduler.update(new_time_dt,planner_wrapper=gp_wrapper)

            self._curr_time_dt = new_time_dt


    def get_plan_db(self):
        return self.scheduler.get_plan_db()

    def get_all_planned_rt_conts(self):
        return self.scheduler.get_plan_db().get_all_rt_conts()

    def get_all_sats_planned_act_hists(self):
        return self.state_recorder.get_all_sats_planned_act_hists()

    def get_all_gs_planned_act_hists(self):
        return self.state_recorder.get_all_gs_planned_act_hists()

    def post_planning_info_rx(self,info_option,new_time,sndr_id):
        pass


    @property
    def last_known_plan_time(self):
        return tt.datetime2mjd(self.scheduler.last_replan_time_dt)
    
    def mark_sat_updated(self, sat_id, time):
        # TODO - check if time is right type (MJD)
        self.last_updated_by_satID[sat_id] = time


    def get_stale_sats(self):
        """
        Retrieve list of stale sat IDs
        Sats are stale if they haven't been updated since a new plan released

        :return: List of IDs which need updating
        """

        return [s for s in self.last_updated_by_satID.keys() if (self.last_updated_by_satID[s] < self.last_known_plan_time)]
