#  Contains objects  that are used in simulation loop to represent high-level agents within simulation, including satellites, ground stations, and the ground station network
#
# @author Kit Kennedy

from datetime import datetime, timedelta
from enum import Enum
import numpy as np
import pickle as pkl

import circinus_tools.metrics.metrics_utils as met_util
import circinus_tools.time_tools as tt
from .sim_agent_components import DataStore, ExecutiveAgentStateRecorder
from .sim_sat_components import SatScheduleArbiter,SatExecutive,SatStateSimulator,SatStateRecorder
from .sim_gsnetwork_components import GroundNetworkPS,GroundNetworkStateRecorder
from .sim_gs_components import GSStateSimulator,GSSchedulePassThru,GSExecutive
from .FreeSpaceTransceiver import FreeSpaceTransceiver
from sprint_tools.Sprint_Types import AgentType

SAT_STATE_JSON_VER = '0.1'

class SimAgent:
    """Super class for simulation agents within constellation"""

    ## Construct Generalized portion of a component sim
    def __init__(self,ID,agType,sim_start_dt,sim_end_dt,simulation,schedule_disruptions=None):
        self.ID = ID
        assert(agType in AgentType)
        self.type = agType # AgentType.GS/SAT/GSNET

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

        # current time for the agent. note that individual components within the agent store their own times
        self._curr_time_dt = sim_start_dt
        self.sim_start_dt = sim_start_dt
        self.sim_end_dt = sim_end_dt
        self.GlobalSim = simulation  # This points to the globally maintained simulator
        if schedule_disruptions:
            self.schedule_disruptions = schedule_disruptions if ID in schedule_disruptions.keys() else None
        else:
            self.schedule_disruptions = schedule_disruptions


    def get_plan_db(self):
        """ get planning info database used by this agent """

        #  intended to be a virtual method
        raise NotImplementedError


    ## Send planning message over transmission medium from cur agent to dest agent
    #
    #  TODO - store plan in msg instead of having the plan_db call each other
    #  TODO - this function should be in a transmitter sim module
    #  TODO - Adjust this to take IDs, and let the TX function reference the objects via the Sim
    #  @param self      The SimAgent object of the sender
    #  @param dest      The SimAgent object of the destination
    #  @param info_option  -  TODO: Depricate this
    def send_planning_info(self,dest,info_option='all'):
        if not info_option in ['all','ttc_only','routes_only']:
            raise RuntimeWarning('unknown info sharing option: %s'%(info_option))

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
            'type':'PLAN',  # tell the receiver to route this to the plan ingestion function
            # Required Plan info: 
            'sndr_SRC_vals' :                   list(sndr_plan_db.sim_rt_conts_by_id.values()),
            'last_state_hists_by_ID' :          last_state_hists_by_ID,
            'sndr_last_ttc_hist_by_agent_id':   sndr_last_ttc_hist_by_agent_id,
            '_curr_time_dt':                    self._curr_time_dt,
            'info_option':                      info_option
        }


        if AgentType.SAT in [self.type, dest.type]:
            _, success = self.fst.transmit(dest.ID,plan_msg)
            return success
        else: # must be GSNet<->GS
            return self.GlobalSim.Transmission_Simulator.hardnet_transmit(self.ID,dest.ID,pkl.dumps(plan_msg)) # TODO - put pass-through NIC (similarly to FST) instead of decoding here

    ## Accept a signal & message from Transmission medium
    #
    #  Reversing paradigm above to focus on "receiver"
    #  The same objects are used, it's just reversed as to how
    #  it actually takes effect.  Some params embedded in message.
    #  TODO - store plan in msg instead of having the plan_db call each other
    #  TODO - this function should be in a receiver sim module, which should use rx_sig to determine if and what is received
    #  @param self      The SimAgent object of the destination
    #  @param sender    The SimAgent object of the sender
    #  @param msg       Arbitrary info/message holder.  Passes params, ultimately the actual plan as well.
    #  @param rx_sig    Descriptive of received signal (power per area at rec)
    def receive_planning_info(self, sndr_ID, plan_msg, rx_sig):
        # TODO - I don't like this referring to GSNet if the other's don't work. Should be a firmer check.
        sender = self.GlobalSim.sats_by_id.get(sndr_ID, self.GlobalSim.gs_by_id.get(sndr_ID,self.GlobalSim.gs_network))

        assert (sndr_ID != self.ID)  # At this moment, no self-sending

        # Parse message components into constituent pieces
        self.get_plan_db().ingest_planning_info(
                plan_msg['sndr_SRC_vals'],
                plan_msg['last_state_hists_by_ID'],
                plan_msg['sndr_last_ttc_hist_by_agent_id'],
                plan_msg['_curr_time_dt'],
                plan_msg['info_option']
            )

        self.post_planning_info_rx(plan_msg["info_option"])

        return True # TODO - use rx_sig, etc to do this dynamically


    def post_planning_info_rx(self,info_option):
        """ perform any actions required after receiving new planning information"""
        # intended to be implemented in subclass
        raise NotImplementedError


class SimExecutiveAgent(SimAgent):
    """ superclass for agents that are capable of executing plans in the simulation (e.g. satellites, ground stations)"""

    def __init__(self,ID,agType,sim_start_dt,sim_end_dt,simulation,schedule_disruptions=None):
        super().__init__(ID,agType,sim_start_dt,sim_end_dt,simulation,schedule_disruptions)

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
    
    def __init__(self,ID,sat_indx,sim_start_dt,sim_end_dt,sat_scenario_params,sim_satellite_params,act_timing_helper,simulation, sat_stn,schedule_disruptions=None):
        """initializes based on parameters
        
        initializes based on parameters
        :type params: dict
        """

        super().__init__(ID,AgentType.SAT,sim_start_dt,sim_end_dt,simulation,schedule_disruptions)

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

        # adds references between sat sim objects
        # TODO - if you're passing down a reference to the top level sim (we are), then it should be the single point of access for its elements (no linking needed)
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


    def get_sat_from_indx(self,sat_indx):
        return self.all_sim_sats[sat_indx]

    def get_gs_from_indx(self,gs_indx):
        return self.all_sim_gs[gs_indx]

    def get_plan_db(self):
        return self.arbiter.get_plan_db()

    def post_planning_info_rx(self,info_option):
        """ perform any actions required after receiving new planning information (satellite-specific)"""

        # todo: this terminology has gotten a bit overloaded, an update to it could be helpful
        # only flag a planning info update (and thus cause replanning) if we received updated routes
        if info_option in ['all','routes_only']:
            self.arbiter.flag_planning_info_rx_external()
            

    def get_ecl_winds(self):
        return self.get_plan_db().get_ecl_winds(self.sat_id)

    def get_ES_hist(self):
        return self.state_recorder.get_ES_hist()

    def inject_obs(self,obs_list):
        self.exec.inject_obs(obs_list)

    def get_merged_cmd_update_hist(self,gs_agents,gs_id_ignore_list):
        """ gets the merged command update history for this satellite
        
        Gets the update history for every ground station as seen by this satellite, and then merges these into a single update history. The update history for ground station gs_indx for this satellite  is a recording of when this satellite last heard from that ground station. By merging across all ground stations, we get a recording of when the satellite last heard from any ground station, which we assume is a good proxy for when ground commanding was last updated. ( note the underlying assumption that all ground stations have equal relevance for commanding the satellite)
        :param all_gs_IDs: [description]
        :type all_gs_IDs: [type]
        :param gs_id_ignore_list: [description]
        :type gs_id_ignore_list: [type]
        :returns: [description]
        :rtype: {[type]}
        """

        agent_ids = [gs_agent.ID for gs_agent in gs_agents if not gs_agent.ID in gs_id_ignore_list]

        update_hists = self.get_plan_db().get_ttc_update_hist_for_agent_ids(agent_ids)

        return met_util.merge_update_histories( update_hists,self.sim_end_dt)


    ## Satellite should try to update sats in view if it has new plans to share (from others, not necessarily locally unless consensus reached)
    #
    #  Get a list of satellites we can see but haven't shared the latest known plan with yet; list is cleared externally when we receive new info; so if we get info we already know from another sat, it doesn't inf-loop.
    #  @self                representing this sat
    #  @const_sim           The sim environmemt which called this (so we can get direct access for the send_planning_info shennanigan...)
    #  @param glob_time     Time to use for checking STN.  TODO - should be  locally tracked time on this check; the transmission should be enforced by actual global time (can be trivially synced at state sim step the same unless we want to enforce drivt)
    #  @en_bb               Can restore the original default backbone behavior, but that nixes this whole function
    def plan_prop(self,const_sim,glob_time,en_bb=False):
        if not en_bb: # else: # No else; as the GS/GSNetwork will fully propagate if bb assumed
            pot_ss = self.local_stn_knowledge.get_sats_with_cur_access_to(self.ID,glob_time,const_sim.sats_by_id) # for all sats, but also filter out GSs and Obs -- TODO: Include GSs?

            sats_to_share_with = [s for s in pot_ss if (s not in self.arbiter.cur_sats_propped_to)]

            for sat_ID in sats_to_share_with:                             
                if self.send_planning_info(const_sim.sats_by_id[sat_ID],info_option='routes_only'): # share your best known plan with them
                    self.arbiter.sats_propped_to[sat_ID].append(glob_time)
                    self.arbiter.cur_sats_propped_to.append(sat_ID)                                                  # If good response, mark updated
                    self.stats['plan_props_succeded'] += 1
                    print('Propagating plan: %s -> %s @ %s' % (self.ID, sat_ID,tt.mjd2datetime(glob_time)))
                self.stats['plan_props_attempted'] += 1

    def push_down_L_plan(self,const_sim,time):
        pot_gss = self.local_stn_knowledge.check_groundlink_available(self.ID,time)
        if(len(pot_gss) > 0):
            if self.send_planning_info(const_sim.gs_by_id[pot_gss[0]],info_option='routes_only'):
                # currently, this updates the GroundStationNetwork's planningDB with any new routes
                self.arbiter.set_external_share_plans_updated(False)
                return True # pushing down L_plan succeeded (now gs_network.plan_db has the new info)
        
        return False # pushing down L_plan failed


class SimGroundStation(SimExecutiveAgent):
    """class for simulation ground stations"""
    
    def __init__(self,ID,gs_indx,name,gs_network,sim_start_dt,sim_end_dt,sim_gs_params,act_timing_helper,simulation,schedule_disruptions=None):
        """initializes based on parameters
        
        initializes based on parameters
        :type params: dict
        """
        super().__init__(ID,AgentType.GS,sim_start_dt,sim_end_dt,simulation,schedule_disruptions)

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

        if new_time_dt < self._curr_time_dt:
            raise RuntimeWarning('Saw earlier time')

        # note that the order of these update steps is not arbitrary. See their definition file for more information.
        self.state_sim.update(new_time_dt)
        self.scheduler_pass_thru.update(new_time_dt,planner_wrapper=None)
        self.exec.update(new_time_dt)

        self._curr_time_dt = new_time_dt

    def get_plan_db(self):
        return self.scheduler_pass_thru.get_plan_db()

    def post_planning_info_rx(self,info_option):
        """ perform any actions required after receiving new planning information (satellite-specific)"""
        if info_option in ['all','routes_only']:
            self.scheduler_pass_thru.flag_planning_info_rx_external()

    def get_sat_tlm_update_hist(self,sat_agent):
        """gets the telemetry update history for a single satellite
        
        Gets the update history for a single satellite as seen by this ground station. The update history for satellite sat_indx for this ground station is a recording of when this ground station last heard from that satellite. By merging the update history returned here across satellites, you can get the merged telemetry update history for a single satellite across the full ground station network
        :param sat_indx: [description]
        :type sat_indx: [type]
        :returns: [description]
        :rtype: {[type]}
        """

        return self.get_plan_db().get_ttc_update_hist_for_agent_ids([sat_agent.ID])[0]

    ## Pass through to GSN (global) if received with keyword from a sat
    #
    #
    #
    def receive_planning_info(self, sndr_ID, msg, rx_sig):
        sender = self.GlobalSim.sats_by_id.get(sndr_ID, self.GlobalSim.gs_by_id.get(sndr_ID, self.GlobalSim.gs_network))
        if sender.type is AgentType.SAT:   # if came from a pass, forward onto intro sender
                # injest TT&C first to update dict
                self.get_plan_db().ingest_agent_ttc(msg['_curr_time_dt'],msg['sndr_last_ttc_hist_by_agent_id']) 
                return self.gs_network.receive_planning_info(sndr_ID, msg, rx_sig)
        else:
            return super().receive_planning_info(sndr_ID, msg, rx_sig)


    ## Groundstation should try to update sats in view if GS_Net thinks their plans are stale
    #
    #  GS_Network is directly accessable, as we've assumed the GS network is monolithic. run through a message if that model is unacceptable.
    #  If the GS->Sat succeeds, exec a Sat->GS for free.
    #  @self                Representing this GS
    #  @const_sim           The sim environmemt which called this (so we can get direct access for the send_planning_info shennanigan...)
    #  @param glob_time     Time to use for checking STN and to mark if a plan is shared.  TODO - should be  locally tracked time on this check; the transmission should be enforced by actual global time (can be trivially synced at state sim step the same unless we want to enforce drivt)
    #  @en_bb               Can restore the original default backbone behavior, sets accessible sats to all
    def plan_prop(self,const_sim,glob_time,en_bb=False):
        # check if there's overlap b/w the non-updated GS's and things we have access to
        ss = self.gs_network.get_stale_sats() # stale sats, kept track in the global resource "GS_Network'
        if not en_bb:
            accessable_ss = self.gs_network.global_stn.get_sats_with_cur_access_to(self.ID,glob_time,ss) 
        else: 
            accessable_ss = ss

        for ss_ID in accessable_ss:                                     # For the list of accessable stale sats
            if self.send_planning_info(const_sim.sats_by_id[ss_ID],info_option='routes_only'):                          # share your best known plan with them
                self.gs_network.mark_sat_updated(ss_ID, glob_time)      #    If good response, mark updated
                const_sim.sats_by_id[ss_ID].send_planning_info(self,info_option='routes_only')      # TODO - This is kindof like the GS puppeting the SAT; move this to the sat and have it trigger on an appropriate condition independently
                self.stats['plan_uplinks_succeded'] += 1
            self.stats['plan_uplinks_attempted'] += 1



class SimGroundNetwork(SimAgent):
    """class for simulation ground network"""
    
    def __init__(self,ID,name,sim_start_dt,sim_end_dt,num_sats,num_gs,sim_gs_network_params,act_timing_helper,simulation,sats_by_id,global_stn,schedule_disruptions=None):
        """initializes based on parameters
        
        initializes based on parameters
        :type params: dict
        """
        super().__init__(ID,AgentType.GSNET,sim_start_dt,sim_end_dt,simulation,schedule_disruptions)
        

        self.name = name
        self.gs_list = []

        self.scheduler = GroundNetworkPS(self,sim_start_dt,sim_end_dt,sim_gs_network_params['gsn_ps_params'],act_timing_helper,simulation)
        self.state_recorder = GroundNetworkStateRecorder(sim_start_dt,num_sats,num_gs)

        self.scheduler.state_recorder = self.state_recorder

        self.time_epsilon_td = timedelta(seconds = sim_gs_network_params['time_epsilon_s'])

        self.last_updated_by_satID = dict.fromkeys(sats_by_id.keys(),0)

        self.global_stn = global_stn



    def state_update_step(self,new_time_dt,gp_wrapper):
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

    def post_planning_info_rx(self,info_option):
        pass


    @property
    def last_known_plan_time(self):
        return tt.datetime2mjd(self.scheduler.last_replan_time_dt)
    
    def mark_sat_updated(self, sat_id, time):
        # TODO - check if time is right type (MJD)
        self.last_updated_by_satID[sat_id] = time


    ## Retrieve list of stale sat IDs
    #
    #  Sats are stale if they haven't been updated since a new plan released
    #  @param  self Sim Ground Network which has access to this update log
    #  @return _    List of IDs which need updating
    def get_stale_sats(self):
        return [s for s in self.last_updated_by_satID.keys() if (self.last_updated_by_satID[s] < self.last_known_plan_time)]
