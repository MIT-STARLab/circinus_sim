from datetime import datetime, timedelta
from collections import OrderedDict
import pickle
import json
import os
from copy import copy, deepcopy

from circinus_tools.scheduling.io_processing import SchedIOProcessor
from circinus_tools.scheduling.custom_window import   ObsWindow,  DlnkWindow, XlnkWindow
from circinus_tools  import time_tools as tt
from circinus_tools.metrics.metrics_calcs import MetricsCalcs
from circinus_tools.plotting import plot_tools as pltl
import circinus_tools.metrics.metrics_utils as met_util
from circinus_tools  import io_tools
from circinus_tools.activity_bespoke_handling import ActivityTimingHelper
from .sim_agents import SimGroundNetwork,SimGroundStation,SimSatellite
from .gp_wrapper import GlobalPlannerWrapper
from .lp_wrapper import LocalPlannerWrapper
from .sim_plotting import SimPlotting

from .Transmission_Simulator import Transmission_Simulator
from sprint_tools.Constellation_STN import Constellation_STN
from sprint_tools.Sprint_Types import AgentType
from circinus_tools import debug_tools
import logging

def print_verbose(string,verbose=False):
    if verbose:
        print(string)

class ConstellationSim:
    """easy interface for running the global planner scheduling algorithm"""

    def __init__(self, sim_params):
        """initializes based on parameters

        initializes based on parameters
        :param sim_params: global namespace parameters created from input files (possibly with some small non-structural modifications to params). The name spaces here should trace up all the way to the input files.
        :type params: dict
        """

        self.params = sim_params
        # self.scenario_params = self.params['orbit_prop_params']['scenario_params']
        self.GPhotstart = self.params['sim_case_config']['sat_schedule_hotstart']
        self.sat_params = self.params['orbit_prop_params']['sat_params']

        orbit_params = self.params['orbit_prop_params']['orbit_params']
        self.gs_params = self.params['orbit_prop_params']['gs_params']
        self.const_sim_inst_params = sim_params['const_sim_inst_params']
        self.sim_run_params = sim_params['const_sim_inst_params']['sim_run_params']
        self.sim_run_perturbations = sim_params['const_sim_inst_params']['sim_run_perturbations']
        self.num_sats=self.sat_params['num_sats']
        self.sat_id_order = self.sat_params['sat_id_order']
        self.gs_id_order = self.gs_params['gs_id_order']
        self.obs_target_id_order = self.params['orbit_prop_params']['obs_params']['obs_target_id_order']
        self.num_gs = len(self.gs_params['gs_id_order'])

        self.restore_pickle_cmdline_arg = sim_params['restore_pickle_cmdline_arg']

        self.gs_id_ignore_list= self.params['gp_general_params']['other_params']['gs_id_ignore_list']

        self.sim_tick = timedelta(seconds=self.sim_run_params['sim_tick_s'])

        self.sim_start_dt = self.sim_run_params['start_utc_dt']
        self.sim_end_dt = self.sim_run_params['end_utc_dt']

        self.io_proc =SchedIOProcessor(self.params)
        self.sim_plotter = SimPlotting(self.params)

        # we create a gp wrapper here, because:
        # 1. it's easy to give it access to sim params right now
        # 2. we want to store it in the constellation sim context, as opposed to within the gs network. It stores a lot of input data (e.g. accesses, data rates inputs...) and we don't want to be pickling/unpickling all that stuff every time we make a checkpoint in the sim. Note that the gp_wrapper does not internally track any constellation state, on purpose
        self.gp_wrapper = GlobalPlannerWrapper(self.params)

        # metrics calculation
        self.mc = MetricsCalcs(self.get_metrics_params())

        # Also create local planner wrapper. it will store inputs that are common across all satellites. the instance parameters passed to it should be satellite-specific
        self.lp_wrapper = LocalPlannerWrapper(self.params,self.mc)

        self.act_timing_helper = ActivityTimingHelper(self.sat_params['activity_params'],orbit_params['sat_ids_by_orbit_name'],self.sat_params['sat_id_order'],None) 


        ##### Simulation State Truth #####
        stn_params = {
            'element_id_by_index': {
                'sat_id_by_indx'    : sim_params['orbit_prop_params']['sat_params']['sat_id_order'],
                'gs_id_by_indx'     : sim_params['orbit_prop_params']['gs_params' ]['gs_id_order'],
                'trget_id_by_indx'  : sim_params['orbit_prop_params']['obs_params']['obs_target_id_order']
            },
            'accesses_data' : sim_params['orbit_prop_params']['orbit_prop_data']['accesses_data']
        }
        self.access_truth_stn = Constellation_STN(stn_params) 
        self.schedule_disruptions = self.params['sim_case_config']['sim_run_perturbations']['schedule_disruptions']

        #####  End Sim State Truth   #####

        # TODO: make sure schedule disruptions also affect the re-planning transmission simulator
        self.Transmission_Simulator = Transmission_Simulator(False,False, self)

        self.CurGlobalTime = 0  # essentially not init

        self.init_data_structs()

        # set up for pickling, expect to save frequent checkpoints
        output_path = self.params['output_path']
        if not os.path.exists(output_path+'pickles'):       # /outputs/pickles
            if not os.path.exists(output_path):             # /outputs
                os.mkdir(output_path)
            os.mkdir(output_path+'pickles')


    def init_data_structs(self):
        """ initialize data structures used in the simulation """

        # dirty hack! I want to prevent these eclipse windows from sharing IDs with any windows returned from GP, so making them all negative. 
        # todo: this is not a long term solution. Need to incorporate mechanism to share window ID namespace across const sim, GP
        window_uid = -9999
        # ecl_winds is an array with index for each sat_indx
        ecl_winds, window_uid =self.io_proc.import_eclipse_winds(window_uid)
        if window_uid >= 0:
            raise RuntimeWarning('Saw positive window ID for ecl window hack')

        ecl_winds_by_sat_id = {self.sat_id_order[sat_indx]:ecl_winds[sat_indx] for sat_indx in range(self.num_sats)}
        self.ecl_winds = ecl_winds

        gsn_id = 'gsn'

        #  note: use sim tick as resource delta T.
        plan_db_inputs = {
            "sat_id_order": self.sat_id_order,
            "gs_id_order" : self.gs_id_order,
            "other_agent_ids": [gsn_id],
            "initial_state_by_sat_id": self.sat_params['initial_state_by_sat_id'],
            "ecl_winds_by_sat_id": ecl_winds_by_sat_id,
            "power_params_by_sat_id": self.sat_params['power_params_by_sat_id'],
            "resource_delta_t_s": self.sim_run_params['sim_tick_s']
        }

        # create sats
        sats_by_id = {}
        all_sats = []
        for sat_indx,sat_id in enumerate(self.sat_id_order):
            # these params come from orbit prop inputs file
            sat_id_scenario_params = {
                "power_params": self.sat_params['power_params_by_sat_id'][sat_id],
                "data_storage_params": self.sat_params['data_storage_params_by_sat_id'][sat_id],
                "initial_state": self.sat_params['initial_state_by_sat_id'][sat_id],
                "activity_params": self.sat_params['activity_params']
            }

            # assuming same for every sat.
            sat_id_sim_satellite_params = self.const_sim_inst_params['sim_satellite_params']

            sat = SimSatellite(
                sat_id,
                sat_indx,
                self.sim_start_dt,
                self.sim_end_dt,
                sat_id_scenario_params,
                sat_id_sim_satellite_params,
                self.act_timing_helper,
                self, 
                self.access_truth_stn
            )
            sats_by_id[sat_id] = sat
            all_sats.append(sat)

            #  initialize the planning info database
            sat.get_plan_db().initialize(plan_db_inputs)


        self.sats_by_id = sats_by_id


        # create ground network
        gs_by_id = {}
        all_gs = []
        gs_network = SimGroundNetwork(
            gsn_id,
            self.gs_params['gs_network_name'],
            self.sim_start_dt,
            self.sim_end_dt,
            self.num_sats,
            self.num_gs,
            self.const_sim_inst_params['sim_gs_network_params'],
            self.act_timing_helper,
            self, 
            self.sats_by_id,
            self.access_truth_stn   # Only in this deterministic case where it is known. Else use a filter of, etc.
        ) 
        for station in self.gs_params['stations']:
            gs = SimGroundStation(
                str(station['id']),
                self.gs_id_order.index(str(station['id'])),
                station['name'], 
                gs_network,
                self.sim_start_dt,
                self.sim_end_dt,
                self.const_sim_inst_params['sim_gs_params'],
                self.act_timing_helper,
                self, 
                self.schedule_disruptions
            )
            gs_by_id[station['id']] = gs
            gs_network.gs_list.append(gs)
            all_gs.append(gs)

            #  initialize the planning info database
            gs.get_plan_db().initialize(plan_db_inputs)
            
        #  initialize the planning info database
        gs_network.get_plan_db().initialize(plan_db_inputs)
        self.gs_network = gs_network
        self.gs_by_id = gs_by_id




        #  set the simulation satellites list for every satellite
        for sat in all_sats:
            sat.all_sim_sats = all_sats
            sat.all_sim_gs = all_gs

        self.inject_obs()


    def inject_obs(self):

        inj_obs_raw = self.sim_run_perturbations['injected_observations']

        if not self.sim_run_perturbations['do_inject_obs']:
            return

        inj_obs_by_sat_id = {}
        for obs_raw in inj_obs_raw:
            if not obs_raw['type'] == 'hardcoded':
                raise NotImplementedError

            sat_id = obs_raw['sat_id']
            obs = ObsWindow(
                obs_raw['indx'], 
                sat_indx= self.sat_id_order.index(sat_id),
                target_IDs=['inject_'+str(obs_raw['indx'])], 
                sat_target_indx=0, 
                start= tt.iso_string_to_dt (obs_raw['start_utc']), 
                end= tt.iso_string_to_dt (obs_raw['end_utc']), 
                wind_obj_type='injected'
            )

            # obs.set_data_vol(self.sat_params['pl_data_rate'])
            # pretty hardcore hacky here, but things seem to do badly when injected obs have huge dv. Try it this way
            obs.data_vol = 300
            obs.original_data_vol = 300

            inj_obs_by_sat_id.setdefault(sat_id, []).append(obs)
        
        for sat_id in self.sat_id_order:
            self.sats_by_id[sat_id].inject_obs(inj_obs_by_sat_id.get(sat_id,[]))


    def getReferenceByID(self,id):
        if id in self.sats_by_id: return self.sats_by_id[id]
        else: return self.gs_by_id.get(id,self.gs_network)
        
    def getAgentType(self,id):
        if id in self.gs_by_id.keys(): return AgentType.GS
        elif id in self.sat_id_order: return AgentType.SAT
        return AgentType.GSNET
        
    def getAllSatIDs(self):
        """
        Gets all satellite ids, ordered by index
        """
        return self.sat_id_order
        
    def getAllGSIDs(self):
        """
        Gets all gs ids, ordered by index
        """
        return self.gs_id_order

    @staticmethod
    def pickle_checkpoint(output_path, global_time,gs_network,sats_by_id,gs_by_id):
        if gs_network.scheduler.outsource:
            return # incompatible with socket

        pickle_name ='pickles/sim_checkpoint_%s' %(global_time.isoformat().replace (':','_').replace ('-','_'))
        with open(output_path + '%s.pkl'%( pickle_name),'wb') as f:
            pickle.dump( {"global_time":global_time, "gs_network": gs_network, "sats_by_id":sats_by_id, "gs_by_id":gs_by_id},f)

    @staticmethod
    def unpickle_checkpoint(output_path, pickle_name):
        pickle_pathname = output_path+'pickles/%s'%pickle_name
        p = pickle.load (open ( pickle_pathname,'rb'))
        return p['global_time'],p['gs_network'],p['sats_by_id'],p['gs_by_id']

    def run(self):
        """ run the simulation """
        logging.basicConfig(filename=(self.params['output_path']+'logs/sim_sats.log'),level=logging.DEBUG)

        verbose = True  # TODO - move to sim config

        global_time             = self.sim_start_dt
        sim_end_dt              = self.sim_end_dt
        last_checkpoint_time    = global_time

        #  used to alert special operations on first iteration of the loop
        first_iter = True

        # unpickle from a checkpoint if so desired
        if self.sim_run_params['restore_from_checkpoint'] or self.restore_pickle_cmdline_arg != "":
            # get pickle name from cmdline if it was provided
            pickle_name = self.restore_pickle_cmdline_arg if self.restore_pickle_cmdline_arg != "" else self.sim_run_params['restore_pkl_name']

            global_time, self.gs_network, self.sats_by_id, self.gs_by_id = self.unpickle_checkpoint(self.params['output_path'], pickle_name)

            # reset Transmission simulator pointer to the current sim, not the previous sim (doesn't tick global time sometimes)
            self.Transmission_Simulator.pointer_to_simulation = self
            first_iter= False
            assert(global_time != self.sim_start_dt)  # to make sure it actually isn't the first iteration
            print_verbose('Unpickled checkpoint file %s'%(pickle_name),verbose)

        if first_iter:
            # Change from building all potential activity windows over the entire sim every time the GP is ran to instead build them once at the start of sim 
            # and then associate the relevant windows with the relevant planning objects (N knows all windows, each satellite knows windows in which they can participate)
            # First: create the params input structure required by SchedIOProcessor

            if not self.gs_network.scheduler.outsource:
                io_proc = SchedIOProcessor(self.params)

                # Load all windows: .
                print_verbose('Load files',verbose)

                # parse the inputs into activity windows
                window_uid = 0
                print_verbose('Load obs',verbose)
                obs_winds, window_uid =io_proc.import_obs_winds(window_uid)
                print_verbose('Load dlnks',verbose)
                dlnk_winds, dlnk_winds_flat, window_uid =io_proc.import_dlnk_winds(window_uid)
                print_verbose('Load xlnks',verbose)
                xlnk_winds, xlnk_winds_flat, window_uid =io_proc.import_xlnk_winds(window_uid)

                # if crosslinks are not allowed to be used (in sim_case_config), then zero out all crosslink windows
                # NOTE: this is done here and not further upstream because the access windows are used for calculating planning comms windows as well
                if not self.params['sim_case_config']['use_crosslinks']:
                    xlnk_winds = [[[]] * self.num_sats] * self.num_sats
                    xlnk_winds_flat = [[]] * self.num_sats # list of N_sats empty lists

                # note: this import is currently done independently from circinus constellation sim. If we ever need to share knowledge about ecl winds between the two, will need to make ecl winds an input from const sim
                print_verbose('Load ecl',verbose)
                ecl_winds, window_uid =io_proc.import_eclipse_winds(window_uid)

                # Note, they are only validated inside the GP (other_helper not imported here)
                """ print_verbose('Validate windows',verbose)
                other_helper.validate_unique_windows(self,obs_winds,dlnk_winds_flat,xlnk_winds,ecl_winds) """

                print_verbose('In windows loaded from file:',verbose)
                print_verbose('obs_winds',verbose)
                print_verbose(sum([len(p) for p in obs_winds]),verbose)
                print_verbose('dlnk_win',verbose)
                print_verbose(sum([len(p) for p in dlnk_winds]),verbose)
                print_verbose('xlnk_win',verbose)
                print_verbose(sum([len(xlnk_winds[i][j]) for i in  range( self.sat_params['num_sats']) for j in  range( self.sat_params['num_sats']) ]),verbose)
                # Make all windows knows to the GSN
                # Each entry into this dictionary (except next_window_uid) is a list of length num_sats, where 
                # each index in the outer list corresponds to the satellite index.
                all_windows_dict = {
                    'obs_winds': obs_winds,
                    'dlnk_winds': dlnk_winds,
                    'dlnk_winds_flat': dlnk_winds_flat,
                    'xlnk_winds': xlnk_winds,
                    'xlnk_winds_flat': xlnk_winds_flat,
                    'ecl_winds': ecl_winds,
                    'next_window_uid': window_uid
                }

                # TODO: BACKBONE or ok at start? -- I am adding directly adding to the GSN planner/scheduler, breaking abstraction a bit
                self.gs_network.scheduler.all_windows_dict = all_windows_dict  


            # TODO: BACKBONE or ok at start?-- I am adding directly adding to each satellite's schedule arbiter, breaking abstraction a bi
            # Parse out windows for each satellite, add them to the queue of info to be sent to that satellite
            for sat_idx,sat in enumerate(self.sats_by_id.values()):
                # For immediate testing, just immediate set the information into their plan_db.

                sat_plan_db = sat.get_plan_db()
                sat_plan_db.sat_windows_dict = {}
                for key in self.gs_network.scheduler.all_windows_dict.keys():
                    if key != 'next_window_uid':
                        # this needs to be a deepcopy otherwise when a window is modified on a satellite, 
                        # the gs_network will instantly "know" about it since they are pointing to the same underlying object.
                        sat_plan_db.sat_windows_dict[key] = deepcopy(self.gs_network.scheduler.all_windows_dict[key][sat_idx])
                    else:
                        sat_plan_db.sat_windows_dict[key] = self.gs_network.scheduler.all_windows_dict[key]

        #######################
        # Simulation loop
        #######################

        numLP = 0
        print_verbose('Starting sim loop',verbose)
        second_iter = False
        while global_time < sim_end_dt:
            self.CurGlobalTime = global_time

            #####################
            # Checkpoint pickling
            if self.sim_run_params['pickle_checkpoints']:
                if (global_time - last_checkpoint_time).total_seconds() >= self.sim_run_params['checkpoint_spacing_s']:
                    self.pickle_checkpoint(self.params['output_path'], global_time, self.gs_network, self.sats_by_id, self.gs_by_id)
                    last_checkpoint_time = global_time
                elif second_iter:
                    # checkpoint after first GP run, since that run takes the longest and we can now run with full sim info
                    self.pickle_checkpoint(self.params['output_path'], global_time, self.gs_network, self.sats_by_id, self.gs_by_id)
                    # keep original checkpoint cadence


            #####################
            # Activity execution

            #  execute activities at this time step before updating state to the next time step
            for sat in self.sats_by_id.values():
                sat.execution_step(global_time)

            #  note that ground stations do not currently do anything in the execution step. including for completeness/API coherence
            for gs in self.gs_by_id.values():
                gs.execution_step(global_time)


            #####################
            # State update

            #  run ground network update step so that we can immediately share plans on first iteration of this loop
            self.gs_network.state_update_step(global_time,self.gp_wrapper)
             # start all the satellites with a first round of GP schedules, if so desired
            if first_iter and self.GPhotstart:
                self.Transmission_Simulator.setBackbone(True)       # Pair with set to false below
                self.gsn_exchange_planning_info_all_exec_agents()   # TODO: Change the names to represent this is just to GS
                for gs in self.gs_by_id.values():                   # TODO: Back this into f() for repeat
                    gs.plan_prop(self,tt.datetime2mjd(global_time),True) # True here plans for backbone availability
                # self.gs_network.display_satplan_staleness(tt.datetime2mjd(global_time))

                self.Transmission_Simulator.setBackbone(False)


            # now update satellite and ground station states
            for sat in self.sats_by_id.values():
                sat.state_update_step(global_time,self.lp_wrapper)

            for gs in self.gs_by_id.values():
                gs.state_update_step(global_time)

            # Share local sat info where appropriate: for downlink over time],
            for sat in self.sats_by_id.values():
                if sat.prop_reg and ((global_time-sat.last_broadcast).seconds > sat.prop_cadence):  # Here at some regularity, elsewhere as needed
                    success = sat.get_exec().state_x_prop(tt.datetime2mjd(global_time))       # [state of self, state of others as seen]
                    if success:
                        sat.last_broadcast = global_time



            #####################
            # Planning info sharing

            # whenever GP has run, share info afterwards
            # todo: seems kinda bad to cross levels of abstraction like this...
            if self.gs_network.scheduler.check_external_share_plans_updated():
                self.gsn_exchange_planning_info_all_exec_agents()

            # Groundstations share with sats if appropriate
            for gs in self.gs_by_id.values():
                gs.plan_prop(self,tt.datetime2mjd(global_time))
            # self.gs_network.display_satplan_staleness(tt.datetime2mjd(global_time)) # used in in dev

            # Sats share with other sats if appropriate    
            #(flag under reference_model_definitions/sat_regs/zhou_original_sat.json
            # ["sat_model_definition"]["sim_satellite_params"]["crosslink_new_plans_only_during_BDT"])
            # if this flag is set to false, then anytime there is a potential crosslink access, plans will propagate.
            # if this flag is set to true, then plans will only propagate over existing scheduled bulk data tranfer
            # (BDT) Xlnk activites
            if not self.params['const_sim_inst_params']['sim_satellite_params']['crosslink_new_plans_only_during_BDT']:
                for sat in self.sats_by_id.values():
                    sat.plan_prop(self,tt.datetime2mjd(global_time))

            # if a sat LP has run, send that info to gs network so it can use in planning with GP
            for sat in (s for s in self.sats_by_id.values() if s.arbiter.check_external_share_plans_updated()):
                numLP += 1
                L_plan_flag = sat.push_down_L_plan(self,tt.datetime2mjd(global_time))
                # if this is successful, then the gsn_network should share the new planning info, other keep previous setting
                if L_plan_flag:
                    if not sat.arbiter.schedule_disruption_replan_communicated:
                        # share updated windows_dict with the gsn for that sat (in plan_db)
                        update_keys = ['dlnk_winds','dlnk_winds_flat']
                        for key in update_keys:
                            # need to copy back here so they don't point to the same underlying objects
                            # TODO - COMM FIX: should be done in a message passing format instead
                            self.gs_network.scheduler.all_windows_dict[key][sat.index] = deepcopy(sat.arbiter.plan_db.sat_windows_dict[key])


                        if sat.arbiter.plan_db.sat_windows_dict['next_window_uid'] > self.gs_network.scheduler.all_windows_dict['next_window_uid']:
                            # if the sat_schedule_arbiter has a new max window ID then share that too
                            self.gs_network.scheduler.all_windows_dict['next_window_uid'] = sat.arbiter.plan_db.sat_windows_dict['next_window_uid']

                            for other_sat in self.sats_by_id.items():
                                if other_sat != sat:
                                    other_sat.arbiter.plan_db.sat_windows_dict['next_window_uid'] = \
                                        self.gs_network.scheduler.all_windows_dict['next_window_uid']
                                    

                                
                        # turn off schedule_disruption_flag now that it has been shared
                        sat.arbiter.schedule_disruption_replan_communicated = True
                        
                    
                    self.gsn_exchange_planning_info_all_exec_agents()


            global_time = global_time+self.sim_tick
            second_iter = True if first_iter else False
            first_iter = False

            print("\n===================================================")
            print("======= CURRENT TIME : {} ========".format(global_time))
            print("===================================================\n")
        print("num LP:",numLP)

        #### 
        # end of sim

        print("GS#: Uplinks / Attempts")
        for gs in self.gs_by_id.values():
            print("{}: {} / {}".format(gs.ID, gs.stats['plan_uplinks_succeded'], gs.stats['plan_uplinks_attempted']))

        print("\nSAT#: Plan Props / Attempts")
        for sat in self.sats_by_id.values():
            print("{}: {} / {}".format(sat.ID, sat.stats['plan_props_succeded'], sat.stats['plan_props_attempted']))

        #  save a pickle for the end ฅ^•ﻌ•^ฅ (meow)
        if self.sim_run_params['pickle_checkpoints']:
            self.pickle_checkpoint(self.params['output_path'], global_time, self.gs_network, self.sats_by_id, self.gs_by_id)


    # TODO: Information always shared in same order: acknoledge this by 
    #        adding mechanism to mix it up, or keep it enforce strict sync
    def gsn_exchange_planning_info_all_exec_agents(self):
        #  every time the ground network re-plans, want to send that updated planning information to the ground stations
        for gs in self.gs_by_id.values():
            self.gs_network.send_planning_info(gs.ID,info_option='routes_only')
            gs.send_planning_info(self.gs_network.ID,info_option='routes_only')

        self.gs_network.scheduler.set_external_share_plans_updated(False)

    def post_run(self, output_path):
        
        # get sats and gs in index order
        sats_in_indx_order = [None for sat in range(len(self.sats_by_id))]
        gs_in_indx_order = [None for gs in range(len(self.gs_by_id))]
        for sat in self.sats_by_id.values():
            sats_in_indx_order[sat.sat_indx] = sat
        for gs in self.gs_by_id.values():
            gs_in_indx_order[gs.gs_indx] = gs


        # report events
        event_logs = OrderedDict()
        event_logs['sats'] = OrderedDict()
        event_logs['gs'] = OrderedDict()
        for sat in sats_in_indx_order:
            sat.state_recorder.log_event(self.sim_end_dt,'constellation_sim.py','final_dv',
                                         [str(dc) for dc in sat.state_sim.get_curr_data_conts()])
            event_logs['sats'][sat.ID] = sat.state_recorder.get_events()
        for gs in gs_in_indx_order:
            gs.state_recorder.log_event(self.sim_end_dt,'constellation_sim.py','final_dv',
                                        [str(dc) for dc in gs.state_sim.get_curr_data_conts()])
            event_logs['gs'][gs.ID] = gs.state_recorder.get_events()

        
        if not os.path.exists(output_path+'logs'):          # /outputs/plots
            if not os.path.exists(output_path):             # /outputs
                os.mkdir(output_path)           
            os.mkdir(output_path+'logs')

        event_log_file = output_path+'logs/agent_events.json'
        with open(event_log_file,'w') as f:
            json.dump(event_logs, f, indent=4, separators=(',', ': '))


        # Get the activities executed for all the satellites
        obs_exe = [[] for indx in range(self.num_sats)]
        dlnks_exe = [[] for indx in range(self.num_sats)]
        gs_dlnks_exe = [[] for indx in range(self.num_gs)]
        xlnks_exe = [[] for indx in range(self.num_sats)]
        # Get activities that execute, but failed (dv_del/dv_sch doesn't meet threshold) for all satellites
        obs_exe_fail = [[] for indx in range(self.num_sats)]
        dlnks_exe_fail = [[] for indx in range(self.num_sats)]
        gs_dlnks_exe_fail = [[] for indx in range(self.num_gs)]
        xlnks_exe_fail = [[] for indx in range(self.num_sats)]
        energy_usage = {'time_mins': [[] for indx in range(self.num_sats)], 'e_sats': [[] for indx in range(self.num_sats)]}
        data_usage = {'time_mins': [[] for indx in range(self.num_sats)], 'd_sats': [[] for indx in range(self.num_sats)]}

        exec_failures_dicts_list = []
        non_exec_failures_dicts_list = []
        for sat in self.sats_by_id.values():
            sat_indx = sat.sat_indx
            acts_exe = sat.get_act_hist()
            obs_exe[sat_indx] = acts_exe['obs']
            dlnks_exe[sat_indx] = acts_exe['dlnk']
            xlnks_exe[sat_indx] = acts_exe['xlnk']
            # activities that failed (see failed_dict)
            all_failures_values = list(sat.state_recorder.failed_dict['exec'].values()) + list(sat.state_recorder.failed_dict['non-exec'].values()) 
            exec_failures_dicts_list.append({
                **sat.state_recorder.failed_dict['exec']
            })
            non_exec_failures_dicts_list.append({
                **sat.state_recorder.failed_dict['non-exec']
            })
            all_failures = [act for set_of_acts in all_failures_values for act in set_of_acts] # this list could have duplicates
            obs_exe_fail[sat_indx] = [act for act in all_failures if isinstance(act,ObsWindow) ]
            dlnks_exe_fail[sat_indx] = [act for act in all_failures if isinstance(act,DlnkWindow)]
            xlnks_exe_fail[sat_indx] = [act for act in all_failures if isinstance(act,XlnkWindow)]
            t,e = sat.get_ES_hist()
            energy_usage['time_mins'][sat_indx] = t
            energy_usage['e_sats'][sat_indx] = e
            t,d = sat.get_DS_hist()
            data_usage['time_mins'][sat_indx] = t
            data_usage['d_sats'][sat_indx] = d
        for gs in self.gs_by_id.values():
            gs_indx = gs.gs_indx
            acts_exe = gs.get_act_hist()
            # NOTE: failure recorder only implemented on sats for now
            # all_failures_values = list(gs.state_recorder.failed_dict['exec'].values()) + list(gs.state_recorder.failed_dict['non-exec'].values())
            # all_failures = [act for set_of_acts in all_failures_values for act in set_of_acts]
            gs_dlnks_exe[gs_indx] =  acts_exe['dlnk']
            # need to pull failed downlinks from the sat lists
            gs_dlnks_exe_fail[gs_indx] = [act for list_of_acts_by_sat in dlnks_exe_fail for act in list_of_acts_by_sat if act.gs_indx == gs_indx]

        #  get scheduled activities as planned by ground network
        obs_gsn_sched,dlnks_gsn_sched,xlnks_gsn_sched = self.gs_network.get_all_sats_planned_act_hists()
        gs_dlnks_gsn_sched = self.gs_network.get_all_gs_planned_act_hists()

        ##########
        # Run Metrics
        self.run_and_plot_metrics(energy_usage,data_usage,sats_in_indx_order,gs_in_indx_order,dlnks_exe,xlnks_exe,non_exec_failures_dicts_list)

        ##########
        # Plot stuff

        sats_to_plot = self.sat_id_order
        # sats_to_plot = ['sat0','sat1','sat2','sat3','sat4']

        # Activity Failure vs. Data Storage Plot
        # goal is to plot over the data-state graph with each failure labeled with the DV_failed and failure_type (exec_failures)
        self.sim_plotter.sim_plot_all_sats_failures_on_data_usage(
            sats_to_plot,
            exec_failures_dicts_list,
            data_usage
        )

        #  plot scheduled and executed activities for satellites
        self.sim_plotter.sim_plot_all_sats_acts(
            sats_to_plot,
            obs_gsn_sched,
            obs_exe,
            dlnks_gsn_sched,
            dlnks_exe,
            xlnks_gsn_sched,
            xlnks_exe,
            sats_obs_winds_failed=obs_exe_fail,
            sats_dlnk_winds_failed=dlnks_exe_fail,
            sats_xlnk_winds_failed=xlnks_exe_fail
        )

        #  plot scheduled and executed down links for ground stations
        self.sim_plotter.sim_plot_all_gs_acts(
            self.gs_id_order,
            gs_dlnks_gsn_sched,
            gs_dlnks_exe,
            gs_dlnks_exe_fail
        )

        #  plot satellite energy usage
        self.sim_plotter.sim_plot_all_sats_energy_usage(
            sats_to_plot,
            energy_usage,
            self.ecl_winds
        )

        #  plot satellite data usage
        self.sim_plotter.sim_plot_all_sats_data_usage(
            sats_to_plot,
            data_usage,
            self.ecl_winds
        )
        
        # [ID,item for ID,item in pdb.sim_rt_cont_update_hist_by_id.items() if len(item) > 1]

        # debug_tools.debug_breakpt()



        return None

    def run_and_plot_metrics(self,energy_usage,data_usage,sats_in_indx_order,gs_in_indx_order,dlnks_exe,xlnks_exe,non_exec_failures_dicts_list = None):

        calc_act_windows = True
        if calc_act_windows:
            print('------------------------------')    
            print('Potential DVs')    
            print('Load obs')
            window_uid = 0  # note this window ID will not match the one for executed windows in the sim! These are dummy windows!
            obs_winds, window_uid =self.io_proc.import_obs_winds(window_uid)
            print('Load dlnks')
            dlnk_winds, dlnk_winds_flat, window_uid =self.io_proc.import_dlnk_winds(window_uid)
            print('Load xlnks')
            xlnk_winds, xlnk_winds_flat, window_uid =self.io_proc.import_xlnk_winds(window_uid)

            total_num_collectible_obs_winds = sum(len(o_list) for o_list in obs_winds)
            total_collectible_obs_dv = sum(obs.original_data_vol for o_list in obs_winds for obs in o_list)
            total_dlnkable_dv = sum(dlnk.original_data_vol for d_list in dlnk_winds_flat for dlnk in d_list)

            print('total_num_collectible_obs_winds: %s'%total_num_collectible_obs_winds)
            print('total_collectible_obs_dv: %s'%total_collectible_obs_dv)
            print('total_dlnkable_dv: %s'%total_dlnkable_dv)


        # data containers mark their data vol in their data routes with the "data_vol" attribute, not "scheduled_dv"
        def dc_dr_dv_getter(dr):
            return dr.data_vol

        # Get the planned dv for a route container. Note that this includes utilization rt_cont
        # it's the same code as the dc_dr one, but including for clarity
        def rt_cont_plan_dv_getter(rt_cont):
            return rt_cont.data_vol



        # get all the rt containers that the gs network ever saw
        planned_routes = self.gs_network.get_all_planned_rt_conts()
        planned_routes_regular = [rt for rt in planned_routes if not rt.get_obs().injected] 
        planned_routes_injected = [rt for rt in planned_routes if rt.get_obs().injected] 
        # get the routes for all the packets at each GS at sim end
        executed_routes_regular = [dc.executed_data_route for gs in self.gs_by_id.values() for dc in gs.get_curr_data_conts() if not dc.injected]

        executed_routes_injected = [dc.executed_data_route for gs in self.gs_by_id.values() for dc in gs.get_curr_data_conts() if dc.injected]

        # debug_tools.debug_breakpt()

        # note that the below functions assume that for all rt_conts:
        # - the observation, downlink for all DMRs in the rt_cont are the same

        print('------------------------------')

        dv_stats = self.mc.assess_dv_by_obs(
            planned_routes_regular, executed_routes_regular,
            rt_poss_dv_getter=rt_cont_plan_dv_getter, rt_exec_dv_getter=dc_dr_dv_getter, verbose = True)

        print('injected dv')
        inj_dv_stats = self.mc.assess_dv_by_obs(
            planned_routes_injected, executed_routes_injected,
            rt_poss_dv_getter=rt_cont_plan_dv_getter, rt_exec_dv_getter=dc_dr_dv_getter ,verbose = True)


        print('------------------------------')
        lat_stats = self.mc.assess_latency_by_obs(planned_routes_regular, executed_routes_regular, rt_exec_dv_getter=dc_dr_dv_getter ,verbose = True)

        print('injected latency')
        inj_lat_stats = self.mc.assess_latency_by_obs(planned_routes_injected, executed_routes_injected, rt_exec_dv_getter=dc_dr_dv_getter ,verbose = True)


        sim_plot_params = self.params['const_sim_inst_params']['sim_plot_params']
        time_units = sim_plot_params['obs_aoi_plot']['x_axis_time_units']
        print('------------------------------')
        print('Average AoI by obs, at collection time')
        obs_aoi_stats_at_collection = self.mc.assess_aoi_by_obs_target(planned_routes, executed_routes_regular,include_routing=False,rt_poss_dv_getter=rt_cont_plan_dv_getter, rt_exec_dv_getter=dc_dr_dv_getter ,aoi_x_axis_units=time_units,verbose = True)
        


        print('------------------------------')
        print('Average AoI by obs, with routing')
        obs_aoi_stats_w_routing = self.mc.assess_aoi_by_obs_target(planned_routes, executed_routes_regular,include_routing=True,rt_poss_dv_getter=rt_cont_plan_dv_getter, rt_exec_dv_getter=dc_dr_dv_getter ,aoi_x_axis_units=time_units,verbose = True)



        time_units = sim_plot_params['sat_cmd_aoi_plot']['x_axis_time_units']
        print('------------------------------')
        #  this is indexed by sat index
        sats_cmd_update_hist = met_util.get_all_sats_cmd_update_hist(sats_in_indx_order,gs_in_indx_order,self.gs_id_ignore_list)
        aoi_sat_cmd_stats = self.mc.assess_aoi_sat_ttc_option(sats_cmd_update_hist,ttc_option='cmd',input_time_type='datetime',aoi_x_axis_units=time_units,verbose = True)



        #  this is  indexed by ground station index
        def end_time_getter(sim_sat):
            return sim_sat.sim_end_dt
        time_units = sim_plot_params['sat_tlm_aoi_plot']['x_axis_time_units']

        print('------------------------------')
        sats_tlm_update_hist = met_util.get_all_sats_tlm_update_hist(sats_in_indx_order,gs_in_indx_order,self.gs_id_ignore_list,end_time_getter)
        aoi_sat_tlm_stats = self.mc.assess_aoi_sat_ttc_option(sats_tlm_update_hist,ttc_option='tlm',input_time_type='datetime',aoi_x_axis_units=time_units,verbose = True)


        print('------------------------------')
        e_rsrc_stats = self.mc.assess_energy_resource_margin(energy_usage,verbose = True)
        d_rsrc_stats = self.mc.assess_data_resource_margin(data_usage,verbose = True)

        calc_window_utilization = True
        if calc_window_utilization:
            all_link_acts = [dlnk for dlnks in dlnk_winds_flat for dlnk in dlnks]
            all_link_acts += [xlnk for xlnks in xlnk_winds_flat for xlnk in xlnks]

            executed_acts = copy([dlnk for dlnks in dlnks_exe for dlnk in dlnks])
            executed_acts += copy([xlnk for xlnks in xlnks_exe for xlnk in xlnks])

            def all_acts_dv_getter(act):
                return act.original_data_vol
            def exec_acts_dv_getter(act):
                return act.executed_data_vol

            link_stats = self.mc.assess_link_utilization(all_link_acts, executed_acts, all_acts_dv_getter,exec_acts_dv_getter,verbose=True) 


        output_path = self.params['output_path']
        if not os.path.exists(output_path+'pickles'):       # /outputs/pickles
            if not os.path.exists(output_path):             # /outputs
                os.mkdir(output_path)           
            os.mkdir(output_path+'pickles')

        # saving cus it broke, json so we can read it
        with open(output_path+'pickles/pre-stat.json','w') as f:
            json.dump( {
                "average_obvs_throughput":dv_stats["average_obvs_throughput"],
                "rg_ave_obs_dv_exec":dv_stats["ave_obs_dv_exec"], 
                "rg_ave_obs_dv_poss":dv_stats["ave_obs_dv_poss"], 
                "inj_ave_obs_dv_exec":inj_dv_stats["ave_obs_dv_exec"], 
                "inj_ave_obs_dv_poss":inj_dv_stats["ave_obs_dv_poss"], 
                "rg_median_obs_initial_lat_exec":lat_stats["median_obs_initial_lat_exec"],
                "inj_median_obs_initial_lat_exec":inj_lat_stats["median_obs_initial_lat_exec"],
                "median_av_aoi_exec":obs_aoi_stats_w_routing["median_av_aoi_exec"],
                "median_ave_e_margin_prcnt":e_rsrc_stats["median_ave_e_margin_prcnt"],
                "median_ave_d_margin_prcnt":d_rsrc_stats["median_ave_d_margin_prcnt"]
                },
                f, indent=4, separators=(',', ': '))

        # but like this so we can reload it perfectly
        with open(output_path+'pickles/pre-stat.pkl','wb') as f:
            pickle.dump( {
                "average_obvs_throughput":dv_stats["average_obvs_throughput"],
                "rg_ave_obs_dv_exec":dv_stats["ave_obs_dv_exec"], 
                "rg_ave_obs_dv_poss":dv_stats["ave_obs_dv_poss"], 
                "inj_ave_obs_dv_exec":inj_dv_stats["ave_obs_dv_exec"], 
                "inj_ave_obs_dv_poss":inj_dv_stats["ave_obs_dv_poss"], 
                "rg_median_obs_initial_lat_exec":lat_stats["median_obs_initial_lat_exec"],
                "inj_median_obs_initial_lat_exec":inj_lat_stats["median_obs_initial_lat_exec"],
                "median_av_aoi_exec":obs_aoi_stats_w_routing["median_av_aoi_exec"],
                "median_ave_e_margin_prcnt":e_rsrc_stats["median_ave_e_margin_prcnt"],
                "median_ave_d_margin_prcnt":d_rsrc_stats["median_ave_d_margin_prcnt"]
                },f)

        # GET AND PRINT ACTIVITY FAILURE STATS
        # 1) get all failure dictionaries and count up failure of each type
        exec_failure_types = list(self.sats_by_id.values())[0].state_recorder.failed_dict['exec'].keys()
        non_exec_failure_types = list(self.sats_by_id.values())[0].state_recorder.failed_dict['non-exec'].keys() # TODO: not saved yet
        total_exec_failures_dict = {}
        total_non_exec_failures_dict = {}
        for key in exec_failure_types:
            total_exec_failures_dict[key] = set()
        for sat in self.sats_by_id.values():
            for key in sat.state_recorder.failed_dict['exec'].keys():
                total_exec_failures_dict[key] |= sat.state_recorder.failed_dict['exec'][key]

        for key in non_exec_failure_types:
            total_non_exec_failures_dict[key] = set()
        for sat in self.sats_by_id.values():
            for key in sat.state_recorder.failed_dict['non-exec'].keys():
                total_non_exec_failures_dict[key] |= sat.state_recorder.failed_dict['non-exec'][key]
                
        # 2) print out each key with the num failures next to it:
        test_metrics_dump = {'Num_Failures_by_Type': {
                'exec': {},
                'non-exec': {}
            }
        }
        print('========Totals for Activity Failures by Type===========')
        total_exec_failures_by_act = {
            'xlnk': set(),
            'dlnk': set(),
            'obs': set()
        }
        # print out and save EXEC failures
        for failure_type in total_exec_failures_dict.keys():
            num_failures = len(total_exec_failures_dict[failure_type])
            xlnk_fails = [act for act in total_exec_failures_dict[failure_type] if isinstance(act,XlnkWindow)]
            dlnk_fails = [act for act in total_exec_failures_dict[failure_type] if isinstance(act,DlnkWindow)]
            obs_fails = [act for act in total_exec_failures_dict[failure_type] if isinstance(act,ObsWindow)]

            
            if num_failures > 0:
                print('"%s": Total: %d, Obs: %d, Xlnk: %d, Dlnk: %d' %(failure_type,num_failures,len(obs_fails),len(xlnk_fails),len(dlnk_fails)))

            test_metrics_dump['Num_Failures_by_Type']['exec'][failure_type] =  {
                'xlnk': len(xlnk_fails),
                'dlnk': len(dlnk_fails),
                'obs': len(obs_fails)
            }

            for act_type in total_exec_failures_by_act.keys():
                # an xlnk or downlink can fail for multiple reasons, so need to get unique total set of each type
                if act_type == 'xlnk':
                    for xlnk in xlnk_fails:
                        total_exec_failures_by_act[act_type].add(xlnk)
                elif act_type == 'dlnk':
                    for dlnk in dlnk_fails:
                        total_exec_failures_by_act[act_type].add(dlnk)
                else:
                    for obs in obs_fails:
                        total_exec_failures_by_act[act_type].add(obs)

        # print out and save NON-EXEC failures:
        for failure_type in total_non_exec_failures_dict.keys():
            num_failures = len(total_non_exec_failures_dict[failure_type])
            xlnk_fails = [act for act in total_non_exec_failures_dict[failure_type] if isinstance(act,XlnkWindow)]
            dlnk_fails = [act for act in total_non_exec_failures_dict[failure_type] if isinstance(act,DlnkWindow)]
            obs_fails = [act for act in total_non_exec_failures_dict[failure_type] if isinstance(act,ObsWindow)]

            if num_failures > 0:
                print('"%s": Total: %d, Obs: %d, Xlnk: %d, Dlnk: %d' %(failure_type,num_failures,len(obs_fails),len(xlnk_fails),len(dlnk_fails)))

            test_metrics_dump['Num_Failures_by_Type']['non-exec'][failure_type] =  {
                'xlnk': len(xlnk_fails),
                'dlnk': len(dlnk_fails),
                'obs': len(obs_fails)
            }


        test_metrics_dump['Percentage_of_Exec_Act_Failure_by_Act'] = {}
        print('======Total activity failure percentages=====')
        executed_link_acts = set(executed_acts) # makes a set of all executed xlnks and dlnks to avoid double counting
        executed_acts_dict = {
            'xlnk': set(act for act in executed_link_acts if isinstance(act,XlnkWindow)),
            'dlnk': set(act for act in executed_link_acts if isinstance(act,DlnkWindow)),
            'obs': set(obs for obs_winds_by_sat in obs_winds for obs in obs_winds_by_sat)
        }
        for act_type in total_exec_failures_by_act.keys():
            try:
                percent_failed = 100*len(total_exec_failures_by_act[act_type])/len(executed_acts_dict[act_type])
            except ZeroDivisionError:
                percent_failed = 0
            print('%s: %.2f %%' % (act_type,percent_failed))
            test_metrics_dump['Percentage_of_Exec_Act_Failure_by_Act'][act_type] = percent_failed

            
        print('=============Total Possible and Executed DV=============')
        print('Regular Possible Routed DV: %.2f Mb' % dv_stats['total_poss_dv'])
        print('Regular Executed DV: %.2f Mb' % dv_stats['total_exec_dv'])
        print('Percentage Regular Executed / Poss DV: %.2f %%' % (dv_stats['total_exec_dv']/dv_stats['total_poss_dv'] * 100))
        
        if inj_dv_stats['total_poss_dv']:
            print('Injected Possible Routed DV: %.2f Mb' % inj_dv_stats['total_poss_dv'])
            print('Injected Executed DV: %.2f Mb' % inj_dv_stats['total_exec_dv'])
            print('Percentage Injected Executed / Poss DV: %.2f %%' % (inj_dv_stats['total_exec_dv']/inj_dv_stats['total_poss_dv'] * 100))

            print('Total Possible Routed DV: %.2f Mb' % (dv_stats['total_poss_dv']+inj_dv_stats['total_poss_dv']))
            print('Total Executed DV: %.2f Mb' % (dv_stats['total_exec_dv']+inj_dv_stats['total_exec_dv']))
            print('Percentage Total Executed / Poss DV: %.2f %%' % ((dv_stats['total_exec_dv']+inj_dv_stats['total_exec_dv'])/(inj_dv_stats['total_poss_dv'] +dv_stats['total_poss_dv'] )* 100))
        else:
            print('No injected obs dv possible')

        # FOR MULTI-RUN TEST STATS - DON'T DELETE  BELOW
        # set run name based on settings:
        SRP_setting = self.params['const_sim_inst_params']['lp_general_params']['use_self_replanner']

        test_metrics_dump['dv_stats'] = deepcopy(dv_stats) # copy this since we are deleting keys
        del test_metrics_dump['dv_stats']['poss_dvs_by_obs'] # remove poss_dvs_by_obs from dv_stats (can't write to json)
        test_metrics_dump['dv_stats']['exec_over_poss'] = dv_stats['total_exec_dv']/dv_stats['total_poss_dv']
        test_metrics_dump['d_rsrc_stats'] = d_rsrc_stats # for data margin
        test_metrics_dump['e_rsrc_stats'] = e_rsrc_stats # for energy margin

        test_metrics_dump['lat_stats'] = deepcopy(lat_stats) # copy this since we are deleting keys
        # delete things that can't be written to json 
        del test_metrics_dump['lat_stats']['executed_final_lat_by_obs_exec']
        del test_metrics_dump['lat_stats']['executed_initial_lat_by_obs_exec']
        del test_metrics_dump['lat_stats']['possible_initial_lat_by_obs_exec']

        test_metrics_dump['obs_aoi_stats_w_routing'] = obs_aoi_stats_w_routing
        test_metrics_dump['obs_aoi_stats_at_collection'] = obs_aoi_stats_at_collection

        test_metrics_dump['cmd_aoi_stats'] = {
            **aoi_sat_cmd_stats,
            'max_aois' : [max(x['y']) for x in aoi_sat_cmd_stats['aoi_curves_by_sat_indx'].values()]
        }
        GS_disrupted_list = list(self.params['const_sim_inst_params']['sim_run_perturbations']['schedule_disruptions'].keys())
        if GS_disrupted_list:
                GS_disrupted = GS_disrupted_list[0] # assumes only 1 GS disrupted at a time
        else:
                GS_disrupted = None
        try:
            setting_name = self.params['orbit_prop_params']['orbit_prop_data']['multirun_setting_name'] 
            scenario_name = '%d_SRP_Test_SRP_%s_GS_%s_%s' % (len(self.sats_by_id.values()),SRP_setting,
                                                             GS_disrupted,setting_name) if GS_disrupted else \
                '%d_Nominal_%s' % (len(self.sats_by_id.values()),setting_name)

            filename_str = '../../../multirun_tests/%s.json' %scenario_name
            with open(filename_str,'w') as f:
                json.dump(test_metrics_dump,f, indent=4, separators=(',', ': ')) 
        except KeyError:
            pass
        
        print('SRP SETTING IS: %s, GS DISRUPTED IS: %s' % (SRP_setting, GS_disrupted))

        # PRINT AVERAGE MEAN AGE OF PLAN INFO AND AVERAGE MAX AGE OF PLAN INFO

        # PRINT FINAL STATS
        print("Global Planner performance under simulation:")
        print("| - | Obs. Throughput |     OT      | Med. Obs Latency |    MOLe     | Med. Obs Target | Med. Sat-ave  | Med. Sat-ave |")
        print("| - |    (ave % Max)  | w/ Inj (LP) |    (executed)    | w/ Inj (LP) |   data ave AOI  | Energy Margin | Data Margin  |")
        print("| - |    {:7.2f}      |   {:7.2f}   |     {}      |   {}   |    {}      |   {}     |   {}    |"
            .format   (   
                dv_stats["average_obvs_throughput"],
                inj_dv_stats["average_obvs_throughput"],
                ( '   -   ' if not lat_stats["median_obs_initial_lat_exec"] else "{:7.2f}".format(lat_stats["median_obs_initial_lat_exec"]) ),  # TODO - reform this stat similarly
                ( '   -   ' if not inj_lat_stats["median_obs_initial_lat_exec"] else "{:7.2f}".format(inj_lat_stats["median_obs_initial_lat_exec"]) ),
                ( '   -   ' if not obs_aoi_stats_w_routing["median_av_aoi_exec"] else "{:7.2f}".format(obs_aoi_stats_w_routing["median_av_aoi_exec"]) ),
                ( '   -   ' if not e_rsrc_stats["median_ave_e_margin_prcnt"] else "{:7.2f}".format(e_rsrc_stats["median_ave_e_margin_prcnt"]) ),
                ( '   -   ' if not d_rsrc_stats["median_ave_d_margin_prcnt"] else "{:7.2f}".format(d_rsrc_stats["median_ave_d_margin_prcnt"]) )
                )
            )

        
        ###### 
        # metrics plots
        output_path = self.params['output_path']
        if not os.path.exists(output_path+'plots'):         # /outputs/plots
            if not os.path.exists(output_path):             # /outputs
                os.mkdir(output_path)           
            os.mkdir(output_path+'plots')
            
        print('Creating and saving plots to: %s' % output_path + 'plots')

        self.sim_plotter.plot_obs_aoi_at_collection(
            # self.obs_target_id_order,
            obs_aoi_stats_at_collection['exec_targIDs_found'] ,
            obs_aoi_stats_at_collection['aoi_curves_by_targID_exec']
        )

        self.sim_plotter.plot_obs_aoi_w_routing(
            # self.obs_target_id_order,
            obs_aoi_stats_w_routing['exec_targIDs_found'] ,
            obs_aoi_stats_w_routing['aoi_curves_by_targID_exec']
        )

        curves_by_indx = aoi_sat_cmd_stats['aoi_curves_by_sat_indx']
        cmd_aoi_curves_by_sat_id = {self.sat_id_order[sat_indx]:curves for sat_indx,curves in curves_by_indx.items()}

        self.sim_plotter.plot_sat_cmd_aoi(
            self.sat_id_order,
            cmd_aoi_curves_by_sat_id,
            all_downlink_winds = self.gs_network.scheduler.all_windows_dict['dlnk_winds_flat'],
            gp_replan_freq = self.const_sim_inst_params['sim_gs_network_params']['gsn_ps_params']['replan_interval_s']
        )

        if non_exec_failures_dicts_list:
            # also want to plot activity failures vs. GP plan age (cmd AoI) (non-exec failures)
            # NOTE: the aoi_curves_by_sad_id is inside "run_and_plot_metrics" so can't call it here
            self.sim_plotter.sim_plot_all_sats_failures_on_cmd_aoi(
                self.sat_id_order,
                non_exec_failures_dicts_list,
                cmd_aoi_curves_by_sat_id
            )

        curves_by_indx = aoi_sat_tlm_stats['aoi_curves_by_sat_indx']
        tlm_aoi_curves_by_sat_id = {self.sat_id_order[sat_indx]:curves for sat_indx,curves in curves_by_indx.items()}
        self.sim_plotter.plot_sat_tlm_aoi(
            self.sat_id_order,
            tlm_aoi_curves_by_sat_id
        )


        # plot obs latency histogram, planned routes
        
        pltl.plot_histogram(
            data=obs_aoi_stats_w_routing['av_aoi_by_targID_exec'].values(),
            num_bins = 40,
            plot_type = 'histogram',
            x_title='AoI (hours)',
            y_title='Number of Obs Targets',
            # plot_title = 'CIRCINUS Sim: Average AoI Histogram, with routing (dv req %.1f Mb)'%(mc.min_obs_dv_dlnk_req),
            plot_title = 'CIRCINUS Sim: Average AoI Histogram, with routing',
            plot_size_inches = (12,5.5),
            show=False,
            fig_name=output_path+'plots/csim_obs_aoi_routing_executed_hist.pdf'
        )
        
        # plot obs latency histogram, planned routes
        pltl.plot_histogram(
            data=obs_aoi_stats_at_collection['av_aoi_by_targID_exec'].values(),
            num_bins = 40,
            plot_type = 'histogram',
            x_title='AoI (hours)',
            y_title='Number of Obs Targets',
            # plot_title = 'CIRCINUS Sim: Average AoI Histogram, at collection (dv req %.1f Mb)'%(mc.min_obs_dv_dlnk_req),
            plot_title = 'CIRCINUS Sim: Average AoI Histogram, at collection',
            plot_size_inches = (12,5.5),
            show=False,
            fig_name=output_path+'plots/csim_obs_aoi_collection_executed_hist.pdf'
        )

        # for SSO
        # lat_hist_x_range = (0,250) # minutes
        # lat_hist_num_bins = 50
        # for walker
        lat_hist_x_range = (0,150) # minutes
        lat_hist_y_range = (0,200) # minutes
        # lat_hist_num_bins = 270
        lat_hist_num_bins = 500

        
        # plot obs latency histogram, planned routes
        pltl.plot_histogram(
            data=lat_stats['possible_initial_lat_by_obs_exec'].values(),
            num_bins = lat_hist_num_bins,
            plot_type = 'histogram',
            plot_x_range = lat_hist_x_range,
            plot_y_range = lat_hist_y_range,
            x_title='Latency (mins)',
            y_title='Number of Obs Windows',
            # plot_title = 'CIRCINUS Sim: Initial Latency Histogram, planned (dv req %.1f Mb)'%(mc.min_obs_dv_dlnk_req),
            plot_title = 'CIRCINUS Sim: Initial Latency Histogram, planned',
            plot_size_inches = (12,3.5),
            show=False,
            fig_name=output_path+'plots/csim_obs_lat_planned_hist.pdf'
        )

        
        # plot obs latency histogram, executed routes
        pltl.plot_histogram(
            data=lat_stats['executed_initial_lat_by_obs_exec'].values(),
            num_bins = lat_hist_num_bins,
            plot_type = 'histogram',
            plot_x_range = lat_hist_x_range,
            plot_y_range = lat_hist_y_range,
            x_title='Latency (mins)',
            y_title='Number of Obs Windows',
            # plot_title = 'CIRCINUS Sim: Initial Latency Histogram, executed (dv req %.1f Mb)'%(mc.min_obs_dv_dlnk_req),
            plot_title = '',
            plot_size_inches = (12,3.5),
            show=False,
            fig_name=output_path+'plots/csim_obs_lat_executed_hist.pdf'
        )

        with open(output_path+'plots/exec_obs_lat_reg_cdf_data.json','w') as f:
            json.dump(list(lat_stats['executed_initial_lat_by_obs_exec'].values()), f, indent=4, separators=(',', ': '))

        
        # plot obs latency histogram, executed routes
        pltl.plot_histogram(
            data=lat_stats['executed_initial_lat_by_obs_exec'].values(),
            num_bins = lat_hist_num_bins,
            plot_type = 'cdf',
            plot_x_range = lat_hist_x_range,
            plot_y_range = lat_hist_y_range,
            x_title='Latency (mins)',
            y_title='Fraction of Obs Windows',
            # plot_title = 'CIRCINUS Sim: Initial Latency Histogram, executed (dv req %.1f Mb)'%(mc.min_obs_dv_dlnk_req),
            plot_title = 'CIRCINUS Sim: Initial Latency CDF, executed regular',
            plot_size_inches = (12,3.5),
            show=False,
            fig_name=output_path+'plots/csim_obs_lat_executed_cdf.pdf'
        )


        ############ 
        # injected routes latency plots

        # plot obs latency histogram, executed routes
        pltl.plot_histogram(
            data=inj_lat_stats['executed_initial_lat_by_obs_exec'].values(),
            num_bins = lat_hist_num_bins,
            plot_type = 'histogram',
            plot_x_range = lat_hist_x_range,
            plot_y_range = lat_hist_y_range,
            x_title='Latency (mins)',
            y_title='Number of Obs Windows',
            # plot_title = 'CIRCINUS Sim: Initial Latency Histogram, executed (dv req %.1f Mb)'%(mc.min_obs_dv_dlnk_req),
            plot_title = 'CIRCINUS Sim: Initial Latency Histogram, executed injected',
            plot_size_inches = (12,3.5),
            show=False,
            fig_name=output_path+'plots/csim_obs_lat_injected_hist.pdf'
        )

        with open(output_path+'plots/exec_obs_lat_inj_cdf_data.json','w') as f:
            json.dump(list(inj_lat_stats['executed_initial_lat_by_obs_exec'].values()), f, indent=4, separators=(',', ': '))

        # plot obs latency histogram, executed routes
        pltl.plot_histogram(
            data=inj_lat_stats['executed_initial_lat_by_obs_exec'].values(),
            num_bins = lat_hist_num_bins,
            plot_type = 'cdf',
            plot_x_range = lat_hist_x_range,
            plot_y_range = lat_hist_y_range,
            x_title='Latency (mins)',
            y_title='Fraction of Obs Windows',
            # plot_title = 'CIRCINUS Sim: Initial Latency Histogram, executed (dv req %.1f Mb)'%(mc.min_obs_dv_dlnk_req),
            plot_title = 'CIRCINUS Sim: Initial Latency CDF, executed injected',
            plot_size_inches = (13,3.5),
            show=False,
            fig_name=output_path+'plots/csim_obs_lat_injected_cdf.pdf'
        )

        # # plot obs aoi histogram, with routing
        # pltl.plot_histogram(
        #     data=obs_aoi_stats_w_routing['av_aoi_by_targID_exec'].values(),
        #     num_bins = 50,
        #     plot_type = 'histogram',
        #     plot_x_range = None,
        #     x_title='Average AoI (hours)',
        #     y_title='Number of Obs Targets',
        #     # plot_title = 'CIRCINUS Sim: Initial Latency Histogram, executed (dv req %.1f Mb)'%(mc.min_obs_dv_dlnk_req),
        #     plot_title = 'CIRCINUS Sim: Obs AoI Histogram, with routing, executed',
        #     plot_size_inches = (12,3),
        #     show=False,
        #     fig_name='plots/csim_obs_aoi_wrouting_hist.pdf'
        # )

    def get_metrics_params(self):
        metrics_params = {}

        scenario_params = self.params['orbit_prop_params']['scenario_params']
        sat_params = self.params['orbit_prop_params']['sat_params']
        obs_params = self.params['orbit_prop_params']['obs_params']
        sim_metrics_params = self.params['const_sim_inst_params']['sim_metrics_params']
        sim_plot_params = self.params['const_sim_inst_params']['sim_plot_params']
        as_params = self.params['gp_general_params']['activity_scheduling_params']

        # these are used for AoI calculation
        metrics_params['met_obs_start_dt']  = self.params['const_sim_inst_params']['sim_run_params']['start_utc_dt']
        metrics_params['met_obs_end_dt']  = self.params['const_sim_inst_params']['sim_run_params']['end_utc_dt']

        # gp_inst_planning_params = gp_params['gp_instance_params']['planning_params']
        # gp_general_other_params = gp_params['gp_general_params']['other_params']
        # metrics_params = gp_params['gp_general_params']['metrics_params']
        # plot_params = gp_params['gp_general_params']['plot_params']

        # self.scenario_start_dt  = scenario_params['start_utc_dt']
        metrics_params['num_sats']=sat_params['num_sats']
        metrics_params['num_targ'] = obs_params['num_targets']
        metrics_params['all_targ_IDs'] = [targ['id'] for targ in obs_params['targets']]
        metrics_params['min_obs_dv_dlnk_req'] = as_params['min_obs_dv_dlnk_req_Mb']

        metrics_params['latency_calculation_params'] = sim_metrics_params['latency_calculation']
        metrics_params['targ_id_ignore_list'] = sim_metrics_params['targ_id_ignore_list']
        metrics_params['aoi_units'] = sim_metrics_params['aoi_units']

        # note: I recognize having the parsing code here is dumb. So shoot me.

        metrics_params['sats_emin_Wh'] = []
        metrics_params['sats_emax_Wh'] = []        
        for p_params in sat_params['power_params_by_sat_id'].values():
            sat_edot_by_mode,sat_batt_storage,power_units,charge_eff,discharge_eff = io_tools.parse_power_consumption_params(p_params)

            metrics_params['sats_emin_Wh'].append(sat_batt_storage['e_min'])
            metrics_params['sats_emax_Wh'].append(sat_batt_storage['e_max'])

        metrics_params['sats_dmin_Gb'] = []
        metrics_params['sats_dmax_Gb'] = []        
        for d_params in sat_params['data_storage_params_by_sat_id'].values():
            d_min = d_params['d_min'] 
            d_max = d_params['d_max'] 

            metrics_params['sats_dmin_Gb'].append(d_min)
            metrics_params['sats_dmax_Gb'].append(d_max)

        metrics_params['timestep_s'] = scenario_params['timestep_s']

        return metrics_params
