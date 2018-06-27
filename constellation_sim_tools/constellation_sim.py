from datetime import datetime, timedelta
from collections import OrderedDict
import pickle
import json

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

from circinus_tools import debug_tools

# debug_tools.debug_breakpt()

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

        # Also create local planner wrapper. it will store inputs that are common across all satellites. the instance parameters passed to it should be satellite-specific
        self.lp_wrapper = LocalPlannerWrapper(self.params)

        self.act_timing_helper = ActivityTimingHelper(self.sat_params['activity_params'],orbit_params['sat_ids_by_orbit_name'],self.sat_params['sat_id_order'],self.params['orbit_prop_params']['version'])

        self.init_data_structs()


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
            "gs_id_order": self.gs_id_order,
            "other_agent_ids": [gsn_id],
            "initial_state_by_sat_id": self.sat_params['initial_state_by_sat_id'],
            "ecl_winds_by_sat_id": ecl_winds_by_sat_id,
            "power_params_by_sat_id": self.sat_params['power_params_by_sat_id'],
            "resource_delta_t_s": self.sim_run_params['sim_tick_s']
        }

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
            self.act_timing_helper
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
                self.act_timing_helper
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

            # assuming same for every sat. todo: make unique for each sat?
            sat_id_sim_satellite_params = self.const_sim_inst_params['sim_satellite_params']

            sat = SimSatellite(
                sat_id,
                sat_indx,
                self.sim_start_dt,
                self.sim_end_dt,
                sat_id_scenario_params,
                sat_id_sim_satellite_params,
                self.act_timing_helper
            )
            sats_by_id[sat_id] = sat
            all_sats.append(sat)

            #  initialize the planning info database
            sat.get_plan_db().initialize(plan_db_inputs)

        #  set the simulation satellites list for every satellite
        for sat in all_sats:
            sat.all_sim_sats = all_sats
            sat.all_sim_gs = all_gs

        self.sats_by_id = sats_by_id

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



    @staticmethod
    def pickle_checkpoint(global_time,gs_network,sats_by_id,gs_by_id):
        pickle_name ='pickles/sim_checkpoint_%s' %(global_time.isoformat().replace (':','_').replace ('-','_'))
        with open('%s.pkl' % ( pickle_name),'wb') as f:
            pickle.dump( {"global_time":global_time, "gs_network": gs_network, "sats_by_id":sats_by_id, "gs_by_id":gs_by_id},f)

    @staticmethod
    def unpickle_checkpoint(pickle_name):
        p = pickle.load (open ( pickle_name,'rb'))
        return p['global_time'],p['gs_network'],p['sats_by_id'],p['gs_by_id']

    def run( self):
        """ run the simulation """

        verbose = True

        global_time = self.sim_start_dt
        sim_end_dt = self.sim_end_dt
        last_checkpoint_time = global_time

        #  used to alert special operations on first iteration of the loop
        first_iter = True

        # unpickle from a checkpoint if so desired
        if self.sim_run_params['restore_from_checkpoint'] or self.restore_pickle_cmdline_arg != "":
            # get pickle name from cmdline if it was provided
            pickle_name = self.restore_pickle_cmdline_arg if self.restore_pickle_cmdline_arg != "" else self.sim_run_params['restore_pkl_name']

            global_time, self.gs_network, self.sats_by_id, self.gs_by_id = self.unpickle_checkpoint(pickle_name)
            first_iter= False
            assert(global_time != self.sim_start_dt)  # to make sure it actually isn't the first iteration
            print_verbose('Unpickled checkpoint file %s'%(pickle_name),verbose)


        #######################
        # Simulation loop
        #######################

        print_verbose('Starting sim loop',verbose)

        while global_time < sim_end_dt:

            print_verbose('global_time: %s'%(global_time.isoformat()),verbose)

            #####################
            # Checkpoint pickling

            if self.sim_run_params['pickle_checkpoints']:
                if (global_time - last_checkpoint_time).total_seconds() >= self.sim_run_params['checkpoint_spacing_s']:
                    self.pickle_checkpoint(global_time, self.gs_network, self.sats_by_id, self.gs_by_id)
                    last_checkpoint_time = global_time


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
            if first_iter and self.sim_run_params['sat_schedule_hotstart']:
                self.gsn_exchange_planning_info_all_exec_agents()

            # now update satellite and ground station states
            for sat_id,sat in self.sats_by_id.items():
                sat.state_update_step(global_time,self.lp_wrapper)
            for gs in self.gs_by_id.values():
                gs.state_update_step(global_time)


            #####################
            # Planning info sharing (currenly assuming update via backbone network to sats)

            # whenever GP has run, share info afterwards
            # todo: seems kinda bad to cross levels of abstraction like this...
            if self.gs_network.scheduler.check_plans_updated():
                self.gsn_exchange_planning_info_all_exec_agents()

            # if a sat LP has run, send that info to gs network so it can use in planning with GP
            for sat_id,sat in self.sats_by_id.items():
                if sat.arbiter.check_plans_updated():
                    sat.send_planning_info(self.gs_network,info_option='routes_only')
                    sat.arbiter.set_plans_updated(False)

                
            global_time = global_time+self.sim_tick
            first_iter = False


        #### 
        # end of sim

        #  save a pickle for the end ฅ^•ﻌ•^ฅ (meow)
        if self.sim_run_params['pickle_checkpoints']:
            self.pickle_checkpoint(global_time, self.gs_network, self.sats_by_id, self.gs_by_id)



    def gsn_exchange_planning_info_all_exec_agents(self):
        # when the GS replans, assume we have the ability to instantaneously update the satellites, and receive a state update from them ( kinda a hack for now...)
        for sat in self.sats_by_id.values():
            self.gs_network.send_planning_info(sat,info_option='routes_only')
            sat.send_planning_info(self.gs_network,info_option='routes_only')

        #  every time the ground network re-plans, want to send that updated planning information to the ground stations
        for gs in self.gs_by_id.values():
            self.gs_network.send_planning_info(gs,info_option='routes_only')
            gs.send_planning_info(self.gs_network,info_option='routes_only')

        self.gs_network.scheduler.set_plans_updated(False)

    def post_run(self):

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
            sat.state_recorder.log_event(self.sim_end_dt,'constellation_sim.py','final_dv',[str(dc) for dc in sat.state_sim.get_curr_data_conts()])
            event_logs['sats'][sat.ID] = sat.state_recorder.get_events()
        for gs in gs_in_indx_order:
            gs.state_recorder.log_event(self.sim_end_dt,'constellation_sim.py','final_dv',[str(dc) for dc in gs.state_sim.get_curr_data_conts()])
            event_logs['gs'][gs.ID] = gs.state_recorder.get_events()

        event_log_file = 'logs/agent_events.json'
        with open(event_log_file,'w') as f:
            json.dump(event_logs ,f)


        # Get the activities executed for all the satellites
        obs_exe = [[] for indx in range(self.num_sats)]
        dlnks_exe = [[] for indx in range(self.num_sats)]
        gs_dlnks_exe = [[] for indx in range(self.num_gs)]
        xlnks_exe = [[] for indx in range(self.num_sats)]
        energy_usage = {'time_mins': [[] for indx in range(self.num_sats)], 'e_sats': [[] for indx in range(self.num_sats)]}
        data_usage = {'time_mins': [[] for indx in range(self.num_sats)], 'd_sats': [[] for indx in range(self.num_sats)]}
        for sat in self.sats_by_id.values():
            sat_indx = sat.sat_indx
            acts_exe = sat.get_act_hist()
            obs_exe[sat_indx] = acts_exe['obs']
            dlnks_exe[sat_indx] = acts_exe['dlnk']
            xlnks_exe[sat_indx] = acts_exe['xlnk']
            t,e = sat.get_ES_hist()
            energy_usage['time_mins'][sat_indx] = t
            energy_usage['e_sats'][sat_indx] = e
            t,d = sat.get_DS_hist()
            data_usage['time_mins'][sat_indx] = t
            data_usage['d_sats'][sat_indx] = d
        for gs in self.gs_by_id.values():
            gs_indx = gs.gs_indx
            acts_exe = gs.get_act_hist()
            gs_dlnks_exe[gs_indx] = acts_exe['dlnk']

        #  get scheduled activities as planned by ground network
        obs_gsn_sched,dlnks_gsn_sched,xlnks_gsn_sched = self.gs_network.get_all_sats_planned_act_hists()
        gs_dlnks_gsn_sched = self.gs_network.get_all_gs_planned_act_hists()

        ##########
        # Run Metrics

        self.run_and_plot_metrics(energy_usage,data_usage,sats_in_indx_order,gs_in_indx_order)

        ##########
        # Plot stuff

        sats_to_plot = self.sat_id_order
        # sats_to_plot = ['sat0','sat1','sat2','sat3','sat4']

        #  plot scheduled and executed activities for satellites
        self.sim_plotter.sim_plot_all_sats_acts(
            sats_to_plot,
            obs_gsn_sched,
            obs_exe,
            dlnks_gsn_sched,
            dlnks_exe,
            xlnks_gsn_sched,
            xlnks_exe
        )

        #  plot scheduled and executed down links for ground stations
        self.sim_plotter.sim_plot_all_gs_acts(
            self.gs_id_order,
            gs_dlnks_gsn_sched,
            gs_dlnks_exe
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

    def run_and_plot_metrics(self,energy_usage,data_usage,sats_in_indx_order,gs_in_indx_order):

        # metrics calculation
        mc = MetricsCalcs(self.get_metrics_params())

        calc_act_windows = False
        if calc_act_windows:
            print('------------------------------')    
            print('Potential DVs')    
            print('Load obs')
            window_uid = 0  # note this window ID will not match the one for executed windows in the sim! These are dummy windows!
            obs_winds, window_uid =self.io_proc.import_obs_winds(window_uid)
            print('Load dlnks')
            dlnk_winds, dlnk_winds_flat, window_uid =self.io_proc.import_dlnk_winds(window_uid)

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

        # note that the below functions assume that for all rt_conts:
        # - the observation, downlink for all DMRs in the rt_cont are the same

        print('------------------------------')

        dv_stats = mc.assess_dv_by_obs(planned_routes_regular, executed_routes_regular,rt_poss_dv_getter=rt_cont_plan_dv_getter, rt_exec_dv_getter=dc_dr_dv_getter ,verbose = True)

        print('injected dv')
        inj_dv_stats = mc.assess_dv_by_obs(planned_routes_injected, executed_routes_injected,rt_poss_dv_getter=rt_cont_plan_dv_getter, rt_exec_dv_getter=dc_dr_dv_getter ,verbose = True)


        print('------------------------------')
        lat_stats = mc.assess_latency_by_obs(planned_routes_regular, executed_routes_regular, rt_exec_dv_getter=dc_dr_dv_getter ,verbose = True)

        print('injected latency')
        inj_lat_stats = mc.assess_latency_by_obs(planned_routes_injected, executed_routes_injected, rt_exec_dv_getter=dc_dr_dv_getter ,verbose = True)


        sim_plot_params = self.params['const_sim_inst_params']['sim_plot_params']
        time_units = sim_plot_params['obs_aoi_plot']['x_axis_time_units']
        print('------------------------------')
        print('Average AoI by obs, at collection time')
        obs_aoi_stats_at_collection = mc.assess_aoi_by_obs_target(planned_routes, executed_routes_regular,include_routing=False,rt_poss_dv_getter=rt_cont_plan_dv_getter, rt_exec_dv_getter=dc_dr_dv_getter ,aoi_x_axis_units=time_units,verbose = True)
        


        print('------------------------------')
        print('Average AoI by obs, with routing')
        obs_aoi_stats_w_routing = mc.assess_aoi_by_obs_target(planned_routes, executed_routes_regular,include_routing=True,rt_poss_dv_getter=rt_cont_plan_dv_getter, rt_exec_dv_getter=dc_dr_dv_getter ,aoi_x_axis_units=time_units,verbose = True)



        time_units = sim_plot_params['sat_cmd_aoi_plot']['x_axis_time_units']
        print('------------------------------')
        #  this is indexed by sat index
        sats_cmd_update_hist = met_util.get_all_sats_cmd_update_hist(sats_in_indx_order,gs_in_indx_order,self.gs_id_ignore_list)
        aoi_sat_cmd_stats = mc.assess_aoi_sat_ttc_option(sats_cmd_update_hist,ttc_option='cmd',input_time_type='datetime',aoi_x_axis_units=time_units,verbose = True)



        #  this is  indexed by ground station index
        def end_time_getter(sim_sat):
            return sim_sat.sim_end_dt
        time_units = sim_plot_params['sat_tlm_aoi_plot']['x_axis_time_units']

        print('------------------------------')
        sats_tlm_update_hist = met_util.get_all_sats_tlm_update_hist(sats_in_indx_order,gs_in_indx_order,self.gs_id_ignore_list,end_time_getter)
        aoi_sat_tlm_stats = mc.assess_aoi_sat_ttc_option(sats_tlm_update_hist,ttc_option='tlm',input_time_type='datetime',aoi_x_axis_units=time_units,verbose = True)


        print('------------------------------')
        e_rsrc_stats = mc.assess_energy_resource_margin(energy_usage,verbose = True)
        d_rsrc_stats = mc.assess_data_resource_margin(data_usage,verbose = True)



        ###### 
        # metrics plots

        self.sim_plotter.plot_obs_aoi_at_colllection(
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
            fig_name='plots/csim_obs_aoi_routing_executed_hist.pdf'
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
            fig_name='plots/csim_obs_aoi_collection_executed_hist.pdf'
        )

        # for SSO
        # lat_hist_x_range = (0,250) # minutes
        # lat_hist_num_bins = 50
        # for walker
        lat_hist_x_range = (0,150) # minutes
        lat_hist_num_bins = 60

        # plot obs latency histogram, planned routes
        pltl.plot_histogram(
            data=lat_stats['possible_initial_lat_by_obs_exec'].values(),
            num_bins = lat_hist_num_bins,
            plot_type = 'histogram',
            x_range = lat_hist_x_range,
            x_title='Latency (mins)',
            y_title='Number of Obs Windows',
            # plot_title = 'CIRCINUS Sim: Initial Latency Histogram, planned (dv req %.1f Mb)'%(mc.min_obs_dv_dlnk_req),
            plot_title = 'CIRCINUS Sim: Initial Latency Histogram, planned',
            plot_size_inches = (12,3),
            show=False,
            fig_name='plots/csim_obs_lat_planned_hist.pdf'
        )

        # plot obs latency histogram, executed routes
        pltl.plot_histogram(
            data=lat_stats['executed_initial_lat_by_obs_exec'].values(),
            num_bins = lat_hist_num_bins,
            plot_type = 'histogram',
            x_range = lat_hist_x_range,
            x_title='Latency (mins)',
            y_title='Number of Obs Windows',
            # plot_title = 'CIRCINUS Sim: Initial Latency Histogram, executed (dv req %.1f Mb)'%(mc.min_obs_dv_dlnk_req),
            plot_title = 'CIRCINUS Sim: Initial Latency Histogram, executed',
            plot_size_inches = (12,3),
            show=False,
            fig_name='plots/csim_obs_lat_executed_hist.pdf'
        )

        # plot obs latency histogram, executed routes
        pltl.plot_histogram(
            data=lat_stats['executed_initial_lat_by_obs_exec'].values(),
            num_bins = lat_hist_num_bins,
            plot_type = 'cdf',
            x_range = lat_hist_x_range,
            x_title='Latency (mins)',
            y_title='Number of Obs Windows',
            # plot_title = 'CIRCINUS Sim: Initial Latency Histogram, executed (dv req %.1f Mb)'%(mc.min_obs_dv_dlnk_req),
            plot_title = 'CIRCINUS Sim: Initial Latency Histogram, executed',
            plot_size_inches = (12,3),
            show=False,
            fig_name='plots/csim_obs_lat_executed_cdf.pdf'
        )


        ############ 
        # injected routes latency plots

        # plot obs latency histogram, executed routes
        pltl.plot_histogram(
            data=inj_lat_stats['executed_initial_lat_by_obs_exec'].values(),
            num_bins = lat_hist_num_bins,
            plot_type = 'histogram',
            x_range = lat_hist_x_range,
            x_title='Latency (mins)',
            y_title='Number of Obs Windows',
            # plot_title = 'CIRCINUS Sim: Initial Latency Histogram, executed (dv req %.1f Mb)'%(mc.min_obs_dv_dlnk_req),
            plot_title = 'CIRCINUS Sim: Initial Latency Histogram, injected obs',
            plot_size_inches = (12,3),
            show=False,
            fig_name='plots/csim_obs_lat_injected_hist.pdf'
        )

        # plot obs latency histogram, executed routes
        pltl.plot_histogram(
            data=inj_lat_stats['executed_initial_lat_by_obs_exec'].values(),
            num_bins = lat_hist_num_bins,
            plot_type = 'cdf',
            x_range = lat_hist_x_range,
            x_title='Latency (mins)',
            y_title='Number of Obs Windows',
            # plot_title = 'CIRCINUS Sim: Initial Latency Histogram, executed (dv req %.1f Mb)'%(mc.min_obs_dv_dlnk_req),
            plot_title = 'CIRCINUS Sim: Initial Latency Histogram, injected obs',
            plot_size_inches = (12,3),
            show=False,
            fig_name='plots/csim_obs_lat_injected_cdf.pdf'
        )

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
            storage_opt = d_params['storage_option']
            d_min = d_params['data_storage_Gbit']['d_min'][storage_opt]
            d_max = d_params['data_storage_Gbit']['d_max'][storage_opt]

            metrics_params['sats_dmin_Gb'].append(d_min)
            metrics_params['sats_dmax_Gb'].append(d_max)

        metrics_params['timestep_s'] = scenario_params['timestep_s']

        return metrics_params
