from datetime import datetime, timedelta
import pickle
import json

from circinus_tools.scheduling.io_processing import SchedIOProcessor
from circinus_tools.scheduling.custom_window import   ObsWindow,  DlnkWindow, XlnkWindow
from circinus_tools  import time_tools as tt
from circinus_tools.metrics.metrics_calcs import MetricsCalcs
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
        self.gs_params = self.params['orbit_prop_params']['gs_params']
        self.const_sim_inst_params = sim_params['const_sim_inst_params']
        self.sim_run_params = sim_params['const_sim_inst_params']['sim_run_params']
        self.sim_run_perturbations = sim_params['const_sim_inst_params']['sim_run_perturbations']
        self.num_sats=self.sat_params['num_sats']
        self.sat_id_order = self.sat_params['sat_id_order']
        self.gs_id_order = self.gs_params['gs_id_order']
        self.num_gs = len(self.gs_params['gs_id_order'])

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

        #  note: use sim tick as resource delta T.
        plan_db_inputs = {
            "sat_id_order": self.sat_id_order,
            "gs_id_order": self.gs_id_order,
            "initial_state_by_sat_id": self.sat_params['initial_state_by_sat_id'],
            "ecl_winds_by_sat_id": ecl_winds_by_sat_id,
            "power_params_by_sat_id": self.sat_params['power_params_by_sat_id'],
            "resource_delta_t_s": self.sim_run_params['sim_tick_s']
        }

        # create ground network
        gs_by_id = {}
        all_gs = []
        gs_network = SimGroundNetwork(
            'gsn',
            self.gs_params['gs_network_name'],
            self.sim_start_dt,
            self.sim_end_dt,
            self.num_sats,
            self.num_gs,
            self.const_sim_inst_params['sim_gs_network_params'],
        ) 
        for station in self.gs_params['stations']:
            gs = SimGroundStation(
                str(station['id']),
                self.gs_id_order.index(str(station['id'])),
                station['name'], 
                gs_network,
                self.sim_start_dt,
                self.sim_end_dt
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
                sim_start_dt=self.sim_start_dt,
                sim_end_dt=self.sim_end_dt,
                sat_scenario_params=sat_id_scenario_params,
                sim_satellite_params=sat_id_sim_satellite_params
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

        windid = 0
        inj_obs_by_sat_id = {}
        for obs_raw in inj_obs_raw:
            if not obs_raw['type'] == 'hardcoded':
                raise NotImplementedError

            sat_id = obs_raw['sat_id']
            obs = ObsWindow(
                windid, 
                sat_indx= self.sat_id_order.index(sat_id),
                target_IDs=['urgent'], 
                sat_target_indx=0, 
                start= tt.iso_string_to_dt (obs_raw['start_utc']), 
                end= tt.iso_string_to_dt (obs_raw['end_utc']), 
                wind_obj_type='injected'
            )

            obs.calc_data_vol(self.sat_params['pl_data_rate'])

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
        if self.sim_run_params['restore_from_checkpoint']:
            global_time, self.gs_network, self.sats_by_id, self.gs_by_id = self.unpickle_checkpoint(self.sim_run_params['restore_pkl_name'])
            first_iter= False
            assert(global_time != self.sim_start_dt)  # to make sure it actually isn't the first iteration
            print_verbose('Unpickled checkpoint file %s'%(self.sim_run_params['restore_pkl_name']),verbose)


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
                for sat in self.sats_by_id.values():
                    self.gs_network.send_planning_info(sat)
                    self.gs_network.scheduler.set_plans_updated(False)

            # now update satellite and ground station states
            for sat_id,sat in self.sats_by_id.items():
                sat.state_update_step(global_time,self.lp_wrapper)
            for gs in self.gs_by_id.values():
                gs.state_update_step(global_time)



            #####################
            # Planning info sharing (currenly assuming update via backbone network to sats)

            # todo: seems kinda bad to cross levels of abstraction like this...
            if self.gs_network.scheduler.check_plans_updated():
                # when the GS replans, assume we have the ability to instantaneously update the satellites, and receive a state update from them ( kinda a hack for now...)
                for sat in self.sats_by_id.values():
                    self.gs_network.send_planning_info(sat)
                    # todo: finish this
                    sat.send_planning_info(self.gs_network)

                #  every time the ground network re-plans, want to send that updated planning information to the ground stations
                for gs in self.gs_by_id.values():
                    self.gs_network.send_planning_info(gs)
                self.gs_network.scheduler.set_plans_updated(False)


            global_time = global_time+self.sim_tick
            first_iter = False

        #  save a pickle for the end ฅ^•ﻌ•^ฅ (meow)
        if self.sim_run_params['pickle_checkpoints']:
            self.pickle_checkpoint(global_time, self.gs_network, self.sats_by_id, self.gs_by_id)

    def post_run(self):

        # report events
        event_logs = {'sats':[],'gs': []}
        for sat in self.sats_by_id.values():
            sat.state_recorder.log_event(self.sim_end_dt,'constellation_sim.py','final_dv',[str(dc) for dc in sat.state_sim.get_curr_data_conts()])
            event_logs['sats'].append(sat.state_recorder.get_events())
        for gs in self.gs_by_id.values():
            gs.state_recorder.log_event(self.sim_end_dt,'constellation_sim.py','final_dv',[str(dc) for dc in gs.state_sim.get_curr_data_conts()])
            event_logs['gs'].append(gs.state_recorder.get_events())

        event_log_file = 'logs/agent_events.json'
        with open(event_log_file,'w') as f:
            json.dump(event_logs ,f)
          

        self.run_metrics()

        debug_tools.debug_breakpt()


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

        # debug_tools.debug_breakpt()

        #  plot scheduled and executed activities for satellites
        self.sim_plotter.sim_plot_all_sats_acts(
            self.sat_id_order,
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
            self.sat_id_order,
            energy_usage,
            self.ecl_winds
        )

        #  plot satellite data usage
        self.sim_plotter.sim_plot_all_sats_data_usage(
            self.sat_id_order,
            data_usage,
            self.ecl_winds
        )
        


        return None

    def run_metrics(self):

        # metrics calculation
        mc = MetricsCalcs(self.params)

        # data containers mark their data vol in their data routes with the "data_vol" attribute, not "scheduled_dv"
        def dc_dr_dv_getter(dr):
            return dr.data_vol

        # Get the planned dv for a route container. Note that this includes utilization rt_cont
        # it's the same code as the dc_dr one, but including for clarity
        def rt_cont_plan_dv_getter(rt_cont):
            return rt_cont.data_vol

        # get all the rt containers that the gs network ever saw
        planned_routes = self.gs_network.get_all_planned_rt_conts()           
        # get the routes for all the packets at each GS at sim end
        executed_routes = [dc.executed_data_route for gs in self.gs_by_id.values() for dc in gs.get_curr_data_conts()]
        mc.assess_dv_by_obs(planned_routes, executed_routes,rt_poss_dv_getter=rt_cont_plan_dv_getter, rt_exec_dv_getter=dc_dr_dv_getter ,verbose = True)

