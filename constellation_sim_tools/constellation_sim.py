from datetime import datetime, timedelta
import pickle

from circinus_tools.scheduling.io_processing import SchedIOProcessor
from circinus_tools.scheduling.custom_window import   ObsWindow,  DlnkWindow, XlnkWindow
from .sim_agents import SimGroundNetwork,SimGroundStation,SimSatellite
from .gp_wrapper import GlobalPlannerWrapper
from .sim_plotting import SimPlotting

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
        self.num_sats=self.sat_params['num_sats']
        self.sat_id_order = self.sat_params['sat_id_order']

        self.sim_tick = timedelta(seconds=self.sim_run_params['sim_tick_s'])

        self.sim_start_dt = self.sim_run_params['start_utc_dt']
        self.sim_end_dt = self.sim_run_params['end_utc_dt']

        self.io_proc =SchedIOProcessor(self.params)
        self.sim_plotter = SimPlotting(self.params)

        # this feels a little dirty, but go ahead and create the GP wrapper here, where params is accessible
        self.gp_wrapper = GlobalPlannerWrapper(self.params)

        self._init_data_structs()


    def _init_data_structs(self):
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
            "initial_state_by_sat_id": self.sat_params['initial_state_by_sat_id'],
            "ecl_winds_by_sat_id": ecl_winds_by_sat_id,
            "power_params_by_sat_id": self.sat_params['power_params_by_sat_id'],
            "resource_delta_t_s": self.sim_run_params['sim_tick_s']
        }

        # create ground network
        gs_network = SimGroundNetwork(
            'gsn',
            self.gs_params['gs_network_name'],
            self.sim_start_dt,
            self.sim_end_dt,
            self.num_sats,
            self.gp_wrapper,
            self.const_sim_inst_params['sim_gs_network_params'],
        ) 
        for station in self.gs_params['stations']:
            gs = SimGroundStation(
                station['id'], 
                station['name'], 
                gs_network
            )
            gs_network.gs_list.append(gs)
        
        #  initialize the planning info database
        gs_network.get_plan_db().initialize(plan_db_inputs)
        self.gs_network = gs_network


        # create sats
        sats_by_id = {}
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

            #  initialize the planning info database
            sat.get_plan_db().initialize(plan_db_inputs)

        self.sats_by_id = sats_by_id

    @staticmethod
    def pickle_checkpoint(global_time,gs_network,sats_by_id):
        pickle_name ='pickles/sim_checkpoint_%s' %(global_time.isoformat().replace (':','_'))
        with open('%s.pkl' % ( pickle_name),'wb') as f:
            pickle.dump( {"global_time":global_time, "gs_network": gs_network, "sats_by_id":sats_by_id},f)

    @staticmethod
    def unpickle_checkpoint(pickle_name):
        p = pickle.load (open ( pickle_name,'rb'))
        return p['global_time'],p['gs_network'],p['sats_by_id']

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
            global_time, self.gs_network, self.sats_by_id = self.unpickle_checkpoint(self.sim_run_params['restore_pkl_name'])
            print_verbose('Unpickled checkpoint file %s'%(self.sim_run_params['restore_pkl_name']),verbose)
            assert(global_time != self.sim_start_dt)


        #######################
        # Simulation loop
        #######################

        print_verbose('Starting sim loop',verbose)

        while global_time < sim_end_dt:

            print_verbose('global_time: %s'%(global_time.isoformat()),verbose)

            # pickle checkpoint if so desired
            if self.sim_run_params['pickle_checkpoints']:
                if (global_time - last_checkpoint_time).total_seconds() >= self.sim_run_params['checkpoint_spacing_s']:
                    self.pickle_checkpoint(global_time, self.gs_network, self.sats_by_id)
                    last_checkpoint_time = global_time

            #####################
            # State update

            # todo
            # add tracking, plotting of sat state

            #  run ground network update step so that we can immediately share plans on first iteration of this loop
            self.gs_network.state_update_step(global_time)

             # start all the satellites with a first round of GP schedules, if so desired
            if first_iter and self.sim_run_params['sat_schedule_hotstart']:
                for sat in self.sats_by_id.values():
                    self.gs_network.send_planning_info(sat)

            # now update satellite state
            for sat in self.sats_by_id.values():
                sat.state_update_step(global_time)


            #####################
            # Activity execution

            # todo: add back in!
            # for sat in self.sats_by_id.values():
            #     sat.execution_step()

            # todo: this is a testing hack.  remove! ( don't cross levels of abstraction like this either!)
            if self.gs_network.scheduler.plans_updated:
                for sat in self.sats_by_id.values():
                    self.gs_network.send_planning_info(sat)
                self.gs_network.scheduler.plans_updated = False

            #####################
            # Replanning

            # todo: this should be added back in when the local planner is included
            # for sat in self.sats_by_id.values():
            #     sat.replan_step()


            global_time = global_time+self.sim_tick
            first_iter = False

    def post_run(self):

        # Get the activities executed for all the satellites
        obs_exe = [[] for indx in range(self.num_sats)]
        dlnks_exe = [[] for indx in range(self.num_sats)]
        xlnks_exe = [[] for indx in range(self.num_sats)]
        energy_usage = {'time_mins': [[] for indx in range(self.num_sats)], 'e_sats': [[] for indx in range(self.num_sats)]}
        for sat_id in self.sat_id_order:
            sat = self.sats_by_id[sat_id]
            sat_indx = sat.sat_indx
            obs_exe[sat_indx],dlnks_exe[sat_indx],xlnks_exe[sat_indx] = sat.get_act_hist()
            t,e = sat.get_ES_hist()
            energy_usage['time_mins'][sat_indx] = t
            energy_usage['e_sats'][sat_indx] = e

        #  get scheduled activities as planned by ground network
        obs_gsn_sched,dlnks_gsn_sched,xlnks_gsn_sched = self.gs_network.get_all_sats_act_hists()

        #  plot scheduled and executed activities
        self.sim_plotter.sim_plot_all_sats_acts(
            self.sat_id_order,
            obs_gsn_sched,
            obs_exe,
            dlnks_gsn_sched,
            dlnks_exe,
            xlnks_gsn_sched,
            xlnks_exe,
            self.sim_start_dt,
            self.sim_end_dt,
            self.sim_start_dt
        )

        #  plot satellite energy usage
        self.sim_plotter.sim_plot_all_sats_energy_usage(
            self.sat_id_order,
            energy_usage,
            self.ecl_winds,
            self.sim_start_dt,
            self.sim_end_dt,
            self.sim_start_dt
        )
        


        return None

