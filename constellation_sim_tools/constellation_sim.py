from datetime import datetime, timedelta

from circinus_tools.scheduling.io_processing import SchedIOProcessor
from .sim_agents import SimGroundNetwork,SimGroundStation,SimSatellite
from .gp_wrapper import GlobalPlannerWrapper

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

        self.sim_tick = timedelta(seconds=self.sim_run_params['sim_tick_s'])

        self.sim_start_dt = self.sim_run_params['start_utc_dt']
        self.sim_end_dt = self.sim_run_params['end_utc_dt']

        self.io_proc =SchedIOProcessor(self.params)

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

        sat_id_order = self.sat_params['sat_id_order']
        ecl_winds_by_sat_id = {sat_id_order[sat_indx]:ecl_winds[sat_indx] for sat_indx in range(self.num_sats)}

        #  note: use sim tick as resource delta T.
        plan_db_inputs = {
            "sat_id_order": sat_id_order,
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
        for sat_id in self.sat_params['sat_id_order']:
            sat_indx = self.sat_params['sat_id_order'].index(sat_id)

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


    def run( self):
        """ run the simulation """

        verbose = True

        global_time = self.sim_start_dt
        sim_end_dt = self.sim_end_dt

        all_sats = list(self.sats_by_id.values())

        #  used to alert special operations on first iteration of the loop
        first_iter = True

        print('Starting sim loop')

        # Simulation loop
        while global_time < sim_end_dt:

            print_verbose('global_time: %s'%(global_time.isoformat()),verbose)

            # todo: add checkpoint pickling?


            #####################
            # State update

            # todo
            # add tracking, plotting of sat state

            #  run ground network update step so that we can immediately share plans on first iteration of this loop
            self.gs_network.state_update_step(global_time)

             # start all the satellites with a first round of GP schedules, if so desired
            if first_iter and self.sim_run_params['sat_schedule_hotstart']:
                for sat in all_sats:
                    self.gs_network.send_planning_info(sat)

            # now update satellite state
            for sat in all_sats:
                sat.state_update_step(global_time)


            #####################
            # Activity execution

            # todo: add back in!
            # for sat in all_sats:
            #     sat.execution_step()

            # todo: this is a testing hack.  remove! ( don't cross levels of abstraction like this either!)
            if self.gs_network.scheduler.plans_updated:
                for sat in all_sats:
                    self.gs_network.send_planning_info(sat)
                self.gs_network.scheduler.plans_updated = False

            #####################
            # Replanning

            # todo: this should be added back in when the local planner is included
            # for sat in all_sats:
            #     sat.replan_step()


            global_time = global_time+self.sim_tick
            first_iter = False
