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
        self.scenario_params = self.params['orbit_prop_params']['scenario_params']
        self.sat_params = self.params['orbit_prop_params']['sat_params']
        self.gs_params = self.params['orbit_prop_params']['gs_params']
        self.const_sim_params = sim_params['const_sim_params']

        self.sim_tick = timedelta(seconds=self.const_sim_params['sim_run_params']['sim_tick_s'])

        self.sim_start_dt = self.scenario_params['start_utc_dt']
        self.sim_end_dt = self.scenario_params['start_utc_dt']

        self.io_proc =SchedIOProcessor(self.params)

        self._init_data_structs()

    def _init_data_structs(self):
        # dirty hack! I want to prevent these eclipse windows from sharing IDs with any windows returned from GP, so making them all negative. 
        # todo: this is not a long term solution. Need to incorporate mechanism to share window ID namespace across const sim, GP
        window_uid = -9999
        # ecl_winds is an array with index for each sat_indx
        ecl_winds, window_uid =self.io_proc.import_eclipse_winds(window_uid)
        if window_uid >= 0:
            raise RuntimeWarning('Saw positive window ID for ecl window hack')

        # create ground network
        gs_network = SimGroundNetwork(self.gs_params['gs_network_name'],self.sim_start_dt) 
        for station in self.gs_params['stations']:
            gs = SimGroundStation(
                station['id'], 
                station['name'], 
                gs_network
            )
            gs_network.gs_list.append(gs)
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
            sat_id_sim_satellite_params = self.const_sim_params['sim_satellite_params']

            sat_id_event_data = {
                "ecl_winds": ecl_winds[sat_indx]
            }

            sat = SimSatellite(
                sat_id,
                start_dt=self.sim_start_dt,
                sat_scenario_params=sat_id_scenario_params,
                sim_satellite_params=sat_id_sim_satellite_params,
                sat_event_data = sat_id_event_data
            )
            sats_by_id[sat_id] = sat
        self.sats_by_id = sats_by_id

    def run( self):

        verbose = True

        global_time = self.sim_start_dt
        sim_end_dt = self.sim_end_dt

        all_sats = list(self.sats_by_id.values())

        # Simulation loop
        while global_time < sim_end_dt:

            global_time = global_time+self.sim_tick

            print_verbose('global_time: %s'%(global_time.iso_format()),verbose)

            # todo: add checkpoint pickling?


            #####################
            # State update

            for sat in all_sats:
                sat.state_update_step(global_time)


            #####################
            # Activity execution

            for sat in all_sats:
                sat.execution_step()

            #####################
            # Replanning

            for sat in all_sats:
                sat.replan_step()

            gs_network.replan_step()




        # move this!
        # gp_wrapper = GlobalPlannerWrapper(self.params)

        # gp_wrapper.run_gp()



