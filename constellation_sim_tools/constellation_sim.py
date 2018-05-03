from .agent_sim import SimGroundNetwork,SimGroundStation,SimSatellite

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
        # self.obs_params = self.params['orbit_prop_params']['obs_params']
        # self.sat_orbit_params = self.params['orbit_prop_params']['sat_orbit_params']
        # self.obs_params = self.params['orbit_prop_params']['obs_params']
        # self.pickle_params = self.params['gp_general_params']['pickle_params']
        # self.other_params = self.params['gp_other_params']
        # self.plot_params = self.params['gp_general_params']['plot_params']
        # self.general_other_params = self.params['gp_general_params']['other_params']
        # self.rs_general_params = sim_params['gp_general_params']['route_selection_general_params']
        # self.rs_v2_params = sim_params['gp_general_params']['route_selection_params_v2']
        # self.as_params = self.params['gp_general_params']['activity_scheduling_params']
        # self.gp_inst_params = self.params['gp_instance_params']['planning_params']
        # self.io_proc =GPProcessorIO(self.params)
        # self.gp_plot =GPPlotting( self.params)

        # # it's no good if the planning window (the activities to select) goes outside of the scenario window (the possible activities, eclipse windows)
        # if self.gp_inst_params['planning_start_dt'] < self.scenario_params['start_utc_dt']:
        #     raise RuntimeError("GP instance start time (%s) is less than scenario start time (%s)"%(self.gp_inst_params['planning_start_dt'],self.scenario_params['start_utc']))
        # if self.gp_inst_params['planning_end_obs_xlnk_dt'] > self.scenario_params['end_utc_dt']:
        #     raise RuntimeError("GP instance obs,xlnk end time (%s) is greater than scenario end time (%s)"%(self.gp_inst_params['planning_end_obs_xlnk_dt'],self.scenario_params['end_utc']))
        # if self.gp_inst_params['planning_end_dlnk_dt'] > self.scenario_params['end_utc_dt']:
        #     raise RuntimeError("GP instance dlnk end time (%s) is greater than scenario end time (%s)"%(self.gp_inst_params['planning_end_dlnk_dt'],self.scenario_params['end_utc']))

        self.gs_network = SimGroundNetwork(self.gs_params['gs_network_name']) 
        for station in self.gs_params['stations']:
            gs = SimGroundStation(
                station['id'], 
                station['name'], 
                gs_network
            )
            self.gs_network.gs_list.append(gs)

        self.sats_by_id = {}
        for sat_id in self.sat_params['sat_id_order']:
            sat = SimSatellite(
                sat_id,
                sat_initial_state 
            )
            self.sats_by_id[sat_id] = sat

    def run( self):

        work on gp wrapper now...




