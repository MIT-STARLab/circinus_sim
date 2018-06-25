
from datetime import datetime

from .lp_processing import LPProcessing
from .lp_scheduling import LPScheduling

from circinus_tools import debug_tools

OUTPUT_JSON_VER = '0.1'

class LocalPlannerRunner:
    """easy™ interface for running the local planner scheduling algorithm"""

    def __init__(self, lp_params):
        """initializes based on parameters

        initializes based on parameters
        :param lp_params: params (Note: left intentionally vague so that it will be super confusing later) ( note 2: look at global planner)
        :type params: dict
        """

        # sat_params = lp_params['orbit_prop_params']['sat_params']

        # lp_inst_planning_params = lp_params['lp_instance_params']['planning_params']

        # self.sat_indx = lp_inst_planning_params['sat_indx']
        # #  the earliest time for which we consider incoming flows
        # self.planning_start_dt  = lp_inst_planning_params['planning_start_dt']
        # #  the earliest time at which we allow a leaving flow to start
        # self.planning_leaving_flow_start_dt  = lp_inst_planning_params['planning_leaving_flow_start_dt']
        # #  the latest time for which we consider an incoming or leaving flow
        # self.planning_end_dt  = lp_inst_planning_params['planning_end_dt']

        self.lp_proc = LPProcessing(lp_params)
        self.lp_sched= LPScheduling(lp_params)

        self.latest_dr_uid = lp_params['lp_instance_params']['latest_dr_uid']
        self.lp_agent_ID = lp_params['lp_instance_params']['lp_agent_ID']


    def run(self,existing_route_data,verbose=False):

        #   determine outflow and inflow routes
        inflows,outflows,planned_rts_outflows_in_planning_window = self.lp_proc.determine_flows(existing_route_data)
        
        self.lp_sched.make_model(inflows,outflows,verbose)
        self.lp_sched.solve()
        scheduled_routes, all_routes_after_update, updated_utilization_by_route_id, latest_dr_uid = self.lp_sched.extract_updated_routes(existing_route_data,planned_rts_outflows_in_planning_window,self.latest_dr_uid,self.lp_agent_ID,verbose)

        return scheduled_routes,all_routes_after_update,updated_utilization_by_route_id,latest_dr_uid

class PipelineRunner:

    def run(self, data,verbose=False):
        """ run the local planner"""
        #  note that this is basically just a pass-thru for now.  someday this should probably be set up in the same way as the global planner, with parsing and version checking added for the inputs

        lp_params = {}
        lp_params['orbit_prop_params'] = data['orbit_prop_params']
        lp_params['const_sim_inst_params'] = data['const_sim_inst_params']
        # lp_params['orbit_link_params'] = orbit_link_params
        lp_params['lp_instance_params'] = data['lp_instance_params']
        lp_params['gp_general_params'] = data['gp_general_params']
        # lp_params['data_rates_params'] = data_rates_params
        # lp_params['gp_other_params'] = gp_other_params
        
        lp_runner = LocalPlannerRunner (lp_params)

        existing_route_data = data['existing_route_data']
        scheduled_routes,all_routes_after_update,updated_utilization_by_route_id,latest_dr_uid = lp_runner.run (existing_route_data,verbose)

        output = {}
        output['version'] = OUTPUT_JSON_VER
        output['scheduled_routes'] = scheduled_routes
        output['all_routes_after_update'] = all_routes_after_update
        output['updated_utilization_by_route_id'] = updated_utilization_by_route_id
        output['latest_dr_uid'] = latest_dr_uid
        output['update_wall_clock_utc'] = datetime.utcnow().isoformat()

        return output