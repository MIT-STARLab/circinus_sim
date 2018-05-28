#! /usr/bin/env python

##
# Python runner for orbit visualization pipeline
# @author Kit Kennedy
#

import time
import os.path
import json
from datetime import datetime, timedelta
import sys
import argparse
# import numpy as np
from copy import deepcopy
# from collections import OrderedDict

#  local repo includes. todo:  make this less hackey
sys.path.append ('..')
from circinus_tools  import time_tools as tt
from circinus_tools  import io_tools
from constellation_sim_tools import constellation_sim as const_sim

from circinus_tools import debug_tools


REPO_BASE = os.path.abspath(os.pardir)  # os.pardir aka '..'

OUTPUT_JSON_VER = '0.1'



class PipelineRunner:

    def run(self, data):
        """

        """

        # deepcopy here because we're making changes for components that this function calls, and don't want to accidentally somehow step on toes somewhere down the call stack (before this function was called)
        orbit_prop_inputs = deepcopy( data['orbit_prop_inputs'])
        orbit_link_inputs = data['orbit_link_inputs']
        data_rates_inputs = data['data_rates_inputs']
        gp_general_params_inputs = data['gp_general_params_inputs']
        const_sim_inst_params_inputs = data['const_sim_inst_params_inputs']

        sim_params = {}
        orbit_prop_params = orbit_prop_inputs
        orbit_link_params = orbit_link_inputs
        data_rates_params = data_rates_inputs
        gp_general_params = gp_general_params_inputs
        const_sim_inst_params = const_sim_inst_params_inputs

        sim_other_params = {}

        if orbit_prop_inputs['version'] == "0.5":
            # do some useful transformations while preserving the structure of the inputs ( important for avoiding namespace clashes)
            orbit_prop_inputs['scenario_params']['start_utc_dt'] = tt.iso_string_to_dt ( orbit_prop_inputs['scenario_params']['start_utc'])
            orbit_prop_inputs['scenario_params']['end_utc_dt'] = tt.iso_string_to_dt ( orbit_prop_inputs['scenario_params']['end_utc'])
            orbit_prop_inputs['sat_params']['num_sats'] = orbit_prop_inputs['sat_params']['num_satellites']
            orbit_prop_inputs['gs_params']['num_gs'] = orbit_prop_inputs['gs_params']['num_stations']
            orbit_prop_inputs['sat_params']['pl_data_rate'] = orbit_prop_inputs['sat_params']['payload_data_rate_Mbps']
            # orbit_prop_inputs['sat_orbit_params'], dummy = io_tools.unpack_sat_entry_list( orbit_prop_inputs['sat_orbit_params'],force_duplicate =  True)
            orbit_prop_inputs['sat_params']['power_params_by_sat_id'], all_sat_ids1 = io_tools.unpack_sat_entry_list( orbit_prop_inputs['sat_params']['power_params'],output_format='dict')
            orbit_prop_inputs['sat_params']['data_storage_params_by_sat_id'], all_sat_ids2 = io_tools.unpack_sat_entry_list( orbit_prop_inputs['sat_params']['data_storage_params'],output_format='dict')
            orbit_prop_inputs['sat_params']['initial_state_by_sat_id'], all_sat_ids3 = io_tools.unpack_sat_entry_list( orbit_prop_inputs['sat_params']['initial_state'],output_format='dict')

            #  check if  we saw the same list of satellite IDs from each unpacking. if not that's a red flag that the inputs could be wrongly specified
            if all_sat_ids1 != all_sat_ids2 or all_sat_ids1 != all_sat_ids3:
                raise Exception('Saw differing sat ID orders')

            #  grab the list for satellite ID order.  if it's "default", we will create it and save it for future use here
            sat_id_order=orbit_prop_inputs['sat_params']['sat_id_order']
            #  make the satellite ID order. if the input ID order is default, then will assume that the order is the same as all of the IDs found in the power parameters
            sat_id_order = io_tools.make_and_validate_sat_id_order(sat_id_order,orbit_prop_inputs['sat_params']['num_sats'],all_sat_ids1)
            orbit_prop_inputs['sat_params']['sat_id_order'] = sat_id_order

            gs_id_order = io_tools.make_and_validate_gs_id_order(orbit_prop_inputs['gs_params'])
            orbit_prop_inputs['gs_params']['gs_id_order'] = gs_id_order
            # orbit_prop_inputs['sat_params']['power_params_sorted'] = io_tools.sort_input_params_by_sat_IDs(orbit_prop_inputs['sat_params']['power_params'],sat_id_order)
            # orbit_prop_inputs['sat_params']['data_storage_params_sorted'] = io_tools.sort_input_params_by_sat_IDs(orbit_prop_inputs['sat_params']['data_storage_params'],sat_id_order)
            # orbit_prop_inputs['sat_params']['initial_state_sorted'] = io_tools.sort_input_params_by_sat_IDs(orbit_prop_inputs['sat_params']['initial_state'],sat_id_order)
        else:
            raise NotImplementedError


        #  check that it's the right version
        if not data_rates_inputs['version'] == "0.3":
            raise NotImplementedError

        #  check that it's the right version
        if not gp_general_params_inputs['version'] == "0.6":
            raise NotImplementedError

        #  check that it's the right version
        if const_sim_inst_params_inputs['version'] == "0.1":
            const_sim_inst_params['sim_run_params']['start_utc_dt'] = tt.iso_string_to_dt ( const_sim_inst_params['sim_run_params']['start_utc'])
            const_sim_inst_params['sim_run_params']['end_utc_dt'] = tt.iso_string_to_dt ( const_sim_inst_params['sim_run_params']['end_utc'])
            const_sim_inst_params['sim_plot_params']['start_utc_dt'] = tt.iso_string_to_dt ( const_sim_inst_params['sim_plot_params']['start_utc'])
            const_sim_inst_params['sim_plot_params']['end_utc_dt'] = tt.iso_string_to_dt ( const_sim_inst_params['sim_plot_params']['end_utc'])
        else:
            raise NotImplementedError

        sim_params['orbit_prop_params'] = orbit_prop_params
        sim_params['orbit_link_params'] = orbit_link_params
        sim_params['gp_general_params'] = gp_general_params
        sim_params['data_rates_params'] = data_rates_params
        sim_params['const_sim_inst_params'] = const_sim_inst_params
        sim_params['other_params'] = sim_other_params
        sim_runner = const_sim.ConstellationSim(sim_params)
        sim_runner.run()
        output = sim_runner.post_run()

        # define orbit prop outputs json
        # todo: add output here
        output_json = {}
        output_json['version'] = OUTPUT_JSON_VER
        output_json['update_time'] = datetime.utcnow().isoformat()

        return output_json


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description='CIRCINUS constellation simulator')
    ap.add_argument('--prop_inputs_file',
                    type=str,
                    default='orbit_prop_inputs.json',
                    help='specify orbit propagation inputs file')

    ap.add_argument('--data_rates_file',
                    type=str,
                    default='data_rates_output.json',
                    help='specify data rate outputs file from orbit link repo')

    ap.add_argument('--link_inputs_file',
                    type=str,
                    default='orbit_link_inputs_ex.json',
                    help='specify orbit link inputs file from orbit link repo')

    ap.add_argument('--gp_general_inputs_file',
                    type=str,
                    default='crux/config/examples/gp_general_params_inputs_ex.json',
                    help='specify global planner general params file')

    ap.add_argument('--const_sim_params_file',
                    type=str,
                    default='crux/config/examples/const_sim_params_ex.json',
                    help='specify constellation simulation parameters file')

    args = ap.parse_args()

    pr = PipelineRunner()

    # with open(os.path.join(REPO_BASE,'crux/config/examples/orbit_prop_inputs_ex.json'),'r') as f:
    with open(os.path.join(REPO_BASE, args.prop_inputs_file),'r') as f:
        orbit_prop_inputs = json.load(f)

    with open(os.path.join(REPO_BASE,args.data_rates_file),'r') as f:
        data_rates_inputs = json.load(f)

    with open(os.path.join(REPO_BASE, args.link_inputs_file),'r') as f:
        orbit_link_inputs = json.load(f)

    with open(os.path.join(REPO_BASE,args.gp_general_inputs_file),'r') as f:
        gp_general_params_inputs = json.load(f)

    with open(os.path.join(REPO_BASE,args.const_sim_params_file),'r') as f:
        const_sim_params_inputs = json.load(f)

    data = {
        "orbit_prop_inputs": orbit_prop_inputs,
        "orbit_link_inputs": orbit_link_inputs,
        "gp_general_params_inputs": gp_general_params_inputs,
        "data_rates_inputs": data_rates_inputs,
        "const_sim_inst_params_inputs": const_sim_params_inputs
    }

    a = time.time()
    output = pr.run(data)
    b = time.time()
    with open('const_sim_outputs.json','w') as f:
        json.dump(output ,f)

    print('run time: %f'%(b-a))