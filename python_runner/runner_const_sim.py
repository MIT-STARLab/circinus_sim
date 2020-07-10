#! /usr/bin/env python

##
# Python runner for constellation simulation
# @author Kit Kennedy
# @edited Bobby Holden
#

import time
import os.path
import json
from datetime import datetime, timedelta
import sys
import argparse

#  local repo includes. todo:  make this less hackey
sys.path.append ('..')

try: # First try will work if subrepo circinus_tools is populated, or if prior module imported from elsewhere (so this covers all the rest of the imports in this module as well)
    from circinus_tools  import time_tools as tt
    from circinus_tools  import io_tools
    from circinus_tools  import debug_tools
except ImportError:
    print("Importing circinus_tools from parent repo...")
    try:
        sys.path.insert(0, "../../")
        from circinus_tools  import time_tools as tt
        from circinus_tools  import io_tools
        from circinus_tools  import debug_tools
    except ImportError:
        print("Neither local nor parent-level circinus_tools found.")

from constellation_sim_tools import constellation_sim as const_sim



REPO_BASE = os.path.abspath(os.pardir)  # os.pardir aka '..'

OUTPUT_JSON_VER = '0.1'



class PipelineRunner:

    def run(self, data):
        """

        """

        # Here we're stripping everything off down to whichever layer is relevant from hereon out
        sim_case_config      = data['sim_case_config']['scenario_params']
        constellation_config = data['constellation_config']['constellation_definition']['constellation_params']
        gs_network_config    = data['gs_network_config']["network_definition"]
        gs_ref_model         = data['gs_ref_model']['gs_model_definition']
        sat_ref_model        = data['sat_ref_model']['sat_model_definition']
        payload_ref_model    = data['payload_ref_model']['payload_model_definition']['payload_params']
        opsProfile_config    = data['opsProfile_config']['ops_profile_params']
        sim_gen_config       = data['sim_gen_config']["general_sim_params"]
        gp_general_params    = data['gp_general_params']
        lp_general_params    = data['lp_general_params']
        data_rates_params    = data['data_rates_inputs']        
        orbit_prop_data      = data['orbit_prop_data']


        # TODO - because we split this apart in our new specification, we have to join part of it back together here. This will be reformed as needed.

        append_power_params_with_enumeration = {
            "power_consumption_W":{
                **sat_ref_model['sat_model_params']['power_params']["power_consumption_W"],
                "obs":payload_ref_model["power_consumption_W"]["obs"]
            },
            "battery_storage_Wh":sat_ref_model['sat_model_params']['power_params']["battery_storage_Wh"],
            'sat_ids':constellation_config['sat_ids'],
            'sat_id_prefix': constellation_config['sat_id_prefix'],
        }
        append_data_params_with_enumeration = {
            **sat_ref_model['sat_model_params']['data_storage_params'],
            'sat_ids':constellation_config['sat_ids'],
            'sat_id_prefix': constellation_config['sat_id_prefix'],
        }
        append_state_params_with_enumeration = {
            **sat_ref_model['sat_model_params']['initial_state'],
            'sat_ids':constellation_config['sat_ids'],
            'sat_id_prefix': constellation_config['sat_id_prefix'],
        }
        power_params_by_sat_id, all_sat_ids1 = io_tools.unpack_sat_entry_list( [ append_power_params_with_enumeration ] , output_format='dict')            
        data_storage_params_by_sat_id, all_sat_ids2 = io_tools.unpack_sat_entry_list( [ append_data_params_with_enumeration ] , output_format='dict')  
        initial_state_by_sat_id, all_sat_ids3 = io_tools.unpack_sat_entry_list( [ append_state_params_with_enumeration ] , output_format='dict') 

        orbit_prop_inputs = {
            'scenario_params' : {
                'start_utc'     : sim_case_config['start_utc'],    # These duplications accomodate runner_gp.py expectations
                'end_utc'       : sim_case_config['end_utc'],      # TODO - update runner_gp.py to expect non-duplicated input 
                'start_utc_dt'  : tt.iso_string_to_dt ( sim_case_config['start_utc']),
                'end_utc_dt'    : tt.iso_string_to_dt ( sim_case_config['end_utc']),
                'timestep_s'    : sim_gen_config["timestep_s"]
            },
            'sat_params' : { 
                'num_sats'          : constellation_config['num_satellites'],
                'num_satellites'    : constellation_config['num_satellites'], # Duplication to accomodate downstream (runner_gp.py among others) -- TODO: cut out duplication
                'sat_id_order'      : constellation_config['sat_id_order'],
                'sat_id_prefix'     : constellation_config['sat_id_prefix'],
                'pl_data_rate'      : payload_ref_model['payload_data_rate_Mbps'],
                'payload_data_rate_Mbps'        : payload_ref_model['payload_data_rate_Mbps'], # Duplication to accomodate downstream (runner_gp.py among others) -- TODO: cut out duplication
                'power_params_by_sat_id'        : power_params_by_sat_id,
                'power_params'                  : [ append_power_params_with_enumeration ], # Duplication to accomodate downstream (runner_gp.py among others) -- TODO: cut out duplication
                'data_storage_params_by_sat_id' : data_storage_params_by_sat_id,
                'data_storage_params'       : [ append_data_params_with_enumeration ], # Duplication to accomodate downstream (runner_gp.py among others) -- TODO: cut out duplication 
                'initial_state_by_sat_id'   : initial_state_by_sat_id,
                'initial_state'             : [ append_state_params_with_enumeration ], # Duplication to accomodate downstream (runner_gp.py among others) -- TODO: cut out duplication 
                'activity_params' : {
                    **sat_ref_model['sat_model_params']["activity_params"],
                    "min_duration_s": {
                        **payload_ref_model["min_duration_s"],
                        **sat_ref_model['sat_model_params']["activity_params"]["min_duration_s"]
                    },
                    "intra-orbit_neighbor_direction_method":constellation_config["intra-orbit_neighbor_direction_method"]
                }
            },
            'gs_params': { 
                'gs_network_name'   : gs_network_config["gs_net_params"]['gs_network_name'],
                'num_stations'      : gs_network_config["gs_net_params"]["num_stations"],
                'num_gs'            : gs_network_config["gs_net_params"]["num_stations"], # TODO: LOL are you serious right now. Get rid of this duplication.
                'stations'          : gs_network_config["gs_net_params"]["stations"]
            },
            'obs_params': opsProfile_config['obs_params'],
            'orbit_params': {
                    'sat_ids_by_orbit_name' : io_tools.expand_orbits_list(constellation_config['orbit_params'],constellation_config['sat_id_prefix']),
                    'sat_orbital_elems'     : constellation_config['orbit_params']['sat_orbital_elems']
                },
            'orbit_prop_data': orbit_prop_data
        }
        

        # make the satellite ID order. if the input ID order is default, then will assume that the order is the same as all of the IDs found in the power parameters
        orbit_prop_inputs['sat_params']['sat_id_order'] = io_tools.make_and_validate_sat_id_order( # Pay close attention to this, because this is a mutated copy
            orbit_prop_inputs['sat_params']['sat_id_order'],
            orbit_prop_inputs['sat_params']['sat_id_prefix'], 
            orbit_prop_inputs['sat_params']['num_sats'],
            all_sat_ids1
        )



        io_tools.validate_ids(validator=orbit_prop_inputs['sat_params']['sat_id_order'],validatee=all_sat_ids1)
        io_tools.validate_ids(validator=orbit_prop_inputs['sat_params']['sat_id_order'],validatee=all_sat_ids2)
        io_tools.validate_ids(validator=orbit_prop_inputs['sat_params']['sat_id_order'],validatee=all_sat_ids3)

        gs_id_order = io_tools.make_and_validate_gs_id_order(orbit_prop_inputs['gs_params'])
        orbit_prop_inputs['gs_params']['gs_id_order'] = gs_id_order

        obs_target_id_order = io_tools.make_and_validate_target_id_order(orbit_prop_inputs['obs_params'])
        orbit_prop_inputs['obs_params']['obs_target_id_order'] = obs_target_id_order


        sim_params = {}
        sim_params['start_utc_dt'] = tt.iso_string_to_dt ( sim_case_config['start_utc'])
        sim_params['end_utc_dt'] = tt.iso_string_to_dt ( sim_case_config['end_utc'])
        sim_params['orbit_prop_params'] = orbit_prop_inputs 
        sim_params['orbit_link_params'] =  { #orbit_link_params TODO: This restructuring is totally gross, but doing it to avoid changing constellation_sim.py substantiall right now
            "link_disables": opsProfile_config["link_disables"],
            "general_link_params": sim_gen_config["general_link_params"]
        }
        sim_params['sim_case_config']=sim_case_config
        sim_params['gp_general_params'] = gp_general_params #TODO: Would prefer to make this live in the GP only, as that's what makes sense.  If it's used outside of the GP itself, shouldn't be called this & should have a spot in another file.
        sim_params['data_rates_params'] = data_rates_params 
        sim_params['sim_gen_config'] = sim_gen_config 
        sim_params['restore_pickle_cmdline_arg'] = data['restore_pickle_cmdline_arg']
        sim_params['output_path'] = data['output_path']
        sim_params['const_sim_inst_params'] = { # TODO: This restructuring is totally gross, but doing it to avoid changing constellation_sim.py directly on this iteration
            "sim_gs_network_params" : gs_network_config["sim_gs_network_params"],
            "sim_gs_params"         : {
                **gs_network_config["gs_net_params"],
                "time_epsilon_s":sim_params['sim_gen_config']["gs_time_epsilon_s"]
            },
            "sim_satellite_params"  : sat_ref_model["sim_satellite_params"],
            "sim_plot_params"       : {
                **sim_gen_config["sim_plot_params"],
                'start_utc_dt'  : sim_params['start_utc_dt'],           # TODO - copies of start_utc_dt/end_utc_dt are the most egregious offenders
                'end_utc_dt'    : sim_params['end_utc_dt']
            },
            "sim_metrics_params"    : sim_gen_config["sim_metrics_params"],
            "sim_run_params"        : {
                **sim_gen_config["sim_run_params"],
                "sim_tick_s":       sim_gen_config["timestep_s"],   # TODO: This renaming is totally gross, but doing it to avoid changing constellation_sim.py directly on this iteration
                "start_utc_dt":     sim_params['start_utc_dt'],     # TODO - More copy stripping
                "end_utc_dt":       sim_params['end_utc_dt']
            },
            "sim_run_perturbations" : sim_case_config['sim_run_perturbations'],
            "gp_wrapper_params"     : gp_general_params["gp_wrapper_params"],
            "lp_wrapper_params"     : lp_general_params["lp_wrapper_params"],
            "lp_general_params"     : lp_general_params["lp_general_params"]    # TODO - normalize this & the gp above, which isn't within this structure
        }

        sim_params["sat_config"] = {
            'sat_model_definition' : data['sat_ref_model']['sat_model_definition']
        }

        sim_runner = const_sim.ConstellationSim(sim_params)
        sim_runner.run()
        output = sim_runner.post_run(data['output_path'])

        # define orbit prop outputs json
        # todo: add output here
        output_json = {}
        output_json['version'] = OUTPUT_JSON_VER
        output_json['update_time'] = datetime.utcnow().isoformat()

        return output_json

def main():

    ap = argparse.ArgumentParser(description='CIRCINUS constellation simulator')

    # Using default arguments for providing the default location that would be used if not w/in larger CIRCINUS repo.
    # TODO - When we settle on our nominal models, will make this example directory.
    ap.add_argument('--inputs_location',
                    type=str,
                    default=os.path.join(REPO_BASE,'example_input'),
                    help='specify directory in which to find input and config files')

    ap.add_argument('--case_name',
                    type=str,
                    default=os.path.join(REPO_BASE,'nom_case'),
                    help='specify name of case to be used for calculations')

    ap.add_argument('--rem_gp',
                    type=str,
                    default=False,
                    help='Indicates to use a standalone GP, presumed started prior to the sim launching.')


    ap.add_argument('--restore_pickle',
                    type=str,
                    default="",
                    help='pickle to restore sim run from. Must be matched with right params file!')

    ap.add_argument('--remote_debug',
                    type=str,
                    default="false",
                    help="attach based debugger for VScode")

    args = ap.parse_args()

    if args.remote_debug == "true":
        try:
            import ptvsd
            # 5678 is the default attach port in the VS Code debug configurations
            print("Waiting for debugger attach")
            ptvsd.enable_attach(address=('localhost', 5678), redirect_output=True)
            ptvsd.wait_for_attach()
            breakpoint()
        except OSError:
            print('skipping debugger wait')

    # ------- Filenames ------ #
    ### Config files ###
    sim_case_config_FILENAME = args.inputs_location+'/cases/'+args.case_name+'/sim_case_config.json'
    constellation_config_FILENAME = args.inputs_location+'/cases/'+args.case_name+'/constellation_config.json'
    gs_network_config_FILENAME = args.inputs_location+'/cases/'+args.case_name+'/ground_station_network_config.json'
    opsProfile_config_FILENAME = args.inputs_location+'/cases/'+args.case_name+'/operational_profile_config.json'
    sim_gen_config_FILENAME = args.inputs_location+'/general_config/sim_general_config.json'

    # TODO - These two need to be moved to the GP and LP and called by those constructors, respectively
    gp_config_FILENAME = args.inputs_location+'/general_config/gp_general_params_inputs.json' # TODO - get this moved into GP
    lp_config_FILENAME = args.inputs_location+'/general_config/lp_general_params_inputs.json' # TODO - get this moved into LP

    ### Intermediate files from previous stages ###
    data_rates_input_FILENAME = args.inputs_location+'/cases/'+args.case_name+'/autogen_files/data_rates_output.json'   # outputs from prev stage become inputs to this one
    orbit_prop_data_FILENAME = args.inputs_location+'/cases/'+args.case_name+'/autogen_files/orbit_prop_data.json'      # For STN - TODO: modularize propagator & use output directly



    pr = PipelineRunner()

    with open(data_rates_input_FILENAME,'r') as f:
        data_rates_inputs = json.load(f)
    with open(orbit_prop_data_FILENAME,'r') as f:
        orbit_prop_data = json.load(f)


    # -------- CASE SPECIFIC CONFIGURATION INPUTS -------- #
    with open(sim_case_config_FILENAME,'r') as f:
        sim_case_config = json.load(f)
    with open(sim_gen_config_FILENAME,'r') as f:
        sim_gen_config = json.load(f)
    with open(constellation_config_FILENAME,'r') as f:
        constellation_config = json.load(f)
    with open(gs_network_config_FILENAME,'r') as f:
        gs_network_config = json.load(f)
    with open(opsProfile_config_FILENAME,'r') as f:
        opsProfile_config = json.load(f)

    # TODO - These two need to be moved to the GP and LP and called by those constructors, respectively
    with open(gp_config_FILENAME,'r') as f:
        gp_general_params = json.load(f)
    with open(lp_config_FILENAME,'r') as f:
        lp_general_params = json.load(f)

    # GS Network Config points us to the GS models being used - # Todo: handle multiple GS models
    GS_ref_model_name = gs_network_config["network_definition"]["default_gs_ref_model_name"] # Only handling default at the moment
    GS_ref_model_FILENAME = args.inputs_location+'/reference_model_definitions/gs_refs/'+GS_ref_model_name+'.json'
    with open(GS_ref_model_FILENAME,'r') as f:
        gs_ref_model = json.load(f)

    # Constellation Config points us to the satellite models being used - # Todo: handle multiple sat models
    sat_ref_model_name = constellation_config["constellation_definition"]["default_sat_ref_model_name"] # Only handling default at the moment
    sat_ref_model_FILENAME = args.inputs_location+'/reference_model_definitions/sat_refs/'+sat_ref_model_name+'.json'
    with open(sat_ref_model_FILENAME,'r') as f:
        sat_ref_model = json.load(f)

    # The satellite models point us to the payload model - # Todo: handle multiple payload models
    payload_ref_model_name = sat_ref_model["sat_model_definition"]["default_payload_ref_model_name"] # Only handling default at the moment
    payload_ref_model_FILENAME = args.inputs_location+'/reference_model_definitions/payload_refs/'+payload_ref_model_name+'.json'
    with open(payload_ref_model_FILENAME,'r') as f:
        payload_ref_model = json.load(f)

    sim_gen_config['general_sim_params']['use_standalone_gp'] = args.rem_gp.lower()=='true' or sim_gen_config['general_sim_params']['use_standalone_gp']

    data = {
        "sim_case_config"       : sim_case_config,
        "constellation_config"  : constellation_config,
        "gs_network_config"     : gs_network_config,
        "gs_ref_model"          : gs_ref_model,
        "sat_ref_model"         : sat_ref_model,
        "payload_ref_model"     : payload_ref_model,
        "opsProfile_config"     : opsProfile_config,
        "sim_gen_config"        : sim_gen_config,

        "data_rates_inputs"     : data_rates_inputs,
        "orbit_prop_data"       : orbit_prop_data,              
        "restore_pickle_cmdline_arg"    : args.restore_pickle,

        #  TODO - These two need to be moved to the GP and LP and called by those constructors, respectively
        "gp_general_params"     : gp_general_params, # This is legacy
        "lp_general_params"     : lp_general_params,  # This is a copy of legacy

        "output_path"   : args.inputs_location+'/cases/'+args.case_name+'/output_files/'
    }

    a = time.time()
    output = pr.run(data)
    b = time.time()

    with open(args.inputs_location+'/cases/'+args.case_name+'/autogen_files/const_sim_outputs.json','w') as f:
        json.dump(output, f, indent=4, separators=(',', ': '))

    print('run time: %f'%(b-a))

if __name__ == "__main__":
    main()
