SAT_STATE_JSON_VER = '0.1'

class SimAgent:
    """Super class for simulation agents within constellation"""

    def __init__(self, sim_params):
        pass


class SimSatellite(SimAgent):
    """class for simulation satellites"""
    
    def __init__(self,sim_params):
        """initializes based on parameters
        
        initializes based on parameters
        :param sim_params: global namespace parameters created from input files (possibly with some small non-structural modifications to params). The name spaces here should trace up all the way to the input files.
        :type params: dict
        """

        super().__init__(sim_params)

class SimGroundStation(SimAgent):
    """class for simulation ground stations"""
    
    def __init__(self,ID,name,gs_network):
        """initializes based on parameters
        
        initializes based on parameters
        :param sim_params: global namespace parameters created from input files (possibly with some small non-structural modifications to params). The name spaces here should trace up all the way to the input files.
        :type params: dict
        """

        super().__init__(sim_params)

        self.ID = ID
        self.name = name
        self.gs_network = gs_network

class SimGroundNetwork(SimAgent):
    """class for simulation ground network"""
    
    def __init__(self,name,gs_list=[]):
        """initializes based on parameters
        
        initializes based on parameters
        :param sim_params: global namespace parameters created from input files (possibly with some small non-structural modifications to params). The name spaces here should trace up all the way to the input files.
        :type params: dict
        """

        super().__init__(sim_params)

        self.name = name
        self.gs_list = gs_list

        self.gs_list = []