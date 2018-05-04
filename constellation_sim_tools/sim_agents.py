SAT_STATE_JSON_VER = '0.1'

class SimAgent:
    """Super class for simulation agents within constellation"""

    def __init__(self):
        pass


class SimSatellite(SimAgent):
    """class for simulation satellites"""
    
    def __init__(self,sat_id):
        """initializes based on parameters
        
        initializes based on parameters
        :type params: dict
        """

        super().__init__()

        self.sat_id = sat_id

class SimGroundStation(SimAgent):
    """class for simulation ground stations"""
    
    def __init__(self,ID,name,gs_network):
        """initializes based on parameters
        
        initializes based on parameters
        :type params: dict
        """

        super().__init__()

        self.ID = ID
        self.name = name
        self.gs_network = gs_network

class SimGroundNetwork(SimAgent):
    """class for simulation ground network"""
    
    def __init__(self,name,gs_list=[]):
        """initializes based on parameters
        
        initializes based on parameters
        :type params: dict
        """

        super().__init__()

        self.name = name
        self.gs_list = gs_list

        self.gs_list = []