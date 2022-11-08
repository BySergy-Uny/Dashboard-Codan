from dotenv import dotenv_values

class setup_dotenv_todict(object):
    def __init__(self, my_dict):
        for key in my_dict:
            setattr(self, key, my_dict[key])


configuration_files = setup_dotenv_todict(dotenv_values(".env"))